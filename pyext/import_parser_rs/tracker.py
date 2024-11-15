import importlib
import sys
import traceback

from typing import Any, Iterable, Mapping, Optional, Set


def _apply_patch(o: object, attr_name: str, attr_val: Any) -> None:
    p = attr_name.split('.')
    for n in p[:-1]:
        o = getattr(o, n)
    setattr(o, p[-1], attr_val.__call__(getattr(o, p[-1], None)))


def apply_patches(name: str, patches) -> None:
    m = sys.modules[name]
    for attr_name, attr_val in patches[name].items():
        _apply_patch(m, attr_name, attr_val)


# Some relevant documentation:
#   - https://docs.python.org/3/reference/import.html#importsystem
#   - https://docs.python.org/3/reference/simple_stmts.html#import
#   - https://docs.python.org/3/reference/datamodel.html#import-related-attributes-on-module-objects
#   - https://github.com/python/cpython/blob/v3.13.0/Lib/importlib/_bootstrap.py
class Tracker:
    __slots__ = ('stack', 'cxt', 'tracked', 'old_find_and_load',
                 'dynamic', 'dynamic_stack')

    def __init__(self):
        self.stack = [""]
        self.cxt = set()
        # map of fully-qualified module name to
        # *full* set of (fully-qualified names of) modules it depends on
        self.tracked = {"": self.cxt}
        # optionally record locations of dynamic imports
        self.dynamic = []
        self.dynamic_stack = 0

    def start_tracking(self, prefixes: Set[str],
                       patches: Optional[Mapping[str, Any]] = None,
                       record_dynamic: bool=False) -> None:
        # The usual "public" hook is builtins.__import__
        # Hooking in there is not great for our purpose as it only catches
        # explicit imports, and the internal logic which implicitly loads
        # parent __init__.py and submodules bypasses __import__ in favor
        # of internal helpers
        # We hook into _find_and_load, which is a private implementation
        # detail but appears to be stable from at least 3.7 (oldest version
        # supported at Affirm) all the way to the most recent 3.13 release.
        # It is a great place for us because it is called before any check
        # for cached values in sys.modules, but after sanity checks and
        # resolution of relative imports, and crucially it is called even
        # for implicit loading
        # NB: we MUST use getattr/setattr to access those private members
        bs = getattr(importlib, '_bootstrap')
        self.old_find_and_load = getattr(bs, '_find_and_load')

        def _new_find_and_load(name: str, import_: Any) -> Any:
            added = False
            dynamic = -1
            # only track relevant namespace
            base_ns = name.partition('.')[0]
            relevant = base_ns in prefixes
            if relevant:
                if record_dynamic:
                    dynamic = self.record_dynamic_imports(traceback.extract_stack())

                self.cxt.add(name)
                if name in self.tracked:
                    # we're already tracking this one
                    #  - fully resolved: tracked[] has the full transitive deps
                    #  - import cycle: tracked[] deps might not be complete
                    if name in sys.modules:
                        self.cxt.update(self.tracked[name])
                    else:
                        # every entry of an import cycle ends up with an identical
                        # set of transitive deps. let's go ahead and consolidate them
                        # so that they all point to the same underlying set() instance
                        start_idx = self.stack.index(name)
                        cycle = self.stack[start_idx:]

                        # print("warn: cycle {} -> {}".format(cycle, name),
                        #       file=sys.stderr)

                        # there might be multiple import cycles overlapping in the stack,
                        # fortunately, we're guaranteed that every module within a cycle
                        # will be part of the current stack.
                        # When consolidating, it is important to preserve the set()
                        # instance used by the first entry in the current cycle, as that
                        # might be part of a previous cycle extending earlier in the
                        # stack. Modifying that set in place means that if the module at
                        # the start of the current cycle is already part of the cycle,
                        # we're transparently extending the previous cycle without having
                        # to even detect its presence!
                        consolidated = self.tracked[name]
                        for mod in cycle[1:]:
                            deps = self.tracked[mod]
                            if deps is not consolidated:
                                consolidated.update(deps)
                                self.tracked[mod] = consolidated

                        self.cxt = consolidated
                else:
                    # not tracked yet: push a new context into the stack
                    # NB: the set is a reference, not a value, so changes to cxt
                    # are reflected in tracked[name], saving some indirections
                    tdeps = set()
                    self.tracked[name] = tdeps
                    self.stack.append(name)
                    self.cxt = tdeps
                    # mark that we need to pop down after forwarding
                    added = True

            try:
                # forward to real implementation
                return self.old_find_and_load(name, import_)
            except Exception as e:
                if added:
                    print(f"{e}", file=sys.stderr)
                    # defer removal from self.tracked[] if we're within an import cycle
                    # NB: this should happen if there's an uncaught import error, in
                    # affirm code, which is not expected in practice, unless something
                    # is wrong with the codebase, but better safe than sorry...
                    if name not in self.stack[:-1]:
                        del self.tracked[name]
                raise
            finally:
                if relevant:
                    # apply any necessary patches
                    if patches is not None and name in patches:
                        apply_patches(name, patches)

                    # parent __init__ are implicitly resolved, but sys.modules is
                    # checked *before* calling _gcd_import so our _find_and_load
                    # monkey-patch only catches the first occurrence of implicit
                    # parent resolution. We need to manually reify this dep.
                    # We only need to do that for the immediate parent as its
                    # set of deps is either already fully resolved, including its
                    # own parent, or partially resolved in a cycle that is being
                    # consolidated...
                    parent = name.rpartition('.')[0]
                    if parent and parent not in self.cxt:
                        self.cxt.add(parent)
                        self.cxt.update(self.tracked[parent])

                    if added:
                        self.stack.pop()
                        down = self.tracked[self.stack[-1]]
                        # avoid potentially expensive no-op for cycles
                        if down is not self.cxt:
                            down.update(self.cxt)
                        self.cxt = down

                    if dynamic != -1:
                        self.dynamic_stack = dynamic

        setattr(bs, '_find_and_load', _new_find_and_load)

    def disable_tracking(self) -> None:
        setattr(getattr(importlib, '_bootstrap'), '_find_and_load', self.old_find_and_load)

    def record_dynamic_imports(self, tb: traceback.StackSummary) -> int:
        # walk down the stack until we either find a recognizable dynamic import,
        # our import hook, or an import from the validator
        n = len(tb)
        found = -1
        for i in range(2, n):
            # we've reached the previous dynamic import
            if self.dynamic_stack == n-i:
                return -1
            frame = tb[n-i]
            if frame.name in {"import_module", "__import__"}:
                found = n-i
                break
            # NB: builtins.__import__ and importlib.__import__ lead to different stacks
            # for some reason the builtin is elided from the stack so catching a dynamic
            # import that uses the builtin requires looking at the actual code, which is
            # less reliable since the code is not always available...
            if "__import__(" in frame.line:
                found = n-i+1
                break
        if found != -1:
            # ignore if it's coming from the validator
            if is_validator_frame(tb[found-1]):
                return -1
            # record stack height of previous dynamic import to restore
            prev_stack = self.dynamic_stack
            # mark stack height of dynamic import
            self.dynamic_stack = len(tb)
            # record relevant slice of backtrace, stripping out anything pre-validator
            start = prev_stack + 1 + max(i if is_validator_frame(frame) else -1
                                         for i, frame in enumerate(tb[prev_stack:found]))
            self.dynamic.append(list(omit_tracker_frames(tb[start:found])))
            return prev_stack
        return -1


IGNORED_FRAMES = {
    __file__,
    "<frozen importlib._bootstrap>",
    "<frozen importlib._bootstrap_external>",
}
def omit_tracker_frames(tb: traceback.StackSummary) -> Iterable[traceback.FrameSummary]:
    """
    Remove stack frames associated with the import machinery or our hooking into it.
    This makes it easier to analyze any error that might be reported by the validator
    """
    return (frame for frame in tb if frame.filename not in IGNORED_FRAMES)


def is_validator_frame(frame: traceback.FrameSummary):
    return frame.name == 'recursive_import_tests' and frame.filename.endswith('validator.py')


def get_stack() -> traceback.StackSummary:
    try:
        raise ValueError()
    except ValueError as e:
        return traceback.extract_tb(e.__traceback__)

