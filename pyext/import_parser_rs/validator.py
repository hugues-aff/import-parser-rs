import importlib
import importlib.util
import os
import sys
import time
import traceback

from typing import Any, Callable, Dict


mono_ref = time.monotonic_ns()


def print_with_timestamp(*args, **kwargs):
    wall_elapsed_ms = (time.monotonic_ns() - mono_ref) // 1_000_000
    (
        kwargs['file'] if 'file' in kwargs else sys.stdout
    ).write("[+{: 8}ms] ".format(wall_elapsed_ms))
    print(*args, **kwargs)


def import_file(name: str, filepath: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, filepath)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def is_test_file(name: str) -> bool:
    # https://docs.pytest.org/en/latest/explanation/goodpractices.html#test-discovery
    return (name.startswith("test_") and name.endswith('.py')) or name.endswith("_test.py")


def recursive_import_tests(path: str, import_prefix: str, hook: Any,
                           errors: Dict[str, BaseException]) -> None:
    with os.scandir(path) as it:
        for e in it:
            if e.is_dir():
                recursive_import_tests(e.path, import_prefix + '.' + e.name, hook, errors)
            elif e.is_file() and is_test_file(e.name):
                if hasattr(hook, 'before_file'):
                    hook.before_file(e, import_prefix)
                try:
                    __import__(import_prefix + "." + e.name[:-3], fromlist=())
                except BaseException as ex:
                    # NB: this should not happen, report so it can be fixed and proceed
                    errors[e.path] = ex
                if hasattr(hook, 'after_file'):
                    hook.after_file(e, import_prefix)


def validate(py_tracked, rust_graph, filter_fn: Callable[[str], bool], package = None) -> int:
    diff_count = 0
    for module, pydeps in py_tracked.items():
        if not filter_fn(module):
            continue
        # python does, in case the module is part of an import cycle
        # remove self-edges (which can be present for modules that are part
        # of an import cycle), to avoid spurious difference since the rust
        # module graph doesn't include self-edges
        pydeps = pydeps - {module}

        rdeps = rust_graph.module_depends_on(module, package) or frozenset()

        # NB: we only care about anything that the rust code might be missing
        # it's safe to consider extra dependencies, and in fact expected since
        # the rust parser goes deep and tracks import statements inside code
        # that might never get executed whereas by design the python validation
        # will only track anything that gets executed during the import phase
        rust_missing = pydeps - rdeps
        if rust_missing:
            diff_count += 1
            print(f'{module} rust missing {len(rdeps)}/{len(pydeps)}: {sorted(rust_missing)}')
    return diff_count


if __name__ == '__main__':
    """
    Usage: validator <path/to/hook.py> [<path/to/serialized/graph>]
    
    Purpose:
    
    This files is part of a multi-pronged system to validate the correctness of
    the rust-implemented ModuleGraph provided by this package.
    
    Specifically, it is concerned with validating that, the transitive closure
    of dependencies for a set of test files is computed properly, to give
    confidence in the computation of its transpose: the set of affected tests to
    run given a set of modified files.
       
    Python import tracking is a *very hard* problem, because arbitrary Python
    code can be executed at import time, and arbitrary imports can be loaded
    at run time! We do our best to deal with that as follows:
     - the rust parser goes deep, extracting import statements even in code that
       might never be executed (it does however ignore typechecking-only
       imports). These are not going to be reported by the Python validator
       and that's OK. Better to have false positives (detected imports that
       are not used) than false negatives (undetected imports).
     - the python validator actually runs arbitrary python code during import
       tracking, because that's how Python rolls, so it is able to find
       dynamically-loaded imports, provided they are resolved at import-time
       (i.e. triggered by a module-level statement). This is good as it shows
       blind spots in the rust parser and gives us an opportunity to make those
       dynamic dependencies explicit.
     - neither the rust parser nor the python validator can catch dependencies
       that are resolved dynamically at run-time. Those are generally not a
       good idea exactly because they escape static analysis and are a major
       source of bugs. These are addressed in a separate validation step that
       need to be incorporated in the actual test runner, to detect whether a
       given test had run-time dynamic imports not covered by the validator.
    
        
    """
    import io
    import contextlib

    hook = import_file("_validator_hook", sys.argv[1])

    from . import ModuleGraph
    from .tracker import Tracker, omit_tracker_frames

    t = Tracker()
    t.start_tracking(hook.GLOBAL_NAMESPACES | hook.LOCAL_NAMESPACES,
                     patches=getattr(hook, 'IMPORT_PATCHES', {}),
                     record_dynamic=getattr(hook, 'RECORD_DYNAMIC', False))

    if hasattr(hook, 'setup'):
        hook.setup()

    # TODO: we could move most of this into a separate thread
    # load graph from file if provided, otherwise parse the repo
    if len(sys.argv) > 2 and os.path.exists(sys.argv[2]):
        print_with_timestamp("--- loading existing rust-based import graph")
        g = ModuleGraph.from_file(sys.argv[2])
    else:
        print_with_timestamp("--- building fresh import graph using rust extension")
        g = ModuleGraph(
            hook.package_map(),
            hook.GLOBAL_NAMESPACES,     # unified namespace
            hook.LOCAL_NAMESPACES,      # per-pkg namespace
        )

        if hasattr(hook, 'dynamic_dependencies'):
            print_with_timestamp("--- computing dynamic dependencies")
            unified, per_pkg = hook.dynamic_dependencies()
            print_with_timestamp("--- incorporating dynamic dependencies")
            g.add_dynamic_dependencies_at_edges(unified, per_pkg)

        if len(sys.argv) > 2:
            print_with_timestamp("--- saving import graph")
            g.to_file(sys.argv[2])

    # keep track or errors and import differences
    files_with_missing_imports = 0
    error_count = 0

    print_with_timestamp(f"--- tracking python imports")
    for base, sub in hook.test_folders().items():
        assert sub in hook.LOCAL_NAMESPACES, f"{sub} not in {hook.LOCAL_NAMESPACES}"

        # some packages do not have tests, simply skip them
        if not os.path.isdir(os.path.join(base, sub)):
            continue

        # insert package path into sys.path to allow finding the tests folder
        sys.path.insert(0, base)
        old_k = set(sys.modules.keys())

        if hasattr(hook, 'before_folder'):
            hook.before_folder(base, sub)

        errors = {}

        # catch stdout/stderr to prevent noise from packages being imported
        capture_out = getattr(hook, 'CAPTURE_STDOUT', True)
        capture_err = getattr(hook, 'CAPTURE_STDERR', True)

        with io.StringIO() as f:
            with contextlib.redirect_stdout(f) if capture_out else contextlib.nullcontext(), \
                contextlib.redirect_stderr(f) if capture_err else contextlib.nullcontext():
                # we want to import every test file in that package, recursively,
                # while preserving the appropriate import name, to allow for:
                #  - resolution of __init__.py
                #  - resolution of test helpers, via absolute or relative import
                recursive_import_tests(os.path.join(base, sub), sub, hook, errors)

            if errors:
                error_count += len(errors)
                print_with_timestamp(f"--- tracking test import graph for {base}/{sub}")
                print(f"{len(errors)} exceptions encountered!")
                if capture_out or capture_err:
                    print_with_timestamp('--- captured output')
                    sys.stderr.write(f.getvalue())

                for filepath, ex in errors.items():
                    print_with_timestamp(f'--- {filepath}')
                    print(f'{type(ex)} {ex}')
                    traceback.print_list(omit_tracker_frames(traceback.extract_tb(ex.__traceback__)))

        # NB: do validation at the package level for the test namespace
        # this is necessary because it is not a unified namespace. There can be
        # conflicts between similarly named test modules across packages.
        #
        # NB: we only validate test files, not test helpers. This is because, for
        # performance reason, dynamic dependencies are only applied to nodes of the
        # import graphs that do not have any ancestors (i.e modules not imported by
        # any other module)
        # This is fine because the purpose of this validation is to ensure that we
        # can determine a set of affected *test files* from a given set of modified
        # files, so as long as we validate that tests have matching imports between
        # python and Rust, we're good to go.
        def is_local_test_module(module: str) -> bool:
            last = module.rpartition('.')[2]
            return module.startswith(sub) and (last.startswith('test_') or last.endswith('_test'))

        files_with_missing_imports += validate(
            t.tracked,
            g,
            package=base,
            filter_fn=is_local_test_module
        )

        # cleanup to avoid contaminating subsequent iterations
        sys.path = sys.path[1:]
        new_k = sys.modules.keys() - old_k
        for m in new_k:
            if m.partition('.')[0] == sub:
                del t.tracked[m]
                del sys.modules[m]

        if hasattr(hook, 'after_folder'):
            hook.after_folder(base, sub)


    t.disable_tracking()

    print_with_timestamp(f"--- locations of dynamic imports")
    dedup_stack = set()
    for dyn_stack in t.dynamic:
        as_tuple = tuple((f.filename, f.lineno) for f in dyn_stack)
        if as_tuple in dedup_stack:
            continue
        dedup_stack.add(as_tuple)
        print("---")
        traceback.print_list(dyn_stack)

    # validate global namespace once all packages have been processed
    print_with_timestamp(f"--- comparing code import graphs")
    files_with_missing_imports += validate(
        t.tracked,
        g,
        filter_fn=lambda module: module.partition('.')[0] in hook.GLOBAL_NAMESPACES
    )

    print_with_timestamp(f"--- validation result")
    if error_count + files_with_missing_imports == 0:
        print(f"The rust module graph can be trusted")
        sys.exit(0)
    else:
        if files_with_missing_imports:
            print(f"The rust module graph is missing some imports")
            print("You may need to make some dynamic imports explicit")
        if error_count:
            print(f"Errors prevented validation of the rust module graph")
            print("Fix them and try again...")
        sys.exit(1)
