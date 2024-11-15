use std::collections::{HashMap, HashSet};
use std::{fs, thread};
use std::sync::{mpsc};
use std::time::Instant;
use dashmap::{DashMap, Entry};
use ignore::{DirEntry, WalkBuilder, WalkState};
use ustr::{ustr, Ustr};
use speedy::{Context, LittleEndian, Readable, Reader, Writable, Writer};
use speedy::private::{read_length_u64_varint, write_length_u64_varint};
use crate::matcher::MatcherNode;
use crate::moduleref::{ModuleRef, ModuleRefCache};
use crate::parser;
use crate::parser::{raw_get_all_imports, split_at_depth};
use crate::transitive_closure::{TransitiveClosure};


pub struct ModuleGraph {
    // input map of python import path to toplevel package path
    packages: HashMap<String, String>,
    global_prefixes: HashSet<String>,
    local_prefixes: HashSet<String>,

    // import to track even without matching code
    // most useful to track importlib and __import___
    external_prefixes: HashSet<String>,

    // prefix matching for package import/package paths
    import_matcher: MatcherNode,
    package_matcher: MatcherNode,

    modules_refs: ModuleRefCache,
    to_module_cache: DashMap<Ustr, ModuleRef>,
    dir_cache: DashMap<String, HashSet<String>>,

    // collected imports
    global_ns: DashMap<ModuleRef, HashSet<ModuleRef>>,
}

impl ModuleGraph {
    pub fn new(
        packages: HashMap<String, String>,
        global_prefixes: HashSet<String>,
        local_prefixes: HashSet<String>,
        external_prefixes: HashSet<String>,
    ) -> ModuleGraph {
        ModuleGraph{
            import_matcher: MatcherNode::from(packages.keys(), '.'),
            package_matcher: MatcherNode::from(packages.values(), '/'),
            packages,
            global_prefixes,
            local_prefixes,
            external_prefixes,
            modules_refs: ModuleRefCache::new(),
            to_module_cache: DashMap::new(),
            dir_cache: DashMap::new(),
            global_ns: DashMap::new(),
        }
    }

    fn from(
        packages: HashMap<String, String>,
        global_prefixes: HashSet<String>,
        local_prefixes: HashSet<String>,
        external_prefixes: HashSet<String>,
        modules_refs: ModuleRefCache,
        global_ns: DashMap<ModuleRef, HashSet<ModuleRef>>,
    ) -> ModuleGraph {
        ModuleGraph {
            import_matcher: MatcherNode::from(packages.keys(), '.'),
            package_matcher: MatcherNode::from(packages.values(), '/'),
            packages,
            global_prefixes,
            local_prefixes,
            external_prefixes,
            modules_refs,
            to_module_cache: DashMap::new(),
            dir_cache: DashMap::new(),
            global_ns,
        }
    }

    pub fn from_file(filepath: &str) -> Result<ModuleGraph, speedy::Error> {
        Self::read_from_file_with_ctx(LittleEndian::default(), filepath)
    }

    pub fn to_file(&self, filepath: &str) -> Result<(), speedy::Error> {
        self.write_to_file_with_ctx(LittleEndian::default(), filepath)
    }

    fn is_local(&self, name: &str) -> Option<bool> {
        let ns = match name.find('.') {
            Some(idx) => &name[..idx],
            None => name,
        };
        if self.local_prefixes.contains(ns) {
            Some(true)
        } else if self.global_prefixes.contains(ns) {
            Some(false)
        } else {
            None
        }
    }

    pub fn add<T: IntoIterator<Item=String>>(&self, filepath: &str, pkg: &str, module: &str, deps: T) {
        let is_local = match self.is_local(module) {
            None => return,
            Some(local) => local,
        };

        let mut imports = HashSet::new();

        for dep in deps {
            if self.external_prefixes.contains(&dep) {
                imports.insert(self.modules_refs.get_or_create(ustr(""), ustr(&dep), None));
                continue
            }
            if dep.ends_with(".*") {
                // NB: per python spec, star import only import submodules that are referenced in
                // the __all__ variable set in a package's __init__.py
                // Handling that accurately would require:
                //  - evaluating __all__ which would necessarily have to rely on heuristics since
                //    it could in theory be touched with arbitrary code, because that's how Python
                //    rolls
                //  - tracking the value of __all__ for all packages
                //  - deferring resolution of * imports until the relevant package is parsed and
                //    its __all__ value is known
                //
                // This is a tremendous amount of complexity for relatively little value. Instead,
                // we can do something much easier: act as if __all__ contained all the submodules
                // present on the filesystem.
                // This might result in spurious additional dependencies, but it cannot possibly
                // result in missed dependencies, and we're more concerned about false negatives
                // than false positives.
                // These "spurious" additional deps are in fact a feature, as it allows us to
                // concisely inform the parser of some programmatically inserted dependencies
                let target_pkg = &dep[..dep.len()-2];
                if let Some(refs) = self.to_module_list_local_aware(pkg, ustr(target_pkg)) {
                    refs.iter().for_each(|r| {
                        imports.insert(*r);
                    });
                    continue
                }
            }
            if let Some(dep_ref) = self.to_module_local_aware(pkg, ustr(&dep)) {
                imports.insert(dep_ref);
            }
        }

        let module = ustr(module);
        let filepath = ustr(filepath);
        let module_ref = if is_local {
            self.modules_refs.get_or_create(filepath, module, Some(ustr(pkg)))
        } else {
            self.modules_refs.get_or_create(filepath, module, None)
        };
        self.global_ns.insert(module_ref, imports);
    }

    fn exists_case_sensitive(&self, dir: &str, name: &str) -> bool {
        if !fs::exists(dir.to_string() + "/" + name).unwrap_or(false) {
            return false;
        }
        // TODO: try the same file name with a different case before falling back to read_dir...
        // NB: read dir is expensive, but it's the only way to ensure we're looking
        // at the proper file unfortunately...
        match self.dir_cache.entry(dir.to_string()) {
            Entry::Vacant(e) => {
                match fs::read_dir(dir) {
                    Err(_) => false,
                    Ok(entries) => {
                        let mut children = HashSet::new();
                        for e in entries {
                            children.insert(e.unwrap().file_name().to_str().unwrap().to_string());
                        }
                        let exists = children.contains(name);
                        e.insert(children);
                        exists
                    }
                }
            },
            Entry::Occupied(e) => {
                e.get().contains(name)
            }
        }
    }

    fn module_path(&self, filepath: &str) -> Option<String> {
        // Oh joy! on case-sensitive filesystems we want to make sure we resolve
        // module paths in a way that is consistent with what Python itself does
        // as formalized in PEP 235 https://peps.python.org/pep-0235/
        // Is this really necessary? well, yes, because some people are fond of
        // re-exporting things in their __init__.py to allow for short and "pretty"
        // import that hide internal structure. So for instance
        // foo/
        //   __init__.py
        //   bar.py
        //
        // with bar.py:
        //      class Bar:
        //
        // and __init__.py:
        //      from .bar import Bar
        //
        // so that other files can do:
        //      from foo import Bar
        //
        // we want to resolve that last form into an import to foo.bar, not foo.Bar!
        if self.exists_case_sensitive(filepath, "__init__.py") {
            return Some(filepath.to_string() + "/__init__.py")
        }
        let py = filepath.to_string() + ".py";
        if let Some((dir, name)) = py.rsplit_once('/') {
            if self.exists_case_sensitive(dir, name) {
                return Some(py)
            }
        }
        None
    }

    fn to_module_no_cache(&self, pkg_path: &str, dep: Ustr, ref_pkg: Option<Ustr>) -> Option<ModuleRef> {
        let depfile = pkg_path.to_string() + "/" + &dep.replace('.', "/");
        let mut idx = dep.rfind('.');
        if let Some(actual) = self.module_path(depfile.as_str()) {
            return Some(self.modules_refs.get_or_create(ustr(&actual), dep, ref_pkg))
        } else {
            // need to allow for a walk all the way up because:
            //
            // from a.b.c import y
            //
            // might be resolved
            //  - value y in a/b/c.py or a/b/c/__init__.py
            //  - value c.y in a/b.py or a/b/__init__.py
            //  ...
            while idx.is_some() {
                let parent = &dep[..idx.unwrap()];
                let parentfile = &depfile[..pkg_path.len()+1+idx.unwrap()];
                if let Some(actual) = self.module_path(parentfile) {
                    return Some(self.modules_refs.get_or_create(ustr(&actual), ustr(parent), ref_pkg))
                }
                idx = parent.rfind('.')
            }
        }
        None
    }

    fn to_module_with_cache(&self, pkg_path: &str, dep: Ustr, ref_pkg: Option<Ustr>) -> Option<ModuleRef> {
        // the target of an import statement could be a module, or a value within that module
        // we only want to deal with modules when building an import graph, so we check if a
        // module path resolves to a file, and omit the final component if we can prove it
        // isn't a valid module
        match self.to_module_cache.get(&dep) {
            Some(module) => Some(*module.value()),
            None => {
                match self.to_module_no_cache(pkg_path, dep, ref_pkg) {
                    Some(module_ref) => {
                        self.to_module_cache.insert(dep, module_ref);
                        Some(module_ref)
                    },
                    None => None,
                }
            }
        }
    }

    fn to_module_local_aware(&self, pkg: &str, dep: Ustr) -> Option<ModuleRef> {
        match self.is_local(&dep) {
            None => None,
            Some(_) => {
                match self.packages.get(self.import_matcher.longest_prefix(&dep, '.')) {
                    Some(dep_pkg_fs) => self.to_module_with_cache(&dep_pkg_fs, dep, None),
                    None => self.to_module_with_cache(pkg, dep, Some(ustr(pkg))),
                }
            },
        }
    }

    fn to_module_list(&self, pkg_path: &str, dep: Ustr, pkg: Option<Ustr>) -> Option<Vec<ModuleRef>>{
        let target_path = pkg_path.to_string() + "/" + &dep.replace('.', "/");
        match fs::read_dir(target_path) {
            Err(_) => None,
            Ok(entries) => {
                Some(entries.filter_map(|entry| {
                    match entry {
                        Err(_) => None,
                        Ok(e) => {
                            if !e.file_type().unwrap().is_file() {
                                return None
                            }
                            let name = e.file_name().to_str().unwrap().to_string();
                            if name.ends_with(".py") && name != "__init__.py" {
                                Some(name[..name.len()-3].to_string())
                            } else {
                                None
                            }
                        }
                    }
                }).filter_map(|sub| {
                    let subdep = dep.to_string() + "." + &sub;
                    self.to_module_with_cache(pkg_path, ustr(&subdep), pkg)
                }).collect())
            }
        }
    }
    fn to_module_list_local_aware(&self, pkg: &str, dep: Ustr) -> Option<Vec<ModuleRef>> {
        match self.is_local(&dep) {
            None => None,
            Some(_) => {
                match self.packages.get(self.import_matcher.longest_prefix(&dep, '.')) {
                    Some(dep_pkg_fs) => self.to_module_list(&dep_pkg_fs, dep, None),
                    None => self.to_module_list(pkg, dep, Some(ustr(pkg))),
                }
            },
        }
    }

    pub fn parse_parallel(&self) -> Result<(), crate::parser::Error> {
        let parallelism = thread::available_parallelism().unwrap().get();

        let mut package_it = self.packages.values();
        let builder = &mut WalkBuilder::new(&package_it.next().unwrap());
        for a in package_it {
            builder.add(a);
        }

        let mut prefixes : HashSet<String> = HashSet::new();
        self.global_prefixes.iter().for_each(|n| { prefixes.insert(n.clone()); });
        self.local_prefixes.iter().for_each(|n| { prefixes.insert(n.clone()); });

        let (tx, rx) = mpsc::channel::<crate::parser::Error>();

        // NB: we have to disable handling of .gitignore because
        // some real smart folks have ignore patterns that match
        // files that are committed in the repo...
        builder
            .standard_filters(false)
            .hidden(true)
            .threads(parallelism)
            .filter_entry(move |e| {
                let name = e.file_name().to_str().unwrap();
                e.depth() > 1 || prefixes.contains(name)
            })
            .build_parallel().run(|| {
            let tx = tx.clone();
            Box::new(move |r| {
                match r {
                    Err(err) => {
                        tx.send(parser::Error::IO(err.into_io_error().unwrap())).unwrap();
                        WalkState::Quit
                    }
                    Ok(e) => self.parse_one_file(e, &tx)
                }
            })
        });

        drop(tx);

        // check for errors during the walk
        for err in rx.iter() {
            eprintln!("{}", err);
            // NB: we only return the first one...
            return Err(err);
        }
        Ok(())
    }

    fn parse_one_file(&self, e: DirEntry, tx: &mpsc::Sender<crate::parser::Error>) -> WalkState {
        let filename = e.file_name().to_str().unwrap();
        //eprintln!("{}", filename);
        if !filename.ends_with(".py") {
            return WalkState::Continue;
        }
        let filepath = e.path().to_str().unwrap();
        // assume depth 0 (root of subtree being walked) is package root
        let (pkg, module) = split_at_depth(filepath, '/', e.depth());

        // remove .py suffix, turn / into .
        // NB: preserve __init__ for correct relative import resolution
        let module = module[..module.len()-3].replace('/', ".");

        match raw_get_all_imports(filepath, &module, true) {
            Ok(imports) => {
                // rip out the __init__ bit now that we've dealt with any relative imports
                let mut module : &str = &module;
                if module.ends_with(".__init__") {
                    module = &module[..module.len() - 9];
                }
                self.add(filepath, pkg, module, imports);
                WalkState::Continue
            },
            Err(err) => {
                tx.send(err).unwrap();
                WalkState::Quit
            },
        }
    }

    pub fn finalize(&self) -> TransitiveClosure {
        reify_deps(&self.global_ns, &self.modules_refs);
        TransitiveClosure::from(&self.global_ns, &self.modules_refs)
    }
}

fn reify_deps(g: &DashMap<ModuleRef, HashSet<ModuleRef>>,
              ref_cache: &ModuleRefCache) {
    // because of the way python import machinery works, namely executing top-level
    // statements in a module body, and the existence of __init__.py:
    //
    //  - import x.y.x implies a dep on x and x.y, not just x.y.z
    //
    // NB: this must happen after the whole graph is constructed to work correctly

    // the reify loop would deadlock otherwise...
    let mut keys: Vec<ModuleRef> = Vec::with_capacity(g.len());
    g.iter().for_each(|it| keys.push(*it.key()));

    for n in &keys {
        let mut ref_add = HashSet::new();

        // add dep on all parent __init__.py
        let module = ref_cache.get(*n);
        let mut idx = module.py.rfind('.');
        while idx.is_some() {
            let parent = ustr(&module.py[..idx.unwrap()]);
            let pref = ref_cache.ref_for_py(parent, module.pkg);
            if pref.is_some() && g.contains_key(&pref.unwrap()) && !ref_add.contains(&pref.unwrap()) {
                ref_add.insert(pref.unwrap());
            }
            idx = parent.rfind('.');
        }

        g.get_mut(n).unwrap().extend(ref_add);
    }
}

impl<C> Writable<C> for ModuleGraph
where
    C: Context
{
    fn write_to<T: ?Sized + Writer<C>>(&self, w: &mut T) -> Result<(), C::Error> {
        w.write_value(&self.packages)?;
        w.write_value(&self.global_prefixes)?;
        w.write_value(&self.local_prefixes)?;
        w.write_value(&self.external_prefixes)?;
        w.write_value(&self.modules_refs)?;

        write_length_u64_varint(self.global_ns.len(), w)?;
        for l in self.global_ns.iter() {
            w.write_u64_varint(*l.key() as u64)?;
            write_length_u64_varint(l.value().len(), w)?;
            for v in l.value() {
                w.write_u64_varint(*v as u64)?;
            }
        }
        Ok(())
    }
}

impl<'a, C> Readable<'a, C> for ModuleGraph
where
    C: Context
{
    fn read_from<R: Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
        let packages = reader.read_value()?;
        let global_prefixes = reader.read_value()?;
        let local_prefixes = reader.read_value()?;
        let external_prefixes = reader.read_value()?;
        let modules_refs = reader.read_value()?;

        let global_ns_len = read_length_u64_varint(reader)?;
        let global_ns = DashMap::with_capacity(global_ns_len);
        for _ in 0..global_ns_len {
            let module_ref = reader.read_u64_varint()? as ModuleRef;
            let dep_len = read_length_u64_varint(reader)?;
            let mut deps = HashSet::new();
            for _ in 0..dep_len {
                deps.insert(reader.read_u64_varint()? as ModuleRef);
            }
            global_ns.insert(module_ref, deps);
        }


        Ok(ModuleGraph::from(
            packages,
            global_prefixes,
            local_prefixes,
            external_prefixes,
            modules_refs,
            global_ns,
        ))
    }
}
