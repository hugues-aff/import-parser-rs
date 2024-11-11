use std::collections::{HashMap, HashSet};
use std::{fs, thread};
use std::sync::{mpsc, Mutex};
use std::time::Instant;
use dashmap::{DashMap, DashSet};
use ignore::{DirEntry, WalkBuilder, WalkState};
use rayon::prelude::*;
use ustr::{ustr, Ustr, UstrSet};
use speedy::{Context, LittleEndian, Readable, Reader, Writable, Writer};
use speedy::private::{read_length_u64_varint, write_length_u64_varint};
use crate::matcher::MatcherNode;
use crate::moduleref::{read_ustr_with_buf, write_ustr_to, ModuleRef, ModuleRefCache};
use crate::{raw_get_all_imports, split_at_depth};

struct FinalizedModuleGraph {
    global_ns: HashMap<ModuleRef, HashSet<ModuleRef>>,
    reversed: HashMap<ModuleRef, HashSet<ModuleRef>>,
}


pub struct ModuleGraph {
    // input map of python import path to toplevel package path
    packages: HashMap<String, String>,
    global_prefixes: HashSet<String>,
    local_prefixes: HashSet<String>,

    // prefix matching for package import/package paths
    import_matcher: MatcherNode,
    package_matcher: MatcherNode,

    modules_refs: ModuleRefCache,
    to_module_cache: DashMap<Ustr, ModuleRef>,

    // collected imports
    global_ns: DashMap<ModuleRef, HashSet<ModuleRef>>,
    per_pkg_ns: DashMap<Ustr, DashMap<ModuleRef, HashSet<ModuleRef>>>,

    finalized: Option<FinalizedModuleGraph>,
}


impl ModuleGraph {
    pub fn new(
        packages: HashMap<String, String>,
        global_prefixes: HashSet<String>,
        local_prefixes: HashSet<String>,
    ) -> ModuleGraph {
        ModuleGraph{
            import_matcher: MatcherNode::from(packages.keys(), '.'),
            package_matcher: MatcherNode::from(packages.values(), '/'),
            packages,
            global_prefixes,
            local_prefixes,
            modules_refs: ModuleRefCache::new(),
            to_module_cache: DashMap::new(),
            global_ns: DashMap::new(),
            per_pkg_ns: DashMap::new(),
            finalized: None,
        }
    }

    fn from(
        packages: HashMap<String, String>,
        global_prefixes: HashSet<String>,
        local_prefixes: HashSet<String>,
        modules_refs: ModuleRefCache,
        global_ns: DashMap<ModuleRef, HashSet<ModuleRef>>,
        per_pkg_ns: DashMap<Ustr, DashMap<ModuleRef, HashSet<ModuleRef>>>,
        finalized: Option<FinalizedModuleGraph>,
    ) -> ModuleGraph {
        ModuleGraph {
            import_matcher: MatcherNode::from(packages.keys(), '.'),
            package_matcher: MatcherNode::from(packages.values(), '/'),
            packages,
            global_prefixes,
            local_prefixes,
            modules_refs,
            to_module_cache: DashMap::new(),
            global_ns,
            per_pkg_ns,
            finalized,
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
                }
            }
            if let Some(dep_ref) = self.to_module_local_aware(pkg, ustr(&dep)) {
                imports.insert(dep_ref);
            }
        }

        let module = ustr(module);
        let filepath = ustr(filepath);
        if is_local {
            self.per_pkg_ns.entry(ustr(pkg))
                .or_default()
                .insert(self.modules_refs.get_or_create(filepath, module, Some(ustr(pkg))), imports);
        } else {
            self.global_ns.insert(self.modules_refs.get_or_create(filepath, module, None), imports);
        }
    }

    fn exists_case_sensitive(dir: &str, name: &str) -> bool {
        match fs::read_dir(dir) {
            Err(_) => false,
            Ok(mut entries) => {
                entries.any(|entry| entry.unwrap().file_name() == name)
            }
        }
    }

    fn module_path(filepath: &str) -> Option<String> {
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
        let py = filepath.to_string() + ".py";
        if let Some((dir, name)) = py.rsplit_once('/') {
            if Self::exists_case_sensitive(dir, name) {
                return Some(py)
            }
        }
        if Self::exists_case_sensitive(filepath, "__init__.py") {
            return Some(filepath.to_string() + "/__init__.py")
        }
        None
    }

    fn to_module_no_cache(&self, pkg_path: &str, dep: Ustr, ref_pkg: Option<Ustr>) -> Option<ModuleRef> {
        let depfile = pkg_path.to_string() + "/" + &dep.replace('.', "/");
        let mut idx = dep.rfind('.');
        if let Some(actual) = Self::module_path(depfile.as_str()) {
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
                if let Some(actual) = Self::module_path(parentfile) {
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

    pub fn parse_parallel(&self) -> Result<(), crate::Err> {
        let parallelism = thread::available_parallelism().unwrap().get();

        let mut package_it = self.packages.values();
        let builder = &mut WalkBuilder::new(&package_it.next().unwrap());
        for a in package_it {
            builder.add(a);
        }

        let mut prefixes : HashSet<String> = HashSet::new();
        self.global_prefixes.iter().for_each(|n| { prefixes.insert(n.clone()); });
        self.local_prefixes.iter().for_each(|n| { prefixes.insert(n.clone()); });

        let (tx, rx) = mpsc::channel::<crate::Err>();

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
                        tx.send(crate::Err::from_io(err.into_io_error().unwrap())).unwrap();
                        WalkState::Quit
                    }
                    Ok(e) => self._parse(e, &tx)
                }
            })
        });

        drop(tx);

        // check for errors during the walk
        for err in rx.iter() {
            eprintln!("{}", err.msg);
            // NB: we only return the first one...
            return Err(err);
        }
        Ok(())
    }

    fn _parse(&self, e: DirEntry, tx: &mpsc::Sender<crate::Err>) -> WalkState {
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

    pub fn finalize(&mut self) {
        let start = Instant::now();

        reify_deps(&self.global_ns, &self.modules_refs);

        eprintln!("reified {}",
                  Instant::now().duration_since(start).as_millis());

        let mut g = closed_graph_par(&self.global_ns);

        eprintln!("closed {}",
                  Instant::now().duration_since(start).as_millis());

        // finish up: close per-package test graph and reverse it into a global
        // map of python files to set of affected test files, grouped by package
        // the end goal is a way to easily map affected test files from modified files

        let rev_tg = affected_test_files(&mut g, &self.per_pkg_ns, &self.modules_refs);

        eprintln!("closed/reversed tests {}",
                  Instant::now().duration_since(start).as_millis());

        self.finalized = Some(FinalizedModuleGraph {
            global_ns: g,
            reversed: rev_tg,
        });
    }

    pub fn file_depends_on(&self, filepath: &str) -> Option<HashSet<Ustr>> {
        self.modules_refs.ref_for_fs(ustr(filepath))
            .map(|m| self.depends_on(m))?
    }

    pub fn module_depends_on(&self, module_import_path: &str, pkg_base: Option<&str>) -> Option<HashSet<Ustr>> {
        self.modules_refs.ref_for_py(ustr(module_import_path), pkg_base.map(ustr))
            .map(|m| self.depends_on(m))?
    }

    fn depends_on(&self, m: ModuleRef) -> Option<HashSet<Ustr>> {
        self.finalized.as_ref().unwrap().global_ns.get(&m)
            .map(|r| HashSet::from_iter(
                r.iter().map(|dep| self.modules_refs.py_for_ref(*dep))
            ))
    }

    pub fn affected_by<T: AsRef<str>, L: IntoIterator<Item=T>>(&self, modified_files: L) -> HashMap<Ustr, UstrSet> {
        let affected = &self.finalized.as_ref().unwrap().reversed;

        let mut all_test_modes = HashSet::new();
        for modified_file in modified_files {
            let modified_file = modified_file.as_ref();
            match self.modules_refs.ref_for_fs(ustr(&modified_file)) {
                None => {
                    eprintln!("not a relevant python module: {}", modified_file);
                    continue
                },
                Some(module_ref) => {
                    match affected.get(&module_ref) {
                        None => {
                            eprintln!("0 tests affected by: {}", modified_file);
                        },
                        Some(test_mods) => {
                            eprintln!("{} tests affected by: {}", modified_file, test_mods.len());
                            all_test_modes.extend(test_mods);
                        }
                    }
                }
            }
        }

        let mut grouped_by_pkg: HashMap<Ustr, UstrSet> = HashMap::new();
        for test_mod in all_test_modes {
            let rv = self.modules_refs.get(test_mod);
            grouped_by_pkg.entry(rv.pkg.unwrap()).or_default().insert(rv.fs);
        }
        grouped_by_pkg
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

fn _dfs(g_in: &DashMap<ModuleRef, HashSet<ModuleRef>>,
        edges_out: &mut HashSet<ModuleRef>,
        a: ModuleRef, b: ModuleRef)
{
    if let Some(deps) = g_in.get(&b) {
        for c in deps.value() {
            if *c != a && edges_out.insert(*c) {
                _dfs(g_in, edges_out, a, *c)
            }
        }
    }
}

fn closed_graph_seq(g: &DashMap<ModuleRef, HashSet<ModuleRef>>) -> HashMap<ModuleRef, HashSet<ModuleRef>> {
    let mut g_out = HashMap::with_capacity(g.len());

    for it in g.iter() {
        let start = it.key().clone();
        let mut deps = HashSet::new();
        _dfs(g, &mut deps, start, start);
        g_out.insert(start, deps);
    }
    g_out
}

fn closed_graph_par(g: &DashMap<ModuleRef, HashSet<ModuleRef>>) -> HashMap<ModuleRef, HashSet<ModuleRef>> {
    let g_out = Mutex::new(HashMap::with_capacity(g.len()));

    g.par_iter().for_each(|it| {
        let start = it.key().clone();
        let mut deps = HashSet::new();
        _dfs(g, &mut deps, start, start);
        {
            g_out.lock().unwrap().insert(start, deps);
        }
    });
    g_out.into_inner().unwrap()
}

fn affected_for_pkg(g: &HashMap<ModuleRef, HashSet<ModuleRef>>,
                    tg: &DashMap<ModuleRef, HashSet<ModuleRef>>,
                    ref_cache: &ModuleRefCache,
) -> (HashMap<ModuleRef, HashSet<ModuleRef>>, HashMap<ModuleRef, HashSet<ModuleRef>>)
{
    reify_deps(tg, ref_cache);
    let mut ctg = closed_graph_seq(tg);

    let mut affected: HashMap<ModuleRef, HashSet<ModuleRef>> = HashMap::new();

    for (test_file, deps) in &mut ctg {
        let extra = DashSet::new();
        for dep in deps.iter() {
            affected.entry(*dep).or_default().insert(*test_file);
            if let Some(trans) = g.get(&dep) {
                for tdep in trans {
                    extra.insert(*tdep);
                    affected.entry(*tdep).or_default().insert(*test_file);
                }
            }
        }
        deps.extend(extra);
        // extend global ns
        //g.insert(*test_file, deps);
    }
    (ctg, affected)
}

fn affected_test_files(g: &mut HashMap<ModuleRef, HashSet<ModuleRef>>,
                       test_imports: &DashMap<Ustr, DashMap<ModuleRef, HashSet<ModuleRef>>>,
                       ref_cache: &ModuleRefCache)
-> HashMap<ModuleRef, HashSet<ModuleRef>> {

    let (tx, rx) =
        mpsc::channel::<HashMap<ModuleRef, HashSet<ModuleRef>>>();

    let (tx2, rx2) =
        mpsc::channel::<HashMap<ModuleRef, HashSet<ModuleRef>>>();

    // build up affected files in parallel
    let t = thread::spawn(move || {
        let mut affected: HashMap<ModuleRef, HashSet<ModuleRef>> = HashMap::new();
        for a in rx.iter() {
            for (file, v) in &a {
                let affected = affected.entry(*file).or_default();
                for test_file in v {
                    affected.insert(*test_file);
                }
            }
        }
        affected
    });

    test_imports.par_iter().for_each_with((tx, tx2), |(tx, tx2), it| {
        let pkg = it.key();
        let (cg, aff) = affected_for_pkg(g, &test_imports.get(pkg).unwrap(), ref_cache);
        tx.send(aff).unwrap();
        tx2.send(cg).unwrap();
    });

    // update global ns after parallel iter it done
    for cg in rx2.iter() {
        g.extend(cg);
    }

    // wait for joiner thread to be done
    t.join().unwrap()
}

impl<C> Writable<C> for FinalizedModuleGraph
where
    C: Context
{
    fn write_to<T: ?Sized + Writer<C>>(&self, w: &mut T) -> Result<(), C::Error> {
        write_length_u64_varint(self.global_ns.len(), w).or_else(|e| return Err(e))?;
        for (module, deps) in &self.global_ns {
            w.write_u64_varint(*module as u64).or_else(|e| return Err(e))?;
            write_length_u64_varint(deps.len(), w).or_else(|e| return Err(e))?;
            for dep in deps {
                w.write_u64_varint(*dep as u64).or_else(|e| return Err(e))?;
            }
        }

        write_length_u64_varint(self.reversed.len(), w).or_else(|e| return Err(e))?;
        for (f, affected) in &self.reversed {
            w.write_u64_varint(*f as u64).or_else(|e| return Err(e))?;
            write_length_u64_varint(affected.len(), w).or_else(|e| return Err(e))?;
            for test_file in affected {
                w.write_u64_varint(*test_file as u64).or_else(|e| return Err(e))?;
            }
        }
        Ok(())
    }
}

impl<'a, C> Readable<'a, C> for FinalizedModuleGraph
where
    C: Context
{
    fn read_from<R: Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
        let global_ns_len = read_length_u64_varint(reader)
            .or_else(|e| return Err(e))?;
        let mut global_ns = HashMap::with_capacity(global_ns_len);
        for _ in 0..global_ns_len {
            let mod_ref = reader.read_u64_varint()
                .or_else(|e| return Err(e))? as ModuleRef;
            let dep_len = read_length_u64_varint(reader).or_else(|e| return Err(e))?;
            let mut deps = HashSet::with_capacity(dep_len);
            for _ in 0..dep_len {
                deps.insert(reader.read_u64_varint()
                    .or_else(|e| return Err(e))? as ModuleRef);
            }
            global_ns.insert(mod_ref, deps);
        }

        let reversed_len = read_length_u64_varint(reader)
            .or_else(|e| return Err(e))?;
        let mut reversed = HashMap::with_capacity(reversed_len);
        for _ in 0..reversed_len {
            let mod_ref = reader.read_u64_varint()
                .or_else(|e| return Err(e))? as ModuleRef;
            let affected_len = read_length_u64_varint(reader).or_else(|e| return Err(e))?;
            let mut affected = HashSet::with_capacity(affected_len);
            for _ in 0..affected_len {
                affected.insert(reader.read_u64_varint()
                    .or_else(|e| return Err(e))? as ModuleRef);
            }
            reversed.insert(mod_ref, affected);
        }

        Ok(FinalizedModuleGraph{
            global_ns,
            reversed,
        })
    }
}

impl<C> Writable<C> for ModuleGraph
where
    C: Context
{
    fn write_to<T: ?Sized + Writer<C>>(&self, w: &mut T) -> Result<(), C::Error> {
        w.write_value(&self.packages).or_else(|e| return Err(e))?;
        w.write_value(&self.global_prefixes).or_else(|e| return Err(e))?;
        w.write_value(&self.local_prefixes).or_else(|e| return Err(e))?;
        w.write_value(&self.modules_refs).or_else(|e| return Err(e))?;

        write_length_u64_varint(self.global_ns.len(), w)
            .or_else(|e| return Err(e))?;
        for l in self.global_ns.iter() {
            w.write_u64_varint(*l.key() as u64).or_else(|e| return Err(e))?;
            write_length_u64_varint(l.value().len(), w).or_else(|e| return Err(e))?;
            for v in l.value() {
                w.write_u64_varint(*v as u64).or_else(|e| return Err(e))?;
            }
        }

        write_length_u64_varint(self.per_pkg_ns.len(), w).or_else(|e| return Err(e))?;
        for l in self.per_pkg_ns.iter() {
            write_ustr_to(*l.key(), w).or_else(|e| return Err(e))?;
            write_length_u64_varint(l.value().len(), w).or_else(|e| return Err(e))?;
            for it in l.value().iter() {
                w.write_u64_varint(*it.key() as u64).or_else(|e| return Err(e))?;
                write_length_u64_varint(it.value().len(), w).or_else(|e| return Err(e))?;
                for v in it.value() {
                    w.write_u64_varint(*v as u64).or_else(|e| return Err(e))?;
                }
            }
        }

        w.write_value(&self.finalized)
    }
}

impl<'a, C> Readable<'a, C> for ModuleGraph
where
    C: Context
{
    fn read_from<R: Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
        let packages = reader.read_value()
            .or_else(|e| return Err(e))?;
        let global_prefixes = reader.read_value()
            .or_else(|e| return Err(e))?;
        let local_prefixes = reader.read_value()
            .or_else(|e| return Err(e))?;
        let modules_refs = reader.read_value()
            .or_else(|e| return Err(e))?;

        let global_ns_len = read_length_u64_varint(reader)
            .or_else(|e| return Err(e))?;
        let global_ns = DashMap::with_capacity(global_ns_len);
        for _ in 0..global_ns_len {
            let module_ref = reader.read_u64_varint()
                .or_else(|e| return Err(e))? as ModuleRef;
            let dep_len = read_length_u64_varint(reader)
                .or_else(|e| return Err(e))?;
            let mut deps = HashSet::new();
            for _ in 0..dep_len {
                deps.insert(reader.read_u64_varint()
                    .or_else(|e| return Err(e))? as ModuleRef);
            }
            global_ns.insert(module_ref, deps);
        }

        let mut buf = Vec::new();
        let per_pkg_ns_len = read_length_u64_varint(reader)
            .or_else(|e| return Err(e))?;
        let per_pkg_ns = DashMap::with_capacity(per_pkg_ns_len);
        for _ in 0..per_pkg_ns_len {
            let pkg = read_ustr_with_buf(reader, &mut buf)
                .or_else(|e| return Err(e))?;

            let ns_len = read_length_u64_varint(reader)
                .or_else(|e| return Err(e))?;
            let m = DashMap::with_capacity(ns_len);
            for _ in 0..ns_len {
                let mod_ref = reader.read_u64_varint()
                    .or_else(|e| return Err(e))? as ModuleRef;
                let dep_len = read_length_u64_varint(reader)
                    .or_else(|e| return Err(e))?;
                let mut deps = HashSet::new();
                for _ in 0..dep_len {
                    deps.insert(reader.read_u64_varint()
                        .or_else(|e| return Err(e))? as ModuleRef);
                }
                m.insert(mod_ref, deps);
            }
            per_pkg_ns.insert(pkg, m);
        }

        let finalized = reader.read_value()
            .or_else(|e| return Err(e))?;

        Ok(ModuleGraph::from(
            packages,
            global_prefixes,
            local_prefixes,
            modules_refs,
            global_ns,
            per_pkg_ns,
            finalized,
        ))
    }
}

