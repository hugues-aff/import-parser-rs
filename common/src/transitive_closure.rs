use std::collections::{HashMap, HashSet};
use std::fs::File;
use dashmap::DashMap;
use speedy::private::{read_length_u64_varint, write_length_u64_varint};
use speedy::{Context, Error, LittleEndian, Readable, Reader, Writable, Writer};
use ustr::{ustr, Ustr, UstrSet};
use hi_sparse_bitset::{BitSet};
use hi_sparse_bitset::config::{_128bit};
use crate::moduleref::{ModuleRef, ModuleRefCache};

type CondensedRef = usize;
type CondensedNode = HashSet<ModuleRef>;
type CondensedEdges = BitSet<_128bit>;

pub struct TransitiveClosure {
    // map filesystem / python paths <-> ModuleRef
    module_refs: ModuleRefCache,

    // map ModuleRef -> CondensedRef
    mod_to_condensed: Vec<CondensedRef>,
    // map CondensedRef -> (set of) ModuleRef
    condensed_to_mod: Vec<CondensedNode>,

    // map CondensedRef -> Set of CondensedRef successors (transitive closure)
    successor: Vec<CondensedEdges>,

    // map CondensedRef -> Set of CondensedRef ancestors (transitive closure)
    ancestor: Vec<CondensedEdges>,
}

impl TransitiveClosure {
    pub fn from(
        g: &DashMap<ModuleRef, HashSet<ModuleRef>>,
        refs: &ModuleRefCache,
    ) -> TransitiveClosure {
        let n = refs.max_value() as usize;
        let mut state = StackTC{
            max_d: 0,
            d: vec![0; n],
            root: vec![ModuleRef::MAX; n],
            comp: vec![CondensedRef::MAX; n],
            saved_height: vec![0; n],
            cstack: Vec::new(),
            vstack: Vec::new(),
            scc: Vec::with_capacity(n),
            succ: Vec::with_capacity(n),
        };
        for it in g {
            let v = *it.key();
            if state.root[v as usize] == ModuleRef::MAX {
                stack_tc(&mut state, v, g);
            }
        }

        let mut ancestor = Vec::with_capacity(n);
        ancestor.resize_with(n, CondensedEdges::new);
        for c in 0..state.scc.len() as CondensedRef {
            for succ in &state.succ[c] {
                ancestor[succ].insert(c);
            }
        }

        TransitiveClosure{
            module_refs: refs.clone(),
            mod_to_condensed: state.comp,
            condensed_to_mod: state.scc,
            successor: state.succ,
            ancestor,
        }
    }

    pub fn from_file(filepath: &str) -> Result<TransitiveClosure, Error> {
        let file = File::open(filepath)
            .map_err(|e| Error::custom(e.to_string()))?;
        let stream = zstd::Decoder::new(file)
            .map_err(|e| Error::custom(e.to_string()))?;
        Self::read_from_stream_buffered_with_ctx(LittleEndian::default(), stream)
    }

    pub fn to_file(&self, filepath: &str) -> Result<(), Error> {
        let file = File::create(filepath)
            .map_err(|e| Error::custom(e.to_string()))?;
        let stream = zstd::Encoder::new(file, 0)
            .map_err(|e| Error::custom(e.to_string()))?
            .auto_finish();
        self.write_to_stream_with_ctx(LittleEndian::default(), stream)
    }

    pub fn apply_dynamic_edges_at_leaves(
        &mut self,
        simple_unified: &HashMap<String, HashSet<String>>,
        simple_per_package: &HashMap<String, HashMap<String, HashSet<String>>>,
    ) {
        apply_dynamic_edges_at_leaves(self, simple_unified, simple_per_package)
    }

    pub fn file_depends_on(&self, filepath: &str) -> Option<HashSet<Ustr>> {
        self.module_refs.ref_for_fs(ustr(filepath))
            .map(|m| self.depends_on(m))?
    }

    pub fn module_depends_on(&self, module_import_path: &str, pkg_base: Option<&str>) -> Option<HashSet<Ustr>> {
        self.module_refs.ref_for_py(ustr(module_import_path), pkg_base.map(ustr))
            .map(|m| self.depends_on(m))?
    }

    pub fn depends_on(&self, m: ModuleRef) -> Option<HashSet<Ustr>> {
        let mut deps = HashSet::new();
        for c in &self.successor[self.mod_to_condensed[m as usize] as usize] {
            for &v in &self.condensed_to_mod[c] {
                deps.insert(self.module_refs.py_for_ref(v));
            }
        }
        Some(deps)
    }

    pub fn affected_by<T: AsRef<str>, L: IntoIterator<Item=T>>(&self, modified_files: L)
                                                               -> HashMap<Ustr, UstrSet>
    {
        let mut all_sccs: HashSet<CondensedRef> = HashSet::new();
        for modified_file in modified_files {
            let modified_file = modified_file.as_ref();
            match self.module_refs.ref_for_fs(ustr(&modified_file)) {
                None => {
                    eprintln!("not a relevant python module: {}", modified_file);
                    continue
                },
                Some(module_ref) => {
                    match self.ancestor.get(self.mod_to_condensed[module_ref as usize]) {
                        None => {
                            // eprintln!("0 tests affected by: {}", modified_file);
                        },
                        Some(scc) => {
                            // eprintln!("{} tests affected by: {}", modified_file, scc.len());
                            all_sccs.extend(scc);
                        }
                    }
                }
            }
        }

        let mut grouped_by_pkg: HashMap<Ustr, UstrSet> = HashMap::new();
        for c in all_sccs {
            for &v in &self.condensed_to_mod[c] {
                let rv = self.module_refs.get(v);
                // NB: filter out non-test
                if rv.pkg.is_some() {
                    grouped_by_pkg.entry(rv.pkg.unwrap()).or_default().insert(rv.fs);
                }
            }
        }
        grouped_by_pkg
    }
}


struct StackTC {
    max_d: usize,
    d: Vec<usize>,
    root: Vec<ModuleRef>,
    comp: Vec<CondensedRef>,
    saved_height: Vec<usize>,

    cstack: Vec<CondensedRef>,
    vstack: Vec<ModuleRef>,

    scc: Vec<CondensedNode>,
    succ: Vec<CondensedEdges>,
}

// The Stack_TC algorithm, outlined in E. Nuutila's thesis (https://www.cs.hut.fi/~enu/thesis.html)
// computes the transitive closure of the condensation of a digraph (i.e. the DAG obtained by
// collapsing every Strongly Connected Component into a single "condensed" vertex.
// Stack_TC builds upon Tarjan's algorithm to derive SCCs, weaves the derivation of the transitive
// closure into the same single Depth-First Traversal, and uses a minimal number of set union to
// compute it. This ensures optimal asymptotic complexity, with low constant factors, as well as
// noticeable improvements in speed and memory usage for graphs with sizeable SCCs
// See also https://dub.uu.nl/sites/default/files/legacy/other/INF-SCR-10-10.pdf for an alternative
// description and related context.
fn stack_tc(s: &mut StackTC, v: ModuleRef, g: &DashMap<ModuleRef, HashSet<ModuleRef>>) {
    s.root[v as usize] = v;
    s.saved_height[v as usize] = s.cstack.len();
    s.d[v as usize] = s.max_d;
    s.max_d += 1;
    s.vstack.push(v);

    if let Some(edges) = g.get(&v) {
        for &w in edges.value() {
            if v != w {
                if s.root[w as usize] == ModuleRef::MAX {
                    stack_tc(s, w, g);
                }
                let cw = s.comp[w as usize];
                if cw == CondensedRef::MAX {
                    let (rw, rv) = (s.root[w as usize], s.root[v as usize]);
                    if s.d[rw as usize] < s.d[rv as usize] {
                        s.root[v as usize] = rw;
                    }
                } else {
                    // if cw is valid, then (v, w) is an inter-component edge
                    s.cstack.push(cw);
                }
            }
        }
    }

    if s.root[v as usize] == v {
        assert_eq!(s.scc.len(), s.succ.len());
        let cv = s.scc.len() as CondensedRef;
        s.comp[v as usize] = cv;
        let mut succ = CondensedEdges::new();
        if *s.vstack.last().unwrap() != v {
            succ.insert(cv);
        }
        assert!(s.cstack.len() >= s.saved_height[v as usize]);
        while s.cstack.len() > s.saved_height[v as usize] {
            let x = s.cstack.pop().unwrap();
            if !succ.contains(x) {
                succ.insert(x);
                for sx in &s.succ[x] {
                    succ.insert(sx);
                }
            }
        }
        s.succ.push(succ);
        let mut scc = CondensedNode::new();
        loop {
            let w = s.vstack.pop().unwrap();
            scc.insert(w);
            s.comp[w as usize] = cv;
            if w == v {
                break;
            }
        }
        s.scc.push(scc);
    }
}


fn convert_deps(tc: &TransitiveClosure, deps: &HashSet<String>) -> CondensedEdges {
    let mut extra_deps = CondensedEdges::new();
    for d in deps {
        if let Some(mod_ref) = tc.module_refs.ref_for_py(ustr(&d), None) {
            let cref = tc.mod_to_condensed[mod_ref as usize];
            extra_deps.insert(cref);
            for succ in &tc.successor[cref as usize] {
                extra_deps.insert(succ);
            }
        }
    }
    extra_deps
}

fn convert_unified(
    tc: &TransitiveClosure,
    unified: &HashMap<String, HashSet<String>>,
) -> HashMap<CondensedRef, CondensedEdges>{
    let mut triggers = HashMap::new();
    for (trigger, deps) in unified {
        if let Some(mod_ref) = tc.module_refs.ref_for_py(ustr(&trigger), None) {
            triggers.insert(tc.mod_to_condensed[mod_ref as usize], convert_deps(tc, deps));
        }
    }
    triggers
}

fn convert_package_varying(
    tc: &TransitiveClosure,
    per_package: &HashMap<String, HashMap<String, HashSet<String>>>,
) -> HashMap<CondensedRef, HashMap<Ustr, CondensedEdges>>{
    let mut triggers = HashMap::with_capacity(per_package.len());
    for (trigger, per_pkg_deps) in per_package {
        if let Some(mod_ref) = tc.module_refs.ref_for_py(ustr(&trigger), None) {
            let c = tc.mod_to_condensed[mod_ref as usize];
            let mut dep_map = HashMap::with_capacity(per_pkg_deps.len());
            for (pkg, deps) in per_pkg_deps {
                dep_map.insert(ustr(pkg.as_str()), convert_deps(tc, deps));
            }
            triggers.insert(c, dep_map);
        }
    }
    triggers
}


pub fn apply_dynamic_edges_at_leaves(
    tc: &mut TransitiveClosure,
    unified: &HashMap<String, HashSet<String>>,
    per_package: &HashMap<String, HashMap<String, HashSet<String>>>,
) {
    for (&t, deps) in &convert_unified(tc, unified) {
        apply_unified_trigger(tc, t, deps)
    }
    for (&t, deps) in &convert_package_varying(tc, per_package) {
        apply_package_vaying_trigger(tc, t, deps)
    }
}

fn apply_trigger(
    successors: &mut Vec<CondensedEdges>,
    ancestors: &Vec<CondensedEdges>,  // NB: we're lying about this, but it's okay...
    trigger: CondensedRef,
    triggered: CondensedRef,
    extra_deps: &CondensedEdges,
) {
    assert_ne!(triggered, trigger);
    for extra_dep in extra_deps {
        if trigger == extra_dep || triggered == extra_dep {
            continue;
        }
        // this is simple because:
        // 1. we have checked that triggered has no ancestors
        // 2. the preprocessing loop earlier ensured that we have
        //    a complete set of extra_deps to work with
        successors[triggered].insert(extra_dep);
        // sigh, this is annoying
        // rust borrow checker doesn't have a way to mix mutable
        // and immutable access to a vector even if the elements
        // being accessed are disjoint. To be fair, it's hard to
        // prove in the general case, although it would be nice
        // if it could leverage assertions about indices being
        // distinct...
        unsafe {
            std::ptr::from_ref(&ancestors[extra_dep])
                .cast_mut().as_mut().unwrap().insert(triggered);
        }
    }
}

fn apply_unified_trigger(
    tc: &mut TransitiveClosure,
    trigger: CondensedRef,
    extra_deps: &CondensedEdges,
) {
    for triggered in &tc.ancestor[trigger as usize] {
        if triggered == trigger {
            continue;
        }
        // NB: only add successors to SCCs that have no ancestors themselves
        if !&tc.ancestor[triggered].is_empty() {
            continue;
        }
        apply_trigger(&mut tc.successor, &tc.ancestor, trigger, triggered, extra_deps);
    }
}

fn apply_package_vaying_trigger(
    tc: &mut TransitiveClosure,
    trigger: CondensedRef,
    per_pkg_dep: &HashMap<Ustr, CondensedEdges>,
) {
    for triggered in &tc.ancestor[trigger as usize] {
        if triggered == trigger {
            continue;
        }
        // NB: only add successors to SCCs that have no ancestors themselves
        if !&tc.ancestor[triggered].is_empty() {
            continue;
        }

        // pick any module within the SCC to determine package membership
        // by construction, nodes in an SCC are either all within the global ns
        // or all within a single package-local ns
        let v = *tc.condensed_to_mod[triggered].iter().next().unwrap();
        let rv = tc.module_refs.get(v);

        if let Some(pkg) = rv.pkg {
            if let Some(extra_deps) = per_pkg_dep.get(&pkg) {
                apply_trigger(&mut tc.successor, &tc.ancestor, trigger, triggered, extra_deps);
            }
        }
    }
}


impl<C> Writable<C> for TransitiveClosure
where
    C: Context
{
    fn write_to<T: ?Sized + Writer<C>>(&self, w: &mut T) -> Result<(), C::Error> {
        w.write_value(&self.module_refs)?;

        let n = self.mod_to_condensed.len();
        write_length_u64_varint(n, w)?;
        for i in 0..n {
            w.write_u64_varint(self.mod_to_condensed[i] as u64)?;
        }

        let n = self.condensed_to_mod.len();
        write_length_u64_varint(n, w)?;
        for i in 0..n {
            let vs = &self.condensed_to_mod[i];
            write_length_u64_varint(vs.len(), w)?;
            for &v in vs {
                w.write_u64_varint(v as u64)?;
            }

            let edges = &self.successor[i];
            write_length_u64_varint(edges.iter().count(), w)?;
            for dep in edges {
                w.write_u64_varint(dep as u64)?;
            }

            let tc = &self.ancestor[i];
            write_length_u64_varint(tc.iter().count(), w)?;
            for dep in tc {
                w.write_u64_varint(dep as u64)?;
            }
        }
        Ok(())
    }
}

impl<'a, C> Readable<'a, C> for TransitiveClosure
where
    C: Context
{
    fn read_from<R: Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
        let module_refs = reader.read_value()?;

        let n = read_length_u64_varint(reader)?;
        let mut mod_to_condensed = Vec::with_capacity(n);
        for _ in 0..n {
            mod_to_condensed.push(reader.read_u64_varint()? as CondensedRef);
        }

        let n = read_length_u64_varint(reader)?;
        let mut condensed_to_mod = Vec::with_capacity(n);
        let mut successor = Vec::with_capacity(n);
        let mut ancestor = Vec::with_capacity(n);
        for _ in 0..n {
            let l = read_length_u64_varint(reader)?;
            let mut scc = HashSet::with_capacity(l);
            for _ in 0..l {
                scc.insert(reader.read_u64_varint()? as ModuleRef);
            }
            condensed_to_mod.push(scc);

            let l = read_length_u64_varint(reader)?;
            let mut succ = CondensedEdges::new();
            for _ in 0..l {
                succ.insert(reader.read_u64_varint()? as CondensedRef);
            }
            successor.push(succ);

            let l = read_length_u64_varint(reader)?;
            let mut anc = CondensedEdges::new();
            for _ in 0..l {
                anc.insert(reader.read_u64_varint()? as CondensedRef);
            }
            ancestor.push(anc);
        }

        Ok(TransitiveClosure{
            module_refs,
            mod_to_condensed,
            condensed_to_mod,
            successor,
            ancestor,
        })
    }
}
