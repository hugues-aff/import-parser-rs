use std::collections::{HashMap, HashSet};
use pyo3::prelude::*;
use pyo3::exceptions::{PyException, PyTypeError};
use pyo3::types::{PyDict, PyNone, PySequence, PySet, PyString};

use common::*;
use common::graph;
use common::transitive_closure::{TransitiveClosure};
use common::cache;

fn to_vec<'py, T>(v: Bound<'py, PyAny>) -> PyResult<Vec<T>>
where
    T: FromPyObject<'py>
{
    if let Ok(_) = v.downcast::<PyNone>() {
        Ok(vec![])
    } else if let Ok(seq) = v.downcast::<PySequence>() {
        Ok(seq.extract::<Vec<T>>()?)
    } else if let Ok(set) = v.downcast::<PySet>() {
        let mut r = Vec::with_capacity(set.len());
        for v in set {
            r.push(v.extract::<T>()?);
        }
        Ok(r)
    } else {
        Err(PyErr::new::<PyTypeError, _>("Expected a sequence or a set"))
    }
}


#[pyclass(subclass, frozen, module="import_parser_rs")]
pub struct ImportParser {
    _p: _ImportParser,
}

#[pymethods]
impl ImportParser {
    #[new]
    #[pyo3(signature = (start_override_comment="", end_override_comment=""))]
    fn new(start_override_comment: &str, end_override_comment: &str) -> Self {
        ImportParser {
            _p: _ImportParser::new(start_override_comment, end_override_comment),
        }
    }

    #[pyo3(signature = (filepath, deep=false))]
    pub fn get_all_imports<'py>(&self, py: Python<'py>, filepath: String, deep: bool)
                                -> PyResult<FileImports> {
        // all python->rust conversion happens prior to this function being called
        // all rust->python conversion happens after this function returning
        // exception ctors are specifically deferred to avoid creation without holding the GIL
        // therefore we can safely release the GIL here
        match py.allow_threads(|| self._p.get_all_imports(filepath, deep)) {
            Ok(r) => Ok(r),
            Err(e) => { Err(e.to_pyerr()) }
        }
    }

    #[pyo3(signature = (directories, ignore=None))]
    pub fn get_recursive_imports<'py>(&self, py: Python<'py>,
                                      directories: Bound<'py, PyAny>,
                                      ignore: Option<Bound<'py, PyAny>>,
    ) -> PyResult<(HashMap<cache::CachedPy<String>, StringSet>, StringSet)> {
        let ignore_vec ;
        if ignore.is_none() {
            ignore_vec = Ok(vec![])
        } else {
            ignore_vec = to_vec(ignore.unwrap())
        }
        if let (Ok(directories), Ok(ignore)) = (to_vec(directories), ignore_vec) {
            // all python->rust conversion happens prior to this function being called
            // all rust->python conversion happens after this function returning
            // exception ctors are specifically deferred to avoid creation without holding the GIL
            // therefore we can safely release the GIL here
            match py.allow_threads(|| self._p.get_recursive_imports(directories, ignore)) {
                Ok(r) => Ok(r),
                Err(e) => { Err(e.to_pyerr()) }
            }
        } else {
            Err(PyErr::new::<PyTypeError, _>("Expected directories nad ignore to be a sequence or a set"))
        }
    }

}

#[pyclass(subclass, module="import_parser_rs")]
pub struct ModuleGraph {
    tc: TransitiveClosure,
}

#[pymethods]
impl ModuleGraph {
    #[new]
    #[pyo3(signature = (packages, global_prefixes, local_prefixes))]
    fn new<'py>(py: Python<'py>, packages: HashMap<String, String>,
           global_prefixes: HashSet<String>,
           local_prefixes: HashSet<String>,
    ) -> PyResult<ModuleGraph> {
        let tc = py.allow_threads(|| {
            let g = graph::ModuleGraph::new(
                packages,
                global_prefixes,
                local_prefixes,
            );
            g.parse_parallel()?;
            Ok(g.finalize())
        }).or_else(|e: parser::Error| return Err(PyErr::new::<PyException, _>(e.to_string())))?;
        Ok(ModuleGraph{ tc })
    }

    #[staticmethod]
    #[pyo3(signature = (filepath))]
    fn from_file<'py>(py: Python<'py>, filepath: &str) -> PyResult<ModuleGraph> {
        Ok(ModuleGraph {
            tc: py.allow_threads(|| TransitiveClosure::from_file(filepath))
                .or_else(|e| Err(PyErr::new::<PyException, _>(e.to_string())))?
        })
    }

    #[pyo3(signature = (filepath))]
    fn to_file<'py>(&self, py: Python<'py>, filepath: &str) -> PyResult<()> {
        py.allow_threads(|| self.tc.to_file(filepath))
            .or_else(|e| Err(PyErr::new::<PyException, _>(e.to_string())))
    }

    #[pyo3(signature = (simple_unified, simple_per_package))]
    fn add_dynamic_dependencies_at_edges<'py>(&mut self, py: Python<'py>,
                    simple_unified: HashMap<String, HashSet<String>>,
                    simple_per_package: HashMap<String, HashMap<String, HashSet<String>>>
    ) -> PyResult<()> {
        py.allow_threads(|| {
            self.tc.apply_dynamic_edges_at_leaves(&simple_unified, &simple_per_package)
        });
        Ok(())
    }

    #[pyo3(signature = (filepath))]
    fn file_depends_on<'py>(&self, py: Python<'py>, filepath: &str) -> PyResult<PyObject>
    {
        Ok(match self.tc.file_depends_on(filepath) {
            None => PyNone::get_bound(py).into_py(py),
            Some(deps) => {
                let r = PySet::empty_bound(py)
                    .or_else(|e| return Err(e))?;
                for dep in deps {
                    r.add(PyString::new_bound(py, &dep))?;
                }
                r.into_py(py)
            }
        })
    }
    #[pyo3(signature = (module_import_path, package_root = None))]
    fn module_depends_on<'py>(&self, py: Python<'py>,
                              module_import_path: &str,
                              package_root: Option<&str>)
        -> PyResult<PyObject>
    {
        Ok(match self.tc.module_depends_on(module_import_path, package_root) {
            None => PyNone::get_bound(py).into_py(py),
            Some(deps) => {
                let r = PySet::empty_bound(py)
                    .or_else(|e| return Err(e))?;
                for dep in deps {
                    r.add(PyString::new_bound(py, &dep))?;
                }
                r.into_py(py)
            }
        })
    }

    #[pyo3(signature = (modified_files))]
    fn affected_by<'py>(&self, py: Python<'py>, modified_files: Bound<'py, PyAny>)
        -> PyResult<Bound<'py, PyDict>>
    {
        let modified_files : Vec<String> = to_vec(modified_files).or_else(|e| return Err(e))?;
        let affected = py.allow_threads(|| self.tc.affected_by(modified_files));

        let r = PyDict::new_bound(py);
        for (pkg, test_files) in &affected {
            let files = PySet::empty_bound(py)?;
            for file in test_files {
                files.add(PyString::new_bound(py, &file))?
            }
            r.set_item(PyString::new_bound(py, &pkg), files)?
        }

        Ok(r)
    }
}

#[pymodule]
fn import_parser_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ImportParser>()?;
    m.add_class::<ModuleGraph>()?;
    m.add("MismatchedOverrideComments", m.py().get_type_bound::<MismatchedOverrideComments>())?;
    Ok(())
}

