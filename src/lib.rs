mod cache;
mod imports;

use std::collections::{HashMap, HashSet};
use pyo3::create_exception;
use pyo3::prelude::*;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::types::{PyFrozenSet, PyNone, PySequence, PySet, PyString};

create_exception!(import_parser_rs, MismatchedOverrideComments, PyValueError);


fn mismatched_comments(t: &str, line: usize) -> PyErr {
    PyErr::new::<MismatchedOverrideComments, _>(format!("unmatched override {} at line {}", t, line))
}


fn freeze_set<T: ToPyObject>(py: Python<'_>, s: &HashSet<T>) -> PyObject {
    // TODO: instead of creating a new frozenset, have a custom Set implementation
    // that provide read-only access to the underlying rust HashSet
    // this would save on memory allocation, and copy, at the cost of added
    // overhead (py->rust transition) when working with the object
    PyFrozenSet::new_bound(py, s).unwrap().to_object(py)
}

fn freeze_string(py: Python<'_>, s: &String) -> PyObject {
    PyString::new_bound(py, s.as_str()).to_object(py)
}


fn to_vec<'py, T>(v: Bound<'py, PyAny>) -> PyResult<Vec<T>>
where
    T: FromPyObject<'py>
{
    if let Ok(_) = v.downcast::<PyNone>() {
        Ok(vec![])
    } else if let Ok(seq) = v.downcast::<PySequence>() {
        Ok(seq.extract::<Vec<T>>().unwrap())
    } else if let Ok(set) = v.downcast::<PySet>() {
        let mut r = Vec::with_capacity(set.len());
        for v in set {
            r.push(v.extract::<T>().unwrap());
        }
        Ok(r)
    } else {
        Err(PyErr::new::<PyTypeError, _>("Expected a sequence or a set"))
    }
}

#[pyclass(subclass, frozen, module="import_parser_rs")]
pub struct ImportParser {
    p: imports::Parser,
}

#[pymethods]
impl ImportParser {
    #[new]
    #[pyo3(signature = (start_override_comment="", end_override_comment=""))]
    fn new(start_override_comment: &str, end_override_comment: &str) -> Self {
        ImportParser {
            p: imports::Parser::new(start_override_comment, end_override_comment),
        }
    }

    #[pyo3(signature = (filepath, deep=false))]
    pub fn get_all_imports<'py>(&self, py: Python<'py>, filepath: String, deep: bool)
        -> PyResult<imports::FileImports> {
        // all python->rust conversion happens prior to this function being called
        // all rust->python conversion happens after this function returning
        // exception ctors are specifically deferred to avoid creation without holding the GIL
        // therefore we can safely release the GIL here
        py.allow_threads(|| self.p.get_all_imports(filepath, deep))
    }

    #[pyo3(signature = (directories, ignore=None))]
    pub fn get_recursive_imports<'py>(&self, py: Python<'py>,
                                      directories: Bound<'py, PyAny>,
                                      ignore: Option<Bound<'py, PyAny>>,
    ) -> PyResult<(HashMap<cache::CachedPy<String>, HashSet<cache::CachedPy<String>>>, HashSet<cache::CachedPy<String>>)> {
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
            py.allow_threads(|| self.p.get_recursive_imports(directories, ignore))
        } else {
            Err(PyErr::new::<PyTypeError, _>("Expected directories nad ignore to be a sequence or a set"))
        }
    }
}

#[pymodule]
fn import_parser_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ImportParser>()?;
    m.add("MismatchedOverrideComments", m.py().get_type_bound::<MismatchedOverrideComments>())?;
    Ok(())
}
