
use std::hash::Hash;
use std::sync::{Arc, OnceLock};
use pyo3::prelude::*;

pub trait CacheablePy : Eq + PartialEq + ToPyObject {}
impl<T> CacheablePy for T where T: Eq + PartialEq + ToPyObject {}

struct RPy<T: CacheablePy> {
    val: T,
    py_val: OnceLock<PyObject>,
    conv: fn(Python, &T) -> PyObject,
}

impl<T: CacheablePy> RPy<T> {
    fn new(val: T, f: fn(Python, &T) -> PyObject) -> Self {
        RPy{
            val,
            conv: f,
            py_val: OnceLock::new(),
        }
    }
}

impl<T: CacheablePy> PartialEq for RPy<T> {
    fn eq(&self, other: &Self) -> bool {
        self.val.eq(&other.val)
    }
}

impl<T: CacheablePy> Eq for RPy<T> {}

impl<T: CacheablePy + Hash> Hash for RPy<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.val.hash(state);
    }
}

impl<T: CacheablePy> AsRef<T> for RPy<T> {
    fn as_ref(&self) -> &T {
        &self.val
    }
}

impl<T: CacheablePy> ToPyObject for RPy<T>
{
    fn to_object(&self, py: Python<'_>) -> PyObject {
        self.py_val.get_or_init(|| (self.conv)(py, &self.val)).clone_ref(py)
    }
}

impl<T: CacheablePy> IntoPy<PyObject> for RPy<T> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.to_object(py)
    }
}

/// A thread-safe reference-counted holder for an immutable value meant
/// to be shared widely, possibly across threads, on both Rust and Python
///
/// It holds one owned Rust value that can only be accessed as an immutable
/// reference, and an internal placeholder for the equivalent Python object
/// that is created on demand, and shared thereafter
///
/// NB: for soundness, care should be taken to use immutable types for the
/// Python object to prevent it from being mutated in a way that diverges
/// from the Rust value. This is the responsibility of the caller
pub struct CachedPy<T: CacheablePy> {
    d: Arc<RPy<T>>
}

impl<T: CacheablePy> CachedPy<T> {
    pub(crate) fn new(val: T, f: fn(Python, &T) -> PyObject) -> Self {
        CachedPy{
            d: Arc::new(RPy::new(val, f)),
        }
    }
}

impl<T: CacheablePy> PartialEq for CachedPy<T> {
    fn eq(&self, other: &Self) -> bool {
        self.d.eq(&other.d)
    }
}

impl<T: CacheablePy> Eq for CachedPy<T> {}

impl<T: CacheablePy + Hash> Hash for CachedPy<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.d.hash(state);
    }
}

impl<T: CacheablePy> AsRef<T> for CachedPy<T> {
    fn as_ref(&self) -> &T {
        &self.d.val
    }
}

impl<T: CacheablePy> Clone for CachedPy<T> {
    fn clone(&self) -> Self {
        CachedPy{
            d: self.d.clone(),
        }
    }
}

impl<T: CacheablePy> ToPyObject for CachedPy<T>
where
    T: ToPyObject
{
    fn to_object(&self, py: Python<'_>) -> PyObject {
        self.d.to_object(py)
    }
}

impl<T: CacheablePy> IntoPy<PyObject> for CachedPy<T> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.to_object(py)
    }
}
