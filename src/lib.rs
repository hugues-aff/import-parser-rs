mod cache;

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs::read_to_string;
use std::io;
use dashmap::DashMap;
use pyo3::create_exception;
use pyo3::prelude::*;
use pyo3::exceptions::{PyException, PyOSError, PyTypeError, PyValueError};
use pyo3::types::{PyFrozenSet, PyNone, PySequence, PySet, PyString, PyTuple};
use ruff_text_size::{Ranged, TextRange};
use ruff_python_parser::{parse_module, Token, TokenKind};
use walkdir::WalkDir;

create_exception!(import_parser_rs, MismatchedOverrideComments, PyValueError);


fn mismatched_comments(t: &str, line: usize) -> PyErr {
    PyErr::new::<MismatchedOverrideComments, _>(format!("unmatched override {} at line {}", t, line))
}

fn next_override_block(it: &mut core::slice::Iter<'_, Token>,
                       line: &mut usize,
                       source: &str,
                       start_override_comment: &str,
                       end_override_comment: &str
) -> Result<TextRange, PyErr> {
    let mut start_of_line = false;
    let mut override_start_line = 1;
    let mut override_range = TextRange::empty(0.into());
    while let Some(token) = it.next() {
        match token.kind() {
            TokenKind::Comment => {
                if start_of_line {
                    if source[token.range()].eq(start_override_comment) {
                        override_range = token.range();
                        override_start_line = *line;
                    } else if source[token.range()].eq(end_override_comment) {
                        if override_range.is_empty() {
                            return Err(mismatched_comments("end", *line))
                        }
                        // return the boundary of the next override block
                        return Ok(override_range.cover(token.range()))
                    }
                }
            }
            TokenKind::Newline | TokenKind::NonLogicalNewline => {
                start_of_line = true;
                *line += 1;
            }
            _ => {
                start_of_line = false
            }
        }
    }
    if !override_range.is_empty() {
        return Err(mismatched_comments("start", override_start_line))
    }
    // reached the end of the file
    Ok(TextRange::empty(u32::try_from(source.len()).ok().unwrap().into()))
}

fn imports_from_module(source: &str,
                       start_override_comment: &str,
                       end_override_comment: &str,
                       deep: bool,
) -> Result<FileImports, PyErr> {
    match parse_module(source) {
        Err(error) => Err(PyException::new_err(error.to_string())),
        Ok(m) => {
            let mut imports : HashSet<cache::CachedPy<String>> = HashSet::new();
            let mut ignored_imports : HashSet<cache::CachedPy<String>> = HashSet::new();

            let mut token_it = m.tokens().iter();
            let mut override_range = TextRange::empty(0.into());
            let check_override = !(
                start_override_comment.is_empty() || end_override_comment.is_empty()
            );

            let mut line = 0;

            for stmt in &m.syntax().body {
                // skip to the next relevant override block
                while check_override && stmt.range().ordering(override_range) == Ordering::Greater {
                    let next_override = next_override_block(
                        &mut token_it, &mut line, source, start_override_comment, end_override_comment
                    );
                    if next_override.is_err() {
                        return Err(next_override.err().unwrap())
                    }
                    override_range = next_override?;
                }

                // put any import into the relevant bucket
                let dest = match override_range.contains_range(stmt.range()) {
                    true => &mut ignored_imports,
                    false => &mut imports,
                };

                if stmt.is_import_stmt() {
                    let imp = stmt.as_import_stmt().unwrap();
                    for n in &imp.names {
                        dest.insert(cache::CachedPy::new(n.name.to_string(), freeze_string));
                    }
                } else if stmt.is_import_from_stmt() {
                    let imp = stmt.as_import_from_stmt().unwrap();
                    if imp.level != 0 {
                        // ignore relative imports
                        continue
                    }
                    let prefix = imp.module.as_ref().unwrap().to_string() + ".";
                    for n in &imp.names {
                        dest.insert(cache::CachedPy::new(prefix.clone() + n.name.as_str(), freeze_string));
                    }
                } else if deep {
                    // TODO:
                    //stmt.visit_source_order()
                }
            }

            Ok(FileImports::new(imports, ignored_imports))
        }
    }
}

fn get_all_imports_no_gil(
    filepath: &str,
    start_override_comment: &str,
    end_override_comment: &str,
    deep: bool,
) -> PyResult<FileImports> {
    match read_to_string(filepath) {
        Err(err) => Err(PyOSError::new_err(err)),
        Ok(source) => imports_from_module(
            &source,
            start_override_comment,
            end_override_comment,
            deep,
        )
    }
}

// TODO: faster version using path trie?
fn is_ignored(path: &str, ignore: &Vec<String>) -> bool {
    for i in ignore {
        if path.starts_with(i) {
            return true
        }
    }
    false
}

fn for_each_python_file<F>(directories: &Vec<String>,
                           ignore: &Vec<String>,
                           mut inner_fn: F
) -> Option<PyErr>
where
    F: FnMut(&str) -> Option<PyErr>
{
    for directory in directories.iter() {
        let walker = WalkDir::new(directory).into_iter();
        for entry in walker.filter_entry(
            |e| (
                !(e.file_name().to_str().unwrap().starts_with(".")
                    || (e.file_type().is_dir() && !ignore.is_empty() &&
                        is_ignored(e.path().to_str().unwrap(), ignore)))
            )) {
            if entry.is_err() {
                return Some(PyOSError::new_err(io::Error::from(entry.err().unwrap())))
            }
            let e = entry.unwrap();
            if e.file_type().is_file() && e.file_name().to_str().unwrap().ends_with(".py") {
                let r = inner_fn(e.path().to_str().unwrap());
                if r.is_some() {
                    return r;
                }
            }
        }
    }
    None
}



struct FileImports {
    valid: cache::CachedPy<HashSet<cache::CachedPy<String>>>,
    ignored: cache::CachedPy<HashSet<cache::CachedPy<String>>>,
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

impl FileImports {
    fn new(valid: HashSet<cache::CachedPy<String>>, ignored: HashSet<cache::CachedPy<String>>) -> Self {
        FileImports{
            valid: cache::CachedPy::new(valid, freeze_set),
            ignored: cache::CachedPy::new(ignored, freeze_set),
        }
    }
}

impl ToPyObject for FileImports {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        PyTuple::new_bound(
            py, vec![self.valid.to_object(py), self.ignored.to_object(py)]
        ).to_object(py)
    }
}

impl IntoPy<PyObject> for FileImports {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.to_object(py)
    }
}

impl Clone for FileImports {
    fn clone(&self) -> FileImports {
        FileImports{
            valid: self.valid.clone(),
            ignored: self.ignored.clone(),
        }
    }
}


pub struct _ImportParser {
    start_override_comment: String,
    end_override_comment: String,
    // NB: use a concurrency-safe container to allow the Python side to leverage threading
    cache: DashMap<String, FileImports>,
}

impl _ImportParser {
    pub fn new(start_override_comment: &str, end_override_comment: &str) -> _ImportParser {
        _ImportParser{
            start_override_comment: start_override_comment.to_string(),
            end_override_comment: end_override_comment.to_string(),
            cache: DashMap::new(),
        }
    }

    fn get_all_imports(&self, filepath: String, deep: bool)
        -> PyResult<FileImports> {
        match self.cache.get(&filepath) {
            Some(r) => {
                Ok(r.value().clone())
            },
            None => {
                match get_all_imports_no_gil(
                    &filepath,
                    &self.start_override_comment,
                    &self.end_override_comment,
                    deep
                ) {
                    Ok(r) => {
                        self.cache.insert(filepath.clone(), r.clone());
                        Ok(r)
                    },
                    Err(err) => Err(err),
                }
            }
        }
    }

    pub fn get_recursive_imports(&self,
                                      directories: Vec<String>,
                                      ignore: Vec<String>,
    ) -> PyResult<(HashMap<cache::CachedPy<String>, HashSet<cache::CachedPy<String>>>, HashSet<cache::CachedPy<String>>)> {
        let mut all_imports: HashMap<cache::CachedPy<String>, HashSet<cache::CachedPy<String>>> = HashMap::new();
        let mut all_ignored: HashSet<cache::CachedPy<String>> = HashSet::new();

        let mut collect_ = |filepath: &cache::CachedPy<String>, file_imports: &FileImports|  {
            for v in file_imports.valid.as_ref() {
                all_imports.entry(v.clone())
                    .or_insert(HashSet::new())
                    .insert(filepath.clone());
            }
            for i in file_imports.ignored.as_ref() {
                all_ignored.insert(i.clone());
            }
        };

        match for_each_python_file(&directories, &ignore, |filepath| {
            let r = self.cache.get(filepath);
            let cowpath: cache::CachedPy<String> = cache::CachedPy::new(filepath.to_string(), freeze_string);
            match r {
                Some(r) => {
                    collect_(&cowpath, r.value())
                }
                None => {
                    match get_all_imports_no_gil(
                        &filepath,
                        &self.start_override_comment,
                        &self.end_override_comment,
                        false
                    ) {
                        Err(err) => return Some(err),
                        Ok(r) => {
                            collect_(&cowpath, &r);
                            self.cache.insert(filepath.to_string(), r);
                        }
                    }
                }
            }
            None
        }) {
            Some(err) => Err(err),
            None => Ok((all_imports, all_ignored)),
        }
    }
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
        py.allow_threads(|| self._p.get_all_imports(filepath, deep))
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
            py.allow_threads(|| self._p.get_recursive_imports(directories, ignore))
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
