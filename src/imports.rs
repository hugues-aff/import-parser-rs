use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs::read_to_string;
use std::io;
use dashmap::DashMap;
use pyo3::{IntoPy, PyErr, PyObject, PyResult, Python, ToPyObject};
use pyo3::exceptions::{PyException, PyOSError};
use pyo3::types::PyTuple;
use ruff_python_parser::{parse_module, Token, TokenKind};
use ruff_text_size::{Ranged, TextRange};
use walkdir::WalkDir;
use crate::{cache, freeze_set, freeze_string, mismatched_comments};


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


pub struct FileImports {
    valid: cache::CachedPy<HashSet<cache::CachedPy<String>>>,
    ignored: cache::CachedPy<HashSet<cache::CachedPy<String>>>,
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


pub struct Parser {
    start_override_comment: String,
    end_override_comment: String,
    // NB: use a concurrency-safe container to allow the Python side to leverage threading
    cache: DashMap<String, FileImports>,
}

impl Parser {
    pub fn new(start_override_comment: &str, end_override_comment: &str) -> Parser {
        Parser{
            start_override_comment: start_override_comment.to_string(),
            end_override_comment: end_override_comment.to_string(),
            cache: DashMap::new(),
        }
    }

    pub(crate) fn get_all_imports(&self, filepath: String, deep: bool)
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