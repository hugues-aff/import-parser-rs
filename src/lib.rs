use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs::read_to_string;
use std::io;
use dashmap::DashMap;
use pyo3::create_exception;
use pyo3::prelude::*;
use pyo3::exceptions::{PyException, PyOSError, PyValueError};
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
) -> Result<(HashSet<Cow<'static, str>>, HashSet<Cow<'static, str>>), PyErr> {
    match parse_module(source) {
        Err(error) => Err(PyException::new_err(error.to_string())),
        Ok(m) => {
            let mut imports : HashSet<Cow<'static, str>> = HashSet::new();
            let mut ignored_imports : HashSet<Cow<'static, str>> = HashSet::new();

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
                        dest.insert(Cow::from(n.name.to_string()));
                    }
                } else if stmt.is_import_from_stmt() {
                    let imp = stmt.as_import_from_stmt().unwrap();
                    if imp.level != 0 {
                        // ignore relative imports
                        continue
                    }
                    let prefix = imp.module.as_ref().unwrap().to_string() + ".";
                    for n in &imp.names {
                        dest.insert(Cow::from(prefix.clone() + n.name.as_str()));
                    }
                } else if deep {
                    // TODO:
                    //stmt.visit_source_order()
                }
            }

            Ok((imports, ignored_imports))
        }
    }
}

fn get_all_imports_no_gil(
    filepath: &str,
    start_override_comment: &str,
    end_override_comment: &str,
    deep: bool,
) -> PyResult<(HashSet<Cow<'static, str>>, HashSet<Cow<'static, str>>)> {
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


pub struct _ImportParser {
    start_override_comment: String,
    end_override_comment: String,
    // NB: use a concurrency-safe container to allow the Python side to leverage threading
    cache: DashMap<String, (HashSet<Cow<'static, str>>, HashSet<Cow<'static, str>>)>,
}

impl _ImportParser {
    pub fn new(start_override_comment: &str, end_override_comment: &str) -> _ImportParser {
        _ImportParser{
            start_override_comment: start_override_comment.to_string(),
            end_override_comment: end_override_comment.to_string(),
            cache: DashMap::new(),
        }
    }

    pub fn get_all_imports(&mut self, filepath: String, deep: bool)
        -> PyResult<(HashSet<Cow<'static, str>>, HashSet<Cow<'static, str>>)> {
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
                        self.cache.insert(filepath, r.clone());
                        Ok(r)
                    },
                    Err(err) => Err(err),
                }
            }
        }
    }

    pub fn get_recursive_imports(&mut self,
                                      directories: Vec<String>,
                                      ignore: Vec<String>,
    ) -> PyResult<(HashMap<Cow<'static, str>, HashSet<Cow<'static, str>>>, HashSet<Cow<'static, str>>)> {
        let mut all_imports: HashMap<Cow<'static, str>, HashSet<Cow<'static, str>>> = HashMap::new();
        let mut all_ignored: HashSet<Cow<'static, str>> = HashSet::new();

        let mut collect_ = |filepath: &Cow<'static, str>, valid: &HashSet<Cow<'static, str>>, ignored: &HashSet<Cow<'static, str>>| {
            for v in valid {
                all_imports.entry(v.clone()).or_insert(HashSet::new()).insert(filepath.clone());
            }
            for i in ignored {
                all_ignored.insert(i.clone());
            }
        };

        match for_each_python_file(&directories, &ignore, |filepath| {
            let r = self.cache.get(filepath);
            let cowpath: Cow<'static, str> = Cow::from(filepath.to_string());
            match r {
                Some(r) => {
                    let (valid, ignored) = r.value();
                    collect_(&cowpath, valid, ignored)
                }
                None => {
                    let r = get_all_imports_no_gil(
                        &filepath,
                        &self.start_override_comment,
                        &self.end_override_comment,
                        false
                    );
                    if r.is_err() {
                        return Some(r.err().unwrap())
                    }
                    let (valid, ignored) = r.unwrap();
                    collect_(&cowpath, &valid, &ignored);
                    self.cache.insert(filepath.to_string(), (valid, ignored));
                }
            }
            None
        }) {
            Some(err) => return Err(err),
            None => return Ok((all_imports, all_ignored)),
        }
    }
}


#[pyclass(subclass, module="import_parser_rs")]
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
    pub fn get_all_imports<'py>(&mut self, py: Python<'py>, filepath: String, deep: bool)
        -> PyResult<(HashSet<Cow<'static, str>>, HashSet<Cow<'static, str>>)> {
        // all python->rust conversion happens prior to this function being called
        // all rust->python conversion happens after this function returning
        // exception ctors are specifically deferred to avoid creation without holding the GIL
        // therefore we can safely release the GIL here
        py.allow_threads(|| self._p.get_all_imports(filepath, deep))
    }

    #[pyo3(signature = (directories, ignore=vec![]))]
    pub fn get_recursive_imports<'py>(&mut self, py: Python<'py>,
                                      directories: Vec<String>,
                                      ignore: Vec<String>,
    ) -> PyResult<(HashMap<Cow<'static, str>, HashSet<Cow<'static, str>>>, HashSet<Cow<'static, str>>)> {
        // all python->rust conversion happens prior to this function being called
        // all rust->python conversion happens after this function returning
        // exception ctors are specifically deferred to avoid creation without holding the GIL
        // therefore we can safely release the GIL here
        py.allow_threads(|| self._p.get_recursive_imports(directories, ignore))
    }
}

#[pymodule]
fn import_parser_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ImportParser>()?;
    m.add("MismatchedOverrideComments", m.py().get_type_bound::<MismatchedOverrideComments>())?;
    Ok(())
}
