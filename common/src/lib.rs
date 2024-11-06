pub mod cache;
pub mod matcher;
pub mod moduleref;
pub mod graph;

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::fs::read_to_string;
use std::io;
use dashmap::DashMap;
use pyo3::create_exception;
use pyo3::prelude::*;
use pyo3::exceptions::{PyException, PyOSError, PyValueError};
use pyo3::types::{PyFrozenSet, PyString, PyTuple};
use ruff_python_ast::{Stmt};
use ruff_python_ast::visitor::source_order::{walk_stmt, SourceOrderVisitor};
use ruff_text_size::{Ranged, TextRange};
use ruff_python_parser::{parse_module, Token, TokenKind};
use walkdir::WalkDir;

#[derive(Debug)]
pub struct Err {
    pub msg: String,
    err: Option<io::Error>,
    py_factory: Option<fn (String) -> PyErr>,
}

impl Err {
    fn new(msg: String, py_factory: fn(String) -> PyErr) -> Err {
        Err {
            msg,
            err: None,
            py_factory: Some(py_factory)
        }
    }

    pub fn from_io(err: io::Error) -> Err {
        Err {
            msg: err.to_string(),
            err: Some(err),
            py_factory: None,
        }
    }

    pub fn to_pyerr(self) -> PyErr {
        match self.py_factory {
            Some(f) => f(self.msg),
            None => match self.err {
                Some(err) => PyOSError::new_err(err),
                None => { panic!("invalid Err") }
            }
        }
    }
}

impl Display for Err {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}


create_exception!(import_parser_rs, MismatchedOverrideComments, PyValueError);


fn mismatched_comments(t: &str, line: usize) -> Err {
    Err::new(
        format!("unmatched override {} at line {}", t, line),
        PyErr::new::<MismatchedOverrideComments, _>,
    )
}

pub fn split_at_depth(filepath: &'_ str, sep: char, depth: usize) -> (&'_ str, &'_ str) {
    let mut idx : usize = filepath.len();
    let mut depth: usize = depth;
    while depth != 0 {
        match filepath[..idx].rfind(sep) {
            Some(next_idx) => {
                idx = next_idx;
                depth -= 1;
            },
            None => {
                panic!("{} @ {} {}", filepath, sep, depth);
            }
        }
    }
    (&filepath[0..idx], &filepath[idx+1..])
}

struct ImportExtractor<'a> {
    source : &'a str,
    module : &'a str,
    deep: bool,

    imports : Vec<String>
}

impl<'a> ImportExtractor<'a> {
    fn new(source : &'a str, module : &'a str, deep : bool) -> ImportExtractor<'a> {
        ImportExtractor{
            source,
            module,
            deep,
            imports: Vec::new(),
        }
    }
}

impl<'a, 'b> SourceOrderVisitor<'b> for ImportExtractor<'a> {
    fn visit_stmt(&mut self, stmt: &'b Stmt) {
        if let Some(imp) = stmt.as_import_stmt() {
            for n in &imp.names {
                self.imports.push(n.name.to_string());
            }
        } else if let Some(imp) = stmt.as_import_from_stmt() {
            let mut target = String::new();
            if imp.level > 0 {
                let (parent, _) = split_at_depth(self.module, '.', imp.level as usize);
                target.push_str(parent);
                target.push('.');
            }
            if imp.module.is_some() {
                target.push_str(imp.module.as_ref().unwrap().as_str());
                target.push('.');
            }
            for n in &imp.names {
                self.imports.push(target.clone() + n.name.as_str());
            }
        } else if self.deep {
            if let Some(if_stmt) = stmt.as_if_stmt() {
                // quick and dirty: skip if TYPE_CHECKING / if typing.TYPE_CHECKING
                // TODO: for added robustness:
                //  - keep track of imports from typing package
                //  - extract identifer from if condition and compare to imported symbol
                let range = if_stmt.test.range();
                let cond = &self.source[range.start().to_usize()..range.end().to_usize()];
                if cond == "TYPE_CHECKING" || cond == "typing.TYPE_CHECKING" {
                    // skip walking under
                    return;
                }
            }
            walk_stmt(self, stmt);
        }
    }

    fn visit_body(&mut self, body: &'b [Stmt]) {
        for stmt in body {
            self.visit_stmt(stmt);
        }
    }
}

fn raw_imports_from_module<'a>(source: &'a str, module: &'a str, deep: bool) -> Result<Vec<String>, Err> {
    match parse_module(source) {
        Err(error) => Err(Err::new(error.to_string(), PyException::new_err)),
        Ok(m) => {
            let mut extractor = ImportExtractor::new(source, module, deep);
            extractor.visit_body(&m.syntax().body);
            Ok(extractor.imports)
        }
    }
}

pub fn raw_get_all_imports(filepath: &str, module: &str, deep: bool) -> Result<Vec<String>, Err> {
    match read_to_string(filepath) {
        Err(err) => Err(Err::from_io(err)),
        Ok(source) => raw_imports_from_module(
            &source, module, deep,
        )
    }
}

fn next_override_block(it: &mut core::slice::Iter<'_, Token>,
                       line: &mut usize,
                       source: &str,
                       start_override_comment: &str,
                       end_override_comment: &str
) -> Result<TextRange, Err> {
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
) -> Result<FileImports, Err> {
    match parse_module(source) {
        Err(error) => Err(Err::new(error.to_string(), PyException::new_err)),
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

pub fn get_all_imports_no_gil(
    filepath: &str,
    start_override_comment: &str,
    end_override_comment: &str,
    deep: bool,
) -> Result<FileImports, Err> {
    match read_to_string(filepath) {
        Err(err) => Err(Err::from_io(err)),
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

pub fn for_each_python_file<F>(directories: &Vec<String>,
                           ignore: &Vec<String>,
                           mut inner_fn: F
) -> Option<Err>
where
    F: FnMut(&str) -> Option<Err>
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
                return Some(Err::from_io(io::Error::from(entry.err().unwrap())))
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
    pub valid: cache::CachedPy<HashSet<cache::CachedPy<String>>>,
    pub ignored: cache::CachedPy<HashSet<cache::CachedPy<String>>>,
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

pub type StringSet = HashSet<cache::CachedPy<String>>;

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

    pub fn get_all_imports(&self, filepath: String, deep: bool)
                           -> Result<FileImports, Err> {
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
    ) -> Result<(HashMap<cache::CachedPy<String>, StringSet>, StringSet), Err> {
        let mut all_imports: HashMap<cache::CachedPy<String>, StringSet> = HashMap::new();
        let mut all_ignored: StringSet = HashSet::new();

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
            let cowpath = cache::CachedPy::new(filepath.to_string(), freeze_string);
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
