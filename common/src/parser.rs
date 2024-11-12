use std::fmt::Display;
use std::fs::read_to_string;
use std::io;
use ruff_python_ast::Stmt;
use ruff_python_ast::visitor::source_order::{walk_stmt, SourceOrderVisitor};
use ruff_text_size::{Ranged};
use ruff_python_parser::{parse_module, ParseError};


#[derive(Debug)]
pub enum Error {
    IO(io::Error),
    Parse(ParseError),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::IO(io) => io.fmt(f),
            Error::Parse(parse) => parse.fmt(f),
        }
    }
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

pub fn raw_imports_from_module<'a>(source: &'a str, module: &'a str, deep: bool)
    -> Result<Vec<String>, ParseError> {
    let m = parse_module(source)?;
    let mut extractor = ImportExtractor::new(source, module, deep);
    extractor.visit_body(&m.syntax().body);
    Ok(extractor.imports)
}

pub fn raw_get_all_imports(filepath: &str, module: &str, deep: bool) -> Result<Vec<String>, Error> {
    let source = read_to_string(filepath).or_else(|e| Err(Error::IO(e)))?;
    raw_imports_from_module(
        &source, module, deep,
    ).or_else(|e| Err(Error::Parse(e)))
}