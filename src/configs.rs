use std::sync::mpsc;
use std::{fs, thread};
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;
use std::str::Chars;
use yaml_rust2::parser::{Parser, EventReceiver};
use ignore::{DirEntry, WalkBuilder, WalkState};
use yaml_rust2::Event;

type BoxedError = Box<dyn std::error::Error + Send + Sync>;


#[inline]
fn eval_filter<F, V>(f: &Option<F>, v: V) -> bool
where
    F: Fn(V) -> bool,
{
    match f {
        Some(f) => f(v),
        None => true,
    }
}

struct Collector<'a, KF, VF> {
    in_map: bool,
    at_key: bool,
    matched_key: bool,
    stack: Vec<(bool, bool)>,
    key_filter: &'a Option<KF>,
    val_filter: &'a Option<VF>,
    collected: HashSet<String>,
}

impl<'a, KF, VF> EventReceiver for Collector<'a, KF, VF>
where
    KF: Fn(&str) -> bool,
    VF: Fn(&str) -> bool,
{
    fn on_event(&mut self, ev: Event) {
        match ev {
            Event::Scalar(v, _, _, _) => {
                let collect = if self.in_map {
                    if self.at_key {
                        self.matched_key = eval_filter(self.key_filter, &v);
                        self.key_filter.is_none()
                    } else {
                        self.matched_key
                    }
                } else {
                    self.key_filter.is_none()
                };
                if collect && eval_filter(self.val_filter, &v) {
                    self.collected.insert(v);
                }
                if self.in_map {
                    self.at_key = !self.at_key;
                }
            }
            Event::SequenceStart(_, _) => {
                self.stack.push((self.in_map, self.at_key));
                self.in_map = false;
            }
            Event::SequenceEnd => {
                let (m, k) = self.stack.pop().unwrap();
                self.in_map = m;
                self.at_key = k;
                if self.in_map {
                    self.at_key = !self.at_key;
                }
            }
            Event::MappingStart(_, _) => {
                self.stack.push((self.in_map, self.at_key));
                self.in_map = true;
                self.at_key = true;
            }
            Event::MappingEnd => {
                let (m, k) = self.stack.pop().unwrap();
                self.in_map = m;
                self.at_key = k;
                if self.in_map {
                    self.at_key = !self.at_key;
                }
            }
            _ => {}
        }
    }
}

fn collect_strings_from_yaml<VF, KF>(parser: &mut Parser<Chars>,
                                     key_filter: &Option<KF>,
                                     val_filter: &Option<VF>)
    -> Result<HashSet<String>, BoxedError>
where
    KF: Fn(&str) -> bool,
    VF: Fn(&str) -> bool,
{
    let mut collector = Collector{
        in_map: false,
        at_key: false,
        matched_key: false,
        stack: Vec::new(),
        key_filter,
        val_filter,
        collected: HashSet::default(),
    };

    parser.load(&mut collector, false).map_err(|e| Box::new(e))?;
    Ok(collector.collected)
}

pub fn collect_strings_from_configs<FF, KF, VF>(
    paths: Vec<String>, file_filter: FF, key_filter: Option<KF>, value_filter: Option<VF>)
    -> Result<Option<HashMap<String, HashSet<String>>>, BoxedError>
where
    FF: Fn(&str) -> bool + Send + Sync,
    KF: Fn(&str) -> bool + Send + Sync,
    VF: Fn(&str) -> bool + Send + Sync,
{
    walk_yaml_parallel(
        paths,
        |entry: &DirEntry| {
            if !entry.file_type().unwrap().is_file() {
                return false;
            }
            let name = entry.file_name().to_str().unwrap();
            return name.ends_with(".yaml") && file_filter(name)
        },
        |path, mut parser| {
            let deps = collect_strings_from_yaml(&mut parser, &key_filter, &value_filter)?;
            Ok(Some(HashMap::from([(path.to_string(), deps)])))
        }, |a, b| {
            for (k, v) in b {
                match a.entry(k) {
                    Entry::Vacant(e) => {
                        e.insert(v);
                    },
                    Entry::Occupied(mut e) => {
                        e.get_mut().extend(v)
                    },
                }
            }
        })
}

pub fn walk_yaml_parallel<FF, F, M, O>(paths: Vec<String>, file_filter: FF, f: F, merge: M)
    -> Result<Option<O>, BoxedError>
where
    FF: Fn(&DirEntry) -> bool + Send + Sync,
    F: Fn(&str, Parser<Chars>) -> Result<Option<O>, BoxedError> + Send + Sync,
    M: Fn(&mut O, O) + Send + Sync + 'static,
    O: Send + 'static
{
    walk_parallel(paths, |entry| {
        if !file_filter(entry) {
            return Ok(None);
        }
        f(entry.path().to_str().unwrap(),
            Parser::new_from_str(
                fs::read_to_string(entry.path())
                    .map_err(|e| Box::new(e))?
                    .as_str()
            )
        )
    }, merge)
}

pub fn walk_parallel<F, M, O>(paths: Vec<String>, f: F, merge: M)
    -> Result<Option<O>, BoxedError>
where
    F: Fn(&DirEntry) -> Result<Option<O>, BoxedError> + Send + Sync,
    M: Fn(&mut O, O) + Send + Sync + 'static,
    O: Send + 'static
{
    let parallelism = thread::available_parallelism().unwrap().get();

    let mut package_it = paths.iter();
    let builder = &mut WalkBuilder::new(&package_it.next().unwrap());
    for a in package_it {
        builder.add(a);
    }

    let (tx, rx) =
        mpsc::channel::<Result<Option<O>, BoxedError>>();

    let t = thread::spawn(move || {
        let mut out = None;
        let mut res = Ok(None);
        for r in rx.iter() {
            if res.is_err() {
                continue
            }
            match r {
                Ok(o) => {
                    match o {
                        Some(v) => {
                            if out.is_some() {
                                merge(out.as_mut().unwrap(), v)
                            } else {
                                out = Some(v);
                            }
                        },
                        None => {},
                    }
                },
                // NB: we only return the first one even if there are several...
                // TODO: consider merging all errors into one?
                Err(e) => {
                    // NB: we can't just exit this thread immediately as it would
                    // drop the rx side of the channel and cause panics on the
                    // sender side...
                    res = Err(e);
                },
            }
        }
        if res.is_err() {
            res
        } else {
            Ok(out)
        }
    });

    // NB: we have to disable handling of .gitignore because
    // some real smart folks have ignore patterns that match
    // files that are committed in the repo...
    builder
        .standard_filters(false)
        .hidden(true)
        .threads(parallelism)
        .build_parallel().run(|| {
        let tx = tx.clone();
        let f = &f;
        Box::new(move |r| {
            match r {
                Err(err) => {
                    tx.send(Err(Box::new(err))).unwrap();
                    WalkState::Quit
                }
                Ok(e) => {
                    match f(&e) {
                        Err(err) => {
                            eprintln!("error in {} {}", e.path().display(), err.as_ref());
                            tx.send(Err(err)).unwrap();
                            WalkState::Quit
                        },
                        Ok(res) => {
                            tx.send(Ok(res)).unwrap();
                            WalkState::Continue
                        },
                    }
                }
            }
        })
    });

    drop(tx);

    t.join().unwrap()
}
