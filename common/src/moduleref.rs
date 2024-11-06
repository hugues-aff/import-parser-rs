use std::sync::RwLock;
use dashmap::DashMap;
use ustr::{ustr, Ustr};
use speedy::{Context, Readable, Reader, Writable, Writer};
use speedy::private::{read_length_u64_varint, write_length_u64_varint};

pub type ModuleRef = u32;

#[derive(Debug, Clone, Copy)]
pub struct ModuleRefVal {
    pub(crate) fs: Ustr,
    pub(crate) py: Ustr,
    pub(crate) pkg: Option<Ustr>,
}

impl ModuleRefVal {
    fn new(fs: Ustr, py: Ustr, pkg: Option<Ustr>) -> ModuleRefVal {
        ModuleRefVal{ fs, py, pkg }
    }
}

pub struct ModuleRefCache {
    values: RwLock<Vec<ModuleRefVal>>,
    fs_to_ref: DashMap<Ustr, ModuleRef>,
    py_to_ref_global: DashMap<Ustr, ModuleRef>,
    py_to_ref_local: DashMap<Ustr, DashMap<Ustr, ModuleRef>>,
}


impl ModuleRefCache {
    pub fn new() -> ModuleRefCache {
        ModuleRefCache{
            values: RwLock::new(Vec::new()),
            fs_to_ref: DashMap::new(),
            py_to_ref_global: DashMap::new(),
            py_to_ref_local: DashMap::new(),
        }
    }
    fn from_values(values: Vec<ModuleRefVal>) -> ModuleRefCache {
        let fs_to_ref = DashMap::new();
        let py_to_ref_global = DashMap::new();
        let py_to_ref_local: DashMap<Ustr, DashMap<Ustr, ModuleRef >> = DashMap::new();
        for (i, v) in values.iter().enumerate() {
            fs_to_ref.insert(v.fs, i as ModuleRef);
            match v.pkg {
                None => py_to_ref_global.insert(v.py, i as ModuleRef),
                Some(pkg) => py_to_ref_local.entry(pkg)
                    .or_default().value().insert(v.py, i as ModuleRef),
            };
        }
        ModuleRefCache{
            values: RwLock::new(values),
            fs_to_ref,
            py_to_ref_global,
            py_to_ref_local,
        }
    }

    pub fn get(&self, r: ModuleRef) -> ModuleRefVal {
        self.values.read().unwrap()[r as usize]
    }

    pub fn py_for_ref(&self, r: ModuleRef) -> Ustr {
        self.values.read().unwrap()[r as usize].py
    }
    pub fn fs_for_ref(&self, r: ModuleRef) -> Ustr {
        self.values.read().unwrap()[r as usize].fs
    }
    pub fn pkg_for_ref(&self, r: ModuleRef) -> Option<Ustr> {
        self.values.read().unwrap()[r as usize].pkg
    }

    pub fn ref_for_py(&self, py: Ustr, pkg: Option<Ustr>) -> Option<ModuleRef> {
        match pkg {
            Some(pkg) => {
                match self.py_to_ref_local.get(&pkg) {
                    Some(m) => {
                        match m.value().get(&py) {
                            Some(m) => Some(*m.value()),
                            None => None,
                        }
                    },
                    None => None,
                }
            },
            None => {
                match self.py_to_ref_global.get(&py) {
                    Some(m) => Some(*m.value()),
                    None => None,
                }
            },
        }
    }

    pub fn ref_for_fs(&self, fs: Ustr) -> Option<ModuleRef> {
        match self.fs_to_ref.get(&fs) {
            Some(module_ref) => Some(*module_ref.value()),
            None => None,
        }
    }

    pub fn get_or_create(&self, fs: Ustr, py: Ustr, pkg: Option<Ustr>) -> ModuleRef {
        let mut g = self.values.write().unwrap();
        if let Some(r) = self.fs_to_ref.get(&fs) {
            *r.value()
        } else {
            let rv = ModuleRefVal::new(fs.clone(), py.clone(), pkg.clone());
            let r = g.len() as ModuleRef;
            g.push(rv);
            self.fs_to_ref.insert(fs, r);
            match pkg {
                Some(pkg) => self.py_to_ref_local.entry(pkg).or_default().insert(py, r),
                None => self.py_to_ref_global.insert(py, r),
            };
            r
        }
    }
}

impl<C> Writable<C> for ModuleRefVal
where
    C: Context
{
    fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
        write_ustr_to(self.fs, writer)
            .and_then(|_| write_ustr_to(self.py, writer))
            .and_then(|_| write_ustr_to(self.pkg.unwrap_or_default(), writer))
    }
}

pub(crate) fn write_ustr_to<C: Context, T: ?Sized + Writer<C>>(s: Ustr, writer: &mut T) -> Result<(), C::Error> {
    write_length_u64_varint(s.len(), writer).and_then(|_| writer.write_bytes(s.as_bytes()))
}

pub(crate) fn read_ustr_with_buf<'a, C: Context, R: Reader<'a, C>>(reader: &mut R, buf: &mut Vec<u8>)
    -> Result<Ustr, C::Error>
{
    buf.resize(read_length_u64_varint(reader).or_else(|e| return Err(e))?, 0);
    reader.read_bytes(buf.as_mut_slice())
        .or_else(|e| return Err(e))?;
    Ok(ustr(std::str::from_utf8(buf.as_slice())
        .map_err(|e| speedy::Error::custom(format!("{:?} {:?}", e, buf)))
        .or_else(|e| return Err(e))?))
}

impl<'a, C> Readable<'a, C> for ModuleRefVal
where
    C: Context
{
    fn read_from<R: Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
        let mut buf: Vec<u8> = Vec::new();
        let fs = read_ustr_with_buf(reader, &mut buf).or_else(|e| return Err(e))?;
        let py = read_ustr_with_buf(reader, &mut buf).or_else(|e| return Err(e))?;
        let pkg = read_ustr_with_buf(reader, &mut buf).or_else(|e| return Err(e))?;
        let pkg = match pkg.len() {
            0 => None,
            _ => Some(pkg),
        };
        Ok(ModuleRefVal{
            fs,
            py,
            pkg,
        })
    }

    fn minimum_bytes_needed() -> usize {
        3
    }
}

impl<C> Writable<C> for ModuleRefCache
where
    C: Context
{
    fn write_to<T: ?Sized + Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
        let g = self.values.read().unwrap();
        write_length_u64_varint(g.len(), writer).or_else(|e| return Err(e))?;
        for v in g.iter() {
            writer.write_value(v).or_else(|e| return Err(e))?;
        }
        Ok(())
    }
}

impl<'a, C> Readable<'a, C> for ModuleRefCache
where
    C: Context
{
    fn read_from<R: Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
        let sz = read_length_u64_varint(reader).or_else(|e| Err(e))?;
        let mut values = Vec::with_capacity(sz);
        for _ in 0..sz {
            values.push(reader.read_value::<ModuleRefVal>().or_else(|e| return Err(e))?);
        }
        Ok(ModuleRefCache::from_values(values))
    }
}
