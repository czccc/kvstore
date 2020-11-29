use crate::*;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::{
    collections::{BTreeMap, HashMap},
    ffi::OsStr,
    fs,
    fs::File,
    fs::OpenOptions,
    io,
    io::BufReader,
    io::BufWriter,
    io::SeekFrom,
    io::{Read, Seek, Write},
    ops::Range,
    path::Path,
    path::PathBuf,
    sync::Arc,
    sync::Mutex,
};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// KvStore is a struct that store Key Value pairs
#[derive(Debug, Clone)]
pub struct KvStore {
    current_gen: Arc<Mutex<u64>>,
    path: Arc<Mutex<PathBuf>>,
    readers: Arc<Mutex<HashMap<u64, BufReaderWithPos<File>>>>,
    writer: Arc<Mutex<BufWriterWithPos<File>>>,
    index: Arc<Mutex<BTreeMap<String, CommandPos>>>,
    uncompacted: Arc<Mutex<u64>>,
}

// impl Clone for KvStore {
//     fn clone(&self) -> Self {
//         todo!()
//     }
// }

impl KvStore {
    /// Open the KvStore at a given path. Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut readers = HashMap::new();
        let mut index = BTreeMap::new();

        // let mut readers = Arc::clone(&readers_arc);
        let gen_list = read_all_logs(&path)?;
        let mut uncompacted = 0;
        for &gen in &gen_list {
            let mut reader = BufReaderWithPos::new(File::open(log_path(&path, gen))?)?;
            uncompacted += load_log(gen, &mut reader, &mut index)?;
            readers.insert(gen, reader);
        }

        let current_gen = gen_list.last().unwrap_or(&0) + 1;
        let writer = BufWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(log_path(&path, current_gen))?,
        )?;
        readers.insert(
            current_gen,
            BufReaderWithPos::new(File::open(log_path(&path, current_gen))?)?,
        );

        Ok(KvStore {
            path: Arc::new(Mutex::new(path)),
            readers: Arc::new(Mutex::new(readers)),
            writer: Arc::new(Mutex::new(writer)),
            current_gen: Arc::new(Mutex::new(current_gen)),
            index: Arc::new(Mutex::new(index)),
            uncompacted: Arc::new(Mutex::new(uncompacted)),
        })
    }

    /// Clears stale entries in the log.
    pub fn compact(&self) -> Result<()> {
        let mut index = self.index.lock().unwrap();
        let mut uncompacted = self.uncompacted.lock().unwrap();
        let mut readers = self.readers.lock().unwrap();
        let mut writer = self.writer.lock().unwrap();
        let mut current_gen = self.current_gen.lock().unwrap();
        let path = self.path.lock().unwrap();

        // increase current gen by 2. current_gen + 1 is for the compaction file.
        let compaction_gen = *current_gen + 1;
        *current_gen += 2;
        *writer = BufWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(log_path(&path, *current_gen))?,
        )?;

        let mut compaction_writer = BufWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(log_path(&path, compaction_gen))?,
        )?;

        let mut new_pos = 0; // pos in the new log file.
        for cmd_pos in &mut index.values_mut() {
            let reader = readers
                .get_mut(&cmd_pos.gen)
                .expect("Cannot find log reader");
            if reader.pos != cmd_pos.pos {
                reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            }

            let mut entry_reader = reader.take(cmd_pos.len);
            let len = io::copy(&mut entry_reader, &mut compaction_writer)?;
            *cmd_pos = (compaction_gen, new_pos..new_pos + len).into();
            new_pos += len;
        }
        compaction_writer.flush()?;

        // remove stale log files.
        let stale_gens: Vec<_> = readers
            .keys()
            .filter(|&&gen| gen < compaction_gen)
            .cloned()
            .collect();
        for stale_gen in stale_gens {
            readers.remove(&stale_gen);
            fs::remove_file(log_path(&path, stale_gen))?;
        }
        *uncompacted = 0;

        Ok(())
    }
}

impl KvsEngine for KvStore {
    fn get(&self, key: String) -> Result<Option<String>> {
        let index = self.index.lock().unwrap();
        if let Some(cmd_pos) = index.get(&key) {
            let mut reader = self.readers.lock().unwrap();
            let reader = reader
                .get_mut(&cmd_pos.gen)
                .expect("Cannot find log reader");
            reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            let cmd_reader = reader.take(cmd_pos.len);
            if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                Ok(Some(value))
            } else {
                Err(KvError::Unknown)
            }
        } else {
            Ok(None)
        }
    }
    fn set(&self, key: String, value: String) -> Result<()> {
        {
            let mut index = self.index.lock().unwrap();
            let mut writer = self.writer.lock().unwrap();
            // let mut uncompacted = self.uncompacted.lock().unwrap();

            let cmd = Command::Set { key, value };
            let pos = writer.pos;
            serde_json::to_writer(&mut writer.by_ref(), &cmd)?;
            writer.flush()?;
            if let Command::Set { key, .. } = cmd {
                if let Some(old_cmd) = index.insert(
                    key,
                    (*self.current_gen.lock().unwrap(), pos..writer.pos).into(),
                ) {
                    *self.uncompacted.lock().unwrap() += old_cmd.len;
                }
            }
        }

        if *self.uncompacted.lock().unwrap() > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }
    fn remove(&self, key: String) -> Result<()> {
        let mut index = self.index.lock().unwrap();
        if index.contains_key(&key) {
            let cmd = Command::Remove { key };
            let mut writer = self.writer.lock().unwrap();
            serde_json::to_writer(&mut writer.by_ref(), &cmd)?;
            writer.flush()?;
            if let Command::Remove { key } = cmd {
                let old_cmd = index.remove(&key).expect("key not found");
                *self.uncompacted.lock().unwrap() += old_cmd.len;
            }
            Ok(())
        } else {
            Err(KvError::KeyNotFound)
        }
    }
}

fn read_all_logs(path: &Path) -> Result<Vec<u64>> {
    let paths = fs::read_dir(path)?;
    let mut gen_list = Vec::new();
    // println!("{}", paths);
    for p in paths {
        let p = p?.path();
        if p.is_file() && p.extension() == Some(OsStr::new("log")) {
            let p = p.file_name().unwrap().to_str().unwrap();
            let gen = p.trim_end_matches(".log").parse::<u64>().unwrap();
            gen_list.push(gen);
        }
    }
    gen_list.sort_unstable();
    // println!("{:?}", gen_list);
    Ok(gen_list)
}

fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

fn load_log(
    gen: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &mut BTreeMap<String, CommandPos>,
) -> Result<u64> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    let mut uncompacted = 0; // number of bytes that can be saved after a compaction.
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                if let Some(old_cmd) = index.insert(key, (gen, pos..new_pos).into()) {
                    uncompacted += old_cmd.len;
                }
            }
            Command::Remove { key } => {
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted += old_cmd.len;
                }
                // the "remove" command itself can be deleted in the next compaction.
                // so we add its length to `uncompacted`.
                uncompacted += new_pos - pos;
            }
        }
        pos = new_pos;
    }
    Ok(uncompacted)
}

#[derive(Debug, Deserialize, Serialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

#[derive(Debug)]
struct CommandPos {
    gen: u64,
    pos: u64,
    len: u64,
}

impl From<(u64, Range<u64>)> for CommandPos {
    fn from((gen, range): (u64, Range<u64>)) -> Self {
        CommandPos {
            gen,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}

#[derive(Debug)]
struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

#[derive(Debug)]
struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
