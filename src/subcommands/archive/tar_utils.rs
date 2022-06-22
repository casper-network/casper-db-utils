use std::{
    collections::VecDeque,
    fs::{self, OpenOptions},
    io::{Error as IoError, Read},
    path::{Path, PathBuf},
};

use log::info;
use tar::{Archive, Builder};

use super::ring_buffer::BlockingProducer;

pub struct ArchiveStream {
    file_paths: VecDeque<PathBuf>,
    builder: Builder<BlockingProducer>,
}

impl ArchiveStream {
    pub fn new<P: AsRef<Path>>(dir: P, producer: BlockingProducer) -> Result<Self, IoError> {
        let mut file_paths = VecDeque::new();
        for entry in fs::read_dir(dir)?.flatten() {
            file_paths.push_back(entry.path());
        }

        Ok(Self {
            file_paths,
            builder: Builder::new(producer),
        })
    }

    pub fn pack(&mut self) -> Result<(), IoError> {
        while let Some(path) = self.file_paths.pop_front() {
            let mut file = OpenOptions::new()
                .read(true)
                .open(&path)
                .expect("can't open file");
            info!("Adding {} to the archive.", path.to_string_lossy());
            self.builder
                .append_file(path.file_name().expect("invalid path"), &mut file)?;
        }
        self.builder.finish()
    }
}

#[allow(unused)]
pub fn archive<P1: AsRef<Path>, P2: AsRef<Path>>(dir: P1, tarball_path: P2) -> Result<(), IoError> {
    let temp_tarball_file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(tarball_path)?;
    let mut tarball_stream = Builder::new(temp_tarball_file);
    for entry in fs::read_dir(dir)?.flatten() {
        info!("Adding {} to the archive.", entry.path().to_string_lossy());
        let mut file = OpenOptions::new().read(true).open(entry.path())?;
        tarball_stream.append_file(entry.file_name(), &mut file)?;
    }
    tarball_stream.finish()
}

pub fn unarchive_stream<R: Read + Sized>(stream: R) -> Archive<R> {
    Archive::new(stream)
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, OpenOptions},
        io::{Read, Write},
    };

    use tempfile::{self, NamedTempFile};

    #[test]
    fn tar_roundtrip() {
        let src_dir = tempfile::tempdir_in(".").unwrap();
        let num_files = 10usize;
        let mut test_files = vec![];

        for idx in 0..num_files {
            let mut file = NamedTempFile::new_in(src_dir.path()).unwrap();
            file.write_all(format!("test file {}", idx).as_bytes())
                .unwrap();
            test_files.push(file);
        }

        let dst_dir = tempfile::tempdir_in(".").unwrap();
        let archive_path = dst_dir.path().to_path_buf().join("archive.tar");

        super::archive(&src_dir, &archive_path).unwrap();

        {
            let archive_file = OpenOptions::new().read(true).open(&archive_path).unwrap();
            let mut archive = super::unarchive_stream(archive_file);
            archive.unpack(&dst_dir).unwrap();
        }

        fs::remove_file(&archive_path).unwrap();

        for (idx, file) in test_files.iter().enumerate().take(num_files) {
            let mut contents = String::new();
            let path = dst_dir.path().join(file.path().file_name().unwrap());
            OpenOptions::new()
                .read(true)
                .open(path)
                .unwrap()
                .read_to_string(&mut contents)
                .unwrap();
            assert_eq!(contents, format!("test file {}", idx));
        }
    }
}
