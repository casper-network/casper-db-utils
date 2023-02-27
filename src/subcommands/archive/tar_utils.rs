use std::{
    collections::VecDeque,
    fs::{self, OpenOptions},
    io::{Error as IoError, Read, Write},
    path::{Path, PathBuf},
};

use log::info;
use tar::{Archive, Builder};

pub struct ArchiveStream<W: Write> {
    file_paths: VecDeque<PathBuf>,
    builder: Builder<W>,
}

impl<W: Write> ArchiveStream<W> {
    pub fn new<P: AsRef<Path>>(dir: P, writer: W) -> Result<Self, IoError> {
        let mut file_paths = VecDeque::new();
        for entry in fs::read_dir(dir)?.flatten() {
            file_paths.push_back(entry.path());
        }

        Ok(Self {
            file_paths,
            builder: Builder::new(writer),
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

pub fn unarchive_stream<R: Read + Sized>(stream: R) -> Archive<R> {
    Archive::new(stream)
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, File},
        io::Write,
    };

    use tempfile::{self, NamedTempFile};

    use super::ArchiveStream;

    #[test]
    fn tar_roundtrip() {
        let src_dir = tempfile::tempdir_in(".").unwrap();
        let num_files = 10usize;
        let mut test_files = vec![];

        for idx in 0..num_files {
            let mut file = NamedTempFile::new_in(src_dir.path()).unwrap();
            file.write_all(format!("test file {idx}").as_bytes())
                .unwrap();
            test_files.push(file);
        }

        let dst_dir = tempfile::tempdir_in(".").unwrap();
        let archive_path = dst_dir.path().to_path_buf().join("archive.tar");
        let archive_file = File::create(&archive_path).unwrap();
        let mut archive_stream =
            ArchiveStream::new(&src_dir, archive_file).expect("couldn't create archive stream");
        assert!(archive_stream.pack().is_ok());

        {
            let archive_file = File::open(&archive_path).unwrap();
            let mut archive = super::unarchive_stream(archive_file);
            archive.unpack(&dst_dir).unwrap();
        }

        fs::remove_file(&archive_path).unwrap();

        for (idx, file) in test_files.iter().enumerate().take(num_files) {
            let path = dst_dir.path().join(file.path().file_name().unwrap());
            let contents = fs::read_to_string(&path).unwrap();
            assert_eq!(contents, format!("test file {idx}"));
        }
    }
}
