use std::{
    fs::{self, OpenOptions},
    io::{Error as IoError, Read},
    path::Path,
};

use log::info;
use tar::{Archive, Builder};

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
