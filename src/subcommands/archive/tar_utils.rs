use std::{
    fs::{self, OpenOptions},
    io::{BufReader, Error as IoError},
    path::PathBuf,
};

use log::info;
use tar::{Archive, Builder};

pub fn archive(dir: &PathBuf, tarball_path: &PathBuf) -> Result<(), IoError> {
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

pub fn unarchive(src: PathBuf, dest: PathBuf) -> Result<(), IoError> {
    let input = OpenOptions::new().read(true).open(src)?;
    let mut archive = Archive::new(BufReader::new(input));
    archive.unpack(dest)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        fs::{self, OpenOptions},
        io::{Read, Write},
    };

    use pathdiff;
    use tempfile::{self, NamedTempFile};

    #[test]
    fn tar_roundtrip() {
        let src_dir = tempfile::tempdir_in(".").unwrap();
        let num_files = 10usize;
        let mut test_files = vec![];
        let cur_dir = env::current_dir().unwrap();

        for idx in 0..num_files {
            let mut file = NamedTempFile::new_in(src_dir.path()).unwrap();
            file.write_all(format!("test file {}", idx).as_bytes())
                .unwrap();
            test_files.push(file);
        }

        let src_dir_relative_path = pathdiff::diff_paths(src_dir.path(), &cur_dir).unwrap();
        let dst_dir = tempfile::tempdir_in(".").unwrap();
        let archive_path = dst_dir.path().to_path_buf().join("archive.tar");

        super::archive(&src_dir_relative_path, &archive_path).unwrap();
        super::unarchive(archive_path.clone(), dst_dir.path().to_path_buf()).unwrap();

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
