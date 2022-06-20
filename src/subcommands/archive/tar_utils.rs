use std::{
    fs::{OpenOptions, self},
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
