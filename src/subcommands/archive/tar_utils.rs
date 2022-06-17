use std::{
    fs::OpenOptions,
    io::{BufReader, Error as IoError},
    path::PathBuf,
};

use tar::Archive;

pub fn unarchive(src: PathBuf, dest: PathBuf) -> Result<(), IoError> {
    let input = OpenOptions::new().read(true).open(src)?;
    let mut archive = Archive::new(BufReader::new(input));
    archive.unpack(dest)?;
    Ok(())
}
