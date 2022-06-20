use std::io::BufReader;
use std::{
    fs::{self, OpenOptions},
    io as std_io,
    path::PathBuf,
    result::Result,
};

use log::{info, warn};

use super::{zstd_encode, Error};
use crate::subcommands::archive::tar_utils;

pub fn create_archive(db_path: PathBuf, dest: PathBuf) -> Result<(), Error> {
    let temp_tarball_path = dest.join("/tmp/temp_casper_db.tar");
    info!(
        "Packing contents at {} to tarball.",
        db_path.as_os_str().to_string_lossy()
    );
    tar_utils::archive(&db_path, &temp_tarball_path).map_err(Error::Tar)?;
    info!(
        "Successfully created temporary tarball at {}",
        temp_tarball_path.as_os_str().to_string_lossy()
    );

    let mut temp_tarball_file = BufReader::new(
        OpenOptions::new()
            .read(true)
            .open(&temp_tarball_path)
            .map_err(Error::Destination)?,
    );
    let output_file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&dest)
        .map_err(Error::Destination)?;

    let mut encoder = zstd_encode::zstd_encode_stream(output_file)?;
    let _ = std_io::copy(&mut temp_tarball_file, &mut encoder).map_err(Error::Streaming)?;
    encoder.finish().map_err(Error::Streaming)?;
    info!(
        "Finished encoding tarball with ZSTD, compressed archive at {}",
        dest.as_os_str().to_string_lossy()
    );
    if let Err(io_err) = fs::remove_file(&temp_tarball_path) {
        warn!(
            "Couldn't remove tarball at {} after compression: {}",
            temp_tarball_path.as_os_str().to_string_lossy(),
            io_err
        );
    }
    Ok(())
}
