use std::{fs::OpenOptions, path::Path, result::Result};

use log::info;

use super::Error;
use crate::subcommands::archive::{tar_utils, zstd_utils};

pub fn file_stream_and_unpack_archive<P1: AsRef<Path>, P2: AsRef<Path>>(
    path: P1,
    dest: P2,
) -> Result<(), Error> {
    let input_file = OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(Error::Source)?;

    let decoder = zstd_utils::zstd_decode_stream(input_file)?;
    let mut unpacker = tar_utils::unarchive_stream(decoder);
    unpacker.unpack(dest).map_err(Error::Streaming)?;
    info!("Decompression complete.");
    Ok(())
}
