use std::{fs::OpenOptions, io as std_io, path::Path, result::Result};

use log::info;

use super::Error;
use crate::subcommands::archive::zstd_utils;

pub fn stream_file_archive<P1: AsRef<Path>, P2: AsRef<Path>>(
    path: P1,
    dest: P2,
) -> Result<(), Error> {
    let input_file = OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(Error::Source)?;

    let mut output_file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(dest)
        .map_err(Error::Destination)?;

    let mut decoder = zstd_utils::zstd_decode_stream(input_file)?;
    let decoded_bytes = std_io::copy(&mut decoder, &mut output_file).map_err(Error::Streaming)?;
    info!("Decompression complete.");
    info!("Decoded {} bytes.", decoded_bytes);
    Ok(())
}
