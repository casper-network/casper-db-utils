use std::{fs::OpenOptions, io as std_io, path::PathBuf, result::Result};

use log::info;

use super::zstd_decode;
use super::Error;

pub fn stream_file_archive(path: PathBuf, dest: PathBuf) -> Result<(), Error> {
    let input_file = OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(Error::Source)?;

    let mut output_file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(dest)
        .map_err(Error::Destination)?;

    let mut decoder = zstd_decode::zstd_decode_stream(input_file)?;
    let decoded_bytes = std_io::copy(&mut decoder, &mut output_file).map_err(Error::Streaming)?;
    info!("Decompression complete.");
    info!("Decoded {} bytes.", decoded_bytes);
    Ok(())
}
