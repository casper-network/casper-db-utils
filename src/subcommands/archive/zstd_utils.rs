use std::{
    io::{BufReader, BufWriter, Error as IoError, Read, Write},
    result::Result,
};

use log::info;
use thiserror::Error as ThisError;
use zstd::{Decoder, Encoder};

const COMPRESSION_LEVEL: i32 = 15;
pub(crate) const WINDOW_LOG_MAX_SIZE: u32 = 31;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("Error enabling frame checksums on zstd stream: {0}")]
    Checksum(IoError),
    #[error("Error setting up zstd decoding stream: {0}")]
    Decode(IoError),
    #[error("Error setting up zstd encoding stream: {0}")]
    Encode(IoError),
    #[error("Error setting zstd window log: {0}")]
    WindowLog(IoError),
}

pub fn zstd_decode_stream<'a, R: Read>(stream: R) -> Result<Decoder<'a, BufReader<R>>, Error> {
    let mut decoder = Decoder::new(stream).map_err(Error::Decode)?;
    decoder
        .window_log_max(WINDOW_LOG_MAX_SIZE)
        .map_err(Error::Decode)?;
    info!("Set zstd window log max size to {}.", WINDOW_LOG_MAX_SIZE);

    Ok(decoder)
}

pub fn zstd_encode_stream<'a, W: Write>(stream: W) -> Result<Encoder<'a, BufWriter<W>>, Error> {
    let mut encoder =
        Encoder::new(BufWriter::new(stream), COMPRESSION_LEVEL).map_err(Error::Encode)?;
    encoder
        .window_log(WINDOW_LOG_MAX_SIZE)
        .map_err(Error::WindowLog)?;
    encoder.include_checksum(true).map_err(Error::Checksum)?;
    Ok(encoder)
}
