use std::{
    io::{BufReader, Read},
    result::Result,
};

use log::info;
use zstd::Decoder;

use super::Error;

const WINDOW_LOG_MAX_SIZE: u32 = 31;

pub fn zstd_decode_stream<'a, R: Read>(stream: R) -> Result<Decoder<'a, BufReader<R>>, Error> {
    let mut decoder = Decoder::new(stream).map_err(Error::ZstdDecoderSetup)?;
    decoder
        .window_log_max(WINDOW_LOG_MAX_SIZE)
        .map_err(Error::ZstdDecoderSetup)?;
    info!("Set zstd window log max size to {}.", WINDOW_LOG_MAX_SIZE);

    Ok(decoder)
}
