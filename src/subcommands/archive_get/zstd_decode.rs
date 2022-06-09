use std::{
    io::{BufReader, Read},
    result::Result,
};

use log::info;
use zstd::Decoder;

use super::Error;

const DEFAULT_WINDOW_LOG_MAX_SIZE: u32 = 27;

pub fn zstd_decode_stream<'a, R: Read>(
    stream: R,
    log_distance: Option<u32>,
) -> Result<Decoder<'a, BufReader<R>>, Error> {
    let mut decoder = Decoder::new(stream).map_err(Error::ZstdDecoderSetup)?;
    if let Some(window_log_distance) = log_distance {
        decoder
            .window_log_max(window_log_distance)
            .map_err(Error::ZstdDecoderSetup)?;
        info!("Set zstd window log max size to {}.", window_log_distance);
    } else {
        info!(
            "Default zstd window log max size {}.",
            DEFAULT_WINDOW_LOG_MAX_SIZE
        );
    }
    Ok(decoder)
}
