use std::{
    io::{BufWriter, Write},
    result::Result,
};

use zstd::Encoder;

use super::Error;

const COMPRESSION_LEVEL: i32 = 15;

pub fn zstd_encode_stream<'a, W: Write>(stream: W) -> Result<Encoder<'a, BufWriter<W>>, Error> {
    Encoder::new(BufWriter::new(stream), COMPRESSION_LEVEL).map_err(Error::ZstdEncoderSetup)
}
