use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

use casper_hashing::Digest;
use casper_types::bytesrepr::FromBytes;

use super::{Database, DeserializationError};

pub struct BlockBodyMerkleDatabase;

impl Display for BlockBodyMerkleDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "block_body_merkle")
    }
}

impl Database for BlockBodyMerkleDatabase {
    fn db_name() -> &'static str {
        "block_body_merkle"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: (Digest, Digest) = FromBytes::from_bytes(bytes)?.0;
        Ok(())
    }
}
