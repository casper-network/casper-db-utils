use std::result::Result;

use casper_hashing::Digest;
use casper_types::bytesrepr::FromBytes;

use crate::db::{Database, DeserializationError};

pub struct BlockBodyMerkleDatabase;

impl std::fmt::Display for BlockBodyMerkleDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "block_body_merkle")
    }
}

impl Database for BlockBodyMerkleDatabase {
    fn db_name() -> &'static str {
        "block_body_merkle"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: (Digest, Digest) = FromBytes::from_bytes(bytes)
            .map_err(|_| DeserializationError::BytesreprError)?
            .0;
        Ok(())
    }
}
