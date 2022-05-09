use std::result::Result;

use casper_node::types::BlockSignatures;

use crate::db::{Database, DeserializationError};

pub struct BlockMetadataDatabase;

impl std::fmt::Display for BlockMetadataDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "block_metadata")
    }
}

impl Database for BlockMetadataDatabase {
    fn db_name() -> &'static str {
        "block_metadata"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: BlockSignatures = bincode::deserialize(bytes)?;
        Ok(())
    }
}
