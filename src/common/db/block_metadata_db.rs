use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

use casper_node::types::BlockSignatures;

use super::{Database, DeserializationError};

pub struct BlockMetadataDatabase;

impl Display for BlockMetadataDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
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
