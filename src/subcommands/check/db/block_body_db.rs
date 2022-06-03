use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

use casper_node::types::BlockBody;

use super::{Database, DeserializationError};

pub struct BlockBodyDatabase;

impl Display for BlockBodyDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "block_body")
    }
}

impl Database for BlockBodyDatabase {
    fn db_name() -> &'static str {
        "block_body"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: BlockBody = bincode::deserialize(bytes)?;
        Ok(())
    }
}
