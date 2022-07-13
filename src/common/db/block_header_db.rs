use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

use casper_node::types::BlockHeader;

use super::{Database, DeserializationError};

pub struct BlockHeaderDatabase;

impl Display for BlockHeaderDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "block_header")
    }
}

impl Database for BlockHeaderDatabase {
    fn db_name() -> &'static str {
        "block_header"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: BlockHeader = bincode::deserialize(bytes)?;
        Ok(())
    }
}
