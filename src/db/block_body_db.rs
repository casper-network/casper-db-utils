use casper_node::types::BlockBody;

use crate::db::{Database, Result};

pub struct BlockBodyDatabase;

impl std::fmt::Display for BlockBodyDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "block_body")
    }
}

impl Database for BlockBodyDatabase {
    fn db_name() -> &'static str {
        "block_body"
    }

    fn parse_element(bytes: &[u8]) -> Result<()> {
        let _: BlockBody = bincode::deserialize(bytes)?;
        Ok(())
    }
}
