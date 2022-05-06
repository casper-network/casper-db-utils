use casper_node::types::BlockHeader;

use crate::db::{Database, Result};

pub struct BlockHeaderDatabase;

impl std::fmt::Display for BlockHeaderDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "block_header")
    }
}

impl Database for BlockHeaderDatabase {
    fn db_name() -> &'static str {
        "block_header"
    }

    fn parse_element(bytes: &[u8]) -> Result<()> {
        let _: BlockHeader = bincode::deserialize(bytes)?;
        Ok(())
    }
}
