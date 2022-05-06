use casper_types::Transfer;

use crate::db::{Database, Result};

pub struct TransferDatabase;

impl std::fmt::Display for TransferDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "transfer")
    }
}

impl Database for TransferDatabase {
    fn db_name() -> &'static str {
        "transfer"
    }

    fn parse_element(bytes: &[u8]) -> Result<()> {
        let _: Vec<Transfer> = bincode::deserialize(bytes)?;
        Ok(())
    }
}
