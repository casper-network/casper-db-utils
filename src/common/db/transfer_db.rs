use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

use casper_types::Transfer;

use super::{Database, DeserializationError};

pub struct TransferDatabase;

impl Display for TransferDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "transfer")
    }
}

impl Database for TransferDatabase {
    fn db_name() -> &'static str {
        "transfer"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: Vec<Transfer> = bincode::deserialize(bytes)?;
        Ok(())
    }
}
