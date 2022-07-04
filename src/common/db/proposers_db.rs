use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

use casper_types::{bytesrepr::FromBytes, PublicKey};

use super::{Database, DeserializationError};

pub struct ProposerDatabase;

impl Display for ProposerDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "proposers")
    }
}

impl Database for ProposerDatabase {
    fn db_name() -> &'static str {
        "proposers"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: PublicKey = FromBytes::from_bytes(bytes)?.0;
        Ok(())
    }
}
