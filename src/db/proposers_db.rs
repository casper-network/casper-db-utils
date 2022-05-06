use casper_types::{bytesrepr::FromBytes, PublicKey};

use crate::db::{Database, Error, Result};

pub struct ProposerDatabase;

impl std::fmt::Display for ProposerDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "proposers")
    }
}

impl Database for ProposerDatabase {
    fn db_name() -> &'static str {
        "proposers"
    }

    fn parse_element(bytes: &[u8]) -> Result<()> {
        let _: PublicKey = FromBytes::from_bytes(bytes)
            .map_err(|_| Error::BytesreprError)?
            .0;
        Ok(())
    }
}
