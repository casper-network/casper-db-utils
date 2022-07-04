use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

use casper_node::types::Deploy;

use super::{Database, DeserializationError};

pub struct DeployDatabase;

impl Display for DeployDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "deploys")
    }
}

impl Database for DeployDatabase {
    fn db_name() -> &'static str {
        "deploys"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: Deploy = bincode::deserialize(bytes)?;
        Ok(())
    }
}
