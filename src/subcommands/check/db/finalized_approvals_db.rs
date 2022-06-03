use std::{
    fmt::{Display, Formatter, Result as FormatterResult},
    result::Result,
};

use casper_node::types::FinalizedApprovals;

use super::{Database, DeserializationError};

pub struct FinalizedApprovalsDatabase;

impl Display for FinalizedApprovalsDatabase {
    fn fmt(&self, f: &mut Formatter<'_>) -> FormatterResult {
        write!(f, "finalized_approvals")
    }
}

impl Database for FinalizedApprovalsDatabase {
    fn db_name() -> &'static str {
        "finalized_approvals"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        let _: FinalizedApprovals = bincode::deserialize(bytes)?;
        Ok(())
    }
}
