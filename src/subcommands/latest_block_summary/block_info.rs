use std::{
    fs,
    io::{Error as IoError, ErrorKind},
    path::Path,
    result::Result,
};

use serde::{Deserialize, Serialize};

use casper_hashing::Digest;
use casper_node::types::{BlockHeader, Timestamp};
use casper_types::{EraId, ProtocolVersion};

#[cfg(test)]
use crate::test_utils::MockBlockHeader;

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub struct BlockInfo {
    network_name: Option<String>,
    body_hash: Digest,
    era_id: EraId,
    height: u64,
    protocol_version: ProtocolVersion,
    state_root_hash: Digest,
    timestamp: Timestamp,
}

impl BlockInfo {
    pub fn new(network_name: Option<String>, block_header: BlockHeader) -> Self {
        Self {
            network_name,
            body_hash: *block_header.body_hash(),
            era_id: block_header.era_id(),
            height: block_header.height(),
            protocol_version: block_header.protocol_version(),
            state_root_hash: *block_header.state_root_hash(),
            timestamp: block_header.timestamp(),
        }
    }

    #[cfg(test)]
    pub fn into_mock(self) -> (MockBlockHeader, Option<String>) {
        (
            MockBlockHeader {
                body_hash: self.body_hash,
                era_id: self.era_id,
                height: self.height,
                protocol_version: self.protocol_version,
                state_root_hash: self.state_root_hash,
                timestamp: self.timestamp,
                parent_hash: Default::default(),
                random_bit: Default::default(),
                accumulated_seed: Default::default(),
                era_end: None,
            },
            self.network_name,
        )
    }
}

pub fn parse_network_name<P: AsRef<Path>>(path: P) -> Result<String, IoError> {
    let canon_path = fs::canonicalize(path)?;
    let network_name = canon_path
        .parent()
        .ok_or_else(|| IoError::from(ErrorKind::NotFound))?
        .file_name()
        .ok_or_else(|| {
            IoError::new(
                ErrorKind::InvalidInput,
                "Path cannot be represented in UTF-8",
            )
        })?;
    network_name
        .to_str()
        .ok_or_else(|| {
            IoError::new(
                ErrorKind::InvalidInput,
                "Path cannot be represented in UTF-8",
            )
        })
        .map(String::from)
}
