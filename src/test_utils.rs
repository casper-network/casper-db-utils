#![cfg(test)]

use std::{collections::HashMap, fs::OpenOptions, path::PathBuf};

use lmdb::{Database as LmdbDatabase, DatabaseFlags, Environment, EnvironmentFlags};
use serde::{Deserialize, Serialize};
use tempfile::{NamedTempFile, TempDir};

use casper_hashing::Digest;
use casper_node::types::{BlockHash, DeployHash, DeployMetadata, Timestamp};
use casper_types::{EraId, ExecutionEffect, ExecutionResult, ProtocolVersion};

pub struct LmdbTestFixture {
    pub env: Environment,
    pub dbs: HashMap<&'static str, LmdbDatabase>,
    pub tmp_dir: TempDir,
    pub file_path: PathBuf,
}

impl LmdbTestFixture {
    pub fn new(names: Vec<&'static str>, file_name: Option<&str>) -> Self {
        let tmp_dir = tempfile::tempdir().unwrap();
        let file_path = if let Some(name) = file_name {
            let path = tmp_dir.as_ref().join(name);
            let _ = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)
                .unwrap();
            path
        } else {
            let path = NamedTempFile::new_in(tmp_dir.as_ref())
                .unwrap()
                .path()
                .to_path_buf();
            let _ = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)
                .unwrap();
            path
        };
        let env = Environment::new()
            .set_flags(
                EnvironmentFlags::WRITE_MAP
                    | EnvironmentFlags::NO_SUB_DIR
                    | EnvironmentFlags::NO_TLS
                    | EnvironmentFlags::NO_READAHEAD,
            )
            .set_max_readers(12)
            .set_map_size(4096 * 1024)
            .set_max_dbs(10)
            .open(&file_path)
            .expect("can't create environment");
        let mut dbs = HashMap::new();
        if names.is_empty() {
            let db = env
                .create_db(None, DatabaseFlags::empty())
                .expect("can't create database");
            dbs.insert("default", db);
        } else {
            for name in names {
                let db = env
                    .create_db(Some(name), DatabaseFlags::empty())
                    .expect("can't create database");
                dbs.insert(name, db);
            }
        }

        LmdbTestFixture {
            env,
            dbs,
            tmp_dir,
            file_path,
        }
    }

    pub fn db(&self, maybe_name: Option<&str>) -> Option<&LmdbDatabase> {
        if let Some(name) = maybe_name {
            self.dbs.get(name)
        } else {
            self.dbs.get("default")
        }
    }
}

// This struct was created in order to generate `BlockHeaders` and then
// insert them into a mock database. Once `Block::random` becomes part
// of the public API of `casper-types`, this will no longer be needed.
#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
pub struct MockBlockHeader {
    pub parent_hash: BlockHash,
    pub state_root_hash: Digest,
    pub body_hash: Digest,
    pub random_bit: bool,
    pub accumulated_seed: Digest,
    pub era_end: Option<()>,
    pub timestamp: Timestamp,
    pub era_id: EraId,
    pub height: u64,
    pub protocol_version: ProtocolVersion,
}

impl Default for MockBlockHeader {
    fn default() -> Self {
        Self {
            parent_hash: Default::default(),
            state_root_hash: Default::default(),
            body_hash: Default::default(),
            random_bit: Default::default(),
            accumulated_seed: Default::default(),
            era_end: Default::default(),
            timestamp: Timestamp::now(),
            era_id: Default::default(),
            height: Default::default(),
            protocol_version: Default::default(),
        }
    }
}

pub(crate) fn mock_deploy_hash(idx: u8) -> DeployHash {
    DeployHash::new([idx; 32].into())
}

pub(crate) fn mock_block_header(idx: u8) -> (BlockHash, MockBlockHeader) {
    let mut block_header = MockBlockHeader::default();
    let block_hash_digest: Digest = [idx; Digest::LENGTH].into();
    let block_hash: BlockHash = block_hash_digest.into();
    block_header.body_hash = [idx; Digest::LENGTH].into();
    (block_hash, block_header)
}

pub(crate) fn mock_deploy_metadata(block_hashes: &[BlockHash]) -> DeployMetadata {
    let mut deploy_metadata = DeployMetadata::default();
    for block_hash in block_hashes {
        deploy_metadata
            .execution_results
            .insert(*block_hash, success_execution_result());
    }
    deploy_metadata
}

pub(crate) fn success_execution_result() -> ExecutionResult {
    ExecutionResult::Success {
        effect: ExecutionEffect::default(),
        transfers: vec![],
        cost: 100.into(),
    }
}
