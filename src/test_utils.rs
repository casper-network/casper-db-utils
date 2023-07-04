#![cfg(test)]

use std::{
    collections::{BTreeMap, HashMap},
    fs::OpenOptions,
    path::PathBuf,
};

use lmdb::{Database as LmdbDatabase, DatabaseFlags, Environment, EnvironmentFlags};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use tempfile::{NamedTempFile, TempDir};

use casper_hashing::Digest;
use casper_node::types::{BlockHash, DeployHash, DeployMetadata};
use casper_types::{
    EraId, ExecutionEffect, ExecutionResult, ProtocolVersion, PublicKey, SecretKey, Timestamp,
    U256, U512,
};

pub(crate) static KEYS: Lazy<Vec<PublicKey>> = Lazy::new(|| {
    (0..10)
        .map(|i| {
            let u256 = U256::from(i);
            let mut u256_bytes = [0u8; 32];
            u256.to_big_endian(&mut u256_bytes);
            let secret_key =
                SecretKey::ed25519_from_bytes(u256_bytes).expect("should create secret key");
            PublicKey::from(&secret_key)
        })
        .collect()
});

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

pub(crate) fn mock_switch_block_header(idx: u8) -> (BlockHash, MockSwitchBlockHeader) {
    let mut block_header = MockSwitchBlockHeader::default();
    let block_hash_digest: Digest = {
        let mut bytes = [idx; Digest::LENGTH];
        bytes[Digest::LENGTH - 1] = 255;
        bytes
    }
    .into();
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

#[derive(Clone, Debug, Default, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct EraReport {
    equivocators: Vec<PublicKey>,
    rewards: BTreeMap<PublicKey, u64>,
    inactive_validators: Vec<PublicKey>,
}

#[derive(Clone, Default, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
pub struct EraEnd {
    era_report: EraReport,
    pub next_era_validator_weights: BTreeMap<PublicKey, U512>,
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
pub struct MockSwitchBlockHeader {
    pub parent_hash: BlockHash,
    pub state_root_hash: Digest,
    pub body_hash: Digest,
    pub random_bit: bool,
    pub accumulated_seed: Digest,
    pub era_end: Option<EraEnd>,
    pub timestamp: Timestamp,
    pub era_id: EraId,
    pub height: u64,
    pub protocol_version: ProtocolVersion,
}

impl MockSwitchBlockHeader {
    pub fn insert_key_weight(&mut self, key: PublicKey, weight: U512) {
        let _ = self
            .era_end
            .as_mut()
            .unwrap()
            .next_era_validator_weights
            .insert(key, weight);
    }
}

impl Default for MockSwitchBlockHeader {
    fn default() -> Self {
        Self {
            parent_hash: Default::default(),
            state_root_hash: Default::default(),
            body_hash: Default::default(),
            random_bit: Default::default(),
            accumulated_seed: Default::default(),
            era_end: Some(Default::default()),
            timestamp: Timestamp::now(),
            era_id: Default::default(),
            height: Default::default(),
            protocol_version: Default::default(),
        }
    }
}
