#![cfg(test)]

use std::{fs::OpenOptions, path::PathBuf};

use lmdb::{Database as LmdbDatabase, DatabaseFlags, Environment, EnvironmentFlags};
use serde::{Deserialize, Serialize};
use tempfile::{NamedTempFile, TempDir};

use casper_hashing::Digest;
use casper_node::types::{BlockHash, Timestamp};
use casper_types::{EraId, ProtocolVersion};

pub struct LmdbTestFixture {
    pub env: Environment,
    pub db: LmdbDatabase,
    pub tmp_dir: TempDir,
    pub file_path: PathBuf,
}

impl LmdbTestFixture {
    pub fn new(name: Option<&str>, file_name: Option<&str>) -> Self {
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
            .set_map_size(4096 * 10)
            .set_max_dbs(10)
            .open(&file_path)
            .expect("can't create environment");
        let db = env
            .create_db(name, DatabaseFlags::empty())
            .expect("can't create database");
        LmdbTestFixture {
            env,
            db,
            tmp_dir,
            file_path,
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
