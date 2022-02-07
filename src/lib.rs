//! # lmdb_utils
//! This is a command-line utility for Casper Node operators.
//!

#[cfg(test)]
mod test;

pub mod error {
    use thiserror::Error;

    #[derive(Debug, Error)]
    pub enum ToolError {
        #[error("failed database operation")]
        Database(#[from] lmdb::Error),
    }
}

mod block {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Hash)]
    pub struct BlockHeader {
        parent_hash: Vec<u8>,
        state_root_hash: Vec<u8>,
        pub body_hash: Vec<u8>,
        random_bit: bool,
        accumulated_seed: Vec<u8>,
        era_end: Option<Vec<u8>>,
        timestamp: u64,
        era_id: u64,
        height: u64,
        protocol_version: [u32; 3],
    }

    impl BlockHeader {
        pub fn body_hash(&self) -> String {
            base16::encode_lower(&self.body_hash)
        }

        pub fn height(&self) -> u64 {
            self.height
        }
    }
}

pub mod db {
    use std::{collections::HashSet, path::PathBuf};

    use lmdb::{
        Cursor, Database, DatabaseFlags, Environment, EnvironmentFlags, Transaction, WriteFlags,
    };

    use super::{block::BlockHeader, error::ToolError};

    type Hashes = HashSet<Vec<u8>>;

    pub fn get_env(path: PathBuf) -> Result<Environment, lmdb::Error> {
        Environment::new()
            .set_flags(
                EnvironmentFlags::NO_SUB_DIR
                    | EnvironmentFlags::NO_TLS
                    | EnvironmentFlags::NO_READAHEAD,
            )
            .set_max_dbs(3)
            .open(&path)
    }

    pub fn run(db_path: PathBuf, block_height: u64) -> Result<(u64, u64, u64), ToolError> {
        let env = get_env(db_path)?;

        let block_header_db = env.create_db(Some("block_header"), DatabaseFlags::default())?;
        let block_metadata_db = env.create_db(Some("block_metadata"), DatabaseFlags::default())?;
        let block_body_db = env.create_db(Some("block_body"), DatabaseFlags::default())?;

        let (header_hashes, body_hashes) =
            delete_headers_and_get_hashes(block_header_db, &env, block_height)?;

        let deleted_bodies = delete_from_db(block_body_db, &env, body_hashes)?;
        let deleted_metas = delete_from_db(block_metadata_db, &env, header_hashes.clone())?;
        let deleted_headers = header_hashes.len() as u64;

        Ok((deleted_headers, deleted_bodies, deleted_metas))
    }

    fn delete_headers_and_get_hashes(
        db: Database,
        env: &Environment,
        max_block_height: u64,
    ) -> Result<(Hashes, Hashes), ToolError> {
        let mut txn = env.begin_rw_txn()?;
        let mut cursor = txn.open_rw_cursor(db)?;

        let mut header_hashes = HashSet::new();
        let mut body_hashes = HashSet::new();

        for (raw_key, raw_val) in cursor.iter() {
            let block_header: BlockHeader =
                bincode::deserialize(raw_val).expect("failed to deserialize block header");
            let block_height = block_header.height();

            if block_height > max_block_height {
                header_hashes.insert(raw_key.to_vec());
                body_hashes.insert(block_header.body_hash.clone());

                cursor.del(WriteFlags::empty())?;
            }
        }

        drop(cursor);
        txn.commit().expect("failed to commit transaction");

        Ok((header_hashes, body_hashes))
    }

    fn delete_from_db(db: Database, env: &Environment, hashes: Hashes) -> Result<u64, ToolError> {
        let mut txn = env.begin_rw_txn()?;
        let mut cursor = txn.open_rw_cursor(db)?;
        let mut count = 0;

        for (raw_key, _) in cursor.iter() {
            if hashes.contains(raw_key) {
                cursor.del(WriteFlags::empty())?;
                count += 1;
            }
        }

        Ok(count)
    }
}

pub use block::BlockHeader;
