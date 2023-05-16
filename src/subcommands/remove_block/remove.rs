use std::path::Path;

use casper_node::types::{BlockHash, BlockHeader, DeployMetadata};
use lmdb::{Error as LmdbError, Transaction, WriteFlags};
use log::warn;

use crate::{
    common::db::{
        self, BlockBodyDatabase, BlockHeaderDatabase, Database, DeployMetadataDatabase,
        STORAGE_FILE_NAME,
    },
    subcommands::execution_results_summary::block_body::BlockBody,
};

use super::Error;

pub(crate) fn remove_block<P: AsRef<Path>>(db_path: P, block_hash: BlockHash) -> Result<(), Error> {
    let storage_path = db_path.as_ref().join(STORAGE_FILE_NAME);
    let env = db::db_env(storage_path)?;

    let mut txn = env.begin_rw_txn()?;
    let header_db = unsafe { txn.open_db(Some(BlockHeaderDatabase::db_name()))? };
    let body_db = unsafe { txn.open_db(Some(BlockBodyDatabase::db_name()))? };
    let deploy_metadata_db = unsafe { txn.open_db(Some(DeployMetadataDatabase::db_name()))? };

    let header: BlockHeader = match txn.get(header_db, &block_hash) {
        Ok(raw_header) => bincode::deserialize(raw_header)
            .map_err(|bincode_err| Error::HeaderParsing(block_hash, bincode_err))?,
        Err(LmdbError::NotFound) => {
            return Err(Error::MissingHeader(block_hash));
        }
        Err(lmdb_err) => {
            return Err(lmdb_err.into());
        }
    };

    let maybe_body: Option<BlockBody> = match txn.get(body_db, header.body_hash()) {
        Ok(raw_body) => Some(
            bincode::deserialize(raw_body)
                .map_err(|bincode_err| Error::BodyParsing(block_hash, bincode_err))?,
        ),
        Err(LmdbError::NotFound) => {
            warn!(
                "No block body found for block header with hash {}",
                block_hash
            );
            None
        }
        Err(lmdb_err) => {
            return Err(lmdb_err.into());
        }
    };

    if let Some(body) = maybe_body {
        // Go through all the deploys in this block and get the execution
        // result of each one.
        for deploy_hash in body.deploy_hashes() {
            // Get this deploy's metadata.
            let mut metadata: DeployMetadata = match txn.get(deploy_metadata_db, deploy_hash) {
                Ok(raw_metadata) => bincode::deserialize(raw_metadata).map_err(|bincode_err| {
                    Error::ExecutionResultsParsing(block_hash, *deploy_hash, bincode_err)
                })?,
                Err(LmdbError::NotFound) => return Err(Error::MissingDeploy(*deploy_hash)),
                Err(lmdb_error) => return Err(lmdb_error.into()),
            };
            // Extract the execution result of this deploy for the current block.
            if let Some(_execution_result) = metadata.execution_results.remove(&block_hash) {
                if metadata.execution_results.is_empty() {
                    txn.del(deploy_metadata_db, deploy_hash, None)?;
                } else {
                    let encoded_metadata = bincode::serialize(&metadata)
                        .map_err(|bincode_err| Error::Serialization(*deploy_hash, bincode_err))?;
                    txn.put(
                        deploy_metadata_db,
                        deploy_hash,
                        &encoded_metadata,
                        WriteFlags::default(),
                    )?;
                }
            }
        }

        txn.del(body_db, header.body_hash(), None)?;
    }

    txn.del(header_db, &block_hash, None)?;
    txn.commit()?;
    Ok(())
}
