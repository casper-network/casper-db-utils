use std::{fs, io::ErrorKind, path::Path, result::Result};

use casper_hashing::Digest;
use lmdb::{DatabaseFlags, Error as LmdbError, Transaction};

use casper_node::types::{BlockHash, BlockHeader, DeployMetadata};
use log::info;

use crate::{
    common::db::{
        self, BlockBodyDatabase, BlockHeaderDatabase, Database, DeployDatabase,
        DeployMetadataDatabase, TransferDatabase, STORAGE_FILE_NAME,
    },
    subcommands::execution_results_summary::block_body::BlockBody,
};

use super::{db_helpers, Error};

pub(crate) fn create_output_db<P: AsRef<Path>>(output_path: P) -> Result<(), Error> {
    if output_path.as_ref().exists() {
        return Err(Error::Output(ErrorKind::AlreadyExists.into()));
    }
    fs::create_dir_all(&output_path)?;

    let storage_path = output_path.as_ref().join(STORAGE_FILE_NAME);
    let storage_env = db::db_env(storage_path)?;

    storage_env.create_db(Some(BlockHeaderDatabase::db_name()), DatabaseFlags::empty())?;
    storage_env.create_db(Some(BlockBodyDatabase::db_name()), DatabaseFlags::empty())?;
    storage_env.create_db(Some(DeployDatabase::db_name()), DatabaseFlags::empty())?;
    storage_env.create_db(Some(TransferDatabase::db_name()), DatabaseFlags::empty())?;
    storage_env.create_db(
        Some(DeployMetadataDatabase::db_name()),
        DatabaseFlags::empty(),
    )?;

    Ok(())
}

/// Given a block hash, reads the information related to the associated block
/// (block header, block body, deploys, transfers, execution results) and
/// copies them over to a new database. Returns the state root hash associated
/// with the block.
pub(crate) fn transfer_block_info<P1: AsRef<Path>, P2: AsRef<Path>>(
    source: P1,
    destination: P2,
    block_hash: BlockHash,
) -> Result<Digest, Error> {
    let source_path = source.as_ref().join(STORAGE_FILE_NAME);
    let source_env = db::db_env(&source_path)?;
    let destination_path = destination.as_ref().join(STORAGE_FILE_NAME);
    let destination_env = db::db_env(&destination_path)?;

    let mut source_txn = source_env.begin_ro_txn()?;
    let mut destination_txn = destination_env.begin_rw_txn()?;

    info!(
        "Initiating block information transfer from {} to {} for block {block_hash}",
        source_path.to_string_lossy(),
        destination_path.to_string_lossy()
    );

    // Read the block header associated with the given block hash.
    let block_header_bytes = db_helpers::transfer_to_new_db(
        &mut source_txn,
        &mut destination_txn,
        BlockHeaderDatabase::db_name(),
        &block_hash,
    )?;
    info!("Successfully transferred block header");
    let block_header: BlockHeader = bincode::deserialize(&block_header_bytes)?;

    // Read the block body associated with the previously read block header.
    let block_body_bytes = db_helpers::transfer_to_new_db(
        &mut source_txn,
        &mut destination_txn,
        BlockBodyDatabase::db_name(),
        block_header.body_hash(),
    )?;
    info!("Successfully transferred block body");
    let block_body: BlockBody = bincode::deserialize(&block_body_bytes)?;

    // Attempt to copy over all entries in the transfer database for the given
    // block hash. If we have no entry under the block hash, we move on.
    match db_helpers::transfer_to_new_db(
        &mut source_txn,
        &mut destination_txn,
        TransferDatabase::db_name(),
        &block_hash,
    ) {
        Ok(_) => info!("Found transfers in the source DB and successfully transferred them"),
        Err(LmdbError::NotFound) => info!("No transfers found in the source DB"),
        Err(lmdb_error) => return Err(Error::Database(lmdb_error)),
    }

    // Copy over all the deploys in this block and construct the execution
    // results to be stored in the new database.
    let deploy_metadata_db =
        unsafe { source_txn.open_db(Some(DeployMetadataDatabase::db_name()))? };
    for deploy_hash in block_body.deploy_hashes() {
        // Copy the deploy to the new database.
        db_helpers::transfer_to_new_db(
            &mut source_txn,
            &mut destination_txn,
            DeployDatabase::db_name(),
            deploy_hash,
        )?;
        info!("Successfully transferred deploy {deploy_hash}");

        // Get this deploy's metadata.
        let metadata_raw = source_txn.get(deploy_metadata_db, &deploy_hash)?;
        let mut metadata: DeployMetadata =
            bincode::deserialize(metadata_raw).map_err(|bincode_err| {
                Error::Parsing(
                    block_hash,
                    DeployMetadataDatabase::db_name().to_string(),
                    bincode_err,
                )
            })?;
        // Extract the execution result of this deploy for this block.
        if let Some(execution_result) = metadata.execution_results.remove(&block_hash) {
            // Construct the metadata to be stored using only the relevant
            // execution results.
            let mut new_metadata = DeployMetadata::default();
            new_metadata
                .execution_results
                .insert(block_hash, execution_result.clone());
            let serialized_new_metadata = bincode::serialize(&new_metadata)?;
            db_helpers::write_to_db(
                &mut destination_txn,
                DeployMetadataDatabase::db_name(),
                deploy_hash,
                &serialized_new_metadata,
            )?;
            info!("Successfully transferred execution results for {deploy_hash}");
        }
    }
    // Commit the transactions.
    source_txn.commit()?;
    destination_txn.commit()?;
    info!("Storage transfer complete");
    Ok(*block_header.state_root_hash())
}
