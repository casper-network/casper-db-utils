use casper_types::{BlockBody, BlockHash, Digest, TransactionHash};
use lmdb::Transaction;
use log::{info, warn};
use std::{fs, io::ErrorKind, path::Path, result::Result, sync::Arc};

use crate::common::db::{
    self, STORAGE_FILE_NAME,
    databases::{
        block_body_database, block_header_database, execution_results_database,
        transactions_database, versioned_transfers_database,
    },
};

use super::Error;

pub(crate) fn create_output_db<P: AsRef<Path>>(output_path: P) -> Result<(), Error> {
    if output_path.as_ref().exists() {
        return Err(Error::Output(ErrorKind::AlreadyExists.into()));
    }
    fs::create_dir_all(&output_path)?;

    let storage_path = output_path.as_ref().join(STORAGE_FILE_NAME);
    let storage_env = Arc::new(db::db_env(storage_path)?);

    block_header_database().create(storage_env.clone())?;
    block_body_database().create(storage_env.clone())?;
    transactions_database().create(storage_env.clone())?;
    versioned_transfers_database().create(storage_env.clone())?;
    execution_results_database().create(storage_env.clone())?;
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
    let source_env = Arc::new(db::db_env(&source_path)?);
    let destination_path = destination.as_ref().join(STORAGE_FILE_NAME);
    let destination_env = Arc::new(db::db_env(&destination_path)?);

    let source_txn = source_env.begin_ro_txn()?;
    let mut destination_txn = destination_env.begin_rw_txn()?;
    let source_block_header_db = block_header_database();
    let destination_block_header_db = block_header_database();
    let source_block_body_db = block_body_database();
    let destination_block_body_db = block_body_database();
    let destination_transactions_db = transactions_database();
    let source_transactions_db = transactions_database();
    let destination_execution_results_db = execution_results_database();
    let source_execution_results_db = execution_results_database();

    let source_transfers_db = versioned_transfers_database();
    let destination_transfers_db = versioned_transfers_database();

    info!(
        "Initiating block information transfer from {} to {} for block {block_hash}",
        source_path.to_string_lossy(),
        destination_path.to_string_lossy()
    );

    // Read the block header associated with the given block hash.
    source_block_header_db
        .transfer_to_other_database(
            vec![block_hash],
            &source_txn,
            &mut destination_txn,
            &destination_block_header_db,
        )
        .map_err(Error::from)?;

    info!("Successfully transferred block header");

    let block_header = source_block_header_db.get(&source_txn, &block_hash)?;
    if block_header.is_none() {
        warn!("Couldn't find BlockHeader with hash {}", block_hash);
    }
    let block_header = block_header.unwrap();

    source_block_body_db.transfer_to_other_database(
        vec![*block_header.body_hash()],
        &source_txn,
        &mut destination_txn,
        &destination_block_body_db,
    )?;

    let block_body = source_block_body_db.get(&source_txn, block_header.body_hash())?;
    if block_body.is_none() {
        warn!("Couldn't find BlockBody with hash {}", block_hash);
    }
    let block_body = block_body.unwrap();

    match source_transfers_db.transfer_to_other_database(
        vec![BlockHash::from(*block_header.body_hash())],
        &source_txn,
        &mut destination_txn,
        &destination_transfers_db,
    ) {
        Ok(_) => {}
        Err(crate::common::db::Error::Database(lmdb::Error::NotFound)) => {
            info!("No transfers found in the source DB");
        }
        Err(e) => return Err(Error::from(e)),
    };

    let transaction_hashes: Vec<TransactionHash> = match block_body {
        BlockBody::V1(block_v1) => block_v1
            .deploy_hashes()
            .iter()
            .map(|h| TransactionHash::Deploy(*h))
            .collect(),
        BlockBody::V2(block_v2) => block_v2.all_transactions().copied().collect(),
    };

    source_transactions_db.transfer_to_other_database(
        transaction_hashes.clone(),
        &source_txn,
        &mut destination_txn,
        &destination_transactions_db,
    )?;

    source_execution_results_db.transfer_to_other_database(
        transaction_hashes,
        &source_txn,
        &mut destination_txn,
        &destination_execution_results_db,
    )?;
    // Commit the transactions.
    source_txn.commit()?;
    destination_txn.commit()?;
    info!("Storage transfer complete");
    Ok(*block_header.state_root_hash())
}
