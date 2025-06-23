use std::{path::Path, sync::Arc};

use casper_types::{BlockHash, TransactionHash};
use lmdb::Transaction;
use log::warn;

use crate::common::db::{
    STORAGE_FILE_NAME,
    databases::{
        block_body_database, block_header_database, execution_results_database,
        transactions_database,
    },
    db_env,
};

use super::Error;

pub(crate) fn remove_block<P: AsRef<Path>>(db_path: P, block_hash: BlockHash) -> Result<(), Error> {
    let storage_path = db_path.as_ref().join(STORAGE_FILE_NAME);
    let env = Arc::new(db_env(storage_path)?);

    let mut txn = env.begin_rw_txn()?;
    let header_db = block_header_database();
    let body_db = block_body_database();
    let transaction_db = transactions_database();
    let results_db = execution_results_database();

    let header = match header_db.get(&txn, &block_hash).map_err(Error::from)? {
        Some(header) => header,
        None => {
            return Err(Error::MissingHeader(block_hash));
        }
    };

    match body_db.get(&txn, header.body_hash()).map_err(Error::from)? {
        Some(body) => match body {
            casper_types::BlockBody::V1(block_body_v1) => {
                for deploy_hash in block_body_v1.deploy_hashes() {
                    // Get this deploy's metadata.
                    let key = TransactionHash::Deploy(*deploy_hash);
                    let maybe_results = results_db.get(&txn, &key)?;
                    if maybe_results.is_none() {
                        return Err(Error::MissingDeploy(*deploy_hash));
                    }
                    results_db.del(&mut txn, &key)?;
                    transaction_db.del(&mut txn, &key)?;
                }
            }
            casper_types::BlockBody::V2(block_body_v2) => {
                for txs in block_body_v2.transactions().values() {
                    for transaction_hash in txs {
                        // Get this deploy's metadata.
                        let key = transaction_hash;
                        let maybe_results = results_db.get(&txn, key)?;
                        if maybe_results.is_none() {
                            return Err(Error::MissingTransaction(*transaction_hash));
                        }
                        results_db.del(&mut txn, key)?;
                        transaction_db.del(&mut txn, key)?;
                    }
                }
            }
        },
        None => {
            warn!("No block body found for block header with hash {block_hash}");
        }
    };
    body_db.del(&mut txn, header.body_hash())?;
    header_db.del(&mut txn, &block_hash)?;
    txn.commit()?;
    Ok(())
}
