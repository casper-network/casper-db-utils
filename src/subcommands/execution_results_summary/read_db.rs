use std::{
    fs::OpenOptions,
    io::{self, Write},
    path::Path,
    result::Result,
    sync::Arc,
};

use lmdb::Environment;
use log::{error, info, warn};
use serde_json::{self, Error as JsonSerializationError};

use casper_types::{BlockHash, TransactionHash};

use crate::common::{
    db::{
        STORAGE_FILE_NAME,
        databases::{block_body_database, block_header_database, execution_results_database},
        db_env,
    },
    progress::ProgressTracker,
};

use super::{
    Error,
    summary::{ExecutionResultsStats, ExecutionResultsSummary},
};

fn get_execution_results_stats(
    env: Arc<Environment>,
    log_progress: bool,
) -> Result<ExecutionResultsStats, Error> {
    let txn = env.begin_ro_txn()?;
    let block_header_db = block_header_database();
    let block_body_db = block_body_database();
    let metadata_db = execution_results_database();

    let entry_count = block_header_db.entry_count(&txn).map_err(Error::from)?;
    let mut maybe_progress_tracker = None;
    let mut stats = ExecutionResultsStats::default();
    if log_progress {
        match ProgressTracker::new(
            entry_count,
            Box::new(|completion| info!("Database parsing {}% complete...", completion)),
        ) {
            Ok(progress_tracker) => maybe_progress_tracker = Some(progress_tracker),
            Err(progress_tracker_error) => warn!(
                "Couldn't initialize progress tracker: {}",
                progress_tracker_error
            ),
        }
    }

    // Go through all the block headers in the database.
    for (idx, maybe_result) in (block_header_db.iter_all(&txn).map_err(Error::from)?).enumerate() {
        let (block_hash_raw, block_header) = maybe_result.map_err(Error::from)?;
        // Deserialize the block hash.
        let block_hash = BlockHash::new(
            block_hash_raw
                .as_slice()
                .try_into()
                .map_err(|_| Error::InvalidKey(idx))?,
        );
        // Get the body hash for this block.
        let block_body = block_body_db
            .get(&txn, block_header.body_hash())
            .map_err(Error::from)?;

        if let Some(block_body) = block_body {
            // Set of execution results of this block.
            let mut execution_results = vec![];

            // Go through all the deploys in this block and get the execution
            // result of each one.
            match block_body {
                casper_types::BlockBody::V1(block_body_v1) => {
                    for deploy_hash in block_body_v1.deploy_hashes() {
                        // Get this deploy's metadata.
                        let execution_result = metadata_db
                            .get(&txn, &TransactionHash::Deploy(*deploy_hash))
                            .map_err(Error::from)?;
                        if let Some(execution_result) = execution_result {
                            execution_results.push(execution_result);
                        } else {
                            error!("Not found metadata for deploy {}", deploy_hash);
                            return Err(Error::Database(lmdb::Error::NotFound));
                        };
                    }
                }
                casper_types::BlockBody::V2(block_body_v2) => {
                    for txs in block_body_v2.transactions().values() {
                        for tx_hash in txs {
                            // Get this deploy's metadata.
                            let execution_result =
                                metadata_db.get(&txn, tx_hash).map_err(Error::from)?;
                            if let Some(execution_result) = execution_result {
                                execution_results.push(execution_result);
                            } else {
                                error!("Not found metadata for transaction {}", tx_hash);
                                return Err(Error::Database(lmdb::Error::NotFound));
                            };
                        }
                    }
                }
            }

            // Update the statistics with this block's execution results.
            stats.feed(execution_results)?;

            if let Some(progress_tracker) = maybe_progress_tracker.as_mut() {
                progress_tracker.advance_by(1);
            }
        } else {
            error!("Not found block body for header with hash {}", block_hash);
            return Err(Error::Database(lmdb::Error::NotFound));
        }
    }

    Ok(stats)
}

pub(crate) fn dump_execution_results_summary<W: Write + ?Sized>(
    summary: &ExecutionResultsSummary,
    out_writer: Box<W>,
) -> Result<(), JsonSerializationError> {
    serde_json::to_writer_pretty(out_writer, summary)
}

pub fn execution_results_summary<P1: AsRef<Path>, P2: AsRef<Path>>(
    db_path: P1,
    output: Option<P2>,
    overwrite: bool,
) -> Result<(), Error> {
    let storage_path = db_path.as_ref().join(STORAGE_FILE_NAME);
    let env = db_env(storage_path)?;
    let mut log_progress = false;
    // Validate the output file early so that, in case this fails
    // we don't unnecessarily read the whole database.
    let out_writer: Box<dyn Write> = if let Some(out_path) = output {
        let file = OpenOptions::new()
            .create_new(!overwrite)
            .write(true)
            .open(out_path)?;
        log_progress = true;
        Box::new(file)
    } else {
        Box::new(io::stdout())
    };

    let execution_results_stats = get_execution_results_stats(Arc::new(env), log_progress)?;
    let execution_results_summary: ExecutionResultsSummary = execution_results_stats.into();
    dump_execution_results_summary(&execution_results_summary, out_writer)?;

    Ok(())
}
