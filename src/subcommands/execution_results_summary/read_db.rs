use std::{
    fs::OpenOptions,
    io::{self, Write},
    path::Path,
    result::Result,
};

use lmdb::{Cursor, Environment, Transaction};
use log::{info, warn};
use serde_json::{self, Error as JsonSerializationError};

use casper_node::types::{BlockHash, BlockHeader, DeployMetadata};

use crate::common::{
    db::{
        self, BlockBodyDatabase, BlockHeaderDatabase, Database, DeployMetadataDatabase,
        STORAGE_FILE_NAME,
    },
    lmdb_utils,
    progress::ProgressTracker,
};

use super::{
    block_body::BlockBody,
    summary::{ExecutionResultsStats, ExecutionResultsSummary},
    Error,
};

fn get_execution_results_stats(
    env: &Environment,
    log_progress: bool,
) -> Result<ExecutionResultsStats, Error> {
    let txn = env.begin_ro_txn()?;
    let block_header_db = unsafe { txn.open_db(Some(BlockHeaderDatabase::db_name()))? };
    let block_body_db = unsafe { txn.open_db(Some(BlockBodyDatabase::db_name()))? };
    let deploy_metadata_db = unsafe { txn.open_db(Some(DeployMetadataDatabase::db_name()))? };

    let maybe_entry_count = lmdb_utils::entry_count(&txn, block_header_db).ok();
    let mut maybe_progress_tracker = None;

    let mut stats = ExecutionResultsStats::default();
    if let Ok(mut cursor) = txn.open_ro_cursor(block_header_db) {
        if log_progress {
            match maybe_entry_count {
                Some(entry_count) => {
                    match ProgressTracker::new(
                        entry_count,
                        Box::new(|completion| {
                            info!("Database parsing {}% complete...", completion)
                        }),
                    ) {
                        Ok(progress_tracker) => maybe_progress_tracker = Some(progress_tracker),
                        Err(progress_tracker_error) => warn!(
                            "Couldn't initialize progress tracker: {}",
                            progress_tracker_error
                        ),
                    }
                }
                None => warn!("Unable to count db entries, progress will not be logged."),
            }
        }

        // Go through all the block headers in the database.
        for (idx, (block_hash_raw, raw_val)) in cursor.iter().enumerate() {
            // Deserialize the block hash.
            let block_hash = BlockHash::new(
                block_hash_raw
                    .try_into()
                    .map_err(|_| Error::InvalidKey(idx))?,
            );
            // Deserialize the header.
            let header: BlockHeader = bincode::deserialize(raw_val).map_err(|bincode_err| {
                Error::Parsing(
                    block_hash,
                    BlockHeaderDatabase::db_name().to_string(),
                    bincode_err,
                )
            })?;
            // Get the body hash for this block.
            let block_body_raw = txn.get(block_body_db, header.body_hash())?;
            // Get the body of this block.
            let block_body: BlockBody =
                bincode::deserialize(block_body_raw).map_err(|bincode_err| {
                    Error::Parsing(
                        block_hash,
                        BlockBodyDatabase::db_name().to_string(),
                        bincode_err,
                    )
                })?;

            // Set of execution results of this block.
            let mut execution_results = vec![];

            // Go through all the deploys in this block and get the execution
            // result of each one.
            for deploy_hash in block_body.deploy_hashes() {
                // Get this deploy's metadata.
                let metadata_raw = txn.get(deploy_metadata_db, &deploy_hash)?;
                let mut metadata: DeployMetadata =
                    bincode::deserialize(metadata_raw).map_err(|bincode_err| {
                        Error::Parsing(
                            block_hash,
                            DeployMetadataDatabase::db_name().to_string(),
                            bincode_err,
                        )
                    })?;
                // Extract the execution result of this deploy for the current block.
                if let Some(execution_result) = metadata.execution_results.remove(&block_hash) {
                    // Add it to this block's set of execution results.
                    execution_results.push(execution_result);
                }
            }

            // Update the statistics with this block's execution results.
            stats.feed(execution_results)?;

            if let Some(progress_tracker) = maybe_progress_tracker.as_mut() {
                progress_tracker.advance_by(1);
            }
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
    let env = db::db_env(storage_path)?;
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

    let execution_results_stats = get_execution_results_stats(&env, log_progress)?;
    let execution_results_summary: ExecutionResultsSummary = execution_results_stats.into();
    dump_execution_results_summary(&execution_results_summary, out_writer)?;

    Ok(())
}
