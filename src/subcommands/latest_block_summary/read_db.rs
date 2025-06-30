use std::{
    fs::OpenOptions,
    io::{self, Write},
    path::Path,
    result::Result,
    sync::Arc,
};

use lmdb::Environment;
use log::{info, warn};
use serde_json::{self, Error as SerializationError};

use casper_types::{BlockHash, BlockHeader};

use crate::common::{
    db::{self, STORAGE_FILE_NAME, databases::block_header_database},
    progress::ProgressTracker,
};

use super::{
    Error,
    block_info::{BlockInfo, parse_network_name},
};

fn get_highest_block(
    env: Arc<Environment>,
    log_progress: bool,
    must_be_switch_block: bool,
) -> Result<(BlockHash, BlockHeader), Error> {
    let block_header_db = block_header_database();
    let txn = env.begin_ro_txn()?;

    let mut max_height = 0u64;
    let mut max_height_header = None;

    let entry_count = block_header_db.entry_count(&txn).map_err(Error::from)?;
    let mut maybe_progress_tracker = None;

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

    for maybe_result in block_header_db.iter_all(&txn).map_err(Error::from)? {
        let (_, header) = maybe_result.map_err(Error::from)?;

        if let Some(progress_tracker) = maybe_progress_tracker.as_mut() {
            progress_tracker.advance_by(1);
        }
        if must_be_switch_block && !header.is_switch_block() {
            continue;
        }

        if header.height() >= max_height {
            max_height = header.height();
            let _ = max_height_header.replace(header);
        }
    }
    let max_height_header = max_height_header.ok_or(Error::EmptyDatabase)?;
    let block_hash = max_height_header.block_hash();
    Ok((block_hash, max_height_header))
}

pub(crate) fn dump_block_info<W: Write + ?Sized>(
    block_header: &BlockInfo,
    out_writer: Box<W>,
) -> Result<(), SerializationError> {
    serde_json::to_writer_pretty(out_writer, block_header)
}

pub fn latest_block_summary<P1: AsRef<Path>, P2: AsRef<Path>>(
    db_path: P1,
    output: Option<P2>,
    overwrite: bool,
    must_be_era_end: bool,
) -> Result<(), Error> {
    let storage_path = db_path.as_ref().join(STORAGE_FILE_NAME);
    let env = Arc::new(db::db_env(storage_path)?);
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
    let network_name = match parse_network_name(db_path) {
        Ok(name) => Some(name),
        Err(io_err) => {
            warn!("Couldn't derive network name from path: {}", io_err);
            None
        }
    };

    let (block_hash, highest_block) = get_highest_block(env, log_progress, must_be_era_end)?;
    let block_info = BlockInfo::new(network_name, block_hash, highest_block);
    dump_block_info(&block_info, out_writer)?;

    Ok(())
}
