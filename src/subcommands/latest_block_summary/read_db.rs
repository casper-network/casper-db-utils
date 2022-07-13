use std::{
    fs::OpenOptions,
    io::{self, Write},
    path::Path,
    result::Result,
};

use lmdb::{Cursor, Environment, Transaction};
use log::warn;
use serde_json::{self, Error as SerializationError};

use casper_node::types::BlockHeader;

use crate::common::db::{self, BlockHeaderDatabase, Database};

use super::{
    block_info::{parse_network_name, BlockInfo},
    Error,
};

fn get_highest_block(env: &Environment) -> Result<BlockHeader, Error> {
    let txn = env.begin_ro_txn()?;
    let db = unsafe { txn.open_db(Some(BlockHeaderDatabase::db_name()))? };

    let mut max_height = 0u64;
    let mut max_height_key = None;
    if let Ok(mut cursor) = txn.open_ro_cursor(db) {
        for (idx, (raw_key, raw_val)) in cursor.iter().enumerate() {
            let header: BlockHeader = bincode::deserialize(raw_val)
                .map_err(|bincode_err| Error::Parsing(idx, bincode_err))?;
            if header.height() >= max_height {
                max_height = header.height();
                let _ = max_height_key.replace(raw_key);
            }
        }
    }

    let max_height_key = max_height_key.ok_or(Error::EmptyDatabase)?;
    let raw_bytes = txn.get(db, &max_height_key)?;
    let highest_block_header: BlockHeader =
        bincode::deserialize(raw_bytes).map_err(|bincode_err| {
            Error::Parsing(
                max_height
                    .try_into()
                    .expect("block height doesn't fit in usize"),
                bincode_err,
            )
        })?;

    Ok(highest_block_header)
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
) -> Result<(), Error> {
    let env = db::db_env(db_path.as_ref())?;
    // Validate the output file early so that, in case this fails
    // we don't unnecessarily read the whole database.
    let out_writer: Box<dyn Write> = if let Some(out_path) = output {
        let file = OpenOptions::new()
            .create_new(!overwrite)
            .write(true)
            .open(out_path)?;
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

    let highest_block = get_highest_block(&env)?;
    let block_info = BlockInfo::new(network_name, highest_block);
    dump_block_info(&block_info, out_writer)?;

    Ok(())
}
