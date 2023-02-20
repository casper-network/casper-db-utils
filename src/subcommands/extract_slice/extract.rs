use std::path::Path;

use casper_node::types::BlockHash;

use super::{global_state, storage, Error};

pub fn extract_slice<P1: AsRef<Path>, P2: AsRef<Path>>(
    db_path: P1,
    output: P2,
    block_hash: BlockHash,
) -> Result<(), Error> {
    storage::create_output_db(&output)?;
    let state_root_hash = storage::transfer_block_info(&db_path, &output, block_hash)?;
    global_state::transfer_global_state(&db_path, &output, state_root_hash)?;
    Ok(())
}
