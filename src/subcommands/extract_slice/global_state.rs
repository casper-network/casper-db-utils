use std::{path::Path, result::Result};

use casper_types::Digest;
use log::info;

use crate::subcommands::trie_compact::{
    DEFAULT_MAX_DB_SIZE, copy_state_root, create_execution_engine, load_execution_engine,
};

use super::Error;

/// Transfers the global state under a state root hash from a trie store to a
/// new one.
pub(crate) fn transfer_global_state<P1: AsRef<Path>, P2: AsRef<Path>>(
    source: P1,
    destination: P2,
    state_root_hash: Digest,
) -> Result<(), Error> {
    let max_db_size = DEFAULT_MAX_DB_SIZE
        .parse()
        .expect("should be able to parse max db size");

    // Load the source trie store.
    let (source_state, _env) = load_execution_engine(source, max_db_size, Digest::default(), true)
        .map_err(Error::LoadExecutionEngine)?;
    // Create the destination trie store.
    let (destination_state, _env) = create_execution_engine(destination, max_db_size, true)
        .map_err(Error::CreateExecutionEngine)?;
    info!("Starting transfer process for state root hash {state_root_hash}");
    // Copy the state root along with missing descendants over to the new trie
    // store.
    copy_state_root(state_root_hash, &source_state, &destination_state)
        .map_err(Error::StateRootTransfer)?;
    let dest_env = destination_state.environment();
    if dest_env.is_manual_sync_enabled() {
        dest_env.sync()?;
    }
    Ok(())
}
