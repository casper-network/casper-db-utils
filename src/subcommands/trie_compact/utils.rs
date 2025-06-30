use std::{fs, path::Path, sync::Arc};

use casper_storage::global_state::{
    state::lmdb::LmdbGlobalState, transaction_source::lmdb::LmdbEnvironment,
    trie_store::lmdb::LmdbTrieStore,
};
use log::info;

use casper_types::Digest;
use lmdb::DatabaseFlags;

use crate::common::db::TRIE_STORE_FILE_NAME;

/// LMDB max readers
///
/// The default value is chosen to be the same as the node itself.
const DEFAULT_MAX_READERS: u32 = 512;

/// Loads an existing execution engine.
pub fn load_execution_engine(
    ee_lmdb_path: impl AsRef<Path>,
    default_max_db_size: usize,
    state_root_hash: Digest,
    manual_sync_enabled: bool,
) -> Result<(Arc<LmdbGlobalState>, Arc<LmdbEnvironment>), anyhow::Error> {
    let lmdb_data_file = ee_lmdb_path.as_ref().join(TRIE_STORE_FILE_NAME);
    if !ee_lmdb_path.as_ref().join(TRIE_STORE_FILE_NAME).exists() {
        return Err(anyhow::anyhow!(
            "lmdb data file not found at: {}",
            lmdb_data_file.display()
        ));
    }
    let lmdb_environment =
        create_lmdb_environment(&ee_lmdb_path, default_max_db_size, manual_sync_enabled)?;
    let lmdb_trie_store = Arc::new(LmdbTrieStore::open(&lmdb_environment, None)?);
    let global_state = LmdbGlobalState::new(
        Arc::clone(&lmdb_environment),
        lmdb_trie_store,
        state_root_hash,
        5,
        false,
    );
    Ok((Arc::new(global_state), lmdb_environment))
}

/// Create an lmdb environment at a given path.
fn create_lmdb_environment(
    lmdb_path: impl AsRef<Path>,
    default_max_db_size: usize,
    manual_sync_enabled: bool,
) -> Result<Arc<LmdbEnvironment>, anyhow::Error> {
    let lmdb_environment = Arc::new(LmdbEnvironment::new(
        &lmdb_path,
        default_max_db_size,
        DEFAULT_MAX_READERS,
        manual_sync_enabled,
    )?);
    Ok(lmdb_environment)
}

/// Creates a new execution engine.
pub fn create_execution_engine(
    ee_lmdb_path: impl AsRef<Path>,
    default_max_db_size: usize,
    manual_sync_enabled: bool,
) -> Result<(Arc<LmdbGlobalState>, Arc<LmdbEnvironment>), anyhow::Error> {
    if !ee_lmdb_path.as_ref().exists() {
        info!(
            "creating new lmdb data dir {}",
            ee_lmdb_path.as_ref().display()
        );
        fs::create_dir_all(&ee_lmdb_path)?;
    }
    fs::create_dir_all(&ee_lmdb_path)?;
    let lmdb_environment =
        create_lmdb_environment(&ee_lmdb_path, default_max_db_size, manual_sync_enabled)?;
    lmdb_environment.env().sync(true)?;

    let _db = lmdb_environment
        .env()
        .create_db(None, DatabaseFlags::empty())?;

    let lmdb_trie_store = Arc::new(LmdbTrieStore::new(
        &lmdb_environment,
        None,
        DatabaseFlags::empty(),
    )?);
    let global_state =
        LmdbGlobalState::empty(Arc::clone(&lmdb_environment), lmdb_trie_store, 5, false)?;

    Ok((Arc::new(global_state), lmdb_environment))
}
