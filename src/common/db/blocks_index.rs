use casper_types::{BlockHash, BlockHeader};
use lmdb::Environment;
use std::cell::OnceCell;
use std::{collections::BTreeMap, sync::Arc};

use crate::common::db::Error;
use crate::common::db::databases::block_header_database;
use crate::common::db::versioned_database::VersionedDatabases;

pub(crate) struct BlocksIndex {
    height_to_hash: BTreeMap<u64, BlockHash>,
    initialized: bool,
    environment: Arc<Environment>,
    blocks_database: OnceCell<VersionedDatabases<BlockHash, BlockHeader>>,
}

impl BlocksIndex {
    pub(crate) fn new(environment: Arc<Environment>) -> Self {
        Self {
            height_to_hash: BTreeMap::new(),
            initialized: false,
            environment: environment.clone(),
            blocks_database: OnceCell::new(),
        }
    }

    /// Performs indexing of the blocks. This can potentially be a long lasting process
    pub(crate) fn initialize(&mut self) -> Result<(), Error> {
        // TODO incorporate logging
        let block_header_database = block_header_database();
        let txn = self.environment.begin_ro_txn()?;
        for res in block_header_database.iter_all(&txn)? {
            let (_raw_key, header) = res?;
            let height = header.height();
            let hash = header.block_hash();
            self.height_to_hash.insert(height, hash);
        }
        self.initialized = true;
        Ok(())
    }

    fn get_block_header_database(&self) -> &VersionedDatabases<BlockHash, BlockHeader> {
        self.blocks_database.get_or_init(block_header_database)
    }

    pub(crate) fn heighest_block_header(&self) -> Result<Option<BlockHeader>, Error> {
        let txn = self.environment.begin_ro_txn()?;
        match self.height_to_hash.iter().next_back() {
            Some((_, hash)) => self.get_block_header_database().get(&txn, hash),
            None => Ok(None),
        }
    }
    pub(crate) fn read_block_header_by_height(
        &self,
        height: u64,
    ) -> Result<Option<BlockHeader>, Error> {
        let txn = self.environment.begin_ro_txn()?;
        match self.height_to_hash.get(&height) {
            Some(hash) => self.get_block_header_database().get(&txn, hash),
            None => Ok(None),
        }
    }
}
