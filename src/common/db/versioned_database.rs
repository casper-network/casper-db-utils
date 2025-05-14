use crate::common::{
    db::Error,
    structs::{ApprovalsHashes, DeployMetadataV1, LegacyApprovalsHashes, Transfers},
};
use casper_types::{
    Approval, BlockBody, BlockBodyV1, BlockHash, BlockHeader, BlockHeaderV1, BlockSignatures,
    BlockSignaturesV1, Deploy, DeployHash, Digest, Transaction, TransactionHash, Transfer,
    TransferV1,
    bytesrepr::{FromBytes, ToBytes},
    execution::ExecutionResult,
};
use lmdb::{
    Cursor, DatabaseFlags, Environment, Error as LmdbError, Iter, RoCursor, RoTransaction,
    RwCursor, RwTransaction, Transaction as LmdbTransaction, WriteFlags,
};
use lmdb_sys::{MDB_stat, MDB_txn, mdb_stat};
use log::{error, info};
use serde::{Serialize, de::DeserializeOwned};
use std::{any::TypeId, collections::BTreeSet, marker::PhantomData};
use std::{result::Result, sync::Arc};

const ENTRY_LOG_INTERVAL: usize = 100_000;

#[cfg(test)]
pub(crate) use tests::MockData;
/// A pair of databases, one holding the original legacy form of the data, and the other holding the
/// new versioned, future-proof form of the data.
///
/// Specific entries should generally not be repeated - they will either be held in the legacy or
/// the current DB, but not both.  Data is not migrated from legacy to current, but newly-stored
/// data will always be written to the current DB, even if it is of the type `V::Legacy`.
///
/// Exceptions to this can occur if a pre-existing legacy entry is re-stored, in which case there
/// will be a duplicated entry in the `legacy` and `current` DBs.  This should not be a common
/// occurrence though.
#[derive(Eq, PartialEq, Debug, Clone)]
pub(crate) struct VersionedDatabases<K, V> {
    legacy_database_name: String,
    current_database_name: String,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> VersionedDatabases<K, V>
where
    K: VersionedKey + std::fmt::Display,
    V: VersionedValue + 'static,
{
    pub(crate) fn new(legacy_name: &str, current_name: &str) -> Self {
        VersionedDatabases {
            legacy_database_name: legacy_name.to_owned(),
            current_database_name: current_name.to_owned(),
            _phantom: PhantomData,
        }
    }

    /// Iterates every row in the legacy database, deserializing the value and calling `f` with the
    /// cursor and the parsed value.
    pub(crate) fn for_each_value_in_legacy<'a, F>(
        &self,
        txn: &'a mut RwTransaction,
        f: &mut F,
        skip_n: usize,
        failfast: bool,
    ) -> Result<(), Error>
    where
        F: FnMut(&mut RwCursor<'a>, V) -> Result<(), Error>,
    {
        let legacy_db = unsafe { txn.open_db(Some(&self.legacy_database_name))? };
        let mut errs = Vec::new();
        let mut cursor = txn.open_rw_cursor(legacy_db).map_err(Error::Database)?;
        for r in cursor.iter().skip(skip_n) {
            if let Err(e) = r {
                if failfast {
                    return Err(Error::Database(e));
                } else {
                    errs.push(Error::Database(e));
                    continue;
                }
            };
            let (_, raw_val) = r.unwrap();
            let maybe_value: Result<V::Legacy, Error> = deserialize(raw_val);
            match maybe_value {
                Ok(value) => match f(&mut cursor, value.into()) {
                    Ok(_) => {}
                    Err(err) if failfast => return Err(err),
                    Err(err) => errs.push(err),
                },
                Err(err) if failfast => {
                    return Err(err);
                }
                Err(err) => errs.push(err),
            }
        }
        if !failfast && !errs.is_empty() {
            return Err(Error::Accumulated(errs));
        }
        Ok(())
    }

    /// Iterates every row in the current database, deserializing the value and calling `f` with the
    /// cursor and the parsed value.
    pub(crate) fn for_each_value_in_current<'a, F>(
        &self,
        txn: &'a mut RwTransaction,
        f: &mut F,
        skip_n: usize,
        failfast: bool,
    ) -> Result<(), Error>
    where
        F: FnMut(&mut RwCursor<'a>, V) -> Result<(), Error>,
    {
        let current_db = unsafe { txn.open_db(Some(&self.current_database_name))? };
        let mut errs = Vec::new();
        let mut cursor = txn.open_rw_cursor(current_db).map_err(Error::Database)?;
        for r in cursor.iter().skip(skip_n) {
            if let Err(e) = r {
                if failfast {
                    return Err(Error::Database(e));
                } else {
                    errs.push(Error::Database(e));
                    continue;
                }
            };
            let (_, raw_val) = r.unwrap();
            let maybe_value: Result<V, Error> = deserialize_bytesrepr(raw_val);
            match maybe_value {
                Ok(value) => match f(&mut cursor, value) {
                    Ok(_) => {}
                    Err(err) if failfast => return Err(err),
                    Err(err) => errs.push(err),
                },
                Err(err) if failfast => {
                    return Err(err);
                }
                Err(err) => errs.push(err),
            }
        }
        if !failfast && !errs.is_empty() {
            return Err(Error::Accumulated(errs));
        }
        Ok(())
    }

    fn entry_count_from_database<T: LmdbTransaction>(
        &self,
        database: lmdb::Database,
        txn: &'_ T,
    ) -> Result<usize, Error> {
        let mut stat = MDB_stat {
            ms_psize: 0,
            ms_depth: 0,
            ms_branch_pages: 0,
            ms_leaf_pages: 0,
            ms_overflow_pages: 0,
            ms_entries: 0,
        };
        let b = database.dbi();
        let result = unsafe { mdb_stat(txn.txn() as *mut MDB_txn, b, &mut stat as *mut MDB_stat) };
        if result != 0 {
            Err(Error::Database(LmdbError::from_err_code(result)))
        } else {
            Ok(stat.ms_entries)
        }
    }

    pub(crate) fn legacy_entry_count<T: LmdbTransaction>(
        &self,
        txn: &'_ T,
    ) -> Result<usize, Error> {
        let legacy_db = unsafe { txn.open_db(Some(&self.legacy_database_name))? };
        self.entry_count_from_database(legacy_db, txn)
    }

    pub(crate) fn current_entry_count<T: LmdbTransaction>(
        &self,
        txn: &'_ T,
    ) -> Result<usize, Error> {
        let current_db = unsafe { txn.open_db(Some(&self.current_database_name))? };
        self.entry_count_from_database(current_db, txn)
    }

    pub(crate) fn entry_count<T: LmdbTransaction>(&self, txn: &'_ T) -> Result<usize, Error> {
        Ok(self.legacy_entry_count(txn)? + self.current_entry_count(txn)?)
    }

    pub(crate) fn legacy_cursor<'a>(&self, txn: &'a RoTransaction) -> Result<RoCursor<'a>, Error> {
        let legacy_db = unsafe { txn.open_db(Some(&self.legacy_database_name))? };
        txn.open_ro_cursor(legacy_db).map_err(Error::Database)
    }

    pub(crate) fn current_cursor<'a>(&self, txn: &'a RoTransaction) -> Result<RoCursor<'a>, Error> {
        let current_db = unsafe { txn.open_db(Some(&self.current_database_name))? };
        txn.open_ro_cursor(current_db).map_err(Error::Database)
    }

    pub(crate) fn get<Tx: LmdbTransaction>(&self, txn: &Tx, key: &K) -> Result<Option<V>, Error> {
        let current_db = unsafe { txn.open_db(Some(&self.current_database_name))? };
        let serialized_key = serialize_bytesrepr(key)?;
        match txn.get(current_db, &serialized_key) {
            Ok(raw_value) => deserialize_bytesrepr(raw_value).map(Some),
            Err(lmdb::Error::NotFound) => {
                //look in legacy db
                let maybe_legacy_key = key.legacy_key();
                match maybe_legacy_key {
                    Some(legacy_key) => {
                        let legacy_db = unsafe { txn.open_db(Some(&self.legacy_database_name))? };
                        let raw_val = match txn.get(legacy_db, &legacy_key) {
                            Ok(raw_value) => raw_value,
                            Err(lmdb::Error::NotFound) => return Ok(None),
                            Err(e) => return Err(Error::Database(e)),
                        };
                        let try_val: Result<V::Legacy, Error> = deserialize(raw_val);
                        try_val.map(|v| Some(v.into()))
                    }
                    None => Ok(None),
                }
            }
            Err(e) => Err(Error::Database(e)),
        }
    }

    pub fn del(&self, txn: &mut RwTransaction, key: &K) -> Result<bool, Error> {
        let current_db = unsafe { txn.open_db(Some(&self.current_database_name))? };
        let serialized_key = serialize_bytesrepr(key)?;
        match txn.del(current_db, &serialized_key, None) {
            Ok(()) => Ok(true),
            Err(lmdb::Error::NotFound) => {
                let legacy_db = unsafe { txn.open_db(Some(&self.legacy_database_name))? };
                let maybe_legacy_key = key.legacy_key();
                match maybe_legacy_key {
                    Some(legacy_key) => match txn.del(legacy_db, legacy_key, None) {
                        Ok(()) => Ok(true),
                        Err(lmdb::Error::NotFound) => Ok(false),
                        Err(e) => Err(Error::Database(e)),
                    },
                    None => Ok(false),
                }
            }
            Err(e) => Err(Error::Database(e)),
        }
    }

    pub fn transfer_to_other_database<Tx: LmdbTransaction>(
        &self,
        keys: Vec<K>,
        source_txn: &Tx,
        destination_txn: &mut RwTransaction,
        other_db: &VersionedDatabases<K, V>,
    ) -> Result<(), Error> {
        let source_current_db = unsafe { source_txn.open_db(Some(&self.current_database_name))? };
        let destination_current_db =
            unsafe { destination_txn.open_db(Some(&other_db.current_database_name))? };

        let source_legacy_db = unsafe { source_txn.open_db(Some(&self.legacy_database_name))? };
        let destination_legacy_db =
            unsafe { destination_txn.open_db(Some(&other_db.legacy_database_name))? };
        for key in keys {
            let serialized_key = serialize_bytesrepr(&key)?;
            match source_txn.get(source_current_db, &serialized_key) {
                Ok(raw_value) => {
                    match destination_txn.put(
                        destination_current_db,
                        &serialized_key,
                        &raw_value,
                        WriteFlags::default(),
                    ) {
                        Ok(_) => (),
                        Err(e) => return Err(Error::Database(e)),
                    };
                }
                Err(lmdb::Error::NotFound) => {
                    let legacy_key = key.legacy_key().unwrap();
                    match source_txn.get(source_legacy_db, &legacy_key) {
                        Ok(raw_value) => {
                            match destination_txn.put(
                                destination_legacy_db,
                                &legacy_key,
                                &raw_value,
                                WriteFlags::default(),
                            ) {
                                Ok(_) => (),
                                Err(e) => return Err(Error::Database(e)),
                            };
                        }
                        Err(e) => return Err(Error::Database(e)),
                    }
                }
                Err(e) => return Err(Error::Database(e)),
            }
        }
        Ok(())
    }

    pub(crate) fn create(&self, env: Arc<Environment>) -> Result<(), Error> {
        env.create_db(Some(&self.legacy_database_name), DatabaseFlags::empty())?;
        env.create_db(Some(&self.current_database_name), DatabaseFlags::empty())?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn put_raw(
        &self,
        txn: &mut RwTransaction,
        key: Vec<u8>,
        value: Vec<u8>,
        put_to_legacy_db: bool,
    ) -> Result<bool, Error> {
        let db_name = if put_to_legacy_db {
            &self.legacy_database_name
        } else {
            &self.current_database_name
        };
        let db = unsafe { txn.open_db(Some(db_name))? };
        match txn.put(db, &key, &value, WriteFlags::default()) {
            Ok(_) => Ok(true),
            Err(e) => Err(Error::Database(e)),
        }
    }

    pub fn iter_all<'a>(
        &'a self,
        txn: &'a RoTransaction<'a>,
    ) -> Result<VersionedDatabasesIterator<'a, K, V>, Error> {
        let mut cursor = self.legacy_cursor(txn)?;
        let iter = cursor.iter();
        Ok(VersionedDatabasesIterator::LegacyIterator {
            inner_transaction: txn,
            versioned_database: self,
            iter,
        })
    }

    /// Validates the database by ensuring every value of an entry can be parsed.
    pub fn check_dbs(
        &self,
        env: Arc<Environment>,
        start_at: usize,
        failfast: bool,
    ) -> Result<(), Error> {
        info!("Checking {} database.", self.legacy_database_name);
        let mut x = start_at;
        let mut visitor = |_cursor: &mut RwCursor, _v: V| {
            //Just looping here
            x += 1;
            if x % ENTRY_LOG_INTERVAL == 0 {
                info!("Parsed {} entries...", x);
            }

            Ok(())
        };
        let mut txn = env.begin_rw_txn()?;
        //TODO change this with an iterator
        self.for_each_value_in_legacy(&mut txn, &mut visitor, start_at, failfast)?;
        info!(
            "Found {} records in {} database.",
            x.saturating_sub(start_at),
            self.legacy_database_name
        );
        let mut x = start_at;
        let mut visitor = |_cursor: &mut RwCursor, _: V| {
            //Just looping here
            x += 1;
            if x % ENTRY_LOG_INTERVAL == 0 {
                info!("Parsed {} entries...", x);
            }

            Ok(())
        };
        info!("Checking {} database.", self.current_database_name);
        //TODO change this with an iterator
        self.for_each_value_in_current(&mut txn, &mut visitor, start_at, failfast)?;
        info!(
            "Found {} records in {} database.",
            x.saturating_sub(start_at),
            self.current_database_name
        );
        Ok(())
    }
}

impl<K, V> VersionedDatabases<K, V>
where
    K: VersionedKey + std::fmt::Display,
    V: UpsertableValue + 'static,
{
    /// Puts value to versioned databases. The `put_legacy_to_legacy_db`
    /// flag forces that a legacy-key based value will be put into the
    /// legacy database
    pub fn put(
        &self,
        txn: &mut RwTransaction,
        key: K,
        value: V,
        put_legacy_to_legacy_db: bool,
    ) -> Result<bool, Error> {
        if put_legacy_to_legacy_db {
            if let Some(legacy_value) = value.legacy_value() {
                let legacy_key = key.legacy_key().unwrap();
                let legacy_db = unsafe { txn.open_db(Some(&self.legacy_database_name))? };
                let raw_data = serialize(&legacy_value)?;
                match txn.put(legacy_db, legacy_key, &raw_data, WriteFlags::default()) {
                    Ok(_) => return Ok(true),
                    Err(e) => return Err(Error::Database(e)),
                }
            }
        }
        let current_db = unsafe { txn.open_db(Some(&self.current_database_name))? };
        let key_raw = serialize_bytesrepr(&key)?;
        let data_raw = serialize_bytesrepr(&value)?;
        match txn.put(current_db, &key_raw, &data_raw, WriteFlags::default()) {
            Ok(_) => Ok(true),
            Err(e) => Err(Error::Database(e)),
        }
    }
}

pub(crate) enum VersionedDatabasesIterator<'txn, K, V> {
    LegacyIterator {
        inner_transaction: &'txn RoTransaction<'txn>,
        iter: Iter<'txn>,
        versioned_database: &'txn VersionedDatabases<K, V>,
    },
    CurrentIterator {
        iter: Iter<'txn>,
    },
}

impl<K, V> Iterator for VersionedDatabasesIterator<'_, K, V>
where
    K: VersionedKey + std::fmt::Display,
    V: VersionedValue + 'static,
{
    type Item = Result<(Vec<u8>, V), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            VersionedDatabasesIterator::LegacyIterator {
                versioned_database,
                inner_transaction,
                iter,
            } => match iter.next() {
                None | Some(Err(lmdb::Error::NotFound)) => {
                    match versioned_database.current_cursor(inner_transaction) {
                        Ok(mut cursor) => {
                            let mut iter = cursor.iter();
                            match iter.next() {
                                None | Some(Err(lmdb::Error::NotFound)) => None,
                                Some(Ok((raw_key, raw_value))) => {
                                    *self = VersionedDatabasesIterator::CurrentIterator { iter };
                                    let maybe_value: Result<V, Error> =
                                        deserialize_bytesrepr(raw_value);
                                    Some(maybe_value.map(|v| (raw_key.into(), v)))
                                }
                                Some(Err(e)) => Some(Err(Error::Database(e))),
                            }
                        }
                        Err(e) => Some(Err(e)),
                    }
                }

                Some(Ok((raw_key, raw_val))) => {
                    let maybe_value: Result<V::Legacy, Error> = deserialize(raw_val);
                    Some(maybe_value.map(|v| (raw_key.into(), v.into())))
                }
                Some(Err(e)) => Some(Err(Error::Database(e))),
            },
            VersionedDatabasesIterator::CurrentIterator { iter } => match iter.next() {
                Some(Ok((raw_key, raw_value))) => {
                    let maybe_value: Result<V, Error> = deserialize_bytesrepr(raw_value);
                    Some(maybe_value.map(|v| (raw_key.into(), v)))
                }
                None | Some(Err(lmdb::Error::NotFound)) => None,
                Some(Err(e)) => Some(Err(Error::Database(e))),
            },
        }
    }
}

#[inline(always)]
fn serialize_bytesrepr<T: ToBytes>(value: &T) -> Result<Vec<u8>, Error> {
    value.to_bytes().map_err(Error::Bytesrepr)
}

pub(crate) fn deserialize_bytesrepr<T: FromBytes + 'static>(raw: &[u8]) -> Result<T, Error> {
    match T::from_bytes(raw).map(|val| val.0) {
        Ok(ret) => Ok(ret),
        Err(err) => {
            // unfortunately, type_name is unstable
            let type_name = {
                if TypeId::of::<DeployMetadataV1>() == TypeId::of::<T>() {
                    "DeployMetadataV1".to_string()
                } else if TypeId::of::<BlockHeader>() == TypeId::of::<T>() {
                    "BlockHeader".to_string()
                } else if TypeId::of::<BlockBody>() == TypeId::of::<T>() {
                    "BlockBody".to_string()
                } else if TypeId::of::<BlockSignatures>() == TypeId::of::<T>() {
                    "BlockSignatures".to_string()
                } else if TypeId::of::<DeployHash>() == TypeId::of::<T>() {
                    "DeployHash".to_string()
                } else if TypeId::of::<Deploy>() == TypeId::of::<T>() {
                    "Deploy".to_string()
                } else if TypeId::of::<ApprovalsHashes>() == TypeId::of::<T>() {
                    "ApprovalsHashes".to_string()
                } else if TypeId::of::<BTreeSet<Approval>>() == TypeId::of::<T>() {
                    "BTreeSet<Approval>".to_string()
                } else if TypeId::of::<ExecutionResult>() == TypeId::of::<T>() {
                    "ExecutionResult".to_string()
                } else if TypeId::of::<Vec<Transfer>>() == TypeId::of::<T>() {
                    "Transfers".to_string()
                } else {
                    format!("{:?}", TypeId::of::<T>())
                }
            };
            error!("deserialize_bytesrepr failed to deserialize: {}", type_name);
            Err(Error::Bytesrepr(err))
        }
    }
}

/// Deserializes from a buffer.
#[inline(always)]
pub(crate) fn deserialize<T: DeserializeOwned + 'static>(raw: &[u8]) -> Result<T, Error> {
    match bincode::deserialize(raw) {
        Ok(value) => Ok(value),
        Err(err) => Err(Error::Bincode(err)),
    }
}

/// Serializes to buffer.
#[inline(always)]
pub(crate) fn serialize<T: Serialize + 'static>(to_serialize: &T) -> Result<Vec<u8>, Error> {
    match bincode::serialize(to_serialize) {
        Ok(value) => Ok(value),
        Err(err) => Err(Error::Bincode(err)),
    }
}

pub(crate) trait VersionedKey: ToBytes {
    type Legacy: AsRef<[u8]>;

    fn legacy_key(&self) -> Option<&Self::Legacy>;
}

pub(crate) trait VersionedValue: ToBytes + FromBytes {
    type Legacy: 'static + Serialize + DeserializeOwned + Into<Self>;
}

impl VersionedValue for Transaction {
    type Legacy = Deploy;
}

/// Trait to implement for values that need to use [`VersionedDatabases::put`]
pub(crate) trait UpsertableValue: ToBytes + FromBytes {
    type Legacy: 'static + Serialize + DeserializeOwned + Into<Self>;
    fn legacy_value(&self) -> Option<Self::Legacy>;
}

impl VersionedKey for TransactionHash {
    type Legacy = DeployHash;

    fn legacy_key(&self) -> Option<&Self::Legacy> {
        match self {
            TransactionHash::Deploy(deploy_hash) => Some(deploy_hash),
            TransactionHash::V1(_) => None,
        }
    }
}

impl VersionedKey for BlockHash {
    type Legacy = BlockHash;

    fn legacy_key(&self) -> Option<&Self::Legacy> {
        Some(self)
    }
}

impl VersionedKey for Digest {
    type Legacy = Digest;

    fn legacy_key(&self) -> Option<&Self::Legacy> {
        Some(self)
    }
}

impl VersionedValue for BlockHeader {
    type Legacy = BlockHeaderV1;
}

impl VersionedValue for BlockBody {
    type Legacy = BlockBodyV1;
}

impl VersionedValue for ApprovalsHashes {
    type Legacy = LegacyApprovalsHashes;
}

impl VersionedValue for ExecutionResult {
    type Legacy = DeployMetadataV1;
}

impl VersionedValue for BTreeSet<Approval> {
    type Legacy = BTreeSet<Approval>;
}

impl VersionedValue for BlockSignatures {
    type Legacy = BlockSignaturesV1;
}

impl UpsertableValue for BlockSignatures {
    type Legacy = BlockSignaturesV1;
    fn legacy_value(&self) -> Option<Self::Legacy> {
        match self {
            BlockSignatures::V1(block_signatures_v1) => Some(block_signatures_v1.clone()),
            BlockSignatures::V2(_) => None,
        }
    }
}

impl VersionedValue for Transfers {
    type Legacy = Vec<TransferV1>;
}

#[cfg(test)]
mod tests {
    use super::{UpsertableValue, VersionedValue};
    use crate::{common::db::databases::mock_database, test_utils::LmdbTestFixture};
    use casper_types::{
        DeployHash, TransactionHash, TransactionV1Hash,
        bytesrepr::{self, FromBytes, ToBytes, U8_SERIALIZED_LENGTH},
    };
    use lmdb::Transaction;
    use rand::{Rng, RngCore, distr::Alphanumeric, rngs::ThreadRng};
    use serde::{Deserialize, Serialize};

    #[test]
    fn db_entry_count() {
        let mut rng = rand::rng();
        let fixture = LmdbTestFixture::new(None);
        let env = &fixture.env;
        let db = mock_database();
        db.create(env.clone()).unwrap();

        if let Ok(txn) = env.begin_ro_txn() {
            assert_eq!(db.entry_count(&txn).unwrap(), 0);
            txn.commit().unwrap();
        }

        // Insert the first entry into the database.
        let mut txn = env.begin_rw_txn().unwrap();
        let (key, data) = MockData::random(&mut rng);
        let first_key = key;
        db.put(&mut txn, key, data, true).unwrap();
        txn.commit().unwrap();

        if let Ok(txn) = env.begin_ro_txn() {
            assert_eq!(db.entry_count(&txn).unwrap(), 1);
            txn.commit().unwrap();
        }

        // Insert the second entry into the database.
        if let Ok(mut txn) = env.begin_rw_txn() {
            let (key, data) = MockData::random(&mut rng);
            db.put(&mut txn, key, data, true).unwrap();
            txn.commit().unwrap();
        };

        if let Ok(txn) = env.begin_ro_txn() {
            assert_eq!(db.entry_count(&txn).unwrap(), 2);
            txn.commit().unwrap();
        }

        // Delete the first entry from the database.
        if let Ok(mut txn) = env.begin_rw_txn() {
            db.del(&mut txn, &first_key).unwrap();
            txn.commit().unwrap();
        };

        if let Ok(txn) = env.begin_ro_txn() {
            assert_eq!(db.entry_count(&txn).unwrap(), 1);
            txn.commit().unwrap();
        }
    }

    const LEGACY_TAG: u8 = 0;
    const CURRENT_TAG: u8 = 1;

    #[derive(Clone, Serialize, Deserialize, Eq, PartialEq)]
    pub(crate) struct MockLegacyStruct {
        pub(crate) x: u32,
    }

    impl ToBytes for MockLegacyStruct {
        fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
            let mut buffer = bytesrepr::allocate_buffer(self)?;
            self.x.write_bytes(&mut buffer)?;
            Ok(buffer)
        }

        fn serialized_length(&self) -> usize {
            self.x.serialized_length()
        }
    }

    impl FromBytes for MockLegacyStruct {
        fn from_bytes(bytes: &[u8]) -> Result<(Self, &[u8]), bytesrepr::Error> {
            let (x, remainder) = u32::from_bytes(bytes)?;
            Ok((MockLegacyStruct { x }, remainder))
        }
    }

    #[derive(Eq, PartialEq)]
    pub(crate) enum MockData {
        Legacy(MockLegacyStruct),
        Current(String),
    }

    impl MockData {
        pub(crate) fn random(rng: &mut ThreadRng) -> (TransactionHash, MockData) {
            let mut key_bytes = [0u8; 32];
            rng.fill_bytes(&mut key_bytes);
            if rng.random_bool(0.5) {
                let rand_string: String = rng
                    .sample_iter(&Alphanumeric)
                    .take(30) // specify the length of the string
                    .map(char::from)
                    .collect();
                (
                    TransactionHash::V1(TransactionV1Hash::from_raw(key_bytes)),
                    MockData::Current(rand_string),
                )
            } else {
                let rand_u32 = rng.next_u32();
                (
                    TransactionHash::Deploy(DeployHash::from_raw(key_bytes)),
                    MockData::Legacy(MockLegacyStruct { x: rand_u32 }),
                )
            }
        }
    }

    impl FromBytes for MockData {
        fn from_bytes(bytes: &[u8]) -> Result<(Self, &[u8]), bytesrepr::Error> {
            let (tag, remainder) = u8::from_bytes(bytes)?;
            match tag {
                LEGACY_TAG => {
                    let (x, remainder) = MockLegacyStruct::from_bytes(remainder)?;
                    Ok((MockData::Legacy(x), remainder))
                }
                CURRENT_TAG => {
                    let (x, remainder) = String::from_bytes(remainder)?;
                    Ok((MockData::Current(x), remainder))
                }
                _ => Err(bytesrepr::Error::Formatting),
            }
        }
    }
    impl VersionedValue for MockData {
        type Legacy = MockLegacyStruct;
    }

    impl From<MockLegacyStruct> for MockData {
        fn from(value: MockLegacyStruct) -> Self {
            MockData::Legacy(value)
        }
    }

    impl ToBytes for MockData {
        fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
            let mut buffer = bytesrepr::allocate_buffer(self)?;
            self.write_bytes(&mut buffer)?;
            Ok(buffer)
        }
        fn write_bytes(&self, writer: &mut Vec<u8>) -> Result<(), bytesrepr::Error> {
            match self {
                MockData::Legacy(x) => {
                    LEGACY_TAG.write_bytes(writer)?;
                    x.write_bytes(writer)
                }
                MockData::Current(x) => {
                    CURRENT_TAG.write_bytes(writer)?;
                    x.write_bytes(writer)
                }
            }
        }

        fn serialized_length(&self) -> usize {
            U8_SERIALIZED_LENGTH
                + match &self {
                    MockData::Legacy(x) => x.serialized_length(),
                    MockData::Current(x) => x.serialized_length(),
                }
        }
    }

    impl UpsertableValue for MockData {
        type Legacy = MockLegacyStruct;

        fn legacy_value(&self) -> Option<MockLegacyStruct> {
            match self {
                MockData::Legacy(mock_legacy_struct) => Some(mock_legacy_struct.clone()),
                MockData::Current(_) => None,
            }
        }
    }
}
