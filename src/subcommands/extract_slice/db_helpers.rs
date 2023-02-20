use std::result::Result;

use lmdb::{Error as LmdbError, RoTransaction, RwTransaction, Transaction, WriteFlags};

/// Reads the value under a key in a database using the given LMDB transaction.
pub(crate) fn read_from_db<K: AsRef<[u8]>>(
    txn: &mut RoTransaction,
    db_name: &str,
    key: &K,
) -> Result<Vec<u8>, LmdbError> {
    let db = unsafe { txn.open_db(Some(db_name))? };
    let value = txn.get(db, key)?.to_vec();
    Ok(value)
}

/// Writes a key-value pair in a database using the given LMDB transaction.
pub(crate) fn write_to_db<K: AsRef<[u8]>, V: AsRef<[u8]>>(
    txn: &mut RwTransaction,
    db_name: &str,
    key: &K,
    value: &V,
) -> Result<(), LmdbError> {
    let db = unsafe { txn.open_db(Some(db_name))? };
    txn.put(db, key, value, WriteFlags::empty())?;
    Ok(())
}

/// Copies the value under a key from the source database to the destination
/// database and returns the raw value bytes.
pub(crate) fn transfer_to_new_db<K: AsRef<[u8]>>(
    source_txn: &mut RoTransaction,
    destination_txn: &mut RwTransaction,
    db_name: &str,
    key: &K,
) -> Result<Vec<u8>, LmdbError> {
    let value = read_from_db(source_txn, db_name, key)?;
    write_to_db(destination_txn, db_name, key, &value)?;
    Ok(value)
}
