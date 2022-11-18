use std::result::Result;

use lmdb::{Database, Error, Transaction};
use lmdb_sys::{mdb_stat, MDB_stat};

/// Retrieves the number of entries in a database.
pub fn entry_count<T: Transaction>(txn: &'_ T, database: Database) -> Result<usize, Error> {
    let mut stat = MDB_stat {
        ms_psize: 0,
        ms_depth: 0,
        ms_branch_pages: 0,
        ms_leaf_pages: 0,
        ms_overflow_pages: 0,
        ms_entries: 0,
    };
    let result = unsafe { mdb_stat(txn.txn(), database.dbi(), &mut stat as *mut MDB_stat) };
    if result != 0 {
        Err(Error::from_err_code(result))
    } else {
        Ok(stat.ms_entries)
    }
}

#[cfg(test)]
mod tests {
    use lmdb::{Transaction, WriteFlags};

    use crate::test_utils::LmdbTestFixture;

    use super::entry_count;

    #[test]
    fn db_entry_count() {
        let fixture = LmdbTestFixture::new(vec![], None);
        let env = &fixture.env;
        let db = fixture.db(None).unwrap();

        if let Ok(txn) = env.begin_ro_txn() {
            assert_eq!(entry_count(&txn, *db).unwrap(), 0);
            txn.commit().unwrap();
        }

        let first_dummy_input = [0u8, 1u8];
        let second_dummy_input = [1u8, 2u8];
        // Insert the first entry into the database.
        if let Ok(mut txn) = env.begin_rw_txn() {
            txn.put(
                *db,
                &first_dummy_input,
                &first_dummy_input,
                WriteFlags::empty(),
            )
            .unwrap();
            txn.commit().unwrap();
        };

        if let Ok(txn) = env.begin_ro_txn() {
            assert_eq!(entry_count(&txn, *db).unwrap(), 1);
            txn.commit().unwrap();
        }

        // Insert the second entry into the database.
        if let Ok(mut txn) = env.begin_rw_txn() {
            txn.put(
                *db,
                &second_dummy_input,
                &second_dummy_input,
                WriteFlags::empty(),
            )
            .unwrap();
            txn.commit().unwrap();
        };

        if let Ok(txn) = env.begin_ro_txn() {
            assert_eq!(entry_count(&txn, *db).unwrap(), 2);
            txn.commit().unwrap();
        };

        // Delete the first entry from the database.
        if let Ok(mut txn) = env.begin_rw_txn() {
            txn.del(*db, &first_dummy_input, None).unwrap();
            txn.commit().unwrap();
        };

        if let Ok(txn) = env.begin_ro_txn() {
            assert_eq!(entry_count(&txn, *db).unwrap(), 1);
            txn.commit().unwrap();
        };
    }
}
