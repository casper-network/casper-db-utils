#![cfg(test)]

use lmdb::{Database as LmdbDatabase, DatabaseFlags, Environment, EnvironmentFlags};
use tempfile::NamedTempFile;

pub struct LmdbTestFixture {
    pub env: Environment,
    pub db: LmdbDatabase,
    pub tmp_file: NamedTempFile,
}

impl LmdbTestFixture {
    pub fn new(name: Option<&str>) -> Self {
        let tmp_file = NamedTempFile::new().unwrap();
        let env = Environment::new()
            .set_flags(
                EnvironmentFlags::WRITE_MAP
                    | EnvironmentFlags::NO_SUB_DIR
                    | EnvironmentFlags::NO_TLS
                    | EnvironmentFlags::NO_READAHEAD,
            )
            .set_max_readers(12)
            .set_map_size(4096 * 10)
            .set_max_dbs(10)
            .open(tmp_file.path())
            .expect("can't create environment");
        let db = env
            .create_db(name, DatabaseFlags::empty())
            .expect("can't create database");
        LmdbTestFixture { env, db, tmp_file }
    }
}
