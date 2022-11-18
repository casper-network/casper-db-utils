use lmdb::{Database as LmdbDatabase, Environment, Transaction, WriteFlags};
use rand::{self, prelude::ThreadRng, Rng, RngCore};
use serde::{Deserialize, Serialize};

use super::{Database, DeserializationError};
use crate::test_utils::LmdbTestFixture;

fn gen_bytes(rng: &mut ThreadRng) -> Vec<u8> {
    let mock = MockStruct::random(rng);
    bincode::serialize(&mock).unwrap()
}

fn gen_faulty_bytes(rng: &mut ThreadRng) -> Vec<u8> {
    let mock = FaultyMockStruct::random(rng);
    bincode::serialize(&mock).unwrap()
}

fn populate_db(env: &Environment, db: &LmdbDatabase) {
    let mut rng = rand::thread_rng();
    let entry_count = rng.gen_range(10u32..100u32);
    let mut rw_tx = env.begin_rw_txn().expect("couldn't begin rw transaction");
    for i in 0..entry_count {
        let bytes = gen_bytes(&mut rng);
        let key: [u8; 4] = i.to_le_bytes();
        rw_tx.put(*db, &key, &bytes, WriteFlags::empty()).unwrap();
    }
    rw_tx.commit().unwrap();
}

fn populate_faulty_db(env: &Environment, db: &LmdbDatabase) {
    let mut rng = rand::thread_rng();
    let entry_count = rng.gen_range(10u32..100u32);
    let mut rw_tx = env.begin_rw_txn().expect("couldn't begin rw transaction");
    for i in 0..entry_count {
        let bytes = if i % 5 == 0 {
            gen_faulty_bytes(&mut rng)
        } else {
            gen_bytes(&mut rng)
        };
        let key: [u8; 4] = i.to_le_bytes();
        rw_tx.put(*db, &key, &bytes, WriteFlags::empty()).unwrap();
    }
    rw_tx.commit().unwrap();
}

#[derive(Deserialize, Serialize)]
enum MockEnum {
    A,
    B([u8; 32]),
}

impl MockEnum {
    fn random(rng: &mut ThreadRng) -> Self {
        if rng.gen::<u32>() % 2 == 0 {
            Self::A
        } else {
            let mut buf = [0u8; 32];
            rng.fill_bytes(&mut buf);
            Self::B(buf)
        }
    }
}

#[derive(Deserialize, Serialize)]
struct MockStruct {
    a: u32,
    b: String,
    c: Option<MockEnum>,
}

impl MockStruct {
    fn random(rng: &mut ThreadRng) -> Self {
        let s = format!("test_string_{}", rng.gen::<u64>());
        Self {
            a: rng.gen::<u32>(),
            b: s,
            c: if rng.gen::<u32>() % 2 == 0 {
                Some(MockEnum::random(rng))
            } else {
                None
            },
        }
    }
}

#[derive(Deserialize, Serialize)]
struct FaultyMockStruct {
    a: u32,
    d: Option<u32>,
    b: String,
    c: Option<MockEnum>,
}

impl FaultyMockStruct {
    fn random(rng: &mut ThreadRng) -> Self {
        let s = format!("test_string_{}", rng.gen::<u64>());
        Self {
            a: rng.gen::<u32>(),
            d: if rng.gen::<u32>() % 2 == 0 {
                Some(rng.gen::<u32>())
            } else {
                None
            },
            b: s,
            c: if rng.gen::<u32>() % 2 == 0 {
                Some(MockEnum::random(rng))
            } else {
                None
            },
        }
    }
}

struct MockDb {}

impl Database for MockDb {
    fn db_name() -> &'static str {
        "test_db"
    }

    fn parse_element(bytes: &[u8]) -> Result<(), DeserializationError> {
        bincode::deserialize::<MockStruct>(bytes)?;
        Ok(())
    }
}

#[test]
fn sanity_check_ser_deser() {
    let mut rng = rand::thread_rng();
    let original = MockStruct::random(&mut rng);
    let ser = bincode::serialize(&original).expect("couldn't serialize");
    let _deser: MockStruct = bincode::deserialize(&ser).expect("couldn't deserialize");

    let original = FaultyMockStruct::random(&mut rng);
    let ser = bincode::serialize(&original).expect("couldn't serialize");
    let deser = bincode::deserialize::<MockStruct>(&ser);
    assert!(deser.is_err());

    assert!(bincode::deserialize::<MockStruct>(&gen_faulty_bytes(&mut rng)).is_err());
}

#[test]
fn good_db_should_pass_check() {
    let fixture = LmdbTestFixture::new(vec![MockDb::db_name()], None);
    populate_db(&fixture.env, fixture.db(Some(MockDb::db_name())).unwrap());

    assert!(MockDb::check_db(&fixture.env, true, 0).is_ok());
    assert!(MockDb::check_db(&fixture.env, false, 0).is_ok());
    assert!(MockDb::check_db(&fixture.env, true, 4).is_ok());
    assert!(MockDb::check_db(&fixture.env, false, 4).is_ok());
}

#[test]
fn bad_db_should_fail_check() {
    let fixture = LmdbTestFixture::new(vec![MockDb::db_name()], None);
    populate_faulty_db(&fixture.env, fixture.db(Some(MockDb::db_name())).unwrap());

    assert!(MockDb::check_db(&fixture.env, true, 0).is_err());
    assert!(MockDb::check_db(&fixture.env, false, 0).is_err());
    assert!(MockDb::check_db(&fixture.env, true, 4).is_err());
    assert!(MockDb::check_db(&fixture.env, false, 4).is_err());
}
