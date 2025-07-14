use crate::common::db::MockData;
use crate::common::db::{databases::mock_database, versioned_database::VersionedDatabases};
use crate::test_utils::LmdbTestFixture;
use casper_types::TransactionHash;
use casper_types::testing::TestRng;
use lmdb::{Environment, Transaction};
use rand::{self, Rng, RngCore};
use serde::{Deserialize, Serialize};

fn gen_bytes(rng: &mut TestRng) -> Vec<u8> {
    let mock = MockStruct::random(rng);
    bincode::serialize(&mock).unwrap()
}

fn gen_faulty_bytes(rng: &mut TestRng) -> Vec<u8> {
    let mock = FaultyMockStruct::random(rng);
    bincode::serialize(&mock).unwrap()
}

fn populate_db(env: &Environment, db: &VersionedDatabases<TransactionHash, MockData>) {
    let mut rng = TestRng::new();
    let entry_count = rng.gen_range(10u32..100u32);
    let mut rw_tx = env.begin_rw_txn().expect("couldn't begin rw transaction");
    for _ in 0..entry_count {
        let (key, data) = MockData::random(&mut rng);
        let put_legacy_to_legacy_db = rng.gen_bool(0.5);
        db.put(&mut rw_tx, key, data, put_legacy_to_legacy_db)
            .unwrap();
    }
    rw_tx.commit().unwrap();
}

fn populate_faulty_db(env: &Environment, db: &VersionedDatabases<TransactionHash, MockData>) {
    let mut rng = TestRng::new();
    let entry_count = rng.gen_range(10u32..100u32);
    let mut rw_tx = env.begin_rw_txn().expect("couldn't begin rw transaction");
    for i in 0..entry_count {
        let bytes = if i % 5 == 0 {
            gen_faulty_bytes(&mut rng)
        } else {
            gen_bytes(&mut rng)
        };
        let key: [u8; 4] = i.to_le_bytes();
        let put_to_legacy = rng.gen_bool(0.5);
        db.put_raw(&mut rw_tx, key.to_vec(), bytes, put_to_legacy)
            .unwrap();
    }
    rw_tx.commit().unwrap();
}

#[derive(Deserialize, Serialize)]
enum MockEnum {
    A,
    B([u8; 32]),
}

impl MockEnum {
    fn random(rng: &mut TestRng) -> Self {
        if rng.r#gen::<u32>() % 2 == 0 {
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
    fn random(rng: &mut TestRng) -> Self {
        let s = format!("test_string_{}", rng.r#gen::<u64>());
        Self {
            a: rng.r#gen::<u32>(),
            b: s,
            c: if rng.r#gen::<u32>() % 2 == 0 {
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
    fn random(rng: &mut TestRng) -> Self {
        let s = format!("test_string_{}", rng.r#gen::<u64>());
        Self {
            a: rng.r#gen::<u32>(),
            d: if rng.r#gen::<u32>() % 2 == 0 {
                Some(rng.r#gen::<u32>())
            } else {
                None
            },
            b: s,
            c: if rng.r#gen::<u32>() % 2 == 0 {
                Some(MockEnum::random(rng))
            } else {
                None
            },
        }
    }
}

#[test]
fn sanity_check_ser_deser() {
    let mut rng = TestRng::new();
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
    let fixture = LmdbTestFixture::new(None);
    let db = mock_database();
    let env = fixture.env.clone();
    db.create(env.clone()).unwrap();
    populate_db(&fixture.env, &db);

    assert!(db.check_dbs(env.clone(), 0, true).is_ok());
    assert!(db.check_dbs(env.clone(), 0, false).is_ok());
    assert!(db.check_dbs(env.clone(), 4, true).is_ok());
    assert!(db.check_dbs(env.clone(), 4, false).is_ok());
}

#[test]
fn bad_db_should_fail_check() {
    let fixture = LmdbTestFixture::new(None);
    let db = mock_database();
    let env = fixture.env.clone();
    db.create(env.clone()).unwrap();
    populate_faulty_db(&fixture.env, &db);

    assert!(db.check_dbs(env.clone(), 0, true).is_err());
    assert!(db.check_dbs(env.clone(), 0, false).is_err());
    assert!(db.check_dbs(env.clone(), 4, true).is_err());
    assert!(db.check_dbs(env.clone(), 4, false).is_err());
}
