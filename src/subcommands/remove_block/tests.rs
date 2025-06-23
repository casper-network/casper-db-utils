use casper_types::{
    BlockBody, BlockHeader, PublicKey, SecretKey, TransactionHash, testing::TestRng,
};
use lmdb::Transaction;

use crate::{
    common::db::{
        Error as DatabaseError, STORAGE_FILE_NAME,
        databases::{
            block_body_database, block_header_database, execution_results_database,
            transactions_database,
        },
        versioned_database::serialize_bytesrepr,
    },
    subcommands::remove_block::{Error, remove::remove_block},
    test_utils::{
        LmdbTestFixture, block_v2_with_height, mock_execution_result, mock_execution_result_v2,
        mock_transaction_hash, store_execution_result, store_execution_result_opt_block_hash,
    },
};

#[test]
fn remove_block_should_work() {
    let mut rng = TestRng::new();
    let secret_key = SecretKey::ed25519_from_bytes([222; SecretKey::ED25519_LENGTH])
        .expect("should create secret key");
    let pk = PublicKey::from(&secret_key);

    const DEPLOY_COUNT: usize = 3;

    let fixture = LmdbTestFixture::new(Some(STORAGE_FILE_NAME));
    let transaction_hashes: Vec<TransactionHash> =
        (0..DEPLOY_COUNT as u8).map(mock_transaction_hash).collect();
    let blocks = vec![
        block_v2_with_height(
            &mut rng,
            pk.clone(),
            vec![transaction_hashes[0]],
            10,
            80,
            None,
        ),
        block_v2_with_height(
            &mut rng,
            pk.clone(),
            vec![transaction_hashes[1], transaction_hashes[2]],
            11,
            90,
            None,
        ),
    ];
    let block_headers: Vec<_> = blocks.iter().map(|b| b.header().clone()).collect();

    let execution_results = [
        (*blocks[0].hash(), mock_execution_result()),
        (*blocks[1].hash(), mock_execution_result()),
        (*blocks[1].hash(), mock_execution_result()),
    ];

    let env = fixture.env;
    let block_header_db = block_header_database();
    block_header_db.create(env.clone()).unwrap();
    let block_body_db = block_body_database();
    block_body_db.create(env.clone()).unwrap();
    let transactions_db = transactions_database();
    transactions_db.create(env.clone()).unwrap();
    let mut execution_results_db = execution_results_database();
    execution_results_db.create(env.clone()).unwrap();

    // Insert the 2 blocks into the database.
    {
        let mut txn = env.begin_rw_txn().unwrap();
        for block in blocks.iter() {
            // Store the header.
            let header = block.header().clone();
            let body_hash = *header.body_hash();
            block_header_db
                .put(&mut txn, *block.hash(), BlockHeader::V2(header), true)
                .unwrap();
            // Store the body.

            block_body_db
                .put(
                    &mut txn,
                    body_hash,
                    casper_types::BlockBody::V2(block.body().clone()),
                    true,
                )
                .unwrap();
        }

        // Insert the 3 deploys into the deploys and deploy_metadata databases.
        for (idx, transaction_hash) in transaction_hashes.iter().enumerate() {
            let (block_hash, execution_result) = &execution_results[idx];
            store_execution_result(
                &mut txn,
                &mut execution_results_db,
                *transaction_hash,
                execution_result.clone(),
                *block_hash,
            );
        }

        txn.commit().unwrap();
    };

    assert!(remove_block(fixture.tmp_dir.path(), *blocks[0].hash()).is_ok());

    {
        let txn = env.begin_ro_txn().unwrap();
        assert!(
            block_header_db
                .get(&txn, blocks[0].hash())
                .unwrap()
                .is_none()
        );
        assert!(
            block_header_db
                .get(&txn, blocks[1].hash())
                .unwrap()
                .is_some()
        );

        assert!(
            block_body_db
                .get(&txn, block_headers[0].body_hash())
                .unwrap()
                .is_none()
        );

        assert!(
            block_body_db
                .get(&txn, block_headers[1].body_hash())
                .unwrap()
                .is_some()
        );

        assert!(
            execution_results_db
                .get(&txn, &transaction_hashes[0])
                .unwrap()
                .is_none()
        );

        assert!(
            execution_results_db
                .get(&txn, &transaction_hashes[1])
                .unwrap()
                .is_some()
        );
    }
}

#[test]
fn remove_block_no_transactions() {
    let mut rng = TestRng::new();
    let secret_key = SecretKey::ed25519_from_bytes([222; SecretKey::ED25519_LENGTH])
        .expect("should create secret key");
    let pk = PublicKey::from(&secret_key);

    const DEPLOY_COUNT: usize = 3;

    let fixture = LmdbTestFixture::new(Some(STORAGE_FILE_NAME));
    let transaction_hashes: Vec<TransactionHash> =
        (0..DEPLOY_COUNT as u8).map(mock_transaction_hash).collect();
    let blocks = vec![
        block_v2_with_height(&mut rng, pk.clone(), vec![], 10, 80, None),
        block_v2_with_height(
            &mut rng,
            pk.clone(),
            vec![transaction_hashes[1], transaction_hashes[2]],
            11,
            90,
            None,
        ),
    ];
    let block_headers: Vec<_> = blocks.iter().map(|b| b.header().clone()).collect();

    let execution_results = [
        (None, mock_execution_result_v2()),
        (Some(*blocks[1].hash()), mock_execution_result_v2()),
        (Some(*blocks[1].hash()), mock_execution_result_v2()),
    ];

    let env = fixture.env;
    let block_header_db = block_header_database();
    block_header_db.create(env.clone()).unwrap();
    let block_body_db = block_body_database();
    block_body_db.create(env.clone()).unwrap();
    let transactions_db = transactions_database();
    transactions_db.create(env.clone()).unwrap();
    let mut execution_results_db = execution_results_database();
    execution_results_db.create(env.clone()).unwrap();

    // Insert the 2 blocks into the database.
    {
        let mut txn = env.begin_rw_txn().unwrap();
        for block in blocks.iter() {
            // Store the header.
            let header = block.header().clone();
            let body_hash = *header.body_hash();
            block_header_db
                .put(&mut txn, *block.hash(), BlockHeader::V2(header), true)
                .unwrap();
            // Store the body.

            block_body_db
                .put(
                    &mut txn,
                    body_hash,
                    casper_types::BlockBody::V2(block.body().clone()),
                    true,
                )
                .unwrap();
        }

        // Insert the 3 deploys into the deploys and deploy_metadata databases.
        for (idx, transaction_hash) in transaction_hashes.iter().enumerate() {
            let (block_hash, execution_result) = &execution_results[idx];
            store_execution_result_opt_block_hash(
                &mut txn,
                &mut execution_results_db,
                *transaction_hash,
                execution_result.clone(),
                *block_hash,
            );
        }

        txn.commit().unwrap();
    };

    assert!(remove_block(fixture.tmp_dir.path(), *blocks[0].hash()).is_ok());

    {
        let txn = env.begin_ro_txn().unwrap();
        assert!(
            block_header_db
                .get(&txn, blocks[0].hash())
                .unwrap()
                .is_none()
        );
        assert!(
            block_header_db
                .get(&txn, blocks[1].hash())
                .unwrap()
                .is_some()
        );

        assert!(
            block_body_db
                .get(&txn, block_headers[0].body_hash())
                .unwrap()
                .is_none()
        );

        assert!(
            block_body_db
                .get(&txn, block_headers[1].body_hash())
                .unwrap()
                .is_some()
        );

        assert!(
            execution_results_db
                .get(&txn, &transaction_hashes[0])
                .unwrap()
                .is_some()
        );

        assert!(
            execution_results_db
                .get(&txn, &transaction_hashes[1])
                .unwrap()
                .is_some()
        );
    }
}

#[test]
fn remove_block_missing_header() {
    let mut rng = TestRng::new();
    let secret_key = SecretKey::ed25519_from_bytes([222; SecretKey::ED25519_LENGTH])
        .expect("should create secret key");
    let pk = PublicKey::from(&secret_key);
    let fixture = LmdbTestFixture::new(Some(STORAGE_FILE_NAME));
    let env = fixture.env;
    let block_header_db = block_header_database();
    block_header_db.create(env.clone()).unwrap();
    let block_body_db = block_body_database();
    block_body_db.create(env.clone()).unwrap();
    let transactions_db = transactions_database();
    transactions_db.create(env.clone()).unwrap();
    let execution_results_db = execution_results_database();
    execution_results_db.create(env.clone()).unwrap();

    let block = block_v2_with_height(&mut rng, pk.clone(), vec![], 10, 80, None);
    let block_hash = *block.hash();
    assert!(
        matches!(remove_block(fixture.tmp_dir.path(), block_hash).unwrap_err(), Error::MissingHeader(actual_block_hash) if block_hash == actual_block_hash)
    );
}

#[test]
fn remove_block_missing_body() {
    const DEPLOY_COUNT: usize = 3;
    let mut rng = TestRng::new();
    let secret_key = SecretKey::ed25519_from_bytes([222; SecretKey::ED25519_LENGTH])
        .expect("should create secret key");
    let pk = PublicKey::from(&secret_key);
    let fixture = LmdbTestFixture::new(Some(STORAGE_FILE_NAME));
    let env = fixture.env;
    let block_header_db = block_header_database();
    block_header_db.create(env.clone()).unwrap();
    let block_body_db = block_body_database();
    block_body_db.create(env.clone()).unwrap();
    let transactions_db = transactions_database();
    transactions_db.create(env.clone()).unwrap();
    let mut execution_results_db = execution_results_database();
    execution_results_db.create(env.clone()).unwrap();

    let transaction_hashes: Vec<TransactionHash> =
        (0..DEPLOY_COUNT as u8).map(mock_transaction_hash).collect();
    let blocks = vec![
        block_v2_with_height(&mut rng, pk.clone(), vec![], 10, 80, None),
        block_v2_with_height(
            &mut rng,
            pk.clone(),
            vec![transaction_hashes[1], transaction_hashes[2]],
            11,
            90,
            None,
        ),
    ];
    let block_headers: Vec<_> = blocks.iter().map(|b| b.header().clone()).collect();
    let execution_results = [
        (Some(*blocks[0].hash()), mock_execution_result_v2()),
        (Some(*blocks[1].hash()), mock_execution_result_v2()),
        (Some(*blocks[1].hash()), mock_execution_result_v2()),
    ];

    // Insert the 2 block headers into the database.
    {
        let mut txn = env.begin_rw_txn().unwrap();
        for block in blocks.iter() {
            let header = block.header().clone();
            block_header_db
                .put(&mut txn, *block.hash(), BlockHeader::V2(header), true)
                .unwrap();
        }

        // Insert the 3 deploys into the deploys and deploy_metadata databases.
        for (idx, transaction_hash) in transaction_hashes.iter().enumerate() {
            let (block_hash, execution_result) = &execution_results[idx];
            store_execution_result_opt_block_hash(
                &mut txn,
                &mut execution_results_db,
                *transaction_hash,
                execution_result.clone(),
                *block_hash,
            );
        }
        txn.commit().unwrap();
    };

    assert!(remove_block(fixture.tmp_dir.path(), block_headers[0].block_hash()).is_ok());

    {
        let txn = env.begin_ro_txn().unwrap();
        assert!(
            block_header_db
                .get(&txn, blocks[0].hash())
                .unwrap()
                .is_none()
        );
        assert!(
            block_header_db
                .get(&txn, blocks[1].hash())
                .unwrap()
                .is_some()
        );

        assert!(
            block_body_db
                .get(&txn, block_headers[0].body_hash())
                .unwrap()
                .is_none()
        );
        assert!(
            block_body_db
                .get(&txn, block_headers[1].body_hash())
                .unwrap()
                .is_none()
        );
        txn.commit().unwrap();
    }
}

#[test]
fn remove_block_missing_transactions() {
    let mut rng = TestRng::new();
    let secret_key = SecretKey::ed25519_from_bytes([222; SecretKey::ED25519_LENGTH])
        .expect("should create secret key");
    let pk = PublicKey::from(&secret_key);

    const DEPLOY_COUNT: usize = 3;

    let fixture = LmdbTestFixture::new(Some(STORAGE_FILE_NAME));
    let transaction_hashes: Vec<TransactionHash> =
        (0..DEPLOY_COUNT as u8).map(mock_transaction_hash).collect();
    let blocks = vec![
        block_v2_with_height(&mut rng, pk.clone(), vec![], 10, 80, None),
        block_v2_with_height(
            &mut rng,
            pk.clone(),
            vec![transaction_hashes[1], transaction_hashes[2]],
            11,
            90,
            None,
        ),
    ];
    let block_headers: Vec<_> = blocks.iter().map(|b| b.header().clone()).collect();

    let execution_results = [
        (None, mock_execution_result_v2()),
        (Some(*blocks[1].hash()), mock_execution_result_v2()),
        (Some(*blocks[1].hash()), mock_execution_result_v2()),
    ];

    let env = fixture.env;
    let block_header_db = block_header_database();
    block_header_db.create(env.clone()).unwrap();
    let block_body_db = block_body_database();
    block_body_db.create(env.clone()).unwrap();
    let transactions_db = transactions_database();
    transactions_db.create(env.clone()).unwrap();
    let mut execution_results_db = execution_results_database();
    execution_results_db.create(env.clone()).unwrap();

    // Insert the 2 blocks into the database.
    {
        let mut txn = env.begin_rw_txn().unwrap();
        for block in blocks.iter() {
            // Store the header.
            let header = block.header().clone();
            let body_hash = *header.body_hash();
            block_header_db
                .put(&mut txn, *block.hash(), BlockHeader::V2(header), true)
                .unwrap();
            // Store the body.

            block_body_db
                .put(
                    &mut txn,
                    body_hash,
                    casper_types::BlockBody::V2(block.body().clone()),
                    true,
                )
                .unwrap();
        }

        // Insert the 3 deploys into the deploys and deploy_metadata databases.
        for (idx, transaction_hash) in transaction_hashes.iter().enumerate() {
            let (block_hash, execution_result) = &execution_results[idx];
            store_execution_result_opt_block_hash(
                &mut txn,
                &mut execution_results_db,
                *transaction_hash,
                execution_result.clone(),
                *block_hash,
            );
        }

        txn.commit().unwrap();
    };

    assert!(remove_block(fixture.tmp_dir.path(), *blocks[0].hash()).is_ok());

    {
        let txn = env.begin_ro_txn().unwrap();
        assert!(
            block_header_db
                .get(&txn, blocks[0].hash())
                .unwrap()
                .is_none()
        );
        assert!(
            block_header_db
                .get(&txn, blocks[1].hash())
                .unwrap()
                .is_some()
        );

        assert!(
            block_body_db
                .get(&txn, block_headers[0].body_hash())
                .unwrap()
                .is_none()
        );

        assert!(
            block_body_db
                .get(&txn, block_headers[1].body_hash())
                .unwrap()
                .is_some()
        );

        assert!(
            execution_results_db
                .get(&txn, &transaction_hashes[0])
                .unwrap()
                .is_some()
        );

        assert!(
            execution_results_db
                .get(&txn, &transaction_hashes[1])
                .unwrap()
                .is_some()
        );
        assert!(
            execution_results_db
                .get(&txn, &transaction_hashes[2])
                .unwrap()
                .is_some()
        );
    }
}

#[test]
fn remove_block_invalid_header() {
    let mut rng = TestRng::new();
    let secret_key = SecretKey::ed25519_from_bytes([222; SecretKey::ED25519_LENGTH])
        .expect("should create secret key");
    let pk = PublicKey::from(&secret_key);

    let fixture = LmdbTestFixture::new(Some(STORAGE_FILE_NAME));

    let block = block_v2_with_height(&mut rng, pk.clone(), vec![], 10, 80, None);
    let block_hash = *block.hash();
    let env = fixture.env;
    let block_header_db = block_header_database();
    block_header_db.create(env.clone()).unwrap();

    // Insert the an invalid block header into the database.
    {
        let mut txn = env.begin_rw_txn().unwrap();

        // Store the header.
        let raw_block_hash = serialize_bytesrepr(&block_hash).unwrap();
        block_header_db
            .put_raw(&mut txn, raw_block_hash, vec![0u8, 1u8, 2u8], true)
            .unwrap();

        txn.commit().unwrap();
    };
    assert!(matches!(
        remove_block(fixture.tmp_dir.path(), block_hash).unwrap_err(),
        Error::DatabaseLayer(DatabaseError::Parsing(_, _))
    ));
}

#[test]
fn remove_block_invalid_body() {
    let mut rng = TestRng::new();
    let secret_key = SecretKey::ed25519_from_bytes([222; SecretKey::ED25519_LENGTH])
        .expect("should create secret key");
    let pk = PublicKey::from(&secret_key);

    let fixture = LmdbTestFixture::new(Some(STORAGE_FILE_NAME));
    let env = fixture.env.clone();
    let block_header_db = block_header_database();
    block_header_db.create(env.clone()).unwrap();
    let block_body_db = block_body_database();
    block_body_db.create(env.clone()).unwrap();

    let block = block_v2_with_height(&mut rng, pk.clone(), vec![], 10, 80, None);

    // Insert the block header along with an invalid body into the database.
    {
        let mut txn = env.begin_rw_txn().unwrap();

        // Store the header.
        let block_header_v2 = block.header().clone();
        let body_hash = *block_header_v2.body_hash();
        block_header_db
            .put(
                &mut txn,
                *block.hash(),
                BlockHeader::V2(block_header_v2),
                true,
            )
            .unwrap();
        // Store the body.
        block_body_db
            .put_raw(
                &mut txn,
                serialize_bytesrepr(&body_hash).unwrap(),
                vec![0u8, 1u8, 2u8],
                true,
            )
            .unwrap();

        txn.commit().unwrap();
    };
    assert!(matches!(
        remove_block(fixture.tmp_dir.path(), *block.hash()).unwrap_err(),
        Error::DatabaseLayer(DatabaseError::Parsing(_, _))
    ));
}

#[test]
fn remove_block_invalid_deploy_metadata() {
    let mut rng = TestRng::new();
    let secret_key = SecretKey::ed25519_from_bytes([222; SecretKey::ED25519_LENGTH])
        .expect("should create secret key");
    let pk = PublicKey::from(&secret_key);

    let fixture = LmdbTestFixture::new(Some(STORAGE_FILE_NAME));
    let env = fixture.env.clone();
    let block_header_db = block_header_database();
    block_header_db.create(env.clone()).unwrap();
    let block_body_db = block_body_database();
    block_body_db.create(env.clone()).unwrap();
    let execution_results_db = execution_results_database();
    execution_results_db.create(env.clone()).unwrap();

    let transaction_hash = mock_transaction_hash(0);
    let block = block_v2_with_height(&mut rng, pk.clone(), vec![transaction_hash], 10, 80, None);

    // Insert the block into the database.
    {
        let mut txn = env.begin_rw_txn().unwrap();
        block_header_db
            .put(
                &mut txn,
                *block.hash(),
                BlockHeader::V2(block.header().clone()),
                true,
            )
            .unwrap();
        block_body_db
            .put(
                &mut txn,
                *block.body_hash(),
                BlockBody::V2(block.body().clone()),
                true,
            )
            .unwrap();

        execution_results_db
            .put_raw(
                &mut txn,
                serialize_bytesrepr(&transaction_hash).unwrap(),
                vec![0u8, 1u8, 2u8],
                false,
            )
            .unwrap();
        txn.commit().unwrap();
    };
    assert!(matches!(
        remove_block(fixture.tmp_dir.path(), *block.hash()).unwrap_err(),
        Error::DatabaseLayer(DatabaseError::Parsing(_, _))
    ));
}
