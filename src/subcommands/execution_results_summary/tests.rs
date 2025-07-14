use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
};

use crate::{
    common::db::Error as DbLayerError,
    test_utils::{block_v1, block_v2, store_execution_result_opt_block_hash},
};
use casper_types::{
    BlockBody, DeployHash, PublicKey, SecretKey, TransactionHash, bytesrepr::ToBytes,
};
use casper_types::{execution::ExecutionResult, testing::TestRng};
use lmdb::Transaction;
use once_cell::sync::Lazy;
use rand::Rng;
use tempfile::{self, TempDir};

use crate::{
    common::db::{
        STORAGE_FILE_NAME,
        databases::{block_body_database, block_header_database, execution_results_database},
        versioned_database::serialize_bytesrepr,
    },
    subcommands::execution_results_summary::{
        Error, read_db,
        summary::{
            CHUNK_SIZE_BYTES, CollectionStatistics, ExecutionResultsStats, ExecutionResultsSummary,
            chunk_count_after_partition, summarize_map,
        },
    },
    test_utils::{self, LmdbTestFixture},
};

static OUT_DIR: Lazy<TempDir> = Lazy::new(|| tempfile::tempdir().unwrap());

#[test]
fn check_chunk_count_after_partition() {
    assert_eq!(chunk_count_after_partition(0), 0);
    assert_eq!(chunk_count_after_partition(1), 1);
    assert_eq!(chunk_count_after_partition(CHUNK_SIZE_BYTES / 2), 1);
    assert_eq!(chunk_count_after_partition(CHUNK_SIZE_BYTES - 1), 1);
    assert_eq!(chunk_count_after_partition(CHUNK_SIZE_BYTES), 1);
    assert_eq!(chunk_count_after_partition(CHUNK_SIZE_BYTES + 1), 2);
    assert_eq!(chunk_count_after_partition((CHUNK_SIZE_BYTES * 3) / 2), 2);
    assert_eq!(chunk_count_after_partition(2 * CHUNK_SIZE_BYTES - 1), 2);
    assert_eq!(chunk_count_after_partition(2 * CHUNK_SIZE_BYTES), 2);
    assert_eq!(chunk_count_after_partition(2 * CHUNK_SIZE_BYTES + 1), 3);
}

#[test]
fn check_summarize_map() {
    // Empty map.
    assert_eq!(
        summarize_map(&BTreeMap::default()),
        CollectionStatistics::default()
    );

    // 1 element map.
    let mut map = BTreeMap::default();
    map.insert(1, 1);
    assert_eq!(summarize_map(&map), CollectionStatistics::new(1.0, 1, 1));

    // 2 different elements map.
    let mut map = BTreeMap::default();
    map.insert(1, 1);
    map.insert(2, 1);
    assert_eq!(summarize_map(&map), CollectionStatistics::new(1.5, 2, 2));

    // 2 identical elements map.
    let mut map = BTreeMap::default();
    map.insert(1, 2);
    assert_eq!(summarize_map(&map), CollectionStatistics::new(1.0, 1, 1));

    // 3 elements map.
    let mut map = BTreeMap::default();
    map.insert(1, 1);
    map.insert(4, 2);
    assert_eq!(summarize_map(&map), CollectionStatistics::new(3.0, 4, 4));

    // 10 elements map.
    let mut map = BTreeMap::default();
    map.insert(1, 2);
    map.insert(3, 2);
    map.insert(4, 4);
    map.insert(8, 2);
    assert_eq!(summarize_map(&map), CollectionStatistics::new(4.0, 4, 8));
}

#[test]
fn check_summarize_map_random() {
    let mut rng = TestRng::new();
    let elem_count = rng.gen_range(50usize..100usize);
    let mut elements: Vec<usize> = vec![];
    let mut sum = 0;
    for _ in 0..elem_count {
        let random_element = rng.gen_range(0usize..25usize);
        sum += random_element;
        elements.push(random_element);
    }
    elements.sort_unstable();
    let median = elements[elem_count / 2];
    let max = *elements.last().unwrap();
    let average = sum as f64 / elem_count as f64;

    let mut map = BTreeMap::default();
    for element in elements {
        if let Some(count) = map.get_mut(&element) {
            *count += 1;
        } else {
            map.insert(element, 1);
        }
    }
    assert_eq!(
        summarize_map(&map),
        CollectionStatistics::new(average, median, max)
    );
}

#[test]
fn dump_execution_results_summary() {
    let mut stats = ExecutionResultsStats::default();
    stats.execution_results_size.insert(1, 2);
    stats.chunk_count.insert(1, 1);
    stats.chunk_count.insert(2, 1);
    let summary: ExecutionResultsSummary = stats.into();
    let reference_json = serde_json::to_string_pretty(&summary).unwrap();

    let out_file_path = OUT_DIR.as_ref().join("no_net_name.json");
    {
        let out_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&out_file_path)
            .unwrap();
        read_db::dump_execution_results_summary(&summary, Box::new(out_file)).unwrap();
    }
    assert_eq!(fs::read_to_string(&out_file_path).unwrap(), reference_json);
}

#[test]
fn empty_execution_results_stats() {
    let stats = ExecutionResultsStats::default();
    let summary: ExecutionResultsSummary = stats.into();
    assert_eq!(summary.execution_results_size.average, 0.0);
    assert_eq!(summary.execution_results_size.median, 0);
    assert_eq!(summary.execution_results_size.max, 0);

    assert_eq!(summary.chunks_statistics.average, 0.0);
    assert_eq!(summary.chunks_statistics.median, 0);
    assert_eq!(summary.chunks_statistics.max, 0);
}

#[test]
fn different_execution_results_stats_feed() {
    let mut stats = ExecutionResultsStats::default();
    let mut sizes = vec![];
    let mut bytesrepr_sizes = vec![];
    let secret_key = SecretKey::ed25519_from_bytes([222; SecretKey::ED25519_LENGTH])
        .expect("should create secret key");
    let pk = PublicKey::from(&secret_key);

    for i in 1..4 {
        let mut execution_results = vec![];
        for _ in 0..(10 * i) {
            execution_results.push(test_utils::v2_success_execution_result(pk.clone()));
        }
        let mut size = 0;
        for er in &execution_results {
            match er {
                ExecutionResult::V1(execution_result_v1) => {
                    size += bincode::serialized_size(execution_result_v1).unwrap() as usize;
                }
                ExecutionResult::V2(_) => size += er.serialized_length(),
            }
        }
        sizes.push(size);
        bytesrepr_sizes.push(chunk_count_after_partition(
            execution_results.serialized_length(),
        ));
        stats.feed(execution_results).unwrap();
    }

    let summary: ExecutionResultsSummary = stats.into();

    let sizes_average: f64 = sizes.iter().sum::<usize>() as f64 / 3.0;
    assert_eq!(summary.execution_results_size.average, sizes_average);
    assert_eq!(summary.execution_results_size.median, sizes[1]);
    assert_eq!(summary.execution_results_size.max, sizes[2]);

    let bytesrepr_sizes_average: f64 = bytesrepr_sizes.iter().sum::<usize>() as f64 / 3.0;
    assert_eq!(summary.chunks_statistics.average, bytesrepr_sizes_average);
    assert_eq!(summary.chunks_statistics.median, bytesrepr_sizes[1]);
    assert_eq!(summary.chunks_statistics.max, bytesrepr_sizes[2]);
}

#[test]
fn identical_execution_results_stats_feed() {
    let mut stats = ExecutionResultsStats::default();
    let mut bincode_sizes = vec![];
    let mut bytesrepr_sizes = vec![];
    let secret_key = SecretKey::ed25519_from_bytes([222; SecretKey::ED25519_LENGTH])
        .expect("should create secret key");
    let pk = PublicKey::from(&secret_key);

    for _ in 1..4 {
        let mut execution_results = vec![];
        for _ in 0..10 {
            execution_results.push(test_utils::v2_success_execution_result(pk.clone()));
        }
        let mut size = 0;
        for er in &execution_results {
            match er {
                ExecutionResult::V1(execution_result_v1) => {
                    size += bincode::serialized_size(execution_result_v1).unwrap() as usize;
                }
                ExecutionResult::V2(_) => size += er.serialized_length(),
            }
        }
        bytesrepr_sizes.push(chunk_count_after_partition(
            execution_results.serialized_length(),
        ));
        bincode_sizes.push(size);
        stats.feed(execution_results).unwrap();
    }
    assert_eq!(stats.execution_results_size.len(), 1);
    assert_eq!(stats.chunk_count.len(), 1);

    let summary: ExecutionResultsSummary = stats.into();

    let bincode_sizes_average: f64 = bincode_sizes.iter().sum::<usize>() as f64 / 3.0;
    assert_eq!(
        summary.execution_results_size.average,
        bincode_sizes_average
    );
    assert_eq!(summary.execution_results_size.median, bincode_sizes[1]);
    assert_eq!(summary.execution_results_size.max, bincode_sizes[2]);
    assert_eq!(
        summary.execution_results_size.median,
        summary.execution_results_size.max
    );

    let bytesrepr_sizes_average: f64 = bytesrepr_sizes.iter().sum::<usize>() as f64 / 3.0;
    assert_eq!(summary.chunks_statistics.average, bytesrepr_sizes_average);
    assert_eq!(summary.chunks_statistics.median, bytesrepr_sizes[1]);
    assert_eq!(summary.chunks_statistics.max, bytesrepr_sizes[2]);
    assert_eq!(
        summary.chunks_statistics.median,
        summary.chunks_statistics.max
    );
}

#[test]
fn execution_results_stats_should_succeed() {
    let mut rng = TestRng::new();
    let secret_key = SecretKey::ed25519_from_bytes([222; SecretKey::ED25519_LENGTH])
        .expect("should create secret key");
    let pk = PublicKey::from(&secret_key);
    const V1_BLOCK_COUNT: u8 = 2;
    const V2_BLOCK_COUNT: u8 = 2;
    const DEPLOY_COUNT: u8 = 4;

    let fixture = LmdbTestFixture::new(Some(STORAGE_FILE_NAME));
    let out_file_path = OUT_DIR.as_ref().join("execution_results_summary.json");

    let env = &fixture.env;
    let block_header_db = block_header_database();
    block_header_db.create(env.clone()).unwrap();
    let block_body_db = block_body_database();
    block_body_db.create(env.clone()).unwrap();
    let mut execution_results_db = execution_results_database();
    execution_results_db.create(env.clone()).unwrap();

    if let Ok(mut txn) = env.begin_rw_txn() {
        for block_index in 0..V1_BLOCK_COUNT {
            let mut execution_results = vec![];
            let (block_hash, block_header) = test_utils::mock_block_header_v1(block_index);
            let mut deploy_hashes = vec![];
            for deploy_index in 0..DEPLOY_COUNT {
                let idx = block_index * (DEPLOY_COUNT + 1) + deploy_index;
                let deploy_hash = DeployHash::new([idx; 32].into());
                deploy_hashes.push(deploy_hash);
                execution_results.push((deploy_hash, test_utils::mock_execution_result()));
            }
            let block_body = block_v1(&mut rng, deploy_hashes.clone()).body().clone();
            block_header_db
                .put(&mut txn, block_hash, block_header.clone(), true)
                .unwrap();
            let body_hash = *block_header.body_hash();
            block_body_db
                .put(&mut txn, body_hash, BlockBody::V1(block_body.clone()), true)
                .unwrap();
            for (deploy_hash, execution_result) in execution_results {
                store_execution_result_opt_block_hash(
                    &mut txn,
                    &mut execution_results_db,
                    TransactionHash::Deploy(deploy_hash),
                    execution_result,
                    Some(block_hash),
                );
            }
        }

        for idx in 0..V2_BLOCK_COUNT {
            let block_index = V1_BLOCK_COUNT + idx;
            let mut execution_results = vec![];
            let (block_hash, block_header) = test_utils::mock_block_header_v2(block_index);
            let mut transaction_hashes = vec![];
            for deploy_index in 0..DEPLOY_COUNT {
                let idx = 100 + block_index * (DEPLOY_COUNT + 1) + deploy_index;
                let transaction_hash = test_utils::mock_transaction_hash(100 + idx);
                transaction_hashes.push(transaction_hash);
                execution_results.push((transaction_hash, test_utils::mock_execution_result_v2()));
            }
            let block_body = block_v2(&mut rng, pk.clone(), transaction_hashes.clone())
                .body()
                .clone();
            block_header_db
                .put(&mut txn, block_hash, block_header.clone(), true)
                .unwrap();
            let body_hash = *block_header.body_hash();
            block_body_db
                .put(&mut txn, body_hash, BlockBody::V2(block_body.clone()), true)
                .unwrap();
            for (transaction_hash, execution_result) in execution_results {
                store_execution_result_opt_block_hash(
                    &mut txn,
                    &mut execution_results_db,
                    transaction_hash,
                    execution_result,
                    None,
                );
            }
        }

        txn.commit().unwrap();
    };

    // Get the execution results summary and ensure it matches with the
    // expected statistics.
    read_db::execution_results_summary(
        fixture.tmp_dir.as_ref(),
        Some(out_file_path.as_path()),
        false,
    )
    .unwrap();
    let json_str = fs::read_to_string(&out_file_path).unwrap();
    let execution_results_summary: ExecutionResultsSummary =
        serde_json::from_str(&json_str).unwrap();

    // Construct the expected statistics.
    let expected_summary = ExecutionResultsSummary::new(
        CollectionStatistics::new(802.0, 1484, 1484),
        CollectionStatistics::new(39.5, 75, 75),
    );
    assert_eq!(execution_results_summary, expected_summary);
}

#[test]
fn execution_results_summary_invalid_key_should_fail() {
    let fixture = LmdbTestFixture::new(Some(STORAGE_FILE_NAME));
    let out_file_path = OUT_DIR.as_ref().join("invalid_key.json");

    let env = &fixture.env;
    let block_header_db = block_header_database();
    block_header_db.create(env.clone()).unwrap();

    if let Ok(mut txn) = env.begin_rw_txn() {
        let (_, block_header) = test_utils::mock_block_header_v2(1);
        let bogus_hash = [0u8; 1];
        // Insert a block header in the database with a key that can't be
        // deserialized.
        let key_raw = serialize_bytesrepr(&bogus_hash).unwrap();
        let value_raw = serialize_bytesrepr(&block_header).unwrap();
        block_header_db
            .put_raw(&mut txn, key_raw, value_raw, false)
            .unwrap();
        txn.commit().unwrap();
    };

    match read_db::execution_results_summary(
        fixture.tmp_dir.as_ref(),
        Some(out_file_path.as_path()),
        false,
    ) {
        Err(Error::InvalidKey(idx)) => assert_eq!(idx, 0),
        Err(error) => panic!("Got unexpected error: {error:?}"),
        Ok(_) => panic!("Command unexpectedly succeeded"),
    }
}

#[test]
fn execution_results_summary_parsing_should_fail() {
    let mut rng = TestRng::new();
    let secret_key = SecretKey::ed25519_from_bytes([222; SecretKey::ED25519_LENGTH])
        .expect("should create secret key");
    let pk = PublicKey::from(&secret_key);
    let fixture = LmdbTestFixture::new(Some(STORAGE_FILE_NAME));
    let out_file_path = OUT_DIR.as_ref().join("parsing.json");

    let deploy_hash = test_utils::mock_transaction_hash(0);
    let (block_hash, block_header) = test_utils::mock_block_header_v2(0);
    let transaction_hashes = vec![deploy_hash];
    let block_body = BlockBody::V2(
        block_v2(&mut rng, pk.clone(), transaction_hashes.clone())
            .body()
            .clone(),
    );

    let env = &fixture.env;
    let block_header_db = block_header_database();
    block_header_db.create(env.clone()).unwrap();
    let block_body_db = block_body_database();
    block_body_db.create(env.clone()).unwrap();
    let execution_results_db = execution_results_database();
    execution_results_db.create(env.clone()).unwrap();

    let raw_value = "bogus_deploy_metadata".to_bytes().unwrap();

    if let Ok(mut txn) = env.begin_rw_txn() {
        // Store the header.
        let body_hash = *block_header.body_hash();
        block_header_db
            .put(&mut txn, block_hash, block_header, true)
            .unwrap();
        // Store the body.
        block_body_db
            .put(&mut txn, body_hash, block_body.clone(), true)
            .unwrap();

        // Store a bogus metadata under the deploy hash key we used before.
        execution_results_db
            .put_raw(
                &mut txn,
                deploy_hash.as_ref().to_vec(),
                raw_value.clone(),
                true,
            )
            .unwrap();
        txn.commit().unwrap();
    };

    match read_db::execution_results_summary(
        fixture.tmp_dir.as_ref(),
        Some(out_file_path.as_path()),
        false,
    ) {
        Err(Error::DbLayer(DbLayerError::Parsing(size, err))) => {
            assert_eq!(size, raw_value.len());
            assert_eq!(
                "failed parsing struct with bincode: io error: unexpected end of file",
                err.to_string()
            );
        }
        Err(error) => panic!("Got unexpected error: {error:?}"),
        Ok(_) => panic!("Command unexpectedly succeeded"),
    }
}

#[test]
fn execution_results_summary_existing_output_should_fail() {
    let fixture = LmdbTestFixture::new(Some(STORAGE_FILE_NAME));
    let out_file_path = OUT_DIR.as_ref().join("existing.json");
    let _ = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&out_file_path)
        .unwrap();
    match read_db::execution_results_summary(
        fixture.tmp_dir.as_ref(),
        Some(out_file_path.as_path()),
        false,
    ) {
        Err(Error::Output(_)) => { /* expected result */ }
        Err(error) => panic!("Got unexpected error: {error:?}"),
        Ok(_) => panic!("Command unexpectedly succeeded"),
    }
}
