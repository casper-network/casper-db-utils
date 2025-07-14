use casper_storage::global_state::{
    store::StoreExt,
    transaction_source::{Transaction, TransactionSource, lmdb::LmdbEnvironment},
    trie::Trie,
    trie_store::lmdb::LmdbTrieStore,
};
use casper_types::{
    BlockBody, BlockHeader, Digest, PublicKey, SecretKey, TransactionHash,
    bytesrepr::{Bytes, ToBytes},
    execution::ExecutionResult,
    testing::TestRng,
};
use lmdb::DatabaseFlags;
use std::sync::Arc;

use crate::{
    common::db::{
        STORAGE_FILE_NAME,
        databases::{
            block_body_database, block_header_database, execution_results_database,
            transactions_database,
        },
    },
    subcommands::{
        extract_slice::{global_state::transfer_global_state, storage},
        trie_compact::{
            DEFAULT_MAX_DB_SIZE, create_data_access_layer, load_data_access_layer,
            tests::create_data,
        },
    },
    test_utils::{
        LmdbTestFixture, block_v1, block_v2, mock_deploy, mock_deploy_transaction,
        mock_v1_transaction, random_execution_result_v1, random_execution_result_v2,
        store_execution_result,
    },
};

#[test]
fn transfer_data_between_dbs() {
    let mut rng = TestRng::new();
    let secret_key = SecretKey::ed25519_from_bytes([222; SecretKey::ED25519_LENGTH])
        .expect("should create secret key");
    let pk = PublicKey::from(&secret_key);
    let (deploy_hash_1, deploy_1) = mock_deploy(&mut rng);
    let (v1_transaction_hash, v1_transaction) = mock_v1_transaction(&mut rng);
    let (deploy_transaction_hash, deploy_transaction) = mock_deploy_transaction(&mut rng);

    let block_v1 = block_v1(&mut rng, vec![deploy_hash_1]);
    let block_v2 = block_v2(
        &mut rng,
        pk.clone(),
        vec![deploy_transaction_hash, v1_transaction_hash],
    );
    let block_hash_1 = *block_v1.hash();
    let block_hash_2 = *block_v2.hash();
    let block_header_v1 = block_v1.header().clone();
    let block_header_v2 = block_v2.header().clone();

    let execution_result_v1 = random_execution_result_v1(&mut rng);
    let execution_result_v2_1 = random_execution_result_v2(&mut rng);
    let execution_result_v2_2 = random_execution_result_v2(&mut rng);

    let source_tmp_dir = Arc::new(tempfile::tempdir().unwrap());
    let source_fixture =
        LmdbTestFixture::new_with_tmp_dir(source_tmp_dir.clone(), Some(STORAGE_FILE_NAME));
    {
        let source_env = source_fixture.env;
        let source_block_header_db = block_header_database();
        source_block_header_db.create(source_env.clone()).unwrap();
        let source_block_body_db = block_body_database();
        source_block_body_db.create(source_env.clone()).unwrap();
        let source_transaction_db = transactions_database();
        source_transaction_db.create(source_env.clone()).unwrap();
        let mut execution_results_db = execution_results_database();
        execution_results_db.create(source_env.clone()).unwrap();

        if let Ok(mut txn) = source_env.begin_rw_txn() {
            source_block_header_db
                .put(
                    &mut txn,
                    block_hash_1,
                    BlockHeader::V1(block_header_v1.clone()),
                    true,
                )
                .unwrap();
            source_block_header_db
                .put(
                    &mut txn,
                    block_hash_2,
                    BlockHeader::V2(block_header_v2.clone()),
                    false,
                )
                .unwrap();

            source_block_body_db
                .put(
                    &mut txn,
                    *block_v1.body_hash(),
                    BlockBody::V1(block_v1.body().clone()),
                    true,
                )
                .unwrap();
            source_block_body_db
                .put(
                    &mut txn,
                    *block_v2.body_hash(),
                    BlockBody::V2(block_v2.body().clone()),
                    true,
                )
                .unwrap();

            source_transaction_db
                .put(
                    &mut txn,
                    TransactionHash::Deploy(deploy_hash_1),
                    casper_types::Transaction::Deploy(deploy_1),
                    true,
                )
                .unwrap();
            source_transaction_db
                .put(
                    &mut txn,
                    deploy_transaction_hash,
                    deploy_transaction.clone(),
                    false,
                )
                .unwrap();
            source_transaction_db
                .put(&mut txn, v1_transaction_hash, v1_transaction.clone(), false)
                .unwrap();
            store_execution_result(
                &mut txn,
                &mut execution_results_db,
                TransactionHash::Deploy(deploy_hash_1),
                execution_result_v1,
                block_hash_1,
            );
            store_execution_result(
                &mut txn,
                &mut execution_results_db,
                deploy_transaction_hash,
                execution_result_v2_1.clone(),
                block_hash_2,
            );
            store_execution_result(
                &mut txn,
                &mut execution_results_db,
                v1_transaction_hash,
                execution_result_v2_2.clone(),
                block_hash_2,
            );
            txn.commit().unwrap();
        } else {
            panic!("Couldn't start transaction!");
        }
    }

    let destination_tmp_dir = Arc::new(tempfile::tempdir().unwrap());
    storage::create_output_db(destination_tmp_dir.as_ref()).unwrap();
    storage::transfer_block_info(
        source_tmp_dir.as_ref(),
        destination_tmp_dir.as_ref(),
        block_hash_2,
    )
    .unwrap();

    let destination_fixture =
        LmdbTestFixture::new_with_tmp_dir(destination_tmp_dir, Some(STORAGE_FILE_NAME));
    let destination_env = destination_fixture.env;

    let destination_block_header_db = block_header_database();
    destination_block_header_db
        .create(destination_env.clone())
        .unwrap();
    let destination_block_body_db = block_body_database();
    destination_block_body_db
        .create(destination_env.clone())
        .unwrap();
    let destination_transaction_db = transactions_database();
    destination_transaction_db
        .create(destination_env.clone())
        .unwrap();
    let destination_execution_results_db = execution_results_database();
    destination_execution_results_db
        .create(destination_env.clone())
        .unwrap();

    if let Ok(txn) = destination_env.begin_ro_txn() {
        let block_headers: Vec<BlockHeader> = destination_block_header_db
            .iter_all(&txn)
            .unwrap()
            .map(|x| x.unwrap().1)
            .collect();
        assert_eq!(destination_block_header_db.entry_count(&txn).unwrap(), 1);
        assert_eq!(block_headers.len(), 1);
        assert_eq!(block_headers[0], BlockHeader::V2(block_v2.header().clone()));
        let block_bodies: Vec<BlockBody> = destination_block_body_db
            .iter_all(&txn)
            .unwrap()
            .map(|x| x.unwrap().1)
            .collect();
        assert_eq!(block_bodies.len(), 1);
        assert!(block_bodies.contains(&BlockBody::V2(block_v2.body().clone())));
        let transactions: Vec<casper_types::Transaction> = destination_transaction_db
            .iter_all(&txn)
            .unwrap()
            .map(|x| x.unwrap().1)
            .collect();
        assert_eq!(transactions.len(), 2);
        assert!(transactions.contains(&deploy_transaction));
        assert!(transactions.contains(&v1_transaction));

        let execution_results: Vec<ExecutionResult> = destination_execution_results_db
            .iter_all(&txn)
            .unwrap()
            .map(|x| x.unwrap().1)
            .collect();
        assert_eq!(execution_results.len(), 2);
        assert!(execution_results.contains(&execution_result_v2_1));
        assert!(execution_results.contains(&execution_result_v2_2));
    } else {
        unreachable!("should have been able to start a transaction!")
    }
}

#[test]
fn transfer_global_state_information() {
    let source_tmp_dir = tempfile::tempdir().unwrap();
    let destination_tmp_dir = tempfile::tempdir().unwrap();
    let max_db_size = DEFAULT_MAX_DB_SIZE
        .parse()
        .expect("should be able to parse max db size");
    let source_env = LmdbEnvironment::new(source_tmp_dir.path(), max_db_size, 512, true).unwrap();
    let source_store = LmdbTrieStore::new(&source_env, None, DatabaseFlags::empty()).unwrap();
    // Construct mock data.
    let data = create_data();

    {
        // Put the generated data into the source trie.
        let mut txn = source_env.create_read_write_txn().unwrap();
        let items = data.iter().map(Into::into);
        source_store.put_many(&mut txn, items).unwrap();
        txn.commit().unwrap();
    }

    let _source = load_data_access_layer(
        source_tmp_dir.path(),
        max_db_size,
        Digest::default(),
        true,
        false,
    )
    .unwrap();

    let destination =
        create_data_access_layer(destination_tmp_dir.path(), max_db_size, true, false).unwrap();

    // Copy from `node2`, the root of the created trie. All data under node 2,
    // which has leaf 2 and 3 under it, should be copied.
    transfer_global_state(
        source_tmp_dir.path(),
        destination_tmp_dir.path(),
        data[4].0,
        false,
    )
    .unwrap();

    let destination_store = destination.state().trie_store();
    {
        let txn = destination
            .state()
            .environment()
            .create_read_write_txn()
            .unwrap();
        let keys = [data[1].0, data[2].0, data[4].0];
        let entries: Vec<Option<Trie<Bytes, Bytes>>> =
            destination_store.get_many(&txn, keys.iter()).unwrap();
        for entry in entries {
            match entry {
                Some(trie) => {
                    let trie_in_data = data.iter().find(|test_data| test_data.1 == trie);
                    // Check we are not missing anything since all data under
                    // node 2 should be copied.
                    assert!(trie_in_data.is_some());
                    // Hashes should be equal.
                    assert_eq!(
                        trie_in_data.unwrap().0,
                        Digest::hash(trie.to_bytes().unwrap())
                    );
                }
                None => panic!(),
            }
        }
        txn.commit().unwrap();
    }

    source_tmp_dir.close().unwrap();
    destination_tmp_dir.close().unwrap();
}
