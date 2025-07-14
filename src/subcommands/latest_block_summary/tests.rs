use std::fs::{self, OpenOptions};

use casper_types::{BlockHeader, BlockHeaderV1, PublicKey, SecretKey, testing::TestRng};
use lmdb::Transaction;
use once_cell::sync::Lazy;
use tempfile::{self, NamedTempFile, TempDir};

use super::block_info::BlockInfo;
use crate::{
    common::db::{STORAGE_FILE_NAME, databases::block_header_database},
    subcommands::latest_block_summary::{block_info, read_db},
    test_utils::{LmdbTestFixture, block_v1, block_v2},
};

static OUT_DIR: Lazy<TempDir> = Lazy::new(|| tempfile::tempdir().unwrap());

#[test]
fn parse_network_name_input() {
    let root_dir = tempfile::tempdir().unwrap();
    let first_node = tempfile::tempdir_in(&root_dir).unwrap();
    let second_node = tempfile::tempdir_in(&first_node).unwrap();
    let file = NamedTempFile::new_in(first_node.as_ref()).unwrap();

    assert_eq!(
        block_info::parse_network_name(&second_node).unwrap(),
        second_node.path().file_name().unwrap().to_str().unwrap()
    );
    assert_eq!(
        block_info::parse_network_name(&first_node).unwrap(),
        first_node.path().file_name().unwrap().to_str().unwrap()
    );
    let relative_path_to_first_node = second_node.as_ref().join("..");
    assert_eq!(
        block_info::parse_network_name(relative_path_to_first_node).unwrap(),
        first_node.path().file_name().unwrap().to_str().unwrap()
    );

    assert!(block_info::parse_network_name("/").is_err());
    assert!(block_info::parse_network_name(file.path()).is_err());
}

#[test]
fn dump_with_net_name() {
    let header = BlockHeader::V1(BlockHeaderV1::example().clone());
    let block_info = BlockInfo::new(Some("casper".to_string()), header.block_hash(), header);
    let reference_json = serde_json::to_string_pretty(&block_info).unwrap();

    let out_file_path = OUT_DIR.as_ref().join("casper_network.json");
    {
        let out_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&out_file_path)
            .unwrap();
        read_db::dump_block_info(&block_info, Box::new(out_file)).unwrap();
    }
    assert_eq!(fs::read_to_string(&out_file_path).unwrap(), reference_json);
}

#[test]
fn dump_without_net_name() {
    let header = BlockHeader::V1(BlockHeaderV1::example().clone());
    let block_info = BlockInfo::new(None, header.block_hash(), header);
    let reference_json = serde_json::to_string_pretty(&block_info).unwrap();

    let out_file_path = OUT_DIR.as_ref().join("no_net_name.json");
    {
        let out_file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&out_file_path)
            .unwrap();
        read_db::dump_block_info(&block_info, Box::new(out_file)).unwrap();
    }
    assert_eq!(fs::read_to_string(&out_file_path).unwrap(), reference_json);
}

#[test]
fn latest_block_should_succeed() {
    let mut rng = TestRng::new();
    let secret_key = SecretKey::ed25519_from_bytes([222; SecretKey::ED25519_LENGTH])
        .expect("should create secret key");
    let pk = PublicKey::from(&secret_key);

    let block_v1 = block_v1(&mut rng, vec![]);
    let block_hash_1 = *block_v1.hash();
    let block_v2 = block_v2(&mut rng, pk.clone(), vec![]);
    let block_hash_2 = *block_v2.hash();
    let fixture = LmdbTestFixture::new(Some(STORAGE_FILE_NAME));
    let out_file_path = OUT_DIR.as_ref().join("latest_block_metadata.json");

    let env = fixture.env;
    let block_header_db = block_header_database();
    block_header_db.create(env.clone()).unwrap();
    // Insert the 2 blocks into the database.
    if let Ok(mut txn) = env.begin_rw_txn() {
        block_header_db
            .put(
                &mut txn,
                block_hash_2,
                BlockHeader::V2(block_v2.header().clone()),
                true,
            )
            .unwrap();
        block_header_db
            .put(
                &mut txn,
                block_hash_1,
                BlockHeader::V1(block_v1.header().clone()),
                true,
            )
            .unwrap();
        txn.commit().unwrap();
    };

    // Get the latest block information and ensure it matches with the second block.
    read_db::latest_block_summary(
        fixture.tmp_dir.as_ref(),
        Some(out_file_path.as_path()),
        false,
        false,
    )
    .unwrap();
    let json_str = fs::read_to_string(&out_file_path).unwrap();
    let block_info: BlockInfo = serde_json::from_str(&json_str).unwrap();
    assert_eq!(block_info.block_hash, block_hash_2);

    // Delete the second block from the database.
    if let Ok(mut txn) = env.begin_rw_txn() {
        block_header_db.del(&mut txn, &block_hash_2).unwrap();
        txn.commit().unwrap();
    };

    // Now latest block summary should return information about the first block.
    // Given that the output exists, another run on the same destination path should fail.
    assert!(
        read_db::latest_block_summary(
            fixture.tmp_dir.as_ref(),
            Some(out_file_path.as_path()),
            false,
            false,
        )
        .is_err()
    );
    // We use `overwrite` on the previous output file.
    read_db::latest_block_summary(
        fixture.tmp_dir.as_ref(),
        Some(out_file_path.as_path()),
        true,
        false,
    )
    .unwrap();

    let json_str = fs::read_to_string(&out_file_path).unwrap();
    let block_info: BlockInfo = serde_json::from_str(&json_str).unwrap();
    assert_eq!(block_info.block_hash, block_hash_1);
}

#[test]
fn latest_block_empty_db_should_fail() {
    let fixture = LmdbTestFixture::new(Some(STORAGE_FILE_NAME));
    let out_file_path = OUT_DIR.as_ref().join("empty.json");
    assert!(
        read_db::latest_block_summary(
            fixture.tmp_dir.as_ref(),
            Some(out_file_path.as_path()),
            false,
            false,
        )
        .is_err()
    );
}

#[test]
fn latest_block_existing_output_should_fail() {
    let fixture = LmdbTestFixture::new(Some(STORAGE_FILE_NAME));
    let out_file_path = OUT_DIR.as_ref().join("existing.json");
    let _ = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&out_file_path)
        .unwrap();
    assert!(
        read_db::latest_block_summary(
            fixture.tmp_dir.as_ref(),
            Some(out_file_path.as_path()),
            false,
            false,
        )
        .is_err()
    );
}
