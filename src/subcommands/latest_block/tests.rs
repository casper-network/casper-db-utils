use std::fs::{self, OpenOptions};

use lmdb::{Transaction, WriteFlags};
use once_cell::sync::Lazy;
use tempfile::{self, TempDir};

use casper_node::{
    rpcs::docs::DocExample,
    types::{BlockHeader, JsonBlockHeader},
};

use super::block_info::BlockInfo;
use crate::{
    subcommands::latest_block::{block_info, read_db},
    test_utils::{LmdbTestFixture, MockBlockHeader},
};

static OUT_DIR: Lazy<TempDir> = Lazy::new(|| tempfile::tempdir().unwrap());

#[test]
fn parse_network_name_input() {
    let root_dir = tempfile::tempdir().unwrap();
    let first_node = tempfile::tempdir_in(&root_dir).unwrap();
    let second_node = tempfile::tempdir_in(&first_node).unwrap();

    assert_eq!(
        block_info::parse_network_name(&second_node).unwrap(),
        first_node.path().file_name().unwrap().to_str().unwrap()
    );
    assert_eq!(
        block_info::parse_network_name(&first_node).unwrap(),
        root_dir.path().file_name().unwrap().to_str().unwrap()
    );
    let relative_path_to_first_node = second_node.as_ref().join("..");
    assert_eq!(
        block_info::parse_network_name(&relative_path_to_first_node).unwrap(),
        root_dir.path().file_name().unwrap().to_str().unwrap()
    );

    assert!(block_info::parse_network_name("/").is_err());
}

#[test]
fn dump_with_net_name() {
    let json_header = JsonBlockHeader::doc_example().clone();
    let header: BlockHeader = json_header.into();
    let block_info = BlockInfo::new(Some("casper".to_string()), header);
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
    let json_header = JsonBlockHeader::doc_example().clone();
    let header: BlockHeader = json_header.into();
    let block_info = BlockInfo::new(None, header);
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
    let fixture = LmdbTestFixture::new(Some("block_header"));
    let out_file_path = OUT_DIR.as_ref().join("latest_block_metadata.json");

    let first_block = MockBlockHeader::default();
    let first_block_key = [0u8, 0u8, 0u8];

    let mut second_block = MockBlockHeader::default();
    let second_block_key = [1u8, 1u8, 1u8];
    second_block.height = 1;

    let env = &fixture.env;
    if let Ok(mut txn) = env.begin_rw_txn() {
        txn.put(
            fixture.db,
            &first_block_key,
            &bincode::serialize(&first_block).unwrap(),
            WriteFlags::empty(),
        )
        .unwrap();
        txn.put(
            fixture.db,
            &second_block_key,
            &bincode::serialize(&second_block).unwrap(),
            WriteFlags::empty(),
        )
        .unwrap();
        txn.commit().unwrap();
    };

    read_db::latest_block(fixture.tmp_file.path(), Some(out_file_path.as_path())).unwrap();
    let json_str = fs::read_to_string(&out_file_path).unwrap();
    let block_info: BlockInfo = serde_json::from_str(&json_str).unwrap();
    let (mock_block_header_deserialized, _network_name) = block_info.into_mock();
    assert_eq!(mock_block_header_deserialized, second_block);
}

#[test]
fn latest_block_empty_db_should_fail() {
    let fixture = LmdbTestFixture::new(Some("block_header_faulty"));
    let out_file_path = OUT_DIR.as_ref().join("empty.json");
    assert!(read_db::latest_block(fixture.tmp_file.path(), Some(out_file_path.as_path())).is_err());
}
