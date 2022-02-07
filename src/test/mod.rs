use casper_node::rpcs::chain::BlockIdentifier;
use retrieve_state::{download_or_read_blocks, storage::create_storage, Client};

use super::db;

const DOWNLOAD_PATH: &str = ".";
const DB_PATH: &str = "./test/storage.lmdb";
const NODE_URL: &str = "http://195.201.174.222:7777/rpc";

async fn prepare_database(block_height: u64) {
    let mut storage = create_storage(DOWNLOAD_PATH).expect("failed to create storage");
    let client = Client::default();

    download_or_read_blocks(
        &client,
        &mut storage,
        NODE_URL,
        Some(&BlockIdentifier::Height(block_height)),
    )
    .await
    .expect("failed to get blocks");
}

#[tokio::test]
async fn should_delete_blocks_over_specified_height() {
    let original_block_height = 100;
    let new_block_height = 50;

    prepare_database(original_block_height).await;

    let (deleted_headers, _deleted_bodies, _deleted_metas) =
        db::run(DB_PATH.into(), new_block_height).expect("failed to delete blocks");

    assert_eq!(deleted_headers, 50);
}

#[tokio::test]
async fn should_create_storage_after_deleting_blocks() {
    let original_block_height = 100;
    let new_block_height = 50;

    prepare_database(original_block_height).await;

    db::run(DB_PATH.into(), new_block_height).expect("failed to delete blocks");

    let storage = create_storage(DOWNLOAD_PATH).expect("failed to create storage");
}
