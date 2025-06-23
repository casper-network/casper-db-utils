#![cfg(test)]

use casper_types::execution::execution_result_v1::ExecutionEffect;
use casper_types::execution::{Effects, ExecutionResult, ExecutionResultV1, ExecutionResultV2};
use casper_types::testing::TestRng;
use lmdb::{Environment, EnvironmentFlags, RwTransaction};
use once_cell::sync::{Lazy, OnceCell};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::{collections::BTreeMap, fs::OpenOptions, path::PathBuf, sync::Arc};
use tempfile::{NamedTempFile, TempDir};

use casper_types::{
    BlockHash, BlockHeader, BlockHeaderV1, BlockHeaderV2, BlockV1, BlockV2, Deploy, DeployHash,
    Digest, EraEndV2, Gas, InitiatorAddr, RewardedSignatures, SecretKey, Transaction,
    TransactionHash, TransactionV1, U256,
};
use casper_types::{EraId, ProtocolVersion, PublicKey, Timestamp, U512};

use crate::common::db::VersionedDatabases;
use crate::common::db::versioned_database::serialize_bytesrepr;
use crate::common::structs::DeployMetadataV1;

pub(crate) static KEYS: Lazy<Vec<PublicKey>> = Lazy::new(|| {
    (0..10)
        .map(|i| {
            let u256 = U256::from(i);
            let mut u256_bytes = [0u8; 32];
            u256.to_big_endian(&mut u256_bytes);
            let secret_key =
                SecretKey::ed25519_from_bytes(u256_bytes).expect("should create secret key");
            PublicKey::from(&secret_key)
        })
        .collect()
});

pub struct LmdbTestFixture {
    pub env: Arc<Environment>,
    pub tmp_dir: Arc<TempDir>,
    pub file_path: PathBuf,
}

impl LmdbTestFixture {
    pub fn new(file_name: Option<&str>) -> Self {
        let tmp_dir = Arc::new(tempfile::tempdir().unwrap());
        let (env, file_path) = Self::build_env(file_name, tmp_dir.as_ref().as_ref());

        LmdbTestFixture {
            tmp_dir,
            env,
            file_path,
        }
    }

    pub fn new_with_tmp_dir(tmp_dir: Arc<TempDir>, file_name: Option<&str>) -> Self {
        let (env, file_path) = Self::build_env(file_name, tmp_dir.as_ref().as_ref());

        LmdbTestFixture {
            tmp_dir,
            env,
            file_path,
        }
    }

    fn build_env(file_name: Option<&str>, test_dir: &Path) -> (Arc<Environment>, PathBuf) {
        let file_path = if let Some(name) = file_name {
            let path = test_dir.join(name);
            if !path.exists() {
                let _ = OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&path)
                    .unwrap();
            }
            path
        } else {
            let path = NamedTempFile::new_in(test_dir)
                .unwrap()
                .path()
                .to_path_buf();
            let _ = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)
                .unwrap();
            path
        };
        let env = Environment::new()
            .set_flags(
                EnvironmentFlags::WRITE_MAP
                    | EnvironmentFlags::NO_SUB_DIR
                    | EnvironmentFlags::NO_TLS
                    | EnvironmentFlags::NO_READAHEAD,
            )
            .set_max_readers(12)
            .set_map_size(4096 * 1024)
            .set_max_dbs(10)
            .open(&file_path)
            .expect("can't create environment");
        let env = Arc::new(env);
        (env, file_path)
    }
}

// This struct was created in order to generate `BlockHeaders` and then
// insert them into a mock database. Once `Block::random` becomes part
// of the public API of `casper-types`, this will no longer be needed.
#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
pub struct MockBlockHeader {
    pub parent_hash: BlockHash,
    pub state_root_hash: Digest,
    pub body_hash: Digest,
    pub random_bit: bool,
    pub accumulated_seed: Digest,
    pub era_end: Option<()>,
    pub timestamp: Timestamp,
    pub era_id: EraId,
    pub height: u64,
    pub protocol_version: ProtocolVersion,
}

impl Default for MockBlockHeader {
    fn default() -> Self {
        Self {
            parent_hash: Default::default(),
            state_root_hash: Default::default(),
            body_hash: Default::default(),
            random_bit: Default::default(),
            accumulated_seed: Default::default(),
            era_end: Default::default(),
            timestamp: Timestamp::now(),
            era_id: Default::default(),
            height: Default::default(),
            protocol_version: Default::default(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct EraReport {
    equivocators: Vec<PublicKey>,
    rewards: BTreeMap<PublicKey, u64>,
    inactive_validators: Vec<PublicKey>,
}

#[derive(Clone, Default, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
pub struct EraEnd {
    era_report: EraReport,
    pub next_era_validator_weights: BTreeMap<PublicKey, U512>,
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Debug)]
pub struct MockSwitchBlockHeader {
    pub parent_hash: BlockHash,
    pub state_root_hash: Digest,
    pub body_hash: Digest,
    pub random_bit: bool,
    pub accumulated_seed: Digest,
    pub era_end: Option<EraEnd>,
    pub timestamp: Timestamp,
    pub era_id: EraId,
    pub height: u64,
    pub protocol_version: ProtocolVersion,
}

impl Default for MockSwitchBlockHeader {
    fn default() -> Self {
        Self {
            parent_hash: Default::default(),
            state_root_hash: Default::default(),
            body_hash: Default::default(),
            random_bit: Default::default(),
            accumulated_seed: Default::default(),
            era_end: Some(Default::default()),
            timestamp: Timestamp::now(),
            era_id: Default::default(),
            height: Default::default(),
            protocol_version: Default::default(),
        }
    }
}

pub(crate) fn mock_execution_result() -> ExecutionResult {
    let execution_result = ExecutionResultV1::Success {
        effect: ExecutionEffect::default(),
        transfers: vec![],
        cost: 100.into(),
    };
    ExecutionResult::V1(execution_result)
}

pub(crate) fn mock_execution_result_v2() -> ExecutionResult {
    let execution_result = ExecutionResultV2::example().clone();
    ExecutionResult::V2(Box::new(execution_result))
}

pub(crate) fn random_execution_result_v1(rng: &mut TestRng) -> ExecutionResult {
    let x = rng.r#gen();
    ExecutionResult::V1(x)
}

pub(crate) fn random_execution_result_v2(rng: &mut TestRng) -> ExecutionResult {
    let x = ExecutionResultV2::random(rng);
    ExecutionResult::V2(Box::new(x))
}

pub(crate) fn mock_deploy(rng: &mut TestRng) -> (DeployHash, Deploy) {
    let deploy = Deploy::random_valid_native_transfer(rng);
    (*deploy.hash(), deploy)
}

pub(crate) fn mock_v1_transaction(rng: &mut TestRng) -> (TransactionHash, Transaction) {
    let transaction = Transaction::V1(TransactionV1::random(rng));
    (transaction.hash(), transaction)
}

pub(crate) fn mock_deploy_transaction(rng: &mut TestRng) -> (TransactionHash, Transaction) {
    let (hash, deploy) = mock_deploy(rng);
    (TransactionHash::Deploy(hash), Transaction::Deploy(deploy))
}

pub(crate) fn mock_transaction_hash(idx: u8) -> TransactionHash {
    TransactionHash::Deploy(DeployHash::new([idx; 32].into()))
}

pub(crate) fn mock_block_header_v1(idx: u8) -> (BlockHash, BlockHeader) {
    let block_hash = BlockHash::new([idx; 32].into());
    let block_header_v1 = BlockHeaderV1::new(
        block_hash,
        [3; 32].into(),
        [idx; Digest::LENGTH].into(),
        false,
        [5; 32].into(),
        None,
        Timestamp::now(),
        EraId::new(111),
        2345,
        ProtocolVersion::V2_0_0,
        OnceCell::new(),
    );
    let block_header = BlockHeader::V1(block_header_v1);
    (block_hash, block_header)
}

pub(crate) fn mock_block_header_v2(idx: u8) -> (BlockHash, BlockHeader) {
    let secret_key = SecretKey::ed25519_from_bytes([100 + idx; SecretKey::ED25519_LENGTH])
        .expect("should create secret key");
    let pk = PublicKey::from(&secret_key);
    let block_hash = BlockHash::new([idx; 32].into());
    let block_header_v2 = BlockHeaderV2::new(
        block_hash,
        [3; 32].into(),
        [idx; Digest::LENGTH].into(),
        false,
        [5; 32].into(),
        None,
        Timestamp::now(),
        EraId::new(111),
        2345,
        ProtocolVersion::V2_0_0,
        pk,
        3,
        None,
        OnceCell::new(),
    );
    let block_header = BlockHeader::V2(block_header_v2);
    (block_hash, block_header)
}

pub(crate) fn v2_success_execution_result(pk: PublicKey) -> ExecutionResult {
    let initiator = InitiatorAddr::PublicKey(pk);
    let er_v2 = ExecutionResultV2 {
        initiator,
        error_message: None,
        current_price: 1,
        limit: Gas::new(200),
        consumed: Gas::new(180),
        cost: 100.into(),
        refund: 0.into(),
        transfers: vec![],
        size_estimate: 110,
        effects: Effects::default(),
    };
    ExecutionResult::V2(Box::new(er_v2))
}

pub(crate) fn block_v2_heigh_and_era_end(
    rng: &mut TestRng,
    pk: PublicKey,
    deploys: Vec<TransactionHash>,
    era_id: u64,
    height: u64,
    era_end: Option<EraEndV2>,
    protocol_version: ProtocolVersion,
) -> BlockV2 {
    let mut transactions = BTreeMap::new();
    deploys
        .into_iter()
        .for_each(|th| match transactions.entry(1) {
            std::collections::btree_map::Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(vec![th]);
            }
            std::collections::btree_map::Entry::Occupied(mut occupied_entry) => {
                occupied_entry.get_mut().push(th);
            }
        });
    BlockV2::new(
        BlockHash::new(rng.r#gen()),
        [2; 32].into(),
        [3; 32].into(),
        false,
        era_end,
        Timestamp::now(),
        EraId::new(era_id),
        height,
        protocol_version,
        pk,
        transactions,
        RewardedSignatures::default(),
        1,
        None,
    )
}

pub(crate) fn block_v2_with_height(
    rng: &mut TestRng,
    pk: PublicKey,
    deploys: Vec<TransactionHash>,
    era_id: u64,
    height: u64,
    era_end: Option<EraEndV2>,
) -> BlockV2 {
    block_v2_heigh_and_era_end(
        rng,
        pk,
        deploys,
        era_id,
        height,
        era_end,
        ProtocolVersion::V2_0_0,
    )
}

pub(crate) fn block_v2(rng: &mut TestRng, pk: PublicKey, deploys: Vec<TransactionHash>) -> BlockV2 {
    block_v2_with_height(rng, pk, deploys, 112, 555, None)
}

pub(crate) fn block_v1(rng: &mut TestRng, deploy_hashes: Vec<DeployHash>) -> BlockV1 {
    BlockV1::random_with_specifics(
        rng,
        EraId::new(10),
        110,
        ProtocolVersion::V1_0_0,
        false,
        deploy_hashes,
    )
}

pub(crate) fn store_execution_result(
    txn: &mut RwTransaction,
    db: &mut VersionedDatabases<TransactionHash, ExecutionResult>,
    transaction_hash: TransactionHash,
    execution_result: casper_types::execution::ExecutionResult,
    block_hash: BlockHash,
) {
    store_execution_result_opt_block_hash(
        txn,
        db,
        transaction_hash,
        execution_result,
        Some(block_hash),
    )
}

pub(crate) fn store_execution_result_opt_block_hash(
    txn: &mut RwTransaction,
    db: &mut VersionedDatabases<TransactionHash, ExecutionResult>,
    transaction_hash: TransactionHash,
    execution_result: casper_types::execution::ExecutionResult,
    block_hash: Option<BlockHash>,
) {
    match (transaction_hash, execution_result) {
        (
            TransactionHash::Deploy(deploy_hash),
            casper_types::execution::ExecutionResult::V1(execution_result_v1),
        ) => {
            //For v1 the block hash is mandatory and we cannot store without it
            let value = DeployMetadataV1::new(block_hash.unwrap(), execution_result_v1);
            let raw = bincode::serialize(&value).unwrap();
            db.put_raw(txn, deploy_hash.as_ref().to_vec(), raw, true)
                .unwrap();
        }
        (transaction_hash, ref er @ casper_types::execution::ExecutionResult::V2(_)) => {
            let key_raw = serialize_bytesrepr(&transaction_hash).unwrap();
            let data_raw = serialize_bytesrepr(er).unwrap();
            db.put_raw(txn, key_raw, data_raw, false).unwrap();
        }
        _ => todo!(),
    }
}
