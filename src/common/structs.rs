use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use casper_types::{
    ApprovalsHash, BlockHash, Key, StoredValue, Transfer, TransferV1,
    bytesrepr::{self, FromBytes, ToBytes},
    execution::{ExecutionResult, ExecutionResultV1},
    global_state::TrieMerkleProof,
};
/// Version 1 metadata related to a single deploy prior to `casper-node` v2.0.0.
/// This is a copy-paste of internal type existing in casper-node
#[derive(Clone, Default, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub(crate) struct DeployMetadataV1 {
    /// The hash of the single block containing the related deploy, along with the results of
    /// executing it.
    ///
    /// Due to reasons, this was implemented as a map, despite the guarantee that there will only
    /// ever be a single entry.
    pub execution_results: HashMap<BlockHash, ExecutionResultV1>,
}

impl From<DeployMetadataV1> for ExecutionResult {
    fn from(v1_results: DeployMetadataV1) -> Self {
        let v1_result = v1_results
            .execution_results
            .into_iter()
            .next()
            // Safe to unwrap as it's guaranteed to contain exactly one entry.
            .expect("must be exactly one result")
            .1;
        ExecutionResult::V1(v1_result)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Default, PartialEq, Eq)]
pub(crate) struct Transfers(Vec<Transfer>);

impl From<Vec<TransferV1>> for Transfers {
    fn from(v1_transfers: Vec<TransferV1>) -> Self {
        Transfers(v1_transfers.into_iter().map(Transfer::V1).collect())
    }
}

impl From<Vec<Transfer>> for Transfers {
    fn from(transfers: Vec<Transfer>) -> Self {
        Transfers(transfers)
    }
}

impl ToBytes for Transfers {
    fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        self.0.to_bytes()
    }

    fn serialized_length(&self) -> usize {
        self.0.serialized_length()
    }

    fn write_bytes(&self, writer: &mut Vec<u8>) -> Result<(), bytesrepr::Error> {
        self.0.write_bytes(writer)
    }
}

impl FromBytes for Transfers {
    fn from_bytes(bytes: &[u8]) -> Result<(Self, &[u8]), bytesrepr::Error> {
        Vec::<Transfer>::from_bytes(bytes)
            .map(|(transfers, remainder)| (Transfers(transfers), remainder))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalsHashes {
    /// Hash of the block that contains deploys that are relevant to the approvals.
    block_hash: BlockHash,
    /// The set of all deploys' finalized approvals' hashes.
    approvals_hashes: Vec<ApprovalsHash>,
    /// The Merkle proof of the checksum registry containing the checksum of the finalized
    /// approvals.
    merkle_proof_approvals: TrieMerkleProof<Key, StoredValue>,
}
impl ApprovalsHashes {
    /// Ctor.
    pub fn new(
        block_hash: BlockHash,
        approvals_hashes: Vec<ApprovalsHash>,
        merkle_proof_approvals: TrieMerkleProof<Key, StoredValue>,
    ) -> Self {
        Self {
            block_hash,
            approvals_hashes,
            merkle_proof_approvals,
        }
    }
}

impl ToBytes for ApprovalsHashes {
    fn write_bytes(&self, writer: &mut Vec<u8>) -> Result<(), bytesrepr::Error> {
        self.block_hash.write_bytes(writer)?;
        self.approvals_hashes.write_bytes(writer)?;
        self.merkle_proof_approvals.write_bytes(writer)
    }

    fn to_bytes(&self) -> Result<Vec<u8>, bytesrepr::Error> {
        let mut buffer = bytesrepr::allocate_buffer(self)?;
        self.write_bytes(&mut buffer)?;
        Ok(buffer)
    }

    fn serialized_length(&self) -> usize {
        self.block_hash.serialized_length()
            + self.approvals_hashes.serialized_length()
            + self.merkle_proof_approvals.serialized_length()
    }
}

impl FromBytes for ApprovalsHashes {
    fn from_bytes(bytes: &[u8]) -> Result<(Self, &[u8]), bytesrepr::Error> {
        let (block_hash, remainder) = BlockHash::from_bytes(bytes)?;
        let (approvals_hashes, remainder) = Vec::<ApprovalsHash>::from_bytes(remainder)?;
        let (merkle_proof_approvals, remainder) =
            TrieMerkleProof::<Key, StoredValue>::from_bytes(remainder)?;
        Ok((
            ApprovalsHashes {
                block_hash,
                approvals_hashes,
                merkle_proof_approvals,
            },
            remainder,
        ))
    }
}

/// Initial version of `ApprovalsHashes` prior to `casper-node` v2.0.0.
#[derive(Deserialize, Serialize)]
pub(crate) struct LegacyApprovalsHashes {
    pub block_hash: BlockHash,
    pub approvals_hashes: Vec<ApprovalsHash>,
    pub merkle_proof_approvals: TrieMerkleProof<Key, StoredValue>,
}

impl From<LegacyApprovalsHashes> for ApprovalsHashes {
    fn from(
        LegacyApprovalsHashes {
            block_hash,
            approvals_hashes,
            merkle_proof_approvals,
        }: LegacyApprovalsHashes,
    ) -> Self {
        ApprovalsHashes::new(block_hash, approvals_hashes, merkle_proof_approvals)
    }
}
