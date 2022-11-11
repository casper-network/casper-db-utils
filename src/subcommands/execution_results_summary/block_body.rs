/// The [`BlockBody`] struct had to be copied over from `casper-node` because
/// it isn't exported outside of the crate.
use std::fmt::{Display, Formatter, Result as FmtResult};

use casper_hashing::Digest;
use casper_types::{DeployHash, PublicKey};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};

/// The body portion of a block.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub struct BlockBody {
    proposer: PublicKey,
    pub deploy_hashes: Vec<DeployHash>,
    pub transfer_hashes: Vec<DeployHash>,
    #[serde(skip)]
    hash: OnceCell<Digest>,
}

impl BlockBody {
    #[cfg(test)]
    /// Creates a new body from deploy and transfer hashes.
    pub(crate) fn new(deploy_hashes: Vec<DeployHash>) -> Self {
        BlockBody {
            proposer: PublicKey::System,
            deploy_hashes,
            transfer_hashes: vec![],
            hash: OnceCell::new(),
        }
    }

    /// Retrieves the deploy hashes within the block.
    pub(crate) fn deploy_hashes(&self) -> &Vec<DeployHash> {
        &self.deploy_hashes
    }

    /// Retrieves the transfer hashes within the block.
    pub(crate) fn transfer_hashes(&self) -> &Vec<DeployHash> {
        &self.transfer_hashes
    }

    /// Returns deploy hashes of transactions in an order in which they were executed.
    pub(crate) fn deploy_and_transfer_hashes(&self) -> impl Iterator<Item = &DeployHash> {
        self.deploy_hashes()
            .iter()
            .chain(self.transfer_hashes().iter())
    }
}

impl Display for BlockBody {
    fn fmt(&self, formatter: &mut Formatter) -> FmtResult {
        write!(
            formatter,
            "block body proposed by {}, {} deploys, {} transfers",
            self.proposer,
            self.deploy_hashes.len(),
            self.transfer_hashes.len()
        )?;
        Ok(())
    }
}
