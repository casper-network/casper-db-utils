use std::{
    collections::BTreeMap,
    fmt::{self, Display, Formatter},
};

use casper_node::types::BlockHash;
use casper_types::{EraId, PublicKey, Signature};
use serde::{Deserialize, Serialize};

/// A storage representation of finality signatures with the associated block
/// hash. This structure had to be copied over from the node codebase because
/// it is not publicly accessible through the API.
#[derive(Clone, Debug, Default, PartialOrd, Ord, Hash, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct BlockSignatures {
    /// The block hash for a given block.
    pub(crate) block_hash: BlockHash,
    /// The era id for the given set of finality signatures.
    pub(crate) era_id: EraId,
    /// The signatures associated with the block hash.
    pub(crate) proofs: BTreeMap<PublicKey, Signature>,
}

#[cfg(test)]
impl BlockSignatures {
    pub(crate) fn new(block_hash: BlockHash, era_id: EraId) -> Self {
        Self {
            block_hash,
            era_id,
            proofs: Default::default(),
        }
    }
}

impl Display for BlockSignatures {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(
            formatter,
            "block signatures for hash: {} in era_id: {} with {} proofs",
            self.block_hash,
            self.era_id,
            self.proofs.len()
        )
    }
}
