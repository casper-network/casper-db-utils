use std::collections::{BTreeMap, BTreeSet};

use casper_types::{BlockSignatures, BlockSignaturesV1, BlockSignaturesV2, PublicKey, U512};

// Returns whether the cumulative `weight` exceeds the weak finality threshold
// for a `total` weight.
fn is_weak_finality(weight: U512, total: U512) -> bool {
    weight * 3 > total
}

// Returns whether the cumulative `weight` exceeds the strict finality
// threshold for a `total` weight.
fn is_strict_finality(weight: U512, total: U512) -> bool {
    weight * 3 > total * 2
}

/// Removes signatures from the given `BlockSignatures` structure until weak
/// but not strict finality is reached and returns whether the operation
/// succeeded. There are signature and weights combinations for which it is
/// not possible to reach a state where weak but not strict finality is
/// reached.
pub(super) fn strip_signatures(
    signatures: BlockSignatures,
    weights: &BTreeMap<PublicKey, U512>,
) -> Option<BlockSignatures> {
    // Calculate the total weight.
    let total_weight: U512 = weights
        .values()
        .fold(U512::zero(), |acc, weight| acc + *weight);

    // Store the signature keys sorted by their respective weight.
    let mut inverse_map: BTreeMap<U512, Vec<&PublicKey>> = BTreeMap::default();
    for (key, weight) in weights.iter() {
        inverse_map.entry(*weight).or_default().push(key);
    }
    let mut accumulated_sigs: BTreeSet<&PublicKey> = Default::default();
    let mut accumulated_weight = U512::zero();
    // Start from the smallest signatures and add them to our pool until weak
    // finality is reached.
    for (weight, key) in inverse_map
        .iter()
        .flat_map(|(weight, keys)| keys.iter().map(move |key| (weight, *key)))
    {
        let mut finality_signatures = signatures.finality_signatures();

        if finality_signatures.any(|finality_signature| finality_signature.public_key().eq(key)) {
            accumulated_weight += *weight;
            accumulated_sigs.insert(key);

            if is_weak_finality(accumulated_weight, total_weight) {
                break;
            }
        }
    }
    // If our pool of signatures is over the strict finality threshold, start
    // removing the smallest ones until we no longer have strict finality.
    while is_strict_finality(accumulated_weight, total_weight) {
        if accumulated_sigs.is_empty() {
            return None;
        }
        let popped_sig = accumulated_sigs.pop_first().unwrap();
        let popped_sig_weight = weights.get(popped_sig).unwrap();
        accumulated_weight -= *popped_sig_weight;
    }
    // At this point, if we don't have weak finality it means it is not
    // possible to create a subset of signatures with weak but not strict
    // finality. This might be because:
    // - the block didn't have weak finality to begin with
    // - there is a super-majority from a very large signature weight (over 2/3
    //   of the weights)
    // - it would have been possible with the given weights, but there are
    //   missing signatures from our set in `BlockSignatures`
    if !is_weak_finality(accumulated_weight, total_weight) {
        return None;
    }

    let trimmed_signatures = match signatures {
        BlockSignatures::V1(block_signatures_v1) => {
            let mut bsv1 = BlockSignaturesV1::new(
                *block_signatures_v1.block_hash(),
                block_signatures_v1.era_id(),
            );
            for signature in block_signatures_v1.finality_signatures() {
                let pk = signature.public_key();
                if accumulated_sigs.contains(pk) {
                    bsv1.insert_signature(pk.clone(), *signature.signature());
                }
            }
            BlockSignatures::V1(bsv1)
        }
        BlockSignatures::V2(block_signatures_v2) => {
            let mut bsv2 = BlockSignaturesV2::new(
                *block_signatures_v2.block_hash(),
                block_signatures_v2.block_height(),
                block_signatures_v2.era_id(),
                block_signatures_v2.chain_name_hash(),
            );
            for signature in block_signatures_v2.finality_signatures() {
                let pk = signature.public_key();
                if accumulated_sigs.contains(pk) {
                    bsv2.insert_signature(pk.clone(), *signature.signature());
                }
            }
            BlockSignatures::V2(bsv2)
        }
    };
    Some(trimmed_signatures)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use casper_types::{
        BlockHash, BlockSignatures, BlockSignaturesV2, ChainNameDigest, EraId, PublicKey,
        Signature, U512,
    };

    use crate::{
        subcommands::purge_signatures::signatures::{
            is_strict_finality, is_weak_finality, strip_signatures,
        },
        test_utils::KEYS,
    };

    #[test]
    fn weak_finality() {
        assert!(!is_weak_finality(1.into(), 3.into()));
        assert!(!is_weak_finality(0.into(), 1_000.into()));
        assert!(!is_weak_finality(10.into(), 1_000.into()));
        assert!(!is_weak_finality(333_333.into(), 1_000_000.into()));

        assert!(is_weak_finality(333_334.into(), 1_000_000.into()));
        assert!(is_weak_finality(666_667.into(), 1_000_000.into()));
        assert!(is_weak_finality(1_000_000.into(), 1_000_000.into()));
    }

    #[test]
    fn strict_finality() {
        assert!(!is_strict_finality(2.into(), 3.into()));
        assert!(!is_strict_finality(0.into(), 1000.into()));
        assert!(!is_strict_finality(10.into(), 1000.into()));
        assert!(!is_strict_finality(333_333.into(), 1_000_000.into()));
        assert!(!is_strict_finality(333_334.into(), 1_000_000.into()));
        assert!(!is_strict_finality(666_666.into(), 1_000_000.into()));

        assert!(is_strict_finality(666_667.into(), 1_000_000.into()));
        assert!(is_strict_finality(900.into(), 1000.into()));
        assert!(is_strict_finality(1000.into(), 1000.into()));
    }

    #[test]
    fn strip_signatures_progressive() {
        let mut block_signatures = build_block_signatures_v2();
        // Create signatures for keys [1..4].
        block_signatures.insert_signature(KEYS[0].clone(), Signature::System);
        block_signatures.insert_signature(KEYS[1].clone(), Signature::System);
        block_signatures.insert_signature(KEYS[2].clone(), Signature::System);
        block_signatures.insert_signature(KEYS[3].clone(), Signature::System);

        let mut weights: BTreeMap<PublicKey, U512> = BTreeMap::default();
        // Add weights for keys [1..4].
        weights.insert(KEYS[0].clone(), 100.into());
        weights.insert(KEYS[1].clone(), 200.into());
        weights.insert(KEYS[2].clone(), 300.into());
        weights.insert(KEYS[3].clone(), 400.into());

        let signatures_after_strip =
            strip_signatures(BlockSignatures::V2(block_signatures.clone()), &weights).unwrap();
        // Signatures from keys [1..3] have a cumulative weight of 600/1000,
        // so signature from key 4 should have been purged.
        let block_signers: Vec<PublicKey> = signatures_after_strip.signers().cloned().collect();
        assert!(block_signers.contains(&KEYS[0]));
        assert!(block_signers.contains(&KEYS[1]));
        assert!(block_signers.contains(&KEYS[2]));
        assert!(!block_signers.contains(&KEYS[3]));
    }

    #[test]
    fn strip_signatures_equal_weights() {
        let mut block_signatures = build_block_signatures_v2();
        // Create signatures for keys [1..2].
        block_signatures.insert_signature(KEYS[0].clone(), Signature::System);
        block_signatures.insert_signature(KEYS[1].clone(), Signature::System);

        let mut weights: BTreeMap<PublicKey, U512> = BTreeMap::default();
        // Add weights for keys [1..2].
        weights.insert(KEYS[0].clone(), 500.into());
        weights.insert(KEYS[1].clone(), 500.into());
        let signatures_after_strip =
            strip_signatures(BlockSignatures::V2(block_signatures.clone()), &weights).unwrap();
        // Any of the signatures has half the weight, so only one should have
        // been kept.
        let block_signers: Vec<PublicKey> = signatures_after_strip.signers().cloned().collect();
        assert_eq!(block_signers.len(), 1);
    }

    #[test]
    fn strip_signatures_one_small_three_large() {
        let mut block_signatures = build_block_signatures_v2();
        // Create signatures for keys [1..4].
        block_signatures.insert_signature(KEYS[0].clone(), Signature::System);
        block_signatures.insert_signature(KEYS[1].clone(), Signature::System);
        block_signatures.insert_signature(KEYS[2].clone(), Signature::System);
        block_signatures.insert_signature(KEYS[2].clone(), Signature::System);

        let mut weights: BTreeMap<PublicKey, U512> = BTreeMap::default();
        // Add weights for keys [1..4].
        weights.insert(KEYS[0].clone(), 1.into());
        weights.insert(KEYS[1].clone(), 333.into());
        weights.insert(KEYS[2].clone(), 333.into());
        weights.insert(KEYS[3].clone(), 333.into());

        let signatures_after_strip =
            strip_signatures(BlockSignatures::V2(block_signatures.clone()), &weights).unwrap();
        // Any of the signatures [2..4] has a third of the weight, so one of
        // them plus the first signature with a weight of 1 make weak but not
        // strict finality.
        let block_signers: Vec<PublicKey> = signatures_after_strip.signers().cloned().collect();
        assert!(block_signers.contains(&KEYS[0]));
        assert_eq!(block_signers.len(), 2);
    }

    #[test]
    fn strip_signatures_split_weights() {
        let mut block_signatures = build_block_signatures_v2();
        // Create signatures for keys [1..3].
        block_signatures.insert_signature(KEYS[0].clone(), Signature::System);
        block_signatures.insert_signature(KEYS[1].clone(), Signature::System);
        block_signatures.insert_signature(KEYS[2].clone(), Signature::System);

        let mut weights: BTreeMap<PublicKey, U512> = BTreeMap::default();
        // Add weights for keys [1..3].
        weights.insert(KEYS[0].clone(), 333.into());
        weights.insert(KEYS[1].clone(), 333.into());
        weights.insert(KEYS[2].clone(), 333.into());

        let signatures_after_strip =
            strip_signatures(BlockSignatures::V2(block_signatures.clone()), &weights).unwrap();
        // Any 2 signatures have a cumulative weight of 666/999, or 2/3 of the
        // weight, so 1 of the 3 signatures should have been purged.
        let block_signers: Vec<PublicKey> = signatures_after_strip.signers().cloned().collect();
        assert_eq!(block_signers.len(), 2);
    }

    #[test]
    fn strip_signatures_one_key_has_strict_finality() {
        let mut block_signatures = build_block_signatures_v2();
        // Create signatures for keys [1..3].
        block_signatures.insert_signature(KEYS[0].clone(), Signature::System);
        block_signatures.insert_signature(KEYS[1].clone(), Signature::System);
        block_signatures.insert_signature(KEYS[2].clone(), Signature::System);

        let mut weights: BTreeMap<PublicKey, U512> = BTreeMap::default();
        // Add weights for keys [1..3].
        weights.insert(KEYS[0].clone(), 100.into());
        weights.insert(KEYS[1].clone(), 200.into());
        weights.insert(KEYS[2].clone(), 700.into());
        // It is not possible to construct a weak but not strict finality set
        // of signatures with the given weights.
        assert!(
            strip_signatures(BlockSignatures::V2(block_signatures.clone()), &weights).is_none()
        );
    }

    #[test]
    fn strip_signatures_single_key() {
        let mut block_signatures = build_block_signatures_v2();
        // Create a signature for key 1.
        block_signatures.insert_signature(KEYS[0].clone(), Signature::System);

        let mut weights: BTreeMap<PublicKey, U512> = BTreeMap::default();
        // Add a weight for key 1.
        weights.insert(KEYS[0].clone(), 1000.into());
        // It is not possible to construct a weak but not strict finality set
        // of signatures with a single weight.
        assert!(
            strip_signatures(BlockSignatures::V2(block_signatures.clone()), &weights).is_none()
        );
    }

    fn build_block_signatures_v2() -> BlockSignaturesV2 {
        BlockSignaturesV2::new(
            BlockHash::new([1; 32].into()),
            100,
            EraId::new(10),
            ChainNameDigest::from_chain_name("abc"),
        )
    }
}
