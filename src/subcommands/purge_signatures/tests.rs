use std::collections::{BTreeMap, BTreeSet};

use casper_types::testing::TestRng;
use casper_types::{
    BlockHash, BlockHeader, BlockSignatures, BlockSignaturesV2, ChainNameDigest, EraEndV2,
    PublicKey, SecretKey,
};
use casper_types::{ProtocolVersion, Signature, U512};
use lmdb::Transaction;

use crate::common::db::Error as DbError;
use crate::common::db::databases::{block_header_database, block_metadata_database};
use crate::common::db::versioned_database::serialize_bytesrepr;
use crate::test_utils::{block_v2_heigh_and_era_end, block_v2_with_height};
use crate::{
    subcommands::purge_signatures::{
        Error,
        purge::{EraWeights, initialize_indices, purge_signatures_for_blocks},
    },
    test_utils::{KEYS, LmdbTestFixture},
};

#[test]
fn indices_initialization() {
    let secret_key = SecretKey::ed25519_from_bytes([111; SecretKey::ED25519_LENGTH])
        .expect("should create secret key");
    let pk = PublicKey::from(&secret_key);
    let mut rng = TestRng::new();

    let fixture = LmdbTestFixture::new(None);

    // Create mock block headers.
    // Set an era and height for each one.
    let block_headers = vec![
        block_v2_with_height(&mut rng, pk.clone(), vec![], 10, 100, None),
        block_v2_with_height(&mut rng, pk.clone(), vec![], 10, 200, None),
        block_v2_with_height(&mut rng, pk.clone(), vec![], 20, 300, None),
        block_v2_with_height(&mut rng, pk.clone(), vec![], 20, 400, None),
    ];

    // Create mock switch blocks for each era.
    let switch_block_headers = vec![
        block_v2_with_height(
            &mut rng,
            pk.clone(),
            vec![],
            9,
            80,
            Some(EraEndV2::example().clone()),
        ),
        block_v2_with_height(
            &mut rng,
            pk.clone(),
            vec![],
            19,
            280,
            Some(EraEndV2::example().clone()),
        ),
    ];

    let env = fixture.env;
    let block_header_db = block_header_database();
    block_header_db.create(env.clone()).unwrap();
    // Insert the blocks into the database.
    if let Ok(mut txn) = env.begin_rw_txn() {
        for block_header in block_headers.iter() {
            // Store the block header.
            let key = *block_header.hash();
            block_header_db
                .put(
                    &mut txn,
                    key,
                    BlockHeader::V2(block_header.header().clone()),
                    true,
                )
                .unwrap();
        }
        for block_header in switch_block_headers.iter() {
            // Store the block header.
            let key = *block_header.hash();
            block_header_db
                .put(
                    &mut txn,
                    key,
                    BlockHeader::V2(block_header.header().clone()),
                    true,
                )
                .unwrap();
        }
        txn.commit().unwrap();
    };

    let indices = initialize_indices(env.clone(), &BTreeSet::from([100, 200, 300])).unwrap();
    // Make sure we have the relevant blocks in the indices.
    assert_eq!(
        indices.heights.get(&block_headers[0].height()).unwrap().0,
        block_headers[0].hash().clone()
    );
    assert_eq!(
        indices.heights.get(&block_headers[1].height()).unwrap().0,
        block_headers[1].hash().clone()
    );
    assert_eq!(
        indices.heights.get(&block_headers[2].height()).unwrap().0,
        block_headers[2].hash().clone()
    );
    // And that the irrelevant ones are not included.
    assert!(!indices.heights.contains_key(&block_headers[3].height()));
    // Make sure we got all the switch blocks.
    assert_eq!(
        *indices
            .switch_blocks
            .get(&block_headers[0].era_id())
            .unwrap(),
        switch_block_headers[0].hash().clone()
    );
    assert_eq!(
        *indices
            .switch_blocks
            .get(&block_headers[2].era_id())
            .unwrap(),
        switch_block_headers[1].hash().clone()
    );

    // Test for a header with a height which we already have in the db.
    let duplicate_header =
        block_v2_with_height(&mut rng, pk, vec![], 10, block_headers[0].height(), None);
    if let Ok(mut txn) = env.begin_rw_txn() {
        // Store the header with duplicated height.
        block_header_db
            .put(
                &mut txn,
                *duplicate_header.hash(),
                BlockHeader::V2(duplicate_header.header().clone()),
                true,
            )
            .unwrap();
        txn.commit().unwrap();
    };

    match initialize_indices(env.clone(), &BTreeSet::from([100, 200, 300])) {
        Err(Error::DuplicateBlock(height)) => assert_eq!(height, block_headers[0].height()),
        _ => panic!("Unexpected error"),
    }
}

#[test]
fn indices_initialization_with_upgrade() {
    let mut rng = TestRng::new();
    let secret_key = SecretKey::ed25519_from_bytes([222; SecretKey::ED25519_LENGTH])
        .expect("should create secret key");
    let pk = PublicKey::from(&secret_key);

    let fixture = LmdbTestFixture::new(None);
    // Create mock block headers.
    let blocks = [
        block_v2_with_height(&mut rng, pk.clone(), vec![], 10, 80, None),
        block_v2_heigh_and_era_end(
            &mut rng,
            pk.clone(),
            vec![],
            11,
            200,
            None,
            ProtocolVersion::from_parts(3, 1, 0),
        ),
        block_v2_heigh_and_era_end(
            &mut rng,
            pk.clone(),
            vec![],
            12,
            290,
            None,
            ProtocolVersion::from_parts(4, 0, 0),
        ),
        block_v2_heigh_and_era_end(
            &mut rng,
            pk.clone(),
            vec![],
            13,
            350,
            None,
            ProtocolVersion::from_parts(4, 0, 0),
        ),
    ];

    // Create mock switch blocks.
    let switch_blocks = vec![
        block_v2_with_height(
            &mut rng,
            pk.clone(),
            vec![],
            9,
            60,
            Some(EraEndV2::example().clone()),
        ),
        block_v2_with_height(
            &mut rng,
            pk.clone(),
            vec![],
            10,
            180,
            Some(EraEndV2::example().clone()),
        ),
        block_v2_heigh_and_era_end(
            &mut rng,
            pk.clone(),
            vec![],
            11,
            250,
            Some(EraEndV2::example().clone()),
            ProtocolVersion::from_parts(3, 1, 0),
        ),
        block_v2_heigh_and_era_end(
            &mut rng,
            pk.clone(),
            vec![],
            12,
            300,
            Some(EraEndV2::example().clone()),
            ProtocolVersion::from_parts(4, 0, 0),
        ),
    ];

    let env = fixture.env;
    let block_header_db = block_header_database();
    block_header_db.create(env.clone()).unwrap();

    // Insert the blocks into the database.
    if let Ok(mut txn) = env.begin_rw_txn() {
        for block in blocks.iter().chain(switch_blocks.iter()) {
            let block_header = block.header().clone();
            // Store the block header.
            let key = *block.hash();
            block_header_db
                .put(&mut txn, key, BlockHeader::V2(block_header.clone()), true)
                .unwrap();
        }
        txn.commit().unwrap();
    };

    let indices = initialize_indices(env.clone(), &BTreeSet::from([100, 200, 300])).unwrap();
    assert!(
        !indices
            .switch_blocks_before_upgrade
            .contains(&switch_blocks[0].height())
    );
    assert!(
        indices
            .switch_blocks_before_upgrade
            .contains(&switch_blocks[1].height())
    );
    assert!(
        indices
            .switch_blocks_before_upgrade
            .contains(&switch_blocks[2].height())
    );
    assert!(
        !indices
            .switch_blocks_before_upgrade
            .contains(&switch_blocks[3].height())
    );
}

fn era_end_with_one_validator_weight(key: PublicKey, weight: u64) -> EraEndV2 {
    EraEndV2::new(
        vec![],
        vec![],
        BTreeMap::from([(key, weight.into())]),
        BTreeMap::new(),
        1,
    )
}

fn era_end_with_validator_weights(weights: Vec<(PublicKey, u64)>) -> EraEndV2 {
    let mut btreemap = BTreeMap::new();
    for (pk, w) in weights {
        btreemap.insert(pk, w.into());
    }
    EraEndV2::new(vec![], vec![], btreemap, BTreeMap::new(), 1)
}

#[test]
fn era_weights() {
    let mut rng = TestRng::new();
    let secret_key = SecretKey::ed25519_from_bytes([222; SecretKey::ED25519_LENGTH])
        .expect("should create secret key");
    let pk = PublicKey::from(&secret_key);

    let fixture = LmdbTestFixture::new(None);

    // Create mock switch blocks.
    let switch_blocks = vec![
        block_v2_with_height(
            &mut rng,
            pk.clone(),
            vec![],
            10,
            80,
            Some(era_end_with_one_validator_weight(KEYS[0].clone(), 100)),
        ),
        block_v2_with_height(
            &mut rng,
            pk.clone(),
            vec![],
            20,
            280,
            Some(era_end_with_one_validator_weight(KEYS[1].clone(), 100)),
        ),
    ];

    let env = fixture.env.clone();
    let block_header_db = block_header_database();
    block_header_db.create(env.clone()).unwrap();

    // Insert the blocks into the database.
    if let Ok(mut txn) = env.begin_rw_txn() {
        for block in switch_blocks.iter() {
            let block_header = block.header().clone();
            // Store the block header.
            let key = *block.hash();
            block_header_db
                .put(&mut txn, key, BlockHeader::V2(block_header.clone()), true)
                .unwrap();
        }
        txn.commit().unwrap();
    };
    let indices = initialize_indices(env.clone(), &BTreeSet::from([80])).unwrap();
    let mut era_weights = EraWeights::default();
    let block_header_db = block_header_database();
    block_header_db.create(env.clone()).unwrap();
    if let Ok(txn) = env.begin_ro_txn() {
        // Try to update the weights for the first switch block.
        assert!(
            !era_weights
                .refresh_weights_for_era(
                    &txn,
                    block_header_db.clone(),
                    &indices,
                    switch_blocks[0].era_id().successor()
                )
                .unwrap()
        );
        assert_eq!(era_weights.era_id(), switch_blocks[0].era_id().successor());
        assert_eq!(
            *era_weights.weights_mut().get(&KEYS[0]).unwrap(),
            U512::from(100)
        );
        assert!(!era_weights.weights_mut().contains_key(&KEYS[1]));

        // Try to update the weights for the second switch block.
        assert!(
            !era_weights
                .refresh_weights_for_era(
                    &txn,
                    block_header_db.clone(),
                    &indices,
                    switch_blocks[1].era_id().successor()
                )
                .unwrap()
        );
        assert_eq!(era_weights.era_id(), switch_blocks[1].era_id().successor());
        assert_eq!(
            *era_weights.weights_mut().get(&KEYS[1]).unwrap(),
            U512::from(100)
        );
        assert!(!era_weights.weights_mut().contains_key(&KEYS[0]));

        // Try to update the weights for the second switch block again.
        assert!(
            !era_weights
                .refresh_weights_for_era(
                    &txn,
                    block_header_db.clone(),
                    &indices,
                    switch_blocks[1].era_id().successor()
                )
                .unwrap()
        );
        assert_eq!(era_weights.era_id(), switch_blocks[1].era_id().successor());
        assert_eq!(
            *era_weights.weights_mut().get(&KEYS[1]).unwrap(),
            U512::from(100)
        );
        assert!(!era_weights.weights_mut().contains_key(&KEYS[0]));

        // Try to update the weights for a nonexistent switch block.
        let expected_missing_era_id = switch_blocks[1].era_id().successor().successor();
        match era_weights.refresh_weights_for_era(
            &txn,
            block_header_db.clone(),
            &indices,
            expected_missing_era_id,
        ) {
            Err(Error::MissingEraWeights(actual_missing_era_id)) => {
                assert_eq!(expected_missing_era_id, actual_missing_era_id)
            }
            _ => panic!("Unexpected failure"),
        }
        txn.commit().unwrap();
    };

    if let Ok(mut txn) = env.begin_rw_txn() {
        // Delete the weights for the first switch block in the db.
        let block = block_v2_with_height(&mut rng, pk.clone(), vec![], 9, 60, None);
        let block_header = block.header().clone();
        let block_hash = switch_blocks[0].hash(); //We want to 
        //store the new era-endless block under the old key not the new randomized one

        block_header_db
            .put(&mut txn, *block_hash, BlockHeader::V2(block_header), true)
            .unwrap();
        txn.commit().unwrap();
    };
    if let Ok(txn) = env.begin_ro_txn() {
        let expected_missing_era_id = switch_blocks[0].era_id().successor();
        // Make sure we get an error when the block has no weights.
        match era_weights.refresh_weights_for_era(
            &txn,
            block_header_db,
            &indices,
            expected_missing_era_id,
        ) {
            Err(Error::MissingEraWeights(actual_missing_era_id)) => {
                assert_eq!(expected_missing_era_id, actual_missing_era_id)
            }
            _ => panic!("Unexpected failure"),
        }
        txn.commit().unwrap();
    };
}

#[test]
fn era_weights_with_upgrade() {
    let mut rng = TestRng::new();
    let secret_key = SecretKey::ed25519_from_bytes([222; SecretKey::ED25519_LENGTH])
        .expect("should create secret key");
    let pk = PublicKey::from(&secret_key);
    let fixture = LmdbTestFixture::new(None);
    // Create mock switch blocks.
    let switch_blocks = vec![
        block_v2_with_height(
            &mut rng,
            pk.clone(),
            vec![],
            10,
            80,
            Some(era_end_with_one_validator_weight(KEYS[0].clone(), 100)),
        ),
        block_v2_heigh_and_era_end(
            &mut rng,
            pk.clone(),
            vec![],
            11,
            280,
            Some(era_end_with_one_validator_weight(KEYS[1].clone(), 100)),
            ProtocolVersion::from_parts(3, 1, 0),
        ),
    ];

    let env = fixture.env.clone();
    let block_header_db = block_header_database();
    block_header_db.create(env.clone()).unwrap();
    // Insert the blocks into the database.
    if let Ok(mut txn) = env.begin_rw_txn() {
        for block in switch_blocks.iter() {
            let block_header = block.header().clone();
            // Store the block header.
            let key = *block.hash();
            block_header_db
                .put(&mut txn, key, BlockHeader::V2(block_header.clone()), true)
                .unwrap();
        }
        txn.commit().unwrap();
    };
    let indices = initialize_indices(env.clone(), &BTreeSet::from([80, 280])).unwrap();
    let mut era_weights = EraWeights::default();
    if let Ok(txn) = env.begin_ro_txn() {
        assert!(
            era_weights
                .refresh_weights_for_era(
                    &txn,
                    block_header_db.clone(),
                    &indices,
                    switch_blocks[0].era_id().successor()
                )
                .unwrap()
        );

        assert!(
            !era_weights
                .refresh_weights_for_era(
                    &txn,
                    block_header_db.clone(),
                    &indices,
                    switch_blocks[1].era_id().successor()
                )
                .unwrap()
        );

        assert!(
            era_weights
                .refresh_weights_for_era(
                    &txn,
                    block_header_db.clone(),
                    &indices,
                    switch_blocks[0].era_id().successor()
                )
                .unwrap()
        );

        assert!(
            !era_weights
                .refresh_weights_for_era(
                    &txn,
                    block_header_db.clone(),
                    &indices,
                    switch_blocks[1].era_id().successor()
                )
                .unwrap()
        );

        txn.commit().unwrap();
    };
}

#[test]
fn purge_signatures_should_work() {
    let mut rng = TestRng::new();
    let secret_key = SecretKey::ed25519_from_bytes([222; SecretKey::ED25519_LENGTH])
        .expect("should create secret key");
    let pk = PublicKey::from(&secret_key);
    let fixture = LmdbTestFixture::new(None);
    // Create mock block headers.

    let blocks = [
        block_v2_with_height(&mut rng, pk.clone(), vec![], 10, 100, None),
        block_v2_with_height(&mut rng, pk.clone(), vec![], 10, 200, None),
        block_v2_with_height(&mut rng, pk.clone(), vec![], 20, 300, None),
        block_v2_with_height(&mut rng, pk.clone(), vec![], 20, 400, None),
    ];
    let mut block_signatures: Vec<BlockSignaturesV2> = blocks
        .iter()
        .map(|block| {
            BlockSignaturesV2::new(
                *block.hash(),
                block.height(),
                block.era_id(),
                ChainNameDigest::from_chain_name("abc"),
            )
        })
        .collect();
    let switch_blocks = vec![
        block_v2_with_height(
            &mut rng,
            pk.clone(),
            vec![],
            9,
            80,
            Some(era_end_with_validator_weights(vec![
                (KEYS[0].clone(), 500),
                (KEYS[1].clone(), 500),
            ])),
        ),
        block_v2_with_height(
            &mut rng,
            pk.clone(),
            vec![],
            19,
            280,
            Some(era_end_with_validator_weights(vec![
                (KEYS[0].clone(), 300),
                (KEYS[1].clone(), 300),
                (KEYS[2].clone(), 400),
            ])),
        ),
    ];
    block_signatures[0].insert_signature(KEYS[0].clone(), Signature::System);
    block_signatures[0].insert_signature(KEYS[1].clone(), Signature::System);

    block_signatures[1].insert_signature(KEYS[0].clone(), Signature::System);

    block_signatures[2].insert_signature(KEYS[0].clone(), Signature::System);
    block_signatures[2].insert_signature(KEYS[1].clone(), Signature::System);
    block_signatures[2].insert_signature(KEYS[2].clone(), Signature::System);

    block_signatures[3].insert_signature(KEYS[0].clone(), Signature::System);
    block_signatures[3].insert_signature(KEYS[2].clone(), Signature::System);

    let env = fixture.env.clone();
    let block_header_db = block_header_database();
    block_header_db.create(env.clone()).unwrap();
    let block_metadata_db = block_metadata_database();
    block_metadata_db.create(env.clone()).unwrap();

    // Insert the blocks and signatures into the database.
    if let Ok(mut txn) = env.begin_rw_txn() {
        for (idx, block) in blocks.iter().enumerate() {
            let block_header = block.header().clone();
            // Store the block header.
            let key = *block.hash();
            block_header_db
                .put(&mut txn, key, BlockHeader::V2(block_header.clone()), true)
                .unwrap();
            let signature = BlockSignatures::V2(block_signatures[idx].clone());
            block_metadata_db
                .put(&mut txn, key, signature, true)
                .unwrap();
        }

        for block in switch_blocks.iter() {
            let block_header = block.header().clone();
            // Store the block header.
            let key = *block.hash();
            block_header_db
                .put(&mut txn, key, BlockHeader::V2(block_header.clone()), true)
                .unwrap();
        }
        txn.commit().unwrap();
    };

    let indices = initialize_indices(env.clone(), &BTreeSet::from([100, 200, 300, 400])).unwrap();

    // Purge signatures for blocks 1, 2 and 3 to weak finality.
    assert!(
        purge_signatures_for_blocks(
            env.clone(),
            &indices,
            BTreeSet::from([100, 200, 300]),
            false
        )
        .is_ok()
    );
    if let Ok(txn) = env.begin_ro_txn() {
        let block_1_sigs = fetch_v2_signature(blocks[0].hash(), &block_metadata_db, &txn);
        // For block 1, any of the 2 signatures will be fine (500/1000), but
        // not both.
        let block_1_signers: Vec<PublicKey> = block_1_sigs.signers().cloned().collect();
        assert!(
            (block_1_signers.contains(&KEYS[0]) && !block_1_signers.contains(&KEYS[1]))
                || (!block_1_signers.contains(&KEYS[0]) && block_1_signers.contains(&KEYS[1]))
        );

        // Block 2 only had the first signature, which already meets the
        // requirements (500/1000).
        let block_2_sigs = fetch_v2_signature(blocks[1].hash(), &block_metadata_db, &txn);
        let block_2_signers: Vec<PublicKey> = block_2_sigs.signers().cloned().collect();
        assert!(block_2_signers.contains(&KEYS[0]));
        assert!(!block_2_signers.contains(&KEYS[1]));

        // Block 3 had all the keys (300, 300, 400), so it should have kept
        // the first 2.
        let block_3_sigs = fetch_v2_signature(blocks[2].hash(), &block_metadata_db, &txn);
        let block_3_signers: Vec<PublicKey> = block_3_sigs.signers().cloned().collect();
        assert!(block_3_signers.contains(&KEYS[0]));
        assert!(block_3_signers.contains(&KEYS[1]));
        assert!(!block_3_signers.contains(&KEYS[2]));

        // Block 4 had signatures for keys 1 (300) and 3 (400), but it was not
        // included in the purge list, so it should have kept both.
        let block_4_sigs = fetch_v2_signature(blocks[3].hash(), &block_metadata_db, &txn);
        let block_4_signers: Vec<PublicKey> = block_4_sigs.signers().cloned().collect();
        assert!(block_4_signers.contains(&KEYS[0]));
        assert!(!block_4_signers.contains(&KEYS[1]));
        assert!(block_4_signers.contains(&KEYS[2]));
        txn.commit().unwrap();
    };

    // Purge signatures for blocks 1 and 4 to no finality.
    assert!(
        purge_signatures_for_blocks(env.clone(), &indices, BTreeSet::from([100, 400]), true)
            .is_ok()
    );
    if let Ok(txn) = env.begin_ro_txn() {
        // We should have no record for the signatures of block 1.

        assert!(
            block_metadata_db
                .get(&txn, blocks[0].hash())
                .unwrap()
                .is_none()
        );

        // Block 2 should be the same as before.
        let block_2_sigs = fetch_v2_signature(blocks[1].hash(), &block_metadata_db, &txn);
        let block_2_signers: Vec<PublicKey> = block_2_sigs.signers().cloned().collect();
        assert!(block_2_signers.contains(&KEYS[0]));
        assert!(!block_2_signers.contains(&KEYS[1]));

        // Block 3 should be the same as before.
        let block_3_sigs = fetch_v2_signature(blocks[2].hash(), &block_metadata_db, &txn);
        let block_3_signers: Vec<PublicKey> = block_3_sigs.signers().cloned().collect();
        assert!(block_3_signers.contains(&KEYS[0]));
        assert!(block_3_signers.contains(&KEYS[1]));
        assert!(!block_3_signers.contains(&KEYS[2]));

        // We should have no record for the signatures of block 4.
        assert!(
            block_metadata_db
                .get(&txn, blocks[3].hash())
                .unwrap()
                .is_none()
        );
        txn.commit().unwrap();
    };
}

#[test]
fn purge_signatures_bad_input() {
    let mut rng = TestRng::new();
    let secret_key = SecretKey::ed25519_from_bytes([222; SecretKey::ED25519_LENGTH])
        .expect("should create secret key");
    let pk = PublicKey::from(&secret_key);
    let fixture = LmdbTestFixture::new(None);
    // Create mock block headers.
    let blocks = [
        block_v2_with_height(&mut rng, pk.clone(), vec![], 10, 100, None),
        block_v2_with_height(&mut rng, pk.clone(), vec![], 20, 200, None),
    ];
    let mut block_signatures: Vec<BlockSignaturesV2> = blocks
        .iter()
        .map(|block| {
            BlockSignaturesV2::new(
                *block.hash(),
                block.height(),
                block.era_id(),
                ChainNameDigest::from_chain_name("abc"),
            )
        })
        .collect();

    let switch_blocks = vec![
        block_v2_with_height(
            &mut rng,
            pk.clone(),
            vec![],
            9,
            80,
            Some(era_end_with_validator_weights(vec![
                (KEYS[0].clone(), 700),
                (KEYS[1].clone(), 300),
            ])),
        ),
        block_v2_with_height(
            &mut rng,
            pk.clone(),
            vec![],
            19,
            180,
            Some(era_end_with_validator_weights(vec![
                (KEYS[0].clone(), 400),
                (KEYS[1].clone(), 600),
            ])),
        ),
    ];
    block_signatures[0].insert_signature(KEYS[0].clone(), Signature::System);
    block_signatures[0].insert_signature(KEYS[1].clone(), Signature::System);

    block_signatures[1].insert_signature(KEYS[0].clone(), Signature::System);
    block_signatures[1].insert_signature(KEYS[1].clone(), Signature::System);

    let env = fixture.env.clone();
    let block_header_db = block_header_database();
    block_header_db.create(env.clone()).unwrap();
    let block_metadata_db = block_metadata_database();
    block_metadata_db.create(env.clone()).unwrap();

    // Insert the blocks and signatures into the database.
    if let Ok(mut txn) = env.begin_rw_txn() {
        for (idx, block) in blocks.iter().enumerate() {
            let block_header = block.header().clone();
            // Store the block header.
            let key = *block.hash();
            block_header_db
                .put(&mut txn, key, BlockHeader::V2(block_header.clone()), true)
                .unwrap();
            let signature = BlockSignatures::V2(block_signatures[idx].clone());
            block_metadata_db
                .put(&mut txn, key, signature, true)
                .unwrap();
        }

        for block in switch_blocks.iter() {
            let block_header = block.header().clone();
            // Store the block header.
            let key = *block.hash();
            block_header_db
                .put(&mut txn, key, BlockHeader::V2(block_header.clone()), true)
                .unwrap();
        }
        txn.commit().unwrap();
    };

    let indices = initialize_indices(env.clone(), &BTreeSet::from([100])).unwrap();
    // Purge signatures for blocks 1 and 2 to weak finality.
    assert!(
        purge_signatures_for_blocks(env.clone(), &indices, BTreeSet::from([100, 200]), false)
            .is_ok()
    );
    if let Ok(txn) = env.begin_ro_txn() {
        let block_1_sigs = fetch_v2_signature(blocks[0].hash(), &block_metadata_db, &txn);
        // For block 1, any of the 2 signatures will be fine (500/1000), but
        // not both.
        let block_1_signers: Vec<PublicKey> = block_1_sigs.signers().cloned().collect();
        // Block 1 has a super-majority signature (700), so the purge would
        // have failed and the signatures are untouched.
        assert!(block_1_signers.contains(&KEYS[0]));
        assert!(block_1_signers.contains(&KEYS[1]));

        let block_2_sigs = fetch_v2_signature(blocks[1].hash(), &block_metadata_db, &txn);
        let block_2_signers: Vec<PublicKey> = block_2_sigs.signers().cloned().collect();
        // Block 2 wasn't in the purge list, so it should be untouched.
        assert!(block_2_signers.contains(&KEYS[0]));
        assert!(block_2_signers.contains(&KEYS[1]));
        txn.commit().unwrap();
    };

    // Overwrite the signatures for block 2 with bogus data.
    if let Ok(mut txn) = env.begin_rw_txn() {
        // Store the signatures.
        let raw_key = serialize_bytesrepr(blocks[1].hash()).unwrap();
        let raw_value = serialize_bytesrepr(&[0u8, 1u8, 2u8]).unwrap();
        block_metadata_db
            .put_raw(&mut txn, raw_key, raw_value, false)
            .unwrap();
        txn.commit().unwrap();
    };

    let indices = initialize_indices(env.clone(), &BTreeSet::from([100, 200])).unwrap();
    // Purge should fail with a deserialization error.
    match purge_signatures_for_blocks(env.clone(), &indices, BTreeSet::from([100, 200]), false) {
        Err(Error::DatabaseLayer(DbError::Parsing(3, _))) => {}
        other => panic!("Unexpected result: {other:?}"),
    };
}

#[test]
fn purge_signatures_missing_from_db() {
    let mut rng = TestRng::new();
    let secret_key = SecretKey::ed25519_from_bytes([222; SecretKey::ED25519_LENGTH])
        .expect("should create secret key");
    let pk = PublicKey::from(&secret_key);

    let fixture = LmdbTestFixture::new(None);
    // Create mock block headers.
    let blocks = [
        block_v2_with_height(&mut rng, pk.clone(), vec![], 10, 100, None),
        block_v2_with_height(&mut rng, pk.clone(), vec![], 10, 200, None),
    ];

    let mut block_signatures: Vec<BlockSignaturesV2> = blocks
        .iter()
        .map(|block| {
            BlockSignaturesV2::new(
                *block.hash(),
                block.height(),
                block.era_id(),
                ChainNameDigest::from_chain_name("abc"),
            )
        })
        .collect();

    // Create mock switch block headers.
    let switch_block = block_v2_with_height(
        &mut rng,
        pk.clone(),
        vec![],
        9,
        80,
        Some(era_end_with_validator_weights(vec![
            (KEYS[0].clone(), 400),
            (KEYS[1].clone(), 600),
        ])),
    );

    // Add keys and signatures for block 1 but skip block 2.
    block_signatures[0].insert_signature(KEYS[0].clone(), Signature::System);
    block_signatures[0].insert_signature(KEYS[1].clone(), Signature::System);

    let env = fixture.env.clone();
    let block_header_db = block_header_database();
    block_header_db.create(env.clone()).unwrap();
    let block_metadata_db = block_metadata_database();
    block_metadata_db.create(env.clone()).unwrap();

    // Insert the blocks and signatures into the database.
    if let Ok(mut txn) = env.begin_rw_txn() {
        for block in blocks.iter() {
            let block_header = block.header().clone();
            // Store the block header.
            let key = *block.hash();
            block_header_db
                .put(&mut txn, key, BlockHeader::V2(block_header.clone()), true)
                .unwrap();
        }
        let signature = BlockSignatures::V2(block_signatures[0].clone());
        block_metadata_db
            .put(&mut txn, *blocks[0].hash(), signature, true)
            .unwrap();

        let switch_block_header = switch_block.header().clone();
        // Store the switch block header.
        let key = *switch_block.hash();
        block_header_db
            .put(
                &mut txn,
                key,
                BlockHeader::V2(switch_block_header.clone()),
                true,
            )
            .unwrap();
        txn.commit().unwrap();
    };

    let indices = initialize_indices(env.clone(), &BTreeSet::from([100, 200])).unwrap();

    // Purge signatures for blocks 1 and 2 to weak finality. The operation
    // should succeed even if the signatures for block 2 are missing.
    assert!(
        purge_signatures_for_blocks(env.clone(), &indices, BTreeSet::from([100, 200]), false)
            .is_ok()
    );
    if let Ok(txn) = env.begin_ro_txn() {
        let block_1_sigs = fetch_v2_signature(blocks[0].hash(), &block_metadata_db, &txn);
        // For block 1, any of the 2 signatures will be fine (500/1000), but
        // not both.
        let block_1_signers: Vec<PublicKey> = block_1_sigs.signers().cloned().collect();
        // Block 1 had both keys (400, 600), so it should have kept
        // the first one.
        assert!(block_1_signers.contains(&KEYS[0]));
        assert!(!block_1_signers.contains(&KEYS[1]));

        // We should have no record for the signatures of block 2.
        assert!(
            block_metadata_db
                .get(&txn, blocks[1].hash())
                .unwrap()
                .is_none()
        );
        txn.commit().unwrap();
    };

    // Purge signatures for blocks 1 and 2 to no finality. The operation
    // should succeed even if the signatures for block 2 are missing.
    assert!(
        purge_signatures_for_blocks(env.clone(), &indices, BTreeSet::from([100, 200]), true)
            .is_ok()
    );
    if let Ok(txn) = env.begin_ro_txn() {
        // We should have no record for the signatures of block 1.
        assert!(
            block_metadata_db
                .get(&txn, blocks[0].hash())
                .unwrap()
                .is_none()
        );

        // We should have no record for the signatures of block 2.
        assert!(
            block_metadata_db
                .get(&txn, blocks[1].hash())
                .unwrap()
                .is_none()
        );
        txn.commit().unwrap();
    };
}

fn fetch_v2_signature(
    block_hash: &BlockHash,
    block_metadata_db: &crate::common::db::VersionedDatabases<BlockHash, BlockSignatures>,
    txn: &lmdb::RoTransaction<'_>,
) -> BlockSignaturesV2 {
    match block_metadata_db.get(txn, block_hash).unwrap().unwrap() {
        BlockSignatures::V1(_) => unreachable!("Expected V2 signature!"),
        BlockSignatures::V2(block_signatures_v2) => block_signatures_v2,
    }
}
