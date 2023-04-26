use std::collections::BTreeSet;

use casper_node::types::BlockHash;
use casper_types::{ProtocolVersion, Signature, U512};
use lmdb::{Error as LmdbError, Transaction, WriteFlags};

use crate::{
    subcommands::purge_signatures::{
        block_signatures::BlockSignatures,
        purge::{initialize_indices, purge_signatures_for_blocks, EraWeights},
        Error,
    },
    test_utils::{self, LmdbTestFixture, MockBlockHeader, MockSwitchBlockHeader, KEYS},
};

// Gets and deserializes a `BlockSignatures` structure from the block
// signatures database.
fn get_sigs_from_db<T: Transaction>(
    txn: &T,
    fixture: &LmdbTestFixture,
    block_hash: &BlockHash,
) -> BlockSignatures {
    let serialized_sigs = txn
        .get(*fixture.db(Some("block_metadata")).unwrap(), block_hash)
        .unwrap();
    let block_sigs: BlockSignatures = bincode::deserialize(serialized_sigs).unwrap();
    assert_eq!(block_sigs.block_hash, *block_hash);
    block_sigs
}

#[test]
fn indices_initialization() {
    const BLOCK_COUNT: usize = 4;
    const SWITCH_BLOCK_COUNT: usize = 2;

    let fixture = LmdbTestFixture::new(vec!["block_header"], None);

    // Create mock block headers.
    let mut block_headers: Vec<(BlockHash, MockBlockHeader)> = (0..BLOCK_COUNT as u8)
        .map(test_utils::mock_block_header)
        .collect();
    // Set an era and height for each one.
    block_headers[0].1.era_id = 10.into();
    block_headers[0].1.height = 100;
    block_headers[1].1.era_id = 10.into();
    block_headers[1].1.height = 200;
    block_headers[2].1.era_id = 20.into();
    block_headers[2].1.height = 300;
    block_headers[3].1.era_id = 20.into();
    block_headers[3].1.height = 400;
    // Create mock switch blocks for each era.
    let mut switch_block_headers: Vec<(BlockHash, MockSwitchBlockHeader)> = (0..BLOCK_COUNT as u8)
        .map(test_utils::mock_switch_block_header)
        .collect();
    // Set an appropriate era and height for each one.
    switch_block_headers[0].1.era_id = block_headers[0].1.era_id - 1;
    switch_block_headers[0].1.height = 80;
    switch_block_headers[1].1.era_id = block_headers[2].1.era_id - 1;
    switch_block_headers[1].1.height = 280;

    let env = &fixture.env;
    // Insert the blocks into the database.
    if let Ok(mut txn) = env.begin_rw_txn() {
        for (block_hash, block_header) in block_headers.iter().take(BLOCK_COUNT) {
            // Store the block header.
            txn.put(
                *fixture.db(Some("block_header")).unwrap(),
                block_hash,
                &bincode::serialize(&block_header).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        for (block_hash, block_header) in switch_block_headers.iter().take(SWITCH_BLOCK_COUNT) {
            // Store the switch block header.
            txn.put(
                *fixture.db(Some("block_header")).unwrap(),
                block_hash,
                &bincode::serialize(block_header).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    };

    let indices = initialize_indices(env, &BTreeSet::from([100, 200, 300])).unwrap();
    // Make sure we have the relevant blocks in the indices.
    assert_eq!(
        indices.heights.get(&block_headers[0].1.height).unwrap().0,
        block_headers[0].0
    );
    assert_eq!(
        indices.heights.get(&block_headers[1].1.height).unwrap().0,
        block_headers[1].0
    );
    assert_eq!(
        indices.heights.get(&block_headers[2].1.height).unwrap().0,
        block_headers[2].0
    );
    // And that the irrelevant ones are not included.
    assert!(!indices.heights.contains_key(&block_headers[3].1.height));
    // Make sure we got all the switch blocks.
    assert_eq!(
        *indices
            .switch_blocks
            .get(&block_headers[0].1.era_id)
            .unwrap(),
        switch_block_headers[0].0
    );
    assert_eq!(
        *indices
            .switch_blocks
            .get(&block_headers[2].1.era_id)
            .unwrap(),
        switch_block_headers[1].0
    );

    // Test for a header with a height which we already have in the db.
    let (duplicate_hash, mut duplicate_header) = test_utils::mock_block_header(4);
    duplicate_header.height = block_headers[0].1.height;
    if let Ok(mut txn) = env.begin_rw_txn() {
        // Store the header with duplicated height.
        txn.put(
            *fixture.db(Some("block_header")).unwrap(),
            &duplicate_hash,
            &bincode::serialize(&duplicate_header).unwrap(),
            WriteFlags::empty(),
        )
        .unwrap();
        txn.commit().unwrap();
    };

    match initialize_indices(env, &BTreeSet::from([100, 200, 300])) {
        Err(Error::DuplicateBlock(height)) => assert_eq!(height, block_headers[0].1.height),
        _ => panic!("Unexpected error"),
    }
}

#[test]
fn indices_initialization_with_upgrade() {
    const BLOCK_COUNT: usize = 4;
    const SWITCH_BLOCK_COUNT: usize = 4;

    let fixture = LmdbTestFixture::new(vec!["block_header"], None);
    // Create mock block headers.
    let mut block_headers: Vec<(BlockHash, MockBlockHeader)> = (0..BLOCK_COUNT as u8)
        .map(test_utils::mock_block_header)
        .collect();
    // Set an era and height for each one.
    block_headers[0].1.era_id = 10.into();
    block_headers[0].1.height = 80;

    block_headers[1].1.era_id = 11.into();
    block_headers[1].1.height = 200;
    block_headers[2].1.protocol_version = ProtocolVersion::from_parts(1, 1, 0);

    block_headers[2].1.era_id = 12.into();
    block_headers[2].1.height = 290;
    block_headers[2].1.protocol_version = ProtocolVersion::from_parts(2, 0, 0);

    block_headers[3].1.era_id = 13.into();
    block_headers[3].1.height = 350;
    block_headers[3].1.protocol_version = ProtocolVersion::from_parts(2, 0, 0);

    // Create mock switch blocks.
    let mut switch_block_headers: Vec<(BlockHash, MockSwitchBlockHeader)> = (0..SWITCH_BLOCK_COUNT
        as u8)
        .map(test_utils::mock_switch_block_header)
        .collect();
    // Set an appropriate era and height for each one.
    switch_block_headers[0].1.era_id = block_headers[0].1.era_id - 1;
    switch_block_headers[0].1.height = 60;

    switch_block_headers[1].1.era_id = block_headers[1].1.era_id - 1;
    switch_block_headers[1].1.height = 180;

    switch_block_headers[2].1.era_id = block_headers[2].1.era_id - 1;
    switch_block_headers[2].1.height = 250;
    switch_block_headers[2].1.protocol_version = ProtocolVersion::from_parts(1, 1, 0);

    switch_block_headers[3].1.height = 300;
    switch_block_headers[3].1.protocol_version = ProtocolVersion::from_parts(2, 0, 0);

    let env = &fixture.env;
    // Insert the blocks into the database.
    if let Ok(mut txn) = env.begin_rw_txn() {
        for (block_hash, block_header) in block_headers.iter().take(BLOCK_COUNT) {
            // Store the block header.
            txn.put(
                *fixture.db(Some("block_header")).unwrap(),
                block_hash,
                &bincode::serialize(&block_header).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        for (block_hash, block_header) in switch_block_headers.iter().take(SWITCH_BLOCK_COUNT) {
            // Store the switch block header.
            txn.put(
                *fixture.db(Some("block_header")).unwrap(),
                block_hash,
                &bincode::serialize(block_header).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    };

    let indices = initialize_indices(env, &BTreeSet::from([100, 200, 300])).unwrap();
    assert!(!indices
        .switch_blocks_before_upgrade
        .contains(&switch_block_headers[0].1.height));
    assert!(indices
        .switch_blocks_before_upgrade
        .contains(&switch_block_headers[1].1.height));
    assert!(indices
        .switch_blocks_before_upgrade
        .contains(&switch_block_headers[2].1.height));
    assert!(!indices
        .switch_blocks_before_upgrade
        .contains(&switch_block_headers[3].1.height));
}

#[test]
fn era_weights() {
    const SWITCH_BLOCK_COUNT: usize = 2;

    let fixture = LmdbTestFixture::new(vec!["block_header"], None);
    // Create mock switch block headers.
    let mut switch_block_headers: Vec<(BlockHash, MockSwitchBlockHeader)> = (0..SWITCH_BLOCK_COUNT
        as u8)
        .map(test_utils::mock_switch_block_header)
        .collect();
    // Set an era and height for each one.
    switch_block_headers[0].1.era_id = 10.into();
    switch_block_headers[0].1.height = 80;
    // Insert some weight for the next era weights.
    switch_block_headers[0]
        .1
        .era_end
        .as_mut()
        .unwrap()
        .next_era_validator_weights
        .insert(KEYS[0].clone(), 100.into());
    // Set an era and height for each one.
    switch_block_headers[1].1.era_id = 20.into();
    switch_block_headers[1].1.height = 280;
    // Insert some weight for the next era weights.
    switch_block_headers[1]
        .1
        .era_end
        .as_mut()
        .unwrap()
        .next_era_validator_weights
        .insert(KEYS[1].clone(), 100.into());

    let env = &fixture.env;
    // Insert the blocks into the database.
    if let Ok(mut txn) = env.begin_rw_txn() {
        for (block_hash, block_header) in switch_block_headers.iter().take(SWITCH_BLOCK_COUNT) {
            // Store the switch block header.
            txn.put(
                *fixture.db(Some("block_header")).unwrap(),
                block_hash,
                &bincode::serialize(block_header).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    };
    let indices = initialize_indices(env, &BTreeSet::from([80])).unwrap();
    let mut era_weights = EraWeights::default();
    if let Ok(txn) = env.begin_ro_txn() {
        let db = env.open_db(Some("block_header")).unwrap();
        // Try to update the weights for the first switch block.
        assert!(!era_weights
            .refresh_weights_for_era(
                &txn,
                db,
                &indices,
                switch_block_headers[0].1.era_id.successor()
            )
            .unwrap());
        assert_eq!(
            era_weights.era_id(),
            switch_block_headers[0].1.era_id.successor()
        );
        assert_eq!(
            *era_weights.weights_mut().get(&KEYS[0]).unwrap(),
            U512::from(100)
        );
        assert!(!era_weights.weights_mut().contains_key(&KEYS[1]));

        // Try to update the weights for the second switch block.
        assert!(!era_weights
            .refresh_weights_for_era(
                &txn,
                db,
                &indices,
                switch_block_headers[1].1.era_id.successor()
            )
            .unwrap());
        assert_eq!(
            era_weights.era_id(),
            switch_block_headers[1].1.era_id.successor()
        );
        assert_eq!(
            *era_weights.weights_mut().get(&KEYS[1]).unwrap(),
            U512::from(100)
        );
        assert!(!era_weights.weights_mut().contains_key(&KEYS[0]));

        // Try to update the weights for the second switch block again.
        assert!(!era_weights
            .refresh_weights_for_era(
                &txn,
                db,
                &indices,
                switch_block_headers[1].1.era_id.successor()
            )
            .unwrap());
        assert_eq!(
            era_weights.era_id(),
            switch_block_headers[1].1.era_id.successor()
        );
        assert_eq!(
            *era_weights.weights_mut().get(&KEYS[1]).unwrap(),
            U512::from(100)
        );
        assert!(!era_weights.weights_mut().contains_key(&KEYS[0]));

        // Try to update the weights for a nonexistent switch block.
        let expected_missing_era_id = switch_block_headers[1].1.era_id.successor().successor();
        match era_weights.refresh_weights_for_era(&txn, db, &indices, expected_missing_era_id) {
            Err(Error::MissingEraWeights(actual_missing_era_id)) => {
                assert_eq!(expected_missing_era_id, actual_missing_era_id)
            }
            _ => panic!("Unexpected failure"),
        }
        txn.commit().unwrap();
    };

    if let Ok(mut txn) = env.begin_rw_txn() {
        // Delete the weights for the first switch block in the db.
        switch_block_headers[0].1.era_end = None;
        txn.put(
            *fixture.db(Some("block_header")).unwrap(),
            &switch_block_headers[0].0,
            &bincode::serialize(&switch_block_headers[0].1).unwrap(),
            WriteFlags::empty(),
        )
        .unwrap();
        txn.commit().unwrap();
    };
    if let Ok(txn) = env.begin_ro_txn() {
        let db = env.open_db(Some("block_header")).unwrap();
        let expected_missing_era_id = switch_block_headers[0].1.era_id.successor();
        // Make sure we get an error when the block has no weights.
        match era_weights.refresh_weights_for_era(&txn, db, &indices, expected_missing_era_id) {
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
    const SWITCH_BLOCK_COUNT: usize = 2;

    let fixture = LmdbTestFixture::new(vec!["block_header"], None);
    // Create mock switch block headers.
    let mut switch_block_headers: Vec<(BlockHash, MockSwitchBlockHeader)> = (0..SWITCH_BLOCK_COUNT
        as u8)
        .map(test_utils::mock_switch_block_header)
        .collect();
    // Set an era and height for the first one.
    switch_block_headers[0].1.era_id = 10.into();
    switch_block_headers[0].1.height = 80;
    // Insert some weight for the next era weights.
    switch_block_headers[0]
        .1
        .era_end
        .as_mut()
        .unwrap()
        .next_era_validator_weights
        .insert(KEYS[0].clone(), 100.into());
    // Set an era and height for the second one.
    switch_block_headers[1].1.era_id = 11.into();
    switch_block_headers[1].1.height = 280;
    // Insert some weight for the next era weights.
    switch_block_headers[1]
        .1
        .era_end
        .as_mut()
        .unwrap()
        .next_era_validator_weights
        .insert(KEYS[1].clone(), 100.into());
    // Upgrade the version of the second and third switch blocks.
    switch_block_headers[1].1.protocol_version = ProtocolVersion::from_parts(1, 1, 0);

    let env = &fixture.env;
    // Insert the blocks into the database.
    if let Ok(mut txn) = env.begin_rw_txn() {
        for (block_hash, block_header) in switch_block_headers.iter().take(SWITCH_BLOCK_COUNT) {
            // Store the switch block header.
            txn.put(
                *fixture.db(Some("block_header")).unwrap(),
                block_hash,
                &bincode::serialize(block_header).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    };
    let indices = initialize_indices(env, &BTreeSet::from([80, 280])).unwrap();
    let mut era_weights = EraWeights::default();
    if let Ok(txn) = env.begin_ro_txn() {
        let db = env.open_db(Some("block_header")).unwrap();

        assert!(era_weights
            .refresh_weights_for_era(
                &txn,
                db,
                &indices,
                switch_block_headers[0].1.era_id.successor()
            )
            .unwrap());

        assert!(!era_weights
            .refresh_weights_for_era(
                &txn,
                db,
                &indices,
                switch_block_headers[1].1.era_id.successor()
            )
            .unwrap());

        assert!(era_weights
            .refresh_weights_for_era(
                &txn,
                db,
                &indices,
                switch_block_headers[0].1.era_id.successor()
            )
            .unwrap());

        assert!(!era_weights
            .refresh_weights_for_era(
                &txn,
                db,
                &indices,
                switch_block_headers[1].1.era_id.successor()
            )
            .unwrap());

        txn.commit().unwrap();
    };
}

#[test]
fn purge_signatures_should_work() {
    const BLOCK_COUNT: usize = 4;
    const SWITCH_BLOCK_COUNT: usize = 2;

    let fixture = LmdbTestFixture::new(vec!["block_header", "block_metadata"], None);
    // Create mock block headers.
    let mut block_headers: Vec<(BlockHash, MockBlockHeader)> = (0..BLOCK_COUNT as u8)
        .map(test_utils::mock_block_header)
        .collect();
    // Set an era and height for each one.
    block_headers[0].1.era_id = 10.into();
    block_headers[0].1.height = 100;
    block_headers[1].1.era_id = 10.into();
    block_headers[1].1.height = 200;
    block_headers[2].1.era_id = 20.into();
    block_headers[2].1.height = 300;
    block_headers[3].1.era_id = 20.into();
    block_headers[3].1.height = 400;
    // Create mock block signatures.
    let mut block_signatures: Vec<BlockSignatures> = block_headers
        .iter()
        .map(|(block_hash, header)| BlockSignatures::new(*block_hash, header.era_id))
        .collect();
    // Create mock switch block headers.
    let mut switch_block_headers: Vec<(BlockHash, MockSwitchBlockHeader)> = (0..SWITCH_BLOCK_COUNT
        as u8)
        .map(test_utils::mock_switch_block_header)
        .collect();
    // Set an appropriate era and height for switch block 1.
    switch_block_headers[0].1.era_id = block_headers[0].1.era_id - 1;
    switch_block_headers[0].1.height = 80;
    // Add weights for this switch block (500, 500).
    switch_block_headers[0]
        .1
        .insert_key_weight(KEYS[0].clone(), 500.into());
    switch_block_headers[0]
        .1
        .insert_key_weight(KEYS[1].clone(), 500.into());

    // Add keys and signatures for block 1.
    block_signatures[0]
        .proofs
        .insert(KEYS[0].clone(), Signature::System);
    block_signatures[0]
        .proofs
        .insert(KEYS[1].clone(), Signature::System);
    // Add keys and signatures for block 2.
    block_signatures[1]
        .proofs
        .insert(KEYS[0].clone(), Signature::System);

    // Set an appropriate era and height for switch block 2.
    switch_block_headers[1].1.era_id = block_headers[2].1.era_id - 1;
    switch_block_headers[1].1.height = 280;
    // Add weights for this switch block (300, 300, 400).
    switch_block_headers[1]
        .1
        .insert_key_weight(KEYS[0].clone(), 300.into());
    switch_block_headers[1]
        .1
        .insert_key_weight(KEYS[1].clone(), 300.into());
    switch_block_headers[1]
        .1
        .insert_key_weight(KEYS[2].clone(), 400.into());

    // Add keys and signatures for block 3.
    block_signatures[2]
        .proofs
        .insert(KEYS[0].clone(), Signature::System);
    block_signatures[2]
        .proofs
        .insert(KEYS[1].clone(), Signature::System);
    block_signatures[2]
        .proofs
        .insert(KEYS[2].clone(), Signature::System);
    // Add keys and signatures for block 4.
    block_signatures[3]
        .proofs
        .insert(KEYS[0].clone(), Signature::System);
    block_signatures[3]
        .proofs
        .insert(KEYS[2].clone(), Signature::System);

    let env = &fixture.env;
    // Insert the blocks and signatures into the database.
    if let Ok(mut txn) = env.begin_rw_txn() {
        for i in 0..BLOCK_COUNT {
            // Store the block header.
            txn.put(
                *fixture.db(Some("block_header")).unwrap(),
                &block_headers[i].0,
                &bincode::serialize(&block_headers[i].1).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
            // Store the signatures.
            txn.put(
                *fixture.db(Some("block_metadata")).unwrap(),
                &block_headers[i].0,
                &bincode::serialize(&block_signatures[i]).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        for (block_hash, block_header) in switch_block_headers.iter().take(SWITCH_BLOCK_COUNT) {
            // Store the switch block header.
            txn.put(
                *fixture.db(Some("block_header")).unwrap(),
                block_hash,
                &bincode::serialize(block_header).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    };

    let indices = initialize_indices(env, &BTreeSet::from([100, 200, 300, 400])).unwrap();

    // Purge signatures for blocks 1, 2 and 3 to weak finality.
    assert!(
        purge_signatures_for_blocks(env, &indices, BTreeSet::from([100, 200, 300]), false).is_ok()
    );
    if let Ok(txn) = env.begin_ro_txn() {
        let block_1_sigs = get_sigs_from_db(&txn, &fixture, &block_headers[0].0);
        // For block 1, any of the 2 signatures will be fine (500/1000), but
        // not both.
        assert!(
            (block_1_sigs.proofs.contains_key(&KEYS[0])
                && !block_1_sigs.proofs.contains_key(&KEYS[1]))
                || (!block_1_sigs.proofs.contains_key(&KEYS[0])
                    && block_1_sigs.proofs.contains_key(&KEYS[1]))
        );

        // Block 2 only had the first signature, which already meets the
        // requirements (500/1000).
        let block_2_sigs = get_sigs_from_db(&txn, &fixture, &block_headers[1].0);
        assert!(block_2_sigs.proofs.contains_key(&KEYS[0]));
        assert!(!block_2_sigs.proofs.contains_key(&KEYS[1]));

        // Block 3 had all the keys (300, 300, 400), so it should have kept
        // the first 2.
        let block_3_sigs = get_sigs_from_db(&txn, &fixture, &block_headers[2].0);
        assert!(block_3_sigs.proofs.contains_key(&KEYS[0]));
        assert!(block_3_sigs.proofs.contains_key(&KEYS[1]));
        assert!(!block_3_sigs.proofs.contains_key(&KEYS[2]));

        // Block 4 had signatures for keys 1 (300) and 3 (400), but it was not
        // included in the purge list, so it should have kept both.
        let block_4_sigs = get_sigs_from_db(&txn, &fixture, &block_headers[3].0);
        assert!(block_4_sigs.proofs.contains_key(&KEYS[0]));
        assert!(!block_4_sigs.proofs.contains_key(&KEYS[1]));
        assert!(block_4_sigs.proofs.contains_key(&KEYS[2]));
        txn.commit().unwrap();
    };

    // Purge signatures for blocks 1 and 4 to no finality.
    assert!(purge_signatures_for_blocks(env, &indices, BTreeSet::from([100, 400]), true).is_ok());
    if let Ok(txn) = env.begin_ro_txn() {
        // We should have no record for the signatures of block 1.
        match txn.get(
            *fixture.db(Some("block_metadata")).unwrap(),
            &block_headers[0].0,
        ) {
            Err(LmdbError::NotFound) => {}
            other => panic!("Unexpected search result: {other:?}"),
        }

        // Block 2 should be the same as before.
        let block_2_sigs = get_sigs_from_db(&txn, &fixture, &block_headers[1].0);
        assert!(block_2_sigs.proofs.contains_key(&KEYS[0]));
        assert!(!block_2_sigs.proofs.contains_key(&KEYS[1]));

        // Block 3 should be the same as before.
        let block_3_sigs = get_sigs_from_db(&txn, &fixture, &block_headers[2].0);
        assert!(block_3_sigs.proofs.contains_key(&KEYS[0]));
        assert!(block_3_sigs.proofs.contains_key(&KEYS[1]));
        assert!(!block_3_sigs.proofs.contains_key(&KEYS[2]));

        // We should have no record for the signatures of block 4.
        match txn.get(
            *fixture.db(Some("block_metadata")).unwrap(),
            &block_headers[3].0,
        ) {
            Err(LmdbError::NotFound) => {}
            other => panic!("Unexpected search result: {other:?}"),
        }
        txn.commit().unwrap();
    };
}

#[test]
fn purge_signatures_bad_input() {
    const BLOCK_COUNT: usize = 2;
    const SWITCH_BLOCK_COUNT: usize = 2;

    let fixture = LmdbTestFixture::new(vec!["block_header", "block_metadata"], None);
    // Create mock block headers.
    let mut block_headers: Vec<(BlockHash, MockBlockHeader)> = (0..BLOCK_COUNT as u8)
        .map(test_utils::mock_block_header)
        .collect();
    // Set an era and height for block 1.
    block_headers[0].1.era_id = 10.into();
    block_headers[0].1.height = 100;
    // Set an era and height for block 2.
    block_headers[1].1.era_id = 20.into();
    block_headers[1].1.height = 200;
    // Create mock block signatures.
    let mut block_signatures: Vec<BlockSignatures> = block_headers
        .iter()
        .map(|(block_hash, header)| BlockSignatures::new(*block_hash, header.era_id))
        .collect();
    // Create mock switch block headers.
    let mut switch_block_headers: Vec<(BlockHash, MockSwitchBlockHeader)> = (0..SWITCH_BLOCK_COUNT
        as u8)
        .map(test_utils::mock_switch_block_header)
        .collect();
    // Set an appropriate era and height for switch block 1.
    switch_block_headers[0].1.era_id = block_headers[0].1.era_id - 1;
    switch_block_headers[0].1.height = 80;
    // Add weights for this switch block (700, 300).
    switch_block_headers[0]
        .1
        .insert_key_weight(KEYS[0].clone(), 700.into());
    switch_block_headers[0]
        .1
        .insert_key_weight(KEYS[1].clone(), 300.into());

    // Add keys and signatures for block 1.
    block_signatures[0]
        .proofs
        .insert(KEYS[0].clone(), Signature::System);
    block_signatures[0]
        .proofs
        .insert(KEYS[1].clone(), Signature::System);

    // Set an appropriate era and height for switch block 2.
    switch_block_headers[1].1.era_id = block_headers[1].1.era_id - 1;
    switch_block_headers[1].1.height = 180;
    // Add weights for this switch block (400, 600).
    switch_block_headers[1]
        .1
        .insert_key_weight(KEYS[0].clone(), 400.into());
    switch_block_headers[1]
        .1
        .insert_key_weight(KEYS[1].clone(), 600.into());

    // Add keys and signatures for block 2.
    block_signatures[1]
        .proofs
        .insert(KEYS[0].clone(), Signature::System);
    block_signatures[1]
        .proofs
        .insert(KEYS[1].clone(), Signature::System);

    let env = &fixture.env;
    // Insert the blocks and signatures into the database.
    if let Ok(mut txn) = env.begin_rw_txn() {
        for i in 0..BLOCK_COUNT {
            // Store the block header.
            txn.put(
                *fixture.db(Some("block_header")).unwrap(),
                &block_headers[i].0,
                &bincode::serialize(&block_headers[i].1).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
            // Store the signatures.
            txn.put(
                *fixture.db(Some("block_metadata")).unwrap(),
                &block_headers[i].0,
                &bincode::serialize(&block_signatures[i]).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        for (block_hash, block_header) in switch_block_headers.iter().take(SWITCH_BLOCK_COUNT) {
            // Store the switch block header.
            txn.put(
                *fixture.db(Some("block_header")).unwrap(),
                block_hash,
                &bincode::serialize(block_header).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        txn.commit().unwrap();
    };

    let indices = initialize_indices(env, &BTreeSet::from([100])).unwrap();
    // Purge signatures for blocks 1 and 2 to weak finality.
    assert!(purge_signatures_for_blocks(env, &indices, BTreeSet::from([100, 200]), false).is_ok());
    if let Ok(txn) = env.begin_ro_txn() {
        let block_1_sigs = get_sigs_from_db(&txn, &fixture, &block_headers[0].0);
        // Block 1 has a super-majority signature (700), so the purge would
        // have failed and the signatures are untouched.
        assert!(block_1_sigs.proofs.contains_key(&KEYS[0]));
        assert!(block_1_sigs.proofs.contains_key(&KEYS[1]));

        let block_2_sigs = get_sigs_from_db(&txn, &fixture, &block_headers[1].0);
        // Block 2 wasn't in the purge list, so it should be untouched.
        assert!(block_2_sigs.proofs.contains_key(&KEYS[0]));
        assert!(block_2_sigs.proofs.contains_key(&KEYS[1]));
        txn.commit().unwrap();
    };

    // Overwrite the signatures for block 2 with bogus data.
    if let Ok(mut txn) = env.begin_rw_txn() {
        // Store the signatures.
        txn.put(
            *fixture.db(Some("block_metadata")).unwrap(),
            &block_headers[1].0,
            &bincode::serialize(&[0u8, 1u8, 2u8]).unwrap(),
            WriteFlags::empty(),
        )
        .unwrap();
        txn.commit().unwrap();
    };

    let indices = initialize_indices(env, &BTreeSet::from([100, 200])).unwrap();
    // Purge should fail with a deserialization error.
    match purge_signatures_for_blocks(env, &indices, BTreeSet::from([100, 200]), false) {
        Err(Error::SignaturesParsing(block_hash, _)) if block_hash == block_headers[1].0 => {}
        other => panic!("Unexpected result: {other:?}"),
    };
}

#[test]
fn purge_signatures_missing_from_db() {
    const BLOCK_COUNT: usize = 2;

    let fixture = LmdbTestFixture::new(vec!["block_header", "block_metadata"], None);
    // Create mock block headers.
    let mut block_headers: Vec<(BlockHash, MockBlockHeader)> = (0..BLOCK_COUNT as u8)
        .map(test_utils::mock_block_header)
        .collect();
    // Set an era and height for each one.
    block_headers[0].1.era_id = 10.into();
    block_headers[0].1.height = 100;
    block_headers[1].1.era_id = 10.into();
    block_headers[1].1.height = 200;
    // Create mock block signatures.
    let mut block_signatures: Vec<BlockSignatures> = block_headers
        .iter()
        .map(|(block_hash, header)| BlockSignatures::new(*block_hash, header.era_id))
        .collect();
    // Create mock switch block headers.
    let (switch_block_hash, mut switch_block_header) = test_utils::mock_switch_block_header(0);
    // Set an appropriate era and height for switch block 1.
    switch_block_header.era_id = block_headers[0].1.era_id - 1;
    switch_block_header.height = 80;
    // Add weights for this switch block (400, 600).
    switch_block_header.insert_key_weight(KEYS[0].clone(), 400.into());
    switch_block_header.insert_key_weight(KEYS[1].clone(), 600.into());

    // Add keys and signatures for block 1 but skip block 2.
    block_signatures[0]
        .proofs
        .insert(KEYS[0].clone(), Signature::System);
    block_signatures[0]
        .proofs
        .insert(KEYS[1].clone(), Signature::System);

    let env = &fixture.env;
    // Insert the blocks and signatures into the database.
    if let Ok(mut txn) = env.begin_rw_txn() {
        for (block_hash, block_header) in block_headers.iter().take(BLOCK_COUNT) {
            // Store the block header.
            txn.put(
                *fixture.db(Some("block_header")).unwrap(),
                block_hash,
                &bincode::serialize(block_header).unwrap(),
                WriteFlags::empty(),
            )
            .unwrap();
        }
        // Store the signatures for block 1.
        txn.put(
            *fixture.db(Some("block_metadata")).unwrap(),
            &block_headers[0].0,
            &bincode::serialize(&block_signatures[0]).unwrap(),
            WriteFlags::empty(),
        )
        .unwrap();
        // Store the switch block header.
        txn.put(
            *fixture.db(Some("block_header")).unwrap(),
            &switch_block_hash,
            &bincode::serialize(&switch_block_header).unwrap(),
            WriteFlags::empty(),
        )
        .unwrap();
        txn.commit().unwrap();
    };

    let indices = initialize_indices(env, &BTreeSet::from([100, 200])).unwrap();

    // Purge signatures for blocks 1 and 2 to weak finality. The operation
    // should succeed even if the signatures for block 2 are missing.
    assert!(purge_signatures_for_blocks(env, &indices, BTreeSet::from([100, 200]), false).is_ok());
    if let Ok(txn) = env.begin_ro_txn() {
        let block_1_sigs = get_sigs_from_db(&txn, &fixture, &block_headers[0].0);
        // Block 1 had both keys (400, 600), so it should have kept
        // the first one.
        assert!(block_1_sigs.proofs.contains_key(&KEYS[0]));
        assert!(!block_1_sigs.proofs.contains_key(&KEYS[1]));

        // We should have no record for the signatures of block 2.
        match txn.get(
            *fixture.db(Some("block_metadata")).unwrap(),
            &block_headers[1].0,
        ) {
            Err(LmdbError::NotFound) => {}
            other => panic!("Unexpected search result: {other:?}"),
        }
        txn.commit().unwrap();
    };

    // Purge signatures for blocks 1 and 2 to no finality. The operation
    // should succeed even if the signatures for block 2 are missing.
    assert!(purge_signatures_for_blocks(env, &indices, BTreeSet::from([100, 200]), true).is_ok());
    if let Ok(txn) = env.begin_ro_txn() {
        // We should have no record for the signatures of block 1.
        match txn.get(
            *fixture.db(Some("block_metadata")).unwrap(),
            &block_headers[0].0,
        ) {
            Err(LmdbError::NotFound) => {}
            other => panic!("Unexpected search result: {other:?}"),
        }

        // We should have no record for the signatures of block 2.
        match txn.get(
            *fixture.db(Some("block_metadata")).unwrap(),
            &block_headers[1].0,
        ) {
            Err(LmdbError::NotFound) => {}
            other => panic!("Unexpected search result: {other:?}"),
        }
        txn.commit().unwrap();
    };
}
