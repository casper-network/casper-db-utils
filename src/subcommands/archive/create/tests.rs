use std::{
    fs::{OpenOptions, self},
    io::Write, path::Path,
};

use once_cell::sync::Lazy;
use rand::{self, RngCore};
use tar::Archive;
use tempfile::{TempDir, NamedTempFile};
use zstd::Decoder;

use crate::subcommands::archive::create::pack;

const NUM_TEST_FILES: usize = 10usize;
const TEST_FILE_SIZE: usize = 10000usize;

static MOCK_DIR: Lazy<(TempDir, TestPayloads)> = Lazy::new(create_mock_src_dir);

struct TestPayloads {
    pub payloads: [[u8; TEST_FILE_SIZE]; NUM_TEST_FILES]
}

fn create_mock_src_dir() -> (TempDir, TestPayloads) {
    let src_dir = tempfile::tempdir().unwrap();
    
    let mut rng = rand::thread_rng();
    let mut payloads = [[0u8; TEST_FILE_SIZE]; NUM_TEST_FILES];
    for (idx, payload) in payloads.iter_mut().enumerate().take(NUM_TEST_FILES) {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(src_dir.path().join(&format!("file_{}", idx)))
            .unwrap();
        rng.fill_bytes(payload);
        file.write_all(payload).unwrap();
    }
    (src_dir, TestPayloads { payloads })
}

fn unpack_mock_archive<P1: AsRef<Path>, P2: AsRef<Path>>(archive_path: P1, dst_dir: P2) {
    let archive_file = OpenOptions::new().read(true).open(&archive_path).unwrap();
    let mut decoder = Decoder::new(archive_file).unwrap();
    decoder.window_log_max(31).unwrap();
    let mut unpacker = Archive::new(decoder);
    unpacker.unpack(&dst_dir).unwrap();
    fs::remove_file(&archive_path).unwrap();
}

#[test]
fn archive_create_roundtrip() {
    // Create the mock test directory with randomly-filled files.
    let src_dir = &(*MOCK_DIR).0;
    let test_payloads = &(*MOCK_DIR).1;
    let dst_dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let archive_path = dst_dir.path().join("test_archive.tar.zst");
    // Create the compressed archive.
    assert!(pack::create_archive(&src_dir, &archive_path, true).is_ok());
    // Unpack and then delete the archive.
    unpack_mock_archive(&archive_path, &out_dir);
    for idx in 0..NUM_TEST_FILES {
        let contents = fs::read(out_dir.path().join(&format!("file_{}", idx))).unwrap();
        if contents != test_payloads.payloads[idx] {
            panic!("Contents of file {} are different from the original", idx);
        }
    }
}

#[test]
fn archive_create_bad_input() {
    let src_dir = &(*MOCK_DIR).0;

    // Source doesn't exist.
    assert!(pack::create_archive("bogus_source", "bogus_dest", false).is_err());

    // Source is not a directory.
    let file = NamedTempFile::new().unwrap();
    assert!(pack::create_archive(file.path(), "bogus_dest", false).is_err());

    // Destination directory doesn't exist.
    let root_dst = tempfile::tempdir().unwrap();
    assert!(pack::create_archive(&src_dir, root_dst.path().join("bogus_dest/test_archive.tar.zst"), false).is_err());
}
