use std::{
    fs::{self, File},
    io::{Read, Write},
    net::TcpListener,
    sync::{Arc, Barrier},
    thread,
};

use rand::{self, RngCore};
use tar::Builder;
use zstd::Encoder;

use crate::subcommands::archive::{
    unpack::{download_stream, file_stream},
    zstd_utils,
};

const TEST_ADDR: &str = "127.0.0.1:9876";
const TEST_FILE: &str = "file.bin";
const TEST_ARCHIVE: &str = "archive.tar";
const TEST_COMPRESSED_ARCHIVE: &str = "archive.tar.zst";

const HTTP_HEADER_END_SEQUENCE: [u8; 4] = [b'\r', b'\n', b'\r', b'\n'];

fn serve_request(payload: Vec<u8>, barrier: Arc<Barrier>, addr: &str) {
    let listener = TcpListener::bind(addr).unwrap();
    {
        // Wait on the barrier to signal to the main thread that we
        // set up the server and accept requests.
        let _ = barrier.wait();
        // Accept the connection we're making.
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = [0u8; 100].to_vec();
        // Read all the bytes of the request.
        loop {
            // Don't care about the request contents.
            let _ = stream.read(&mut buf).unwrap();
            // Since this is a GET request, it will end with a sequence of
            // [CR, LF, CR, LF], which marks the end of the header section.
            if buf
                .windows(HTTP_HEADER_END_SEQUENCE.len())
                .any(|slice| *slice == HTTP_HEADER_END_SEQUENCE)
            {
                break;
            }
        }

        // Write the mock file contents back with a simple HTTP response.
        stream
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
                    payload.len()
                )
                .as_bytes(),
            )
            .unwrap();
        stream.write_all(&payload).unwrap();
        // Wait on the barrier here so we don't drop the stream until we finish
        // reading on the other end.
        let _ = barrier.wait();
    }
}

#[test]
fn zstd_decode_roundtrip() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut rng = rand::thread_rng();
    // Generate a random payload.
    let mut payload = [0u8; 100];
    rng.fill_bytes(&mut payload);

    // Encode the payload with zstd.
    let mut encoder = Encoder::new(vec![], 0).unwrap();
    encoder.write_all(&payload).unwrap();
    let encoded = encoder.finish().unwrap();

    // Write the encoded contents to a file as well.
    let encoded_path = tmp_dir.path().join("encoded");
    fs::write(encoded_path, &encoded).unwrap();

    // Decode the response with the zstd streaming function.
    let mut decoder = zstd_utils::zstd_decode_stream(encoded.as_slice()).unwrap();
    let mut decoded = vec![];
    decoder.read_to_end(&mut decoded).unwrap();

    // Check that the output is the same as the payload.
    assert_eq!(payload.to_vec(), decoded);
}

#[test]
fn archive_unpack_decode_network() {
    let mut rng = rand::thread_rng();
    // Generate a random payload.
    let mut payload = [0u8; 100];
    rng.fill_bytes(&mut payload);

    let src_dir = tempfile::tempdir().unwrap();
    let file_payload_path = src_dir.path().join(TEST_FILE);
    fs::write(&file_payload_path, payload).unwrap();
    let archive_path = src_dir.path().join(TEST_ARCHIVE);
    {
        let archive_file = File::create(&archive_path).unwrap();
        let mut payload_file = File::open(&file_payload_path).unwrap();
        let mut archive = Builder::new(archive_file);
        archive.append_file(TEST_FILE, &mut payload_file).unwrap();
        archive.finish().unwrap();
    }

    let archive_payload = fs::read(&archive_path).unwrap();
    // Encode the payload with zstd.
    let mut encoder = Encoder::new(vec![], 0).unwrap();
    encoder.write_all(&archive_payload).unwrap();
    let encoded = encoder.finish().unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let server_barrier = barrier.clone();

    // Set up a server on another thread which will be our
    // `get` target.
    let join_handle = thread::spawn(move || {
        serve_request(encoded, server_barrier, TEST_ADDR);
    });

    // Wait for the server thread to do its setup and bind to the port.
    let _ = barrier.wait();

    // Create the directory where we save the extracted file.
    let temp_dir = tempfile::tempdir().unwrap();

    // Reqwest needs the schema to be present in the URL.
    let mut http_addr = "http://".to_string();
    http_addr.push_str(TEST_ADDR);

    // Download the file with zstd encoding.
    download_stream::download_and_unpack_archive(&http_addr, &temp_dir)
        .expect("Error downloading and decoding payload");

    // Check that the downloaded contents are the same as our payload.
    let output_bytes = fs::read(temp_dir.path().join(TEST_FILE))
        .expect("Couldn't read output from destination file");
    assert_eq!(payload.to_vec(), output_bytes);

    // Let the server thread finish execution.
    let _ = barrier.wait();
    join_handle.join().unwrap();
}

#[test]
fn archive_unpack_decode_file() {
    let mut rng = rand::thread_rng();
    // Generate a random payload.
    let mut payload = [0u8; 100];
    rng.fill_bytes(&mut payload);

    let src_dir = tempfile::tempdir().unwrap();
    let file_payload_path = src_dir.path().join(TEST_FILE);
    fs::write(&file_payload_path, payload).unwrap();
    let archive_path = src_dir.path().join(TEST_ARCHIVE);
    {
        let archive_file = File::create(&archive_path).unwrap();
        let mut payload_file = File::open(&file_payload_path).unwrap();
        let mut archive = Builder::new(archive_file);
        archive.append_file(TEST_FILE, &mut payload_file).unwrap();
        archive.finish().unwrap();
    }

    let archive_payload = fs::read(&archive_path).unwrap();
    // Encode the payload with zstd.
    let compressed_archive_path = src_dir.path().join(TEST_COMPRESSED_ARCHIVE);
    let compressed_archive = File::create(&compressed_archive_path).unwrap();
    let mut encoder = Encoder::new(compressed_archive, 0).unwrap();
    encoder.write_all(&archive_payload).unwrap();
    let _ = encoder.finish().unwrap();

    // Create the directory where we save the extracted file.
    let temp_dir = tempfile::tempdir().unwrap();

    // Stream the file with zstd encoding.
    file_stream::file_stream_and_unpack_archive(&compressed_archive_path, &temp_dir)
        .expect("Error downloading and decoding payload");

    // Check that the streamed contents are the same as our payload.
    let output_bytes = fs::read(temp_dir.path().join(TEST_FILE))
        .expect("Couldn't read output from destination file");
    assert_eq!(payload.to_vec(), output_bytes);
}

#[test]
fn archive_unpack_invalid_url() {
    let temp_dir = tempfile::tempdir().unwrap();
    let dest_path = temp_dir.path().join(TEST_FILE);

    // No HTTP schema.
    assert!(download_stream::download_and_unpack_archive("localhost:10000", &dest_path).is_err());
    // No server running at `localhost:10000`.
    assert!(
        download_stream::download_and_unpack_archive("http://localhost:10000", dest_path).is_err()
    );
}

#[test]
fn archive_unpack_existing_destination() {
    // Create the directory where we save the downloaded file.
    let temp_dir = tempfile::tempdir().unwrap();
    let dest_path = temp_dir.path().join(TEST_FILE);

    // Create the destination file before downloading.
    let _ = File::create(&dest_path).unwrap();
    // Download should fail because a file is already present at the destination
    // directory. Address doesn't matter because the file check is performed first.
    assert!(download_stream::download_and_unpack_archive("bogus_address", dest_path).is_err());
}

#[test]
fn archive_unpack_missing_file() {
    // Create the directory where we save the downloaded file.
    let temp_dir = tempfile::tempdir().unwrap();
    let missing_src_path = temp_dir.path().join(TEST_FILE);

    // Streaming from file should fail because the source is missing. Destination
    // doesn't matter because the source check is performed first.
    assert!(file_stream::file_stream_and_unpack_archive(missing_src_path, "bogus_path").is_err());
}

#[test]
fn archive_unpack_file_existing_destination() {
    // Create the directory where we save the downloaded file.
    let temp_dir = tempfile::tempdir().unwrap();
    let src_path = temp_dir.path().join("src_file");
    let dest_path = temp_dir.path().join("dst_file");

    // Create the source file before streaming.
    let _ = File::create(&src_path).unwrap();

    // Create the destination file before streaming.
    let _ = File::create(&dest_path).unwrap();

    // File streaming should fail because the destination file is already present.
    // The source doesn't matter because the existing destination check is
    // performed first.
    assert!(file_stream::file_stream_and_unpack_archive(src_path, dest_path).is_err());
}
