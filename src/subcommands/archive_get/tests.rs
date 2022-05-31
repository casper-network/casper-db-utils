use std::{
    fs::OpenOptions,
    io::{Read, Write},
    net::TcpListener,
    sync::{Arc, Barrier},
    thread,
};

use rand::{self, RngCore};
use zstd::Encoder;

use super::{download_stream::download_archive, zstd_decode::zstd_decode_stream};

const TEST_ADDR_DECODE: &str = "127.0.0.1:9876";
const TEST_ADDR_NO_DECODE: &str = "127.0.0.1:9875";

fn serve_request(payload: Vec<u8>, barrier: Arc<Barrier>, addr: &str) {
    let listener = TcpListener::bind(addr).unwrap();
    {
        // Accept the connection we're making.
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = vec![];
        // Don't care about the request contents.
        stream.read(&mut buf).unwrap();
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
        // Have a barrier here so we don't drop the stream until we finish
        // reading on the other end.
        let _ = barrier.wait();
    }
}

#[test]
fn zstd_decode_roundtrip() {
    let mut rng = rand::thread_rng();
    // Generate a random payload.
    let mut payload = [0u8; 100];
    rng.fill_bytes(&mut payload);

    // Encode the payload with zstd.
    let mut encoder = Encoder::new(vec![], 0).unwrap();
    encoder.write_all(&mut payload).unwrap();
    let encoded = encoder.finish().unwrap();

    // Decode the response with our function.
    let mut decoder = zstd_decode_stream(encoded.as_slice(), None).unwrap();
    let mut decoded = vec![];
    decoder.read_to_end(&mut decoded).unwrap();

    // Check that the output is the same as the payload.
    assert_eq!(payload.to_vec(), decoded);
}

#[test]
fn archive_get_no_decode() {
    let mut rng = rand::thread_rng();
    // Generate a random payload.
    let mut payload = [0u8; 100];
    rng.fill_bytes(&mut payload);
    let payload_copy = payload.to_vec();

    let barrier = Arc::new(Barrier::new(2));
    let server_barrier = barrier.clone();

    // Set up a server on another thread which will be our
    // `get` target.
    let join_handle = thread::spawn(move || {
        serve_request(payload_copy, server_barrier, TEST_ADDR_NO_DECODE);
    });

    // Create the directory where we save the downloaded file.
    let temp_dir = tempfile::tempdir().unwrap();
    let dest_path = temp_dir.path().join("file.bin");

    // Reqwest needs the schema to be present in the URL.
    let mut http_addr = "http://".to_string();
    http_addr.push_str(TEST_ADDR_NO_DECODE);

    // Download the file without zstd encoding.
    download_archive(&http_addr, dest_path.clone(), false, None)
        .expect("Error downloading payload");

    // Check that the downloaded contents are the same as our payload.
    let mut dest_file = OpenOptions::new()
        .read(true)
        .open(dest_path.as_path())
        .expect("Couldn't open destination file");
    let mut output_bytes = vec![];
    dest_file
        .read_to_end(&mut output_bytes)
        .expect("Couldn't read from destination file");
    assert_eq!(payload.to_vec(), output_bytes);

    // Let the server thread finish execution.
    let _ = barrier.wait();
    join_handle.join().unwrap();
}

#[test]
fn archive_get_with_decode() {
    let mut rng = rand::thread_rng();
    // Generate a random payload.
    let mut payload = [0u8; 100];
    rng.fill_bytes(&mut payload);

    // Encode the payload with zstd.
    let mut encoder = Encoder::new(vec![], 0).unwrap();
    encoder.write_all(&mut payload).unwrap();
    let encoded = encoder.finish().unwrap();
    let encoded_copy = encoded.clone();

    let barrier = Arc::new(Barrier::new(2));
    let server_barrier = barrier.clone();

    // Set up a server on another thread which will be our
    // `get` target.
    let join_handle = thread::spawn(move || {
        serve_request(encoded_copy, server_barrier, TEST_ADDR_DECODE);
    });

    // Create the directory where we save the downloaded file.
    let temp_dir = tempfile::tempdir().unwrap();
    let dest_path = temp_dir.path().join("file.bin");

    // Reqwest needs the schema to be present in the URL.
    let mut http_addr = "http://".to_string();
    http_addr.push_str(TEST_ADDR_DECODE);

    // Download the file without zstd encoding.
    download_archive(&http_addr, dest_path.clone(), true, None)
        .expect("Error downloading and decoding payload");

    // Check that the downloaded contents are the same as our payload.
    let mut dest_file = OpenOptions::new()
        .read(true)
        .open(dest_path.as_path())
        .expect("Couldn't open destination file");
    let mut output_bytes = vec![];
    dest_file
        .read_to_end(&mut output_bytes)
        .expect("Couldn't read from destination file");
    assert_eq!(payload.to_vec(), output_bytes);

    // Let the server thread finish execution.
    let _ = barrier.wait();
    join_handle.join().unwrap();
}

#[test]
fn archive_get_invalid_url() {
    let temp_dir = tempfile::tempdir().unwrap();
    let dest_path = temp_dir.path().join("file.bin");

    // No HTTP schema.
    assert!(download_archive("localhost:10000", dest_path.clone(), false, None).is_err());
    // No server running at `localhost:10000`.
    assert!(download_archive("http://localhost:10000", dest_path, false, None).is_err());
}

#[test]
fn archive_get_existing_destination() {
    // Create the directory where we save the downloaded file.
    let temp_dir = tempfile::tempdir().unwrap();
    let dest_path = temp_dir.path().join("file.bin");

    // Create the destination file before downloading.
    let _ = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&dest_path)
        .unwrap();
    // Download should fail because the file is already present. Address doesn't
    // matter because the file check is performed first.
    assert!(download_archive("bogus_address", dest_path, false, None).is_err());
}
