pub mod archive;
pub mod check;
pub mod execution_results_summary;
pub mod extract_slice;
pub mod latest_block_summary;
pub mod purge_signatures;
pub mod remove_block;
pub mod trie_compact;
pub mod unsparse;

use thiserror::Error as ThisError;

use archive::{CreateError, UnpackError};
use check::Error as CheckError;
use execution_results_summary::Error as ExecutionResultsSummaryError;
use extract_slice::Error as ExtractSliceError;
use latest_block_summary::Error as LatestBlockSummaryError;
use purge_signatures::Error as PurgeSignaturesError;
use remove_block::Error as RemoveBlockError;
use trie_compact::Error as TrieCompactError;
use unsparse::Error as UnsparseError;

#[derive(ThisError, Debug)]
pub enum Error {
    #[error("Archive create failed: {0}")]
    ArchiveCreate(#[from] CreateError),
    #[error("Archive unpack failed: {0}")]
    ArchiveUnpack(#[from] UnpackError),
    #[error("Check command failed: {0}")]
    Check(#[from] CheckError),
    #[error("Execution results summary command failed: {0}")]
    ExecutionResultsSummary(#[from] ExecutionResultsSummaryError),
    #[error("Extract slice command failed: {0}")]
    ExtractSlice(#[from] ExtractSliceError),
    #[error("Latest block summary command failed: {0}")]
    LatestBlockSummary(#[from] LatestBlockSummaryError),
    #[error("Purge signatures failed: {0}")]
    PurgeSignatures(#[from] PurgeSignaturesError),
    #[error("Remove block failed: {0}")]
    RemoveBlock(#[from] RemoveBlockError),
    #[error("Trie compact failed: {0}")]
    TrieCompact(#[from] TrieCompactError),
    #[error("Unsparse failed: {0}")]
    Unsparse(#[from] UnsparseError),
}
