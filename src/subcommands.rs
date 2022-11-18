pub mod archive;
pub mod check;
pub mod execution_results_summary;
pub mod latest_block_summary;
pub mod trie_compact;
pub mod unsparse;

use thiserror::Error as ThisError;

use archive::{CreateError, UnpackError};
use check::Error as CheckError;
use execution_results_summary::Error as ExecutionResultsSummaryError;
use latest_block_summary::Error as LatestBlockSummaryError;
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
    #[error("Latest block summary command failed: {0}")]
    LatestBlockSummary(#[from] LatestBlockSummaryError),
    #[error("Trie compact failed: {0}")]
    TrieCompact(#[from] TrieCompactError),
    #[error("Unsparse failed: {0}")]
    Unsparse(#[from] UnsparseError),
}
