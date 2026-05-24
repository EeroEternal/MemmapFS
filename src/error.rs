use thiserror::Error;

/// The unified error type for all MemMapFS operations.
#[derive(Debug, Error)]
pub enum MemMapError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Bincode(#[from] bincode::Error),

    #[error("Search index error: {0}")]
    Tantivy(#[from] tantivy::TantivyError),

    #[error("Search directory error: {0}")]
    TantivyDir(#[from] tantivy::directory::error::OpenDirectoryError),

    #[error("Search query parse error: {0}")]
    QueryParse(#[from] tantivy::query::QueryParserError),

    #[error("Block not found: chunk {chunk_id}, offset {offset}")]
    BlockNotFound { chunk_id: u32, offset: u64 },

    #[error("Corrupt WAL entry at file offset {offset}: {reason}")]
    CorruptWal { offset: u64, reason: String },
}
