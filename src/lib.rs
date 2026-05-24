pub mod error;
pub mod storage;
pub mod engine;
pub mod search;

use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::engine::{AgentState, MemMapEngine};
use crate::error::MemMapError;
use crate::search::SearchProvider;
use crate::storage::{BlockStorage, LogManager, WalCommand};

/// The primary handle to a MemMapFS instance.
///
/// `MemMapFS` is cheaply `Clone`able because all mutable state is held behind
/// `Arc<RwLock<…>>` or `Arc<…>` smart pointers.  The writer components
/// (`LogManager`, `BlockStorage`) use exclusive write locks so that concurrent
/// callers serialize naturally without external coordination.
#[derive(Clone)]
pub struct MemMapFS {
    root_dir: PathBuf,
    engine: Arc<RwLock<MemMapEngine>>,
    log_manager: Arc<RwLock<LogManager>>,
    block_storage: Arc<RwLock<BlockStorage>>,
    search_provider: Arc<SearchProvider>,
}

impl MemMapFS {
    /// Initializes or recovers a `MemMapFS` instance from `root`.
    ///
    /// 1. Creates the directory layout if it does not exist.
    /// 2. Opens `LogManager`, `BlockStorage`, and `SearchProvider`.
    /// 3. Replays every `WalCommand` in `state.wal` to rebuild the engine.
    /// 4. Returns the fully initialized handle.
    pub async fn init<P: Into<PathBuf>>(root: P) -> Result<Self, MemMapError> {
        let root_dir: PathBuf = root.into();

        // 1. Ensure the directory layout exists.
        tokio::fs::create_dir_all(&root_dir).await?;
        tokio::fs::create_dir_all(root_dir.join("index")).await?;
        tokio::fs::create_dir_all(root_dir.join("blocks")).await?;

        // 2. Open storage components (blocking I/O run on the current thread
        //    during init is acceptable; use spawn_blocking for hot paths).
        let log_manager = LogManager::open(&root_dir)?;
        let block_storage = BlockStorage::open(&root_dir)?;
        let search_provider = SearchProvider::open(&root_dir)?;

        // 3. Replay WAL to reconstruct in-memory state.
        let commands = log_manager.replay()?;
        let (mut engine_inner, _state_rx) = MemMapEngine::new();
        engine_inner.replay_commands(&commands);

        Ok(Self {
            root_dir,
            engine: Arc::new(RwLock::new(engine_inner)),
            log_manager: Arc::new(RwLock::new(log_manager)),
            block_storage: Arc::new(RwLock::new(block_storage)),
            search_provider: Arc::new(search_provider),
        })
    }

    /// Appends `text` as a new memory entry, indexes it with Tantivy, and
    /// persists the operation to the WAL atomically.
    ///
    /// Returns the unique ID assigned to this memory entry.
    pub async fn append_memory(&self, text: &str, tags: Vec<String>) -> Result<u64, MemMapError> {
        // Allocate an ID before acquiring the storage locks so that we hold
        // the engine write-lock for the shortest possible time.
        let id = {
            let mut eng = self.engine.write().await;
            eng.next_memory_id()
        };

        // Write the raw bytes to the block store.
        let data = text.as_bytes();
        let (chunk_id, offset, length) = {
            let mut store = self.block_storage.write().await;
            store.append(data)?
        };

        // Persist the WAL entry.
        let cmd = WalCommand::AppendMemory {
            id,
            chunk_id,
            offset,
            length,
            tags: tags.clone(),
        };
        {
            let mut wal = self.log_manager.write().await;
            wal.append(&cmd)?;
        }

        // Update the in-memory metadata index.
        {
            let mut eng = self.engine.write().await;
            eng.insert_metadata(engine::MemoryMetadata {
                id,
                chunk_id,
                offset,
                length,
                tags: tags.clone(),
            });
        }

        // Index the text in Tantivy (after WAL commit so the operation is
        // already durable if the indexer crashes).
        self.search_provider.index_memory(id, text, &tags).await?;

        Ok(id)
    }

    /// Searches memories using Tantivy full-text search and returns the
    /// matching text values read via zero-copy `memmap2`.
    pub async fn query_memory(&self, query: &str) -> Result<Vec<String>, MemMapError> {
        const MAX_RESULTS: usize = 20;

        let ids = self.search_provider.search(query, MAX_RESULTS)?;

        let mut results = Vec::with_capacity(ids.len());
        for id in ids {
            let meta = {
                let eng = self.engine.read().await;
                eng.get_metadata(id).cloned()
            };
            if let Some(meta) = meta {
                let bytes = {
                    let store = self.block_storage.read().await;
                    store.read(meta.chunk_id, meta.offset, meta.length)?
                };
                results.push(String::from_utf8_lossy(&bytes).into_owned());
            }
        }

        Ok(results)
    }

    /// Broadcasts `state` to all Tokio watch subscribers and records it in
    /// the engine so that new subscribers see the latest value immediately.
    pub async fn update_state(&self, state: AgentState) -> Result<(), MemMapError> {
        let eng = self.engine.write().await;
        eng.broadcast_state(state)
    }

    /// Returns a watch receiver that yields the latest [`AgentState`]
    /// whenever [`update_state`] is called.
    pub fn subscribe_state(&self) -> tokio::sync::watch::Receiver<AgentState> {
        // Acquiring a read lock here is cheap.
        let eng = self.engine.blocking_read();
        eng.subscribe_state()
    }

    /// The root directory this instance is backed by.
    pub fn root_dir(&self) -> &PathBuf {
        &self.root_dir
    }
}
