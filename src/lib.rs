pub mod error;
pub mod storage;
pub mod engine;
pub mod search;

use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use tokio::sync::watch;

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
    state_rx: watch::Receiver<AgentState>,
}

impl MemMapFS {
    /// Initializes or recovers a `MemMapFS` instance from `root`.
    ///
    /// 1. Creates the directory layout if it does not exist.
    /// 2. Opens `LogManager`, `BlockStorage`, and `SearchProvider`.
    /// 3. Replays every `WalCommand` in `state.wal` to rebuild the engine.
    /// 4. Returns the fully initialized handle.
    pub fn init<P: Into<PathBuf>>(root: P) -> Result<Self, MemMapError> {
        let root_dir: PathBuf = root.into();

        // 1. Ensure the directory layout exists.
        std::fs::create_dir_all(&root_dir)?;
        std::fs::create_dir_all(root_dir.join("index"))?;
        std::fs::create_dir_all(root_dir.join("blocks"))?;

        // 2. Open storage components.
        let log_manager = LogManager::open(&root_dir)?;
        let block_storage = BlockStorage::open(&root_dir)?;
        let search_provider = SearchProvider::open(&root_dir)?;

        // 3. Replay WAL to reconstruct in-memory state.
        let commands = log_manager.replay()?;
        let (mut engine_inner, state_rx) = MemMapEngine::new();
        engine_inner.replay_commands(&commands);

        Ok(Self {
            root_dir,
            engine: Arc::new(RwLock::new(engine_inner)),
            log_manager: Arc::new(RwLock::new(log_manager)),
            block_storage: Arc::new(RwLock::new(block_storage)),
            search_provider: Arc::new(search_provider),
            state_rx,
        })
    }

    /// Appends `text` as a new memory entry, indexes it with Tantivy, and
    /// persists the operation to the WAL atomically.
    ///
    /// Returns the unique ID assigned to this memory entry.
    pub fn append_memory(&self, text: &str, tags: Vec<String>) -> Result<u64, MemMapError> {
        // Allocate an ID before acquiring the storage locks so that we hold
        // the engine write-lock for the shortest possible time.
        let id = {
            let mut eng = self.engine.write().unwrap();
            eng.next_memory_id()
        };

        // Write the raw bytes to the block store.
        let data = text.as_bytes();
        let (chunk_id, offset, length) = {
            let mut store = self.block_storage.write().unwrap();
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
            let mut wal = self.log_manager.write().unwrap();
            wal.append(&cmd)?;
        }

        // Update the in-memory metadata index.
        {
            let mut eng = self.engine.write().unwrap();
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
        self.search_provider.index_memory(id, text, &tags)?;

        Ok(id)
    }

    /// Searches memories using Tantivy full-text search and returns the
    /// matching text values read via zero-copy `memmap2`.
    pub fn query_memory(&self, query: &str) -> Result<Vec<String>, MemMapError> {
        const MAX_RESULTS: usize = 20;

        let ids = self.search_provider.search(query, MAX_RESULTS)?;

        let mut results = Vec::with_capacity(ids.len());
        for id in ids {
            let meta = {
                let eng = self.engine.read().unwrap();
                eng.get_metadata(id).cloned()
            };
            if let Some(meta) = meta {
                let bytes = {
                    let store = self.block_storage.read().unwrap();
                    store.read(meta.chunk_id, meta.offset, meta.length)?
                };
                results.push(String::from_utf8_lossy(&bytes).into_owned());
            }
        }

        Ok(results)
    }

    /// Broadcasts `state` to all Tokio watch subscribers and records it in
    /// the engine so that new subscribers see the latest value immediately.
    pub fn update_state(&self, state: AgentState) -> Result<(), MemMapError> {
        let eng = self.engine.write().unwrap();
        eng.broadcast_state(state)
    }

    /// Returns a watch receiver that yields the latest [`AgentState`]
    /// whenever [`update_state`] is called.
    pub fn subscribe_state(&self) -> watch::Receiver<AgentState> {
        self.state_rx.clone()
    }

    /// Returns the bytes associated with `key` from the in-memory KV store if present.
    pub fn get_kv(&self, key: &str) -> Option<Vec<u8>> {
        let eng = self.engine.read().unwrap();
        eng.kv_store.get(key).cloned()
    }

    /// Sets the value for `key` in the KV store, persisting to WAL first.
    pub fn set_kv(&self, key: String, value: Vec<u8>) -> Result<(), MemMapError> {
        let cmd = WalCommand::SetKv {
            key: key.clone(),
            value: value.clone(),
        };

        // Persist the WAL entry first.
        {
            let mut wal = self.log_manager.write().unwrap();
            wal.append(&cmd)?;
        }

        // Update the in-memory engine KV store.
        {
            let mut eng = self.engine.write().unwrap();
            eng.kv_store.insert(key, value);
        }

        Ok(())
    }

    /// Deletes `key` from the KV store, persisting to WAL first.
    pub fn delete_kv(&self, key: String) -> Result<(), MemMapError> {
        let cmd = WalCommand::DeleteKv { key: key.clone() };

        // Persist the WAL entry first.
        {
            let mut wal = self.log_manager.write().unwrap();
            wal.append(&cmd)?;
        }

        // Update the in-memory engine KV store.
        {
            let mut eng = self.engine.write().unwrap();
            eng.kv_store.remove(&key);
        }

        Ok(())
    }

    // ─── IntentLoop Streaming APIs ────────────────────────────────────────────

    /// Appends `data` to the stream identified by `key`.
    pub fn append_stream(&self, key: &str, data: &[u8]) -> Result<(), MemMapError> {
        // Write raw bytes to block store
        let (chunk_id, offset, length) = {
            let mut store = self.block_storage.write().unwrap();
            store.append(data)?
        };

        // Write WalCommand
        let cmd = WalCommand::AppendStream {
            key: key.to_string(),
            chunk_id,
            offset,
            length,
        };
        {
            let mut wal = self.log_manager.write().unwrap();
            wal.append(&cmd)?;
        }

        // Update in-memory index
        {
            let mut eng = self.engine.write().unwrap();
            eng.streams
                .entry(key.to_string())
                .or_default()
                .push(crate::engine::StreamSegment {
                    chunk_id,
                    offset,
                    length,
                });
        }

        Ok(())
    }

    /// Opens a stream reader for the given `key`.
    pub fn open_read(&self, key: &str) -> Result<StreamReader, MemMapError> {
        let segments = {
            let eng = self.engine.read().unwrap();
            eng.streams.get(key).cloned().unwrap_or_default()
        };

        Ok(StreamReader {
            block_storage: self.block_storage.clone(),
            segments,
            current_segment_idx: 0,
            current_segment_offset: 0,
        })
    }

    // ─── IntentLoop Search APIs ───────────────────────────────────────────────

    /// Indexes a custom key and text in the full-text search engine.
    pub fn index(&self, key: &str, text: &str) -> Result<(), MemMapError> {
        self.search_provider.index(key, text)
    }

    /// Searches the full-text index and returns matching hits.
    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<crate::search::Hit>, MemMapError> {
        self.search_provider.search_hits(query, limit)
    }

    /// The root directory this instance is backed by.
    pub fn root_dir(&self) -> &PathBuf {
        &self.root_dir
    }
}

/// A reader that implements `std::io::Read` to stream segments of block storage sequentially.
pub struct StreamReader {
    block_storage: Arc<RwLock<BlockStorage>>,
    segments: Vec<crate::engine::StreamSegment>,
    current_segment_idx: usize,
    current_segment_offset: u64,
}

impl std::io::Read for StreamReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.current_segment_idx >= self.segments.len() {
            return Ok(0); // EOF
        }

        let seg = &self.segments[self.current_segment_idx];
        let remaining = seg.length - self.current_segment_offset;
        if remaining == 0 {
            // Move to next segment
            self.current_segment_idx += 1;
            self.current_segment_offset = 0;
            return self.read(buf);
        }

        let read_len = (buf.len() as u64).min(remaining);
        let bytes = {
            let store = self.block_storage.read().unwrap();
            store
                .read(
                    seg.chunk_id,
                    seg.offset + self.current_segment_offset,
                    read_len,
                )
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?
        };

        buf[..bytes.len()].copy_from_slice(&bytes);
        self.current_segment_offset += bytes.len() as u64;
        Ok(bytes.len())
    }
}
// Force rebuild comment
