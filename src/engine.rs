use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::error::MemMapError;
use crate::storage::WalCommand;

// ─── Public domain types ──────────────────────────────────────────────────────

/// Metadata recorded for every memory block stored in `BlockStorage`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryMetadata {
    pub id: u64,
    pub chunk_id: u32,
    pub offset: u64,
    pub length: u64,
    pub tags: Vec<String>,
}

/// Observable agent state broadcast via a Tokio watch channel.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AgentState {
    pub status: String,
    pub active_memory_count: u64,
    pub last_updated_id: u64,
    pub extra: HashMap<String, String>,
}

// ─── MemMapEngine ─────────────────────────────────────────────────────────────

/// In-memory index and KV store reconstructed from WAL replay on startup and
/// kept up-to-date on every write.
///
/// The engine owns a Tokio `watch` sender so that any number of async tasks
/// can subscribe to state changes without polling.
pub struct MemMapEngine {
    /// Ordered index from memory ID → block location metadata.
    pub metadata_index: BTreeMap<u64, MemoryMetadata>,
    /// Arbitrary binary key/value store.
    pub kv_store: HashMap<String, Vec<u8>>,
    /// Broadcast channel for [`AgentState`] updates.
    state_tx: watch::Sender<AgentState>,
    /// Monotonic counter used to assign unique IDs to new memories.
    next_id: u64,
}

impl MemMapEngine {
    /// Creates a fresh, empty engine and returns both the engine and the
    /// corresponding watch receiver.
    pub fn new() -> (Self, watch::Receiver<AgentState>) {
        let (state_tx, state_rx) = watch::channel(AgentState::default());
        let engine = Self {
            metadata_index: BTreeMap::new(),
            kv_store: HashMap::new(),
            state_tx,
            next_id: 1,
        };
        (engine, state_rx)
    }

    /// Replays a sequence of [`WalCommand`]s to rebuild the in-memory state.
    ///
    /// Called once during [`MemMapFS::init`] after the WAL has been read from
    /// disk.  The watch channel is *not* triggered for replayed commands so
    /// that subscribers only see live updates.
    pub fn replay_commands(&mut self, commands: &[WalCommand]) {
        for cmd in commands {
            match cmd {
                WalCommand::AppendMemory {
                    id,
                    chunk_id,
                    offset,
                    length,
                    tags,
                } => {
                    self.metadata_index.insert(
                        *id,
                        MemoryMetadata {
                            id: *id,
                            chunk_id: *chunk_id,
                            offset: *offset,
                            length: *length,
                            tags: tags.clone(),
                        },
                    );
                    if *id >= self.next_id {
                        self.next_id = id + 1;
                    }
                }
                WalCommand::SetKv { key, value } => {
                    self.kv_store.insert(key.clone(), value.clone());
                }
                WalCommand::DeleteKv { key } => {
                    self.kv_store.remove(key);
                }
            }
        }
    }

    /// Allocates the next monotonic memory ID.
    pub fn next_memory_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Inserts metadata for a newly written memory block.
    pub fn insert_metadata(&mut self, meta: MemoryMetadata) {
        self.metadata_index.insert(meta.id, meta);
    }

    /// Looks up metadata by memory ID.
    pub fn get_metadata(&self, id: u64) -> Option<&MemoryMetadata> {
        self.metadata_index.get(&id)
    }

    /// Returns an iterator over all stored memory IDs in ascending order.
    pub fn all_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.metadata_index.keys().copied()
    }

    /// Publishes a new [`AgentState`] to all watch subscribers.
    ///
    /// Uses `send_replace` so the call succeeds even when no subscribers are
    /// currently active; the value is stored and delivered to the next
    /// subscriber that calls `borrow()` or `changed()`.
    pub fn broadcast_state(&self, state: AgentState) -> Result<(), MemMapError> {
        self.state_tx.send_replace(state);
        Ok(())
    }

    /// Returns a new receiver subscribed to state change events.
    pub fn subscribe_state(&self) -> watch::Receiver<AgentState> {
        self.state_tx.subscribe()
    }

    /// A snapshot of the current broadcast state.
    pub fn current_state(&self) -> AgentState {
        self.state_tx.borrow().clone()
    }
}
