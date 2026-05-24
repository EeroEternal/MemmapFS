# MemMapFS

A database-less, zero-daemon, local-first memory and state synchronisation file system for AI Agents, written in Rust.

## Features

| Capability | Implementation |
|---|---|
| **Crash-safe persistence** | Binary append-only Write-Ahead Log (`state.wal`) using `bincode` |
| **Zero-copy reads** | Raw context blocks read via `memmap2` memory mappings |
| **Full-text search** | Embedded [Tantivy](https://github.com/quickwit-oss/tantivy) inverted index |
| **Async-first** | Fully non-blocking API built on [Tokio](https://tokio.rs) |
| **Live state broadcast** | `AgentState` published via Tokio `watch` channel |

## Directory Layout

```
.memmap_fs_root/
├── state.wal          # Binary append-only Write-Ahead Log (Serde + Bincode)
├── index/             # Embedded Tantivy full-text inverted index
└── blocks/            # Partitioned raw context memory data chunks
    ├── chunk_0000.bin
    └── chunk_0001.bin
```

## Quick Start

```toml
# Cargo.toml
[dependencies]
memmap_fs = { path = "." }
tokio = { version = "1", features = ["full"] }
```

```rust
use memmap_fs::{MemMapFS, engine::AgentState};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open (or recover) a MemMapFS instance.
    let fs = MemMapFS::init(".memmap_fs_root").await?;

    // Persist a memory entry.
    let id = fs.append_memory("Hello from MemMapFS!", vec!["demo".into()]).await?;

    // Full-text search with zero-copy block reads.
    let results = fs.query_memory("MemMapFS").await?;
    println!("{results:?}");

    // Broadcast agent state to any subscribers.
    fs.update_state(AgentState {
        status: "running".into(),
        active_memory_count: 1,
        last_updated_id: id,
        ..Default::default()
    }).await?;

    Ok(())
}
```

## Module Overview

| Module | Responsibility |
|---|---|
| `lib.rs` | `MemMapFS` public facade — coordinates all sub-systems |
| `engine.rs` | `MemMapEngine` — in-memory BTreeMap index + KV store + watch channel |
| `storage.rs` | `LogManager` (WAL) + `BlockStorage` (chunk files + memmap2 reads) |
| `search.rs` | `SearchProvider` — Tantivy index writer and query runner |
| `error.rs` | `MemMapError` — unified error type via `thiserror` |

## Running Tests

```bash
cargo test
```

## License

MIT
