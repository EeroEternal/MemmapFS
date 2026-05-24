use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use memmap2::Mmap;
use serde::{Deserialize, Serialize};

use crate::error::MemMapError;

// ─── WAL types ────────────────────────────────────────────────────────────────

/// A single entry appended to `state.wal`.
///
/// Each entry is length-prefixed (u64 LE) followed by the bincode-encoded
/// payload so that the replay loop can skip or detect truncated records.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalCommand {
    /// A new memory block was written.
    AppendMemory {
        id: u64,
        chunk_id: u32,
        offset: u64,
        length: u64,
        tags: Vec<String>,
    },
    /// A key/value pair was upserted into the KV store.
    SetKv { key: String, value: Vec<u8> },
    /// A key was removed from the KV store.
    DeleteKv { key: String },
}

/// Manages sequential, append-only binary writes to `state.wal` and provides
/// a linear replay pass for crash recovery.
pub struct LogManager {
    wal_path: PathBuf,
    writer: BufWriter<File>,
}

impl LogManager {
    /// Opens (or creates) the WAL file and positions the write cursor at EOF.
    pub fn open(root: &Path) -> Result<Self, MemMapError> {
        let wal_path = root.join("state.wal");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)?;
        Ok(Self {
            wal_path,
            writer: BufWriter::new(file),
        })
    }

    /// Encodes `cmd` with bincode, writes a u64 length prefix, then the payload.
    ///
    /// The length prefix enables the replay loop to skip or detect corrupt
    /// tail records caused by an unclean shutdown.
    pub fn append(&mut self, cmd: &WalCommand) -> Result<(), MemMapError> {
        let payload = bincode::serialize(cmd)?;
        let len = payload.len() as u64;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&payload)?;
        self.writer.flush()?;
        Ok(())
    }

    /// Reads every valid WAL entry from file offset 0 and returns them in
    /// order.  Truncated records at the very end of the file (caused by a
    /// crash mid-write) are silently ignored; any corruption in the middle of
    /// the file is surfaced as [`MemMapError::CorruptWal`].
    pub fn replay(&self) -> Result<Vec<WalCommand>, MemMapError> {
        let mut file = File::open(&self.wal_path)?;
        let file_len = file.metadata()?.len();
        let mut commands = Vec::new();
        let mut pos: u64 = 0;

        loop {
            // Need at least 8 bytes for the length prefix.
            if pos + 8 > file_len {
                break;
            }

            let mut len_buf = [0u8; 8];
            file.seek(SeekFrom::Start(pos))?;
            file.read_exact(&mut len_buf)?;
            let payload_len = u64::from_le_bytes(len_buf);

            // Truncated payload at EOF is acceptable (crash mid-write).
            if pos + 8 + payload_len > file_len {
                break;
            }

            let mut payload = vec![0u8; payload_len as usize];
            file.read_exact(&mut payload)?;

            let cmd: WalCommand =
                bincode::deserialize(&payload).map_err(|e| MemMapError::CorruptWal {
                    offset: pos,
                    reason: e.to_string(),
                })?;

            commands.push(cmd);
            pos += 8 + payload_len;
        }

        Ok(commands)
    }
}

// ─── Block Storage ────────────────────────────────────────────────────────────

const CHUNK_SIZE_LIMIT: u64 = 64 * 1024 * 1024; // 64 MiB per chunk
const CHUNK_NAME_PREFIX: &str = "chunk_";

/// Stores and retrieves raw context-memory text across a set of partitioned
/// binary chunk files inside `<root>/blocks/`.
///
/// Writes are sequential appends; reads use zero-copy `memmap2` mappings so
/// that large reads do not go through an extra kernel-space copy.
pub struct BlockStorage {
    blocks_dir: PathBuf,
    /// The chunk file currently accepting appends.
    active_chunk_id: u32,
    /// Write cursor position inside the active chunk.
    active_offset: u64,
    writer: BufWriter<File>,
}

impl BlockStorage {
    /// Opens the blocks directory, finds the last chunk, and positions the
    /// writer at its end.
    pub fn open(root: &Path) -> Result<Self, MemMapError> {
        let blocks_dir = root.join("blocks");
        std::fs::create_dir_all(&blocks_dir)?;

        let (active_chunk_id, active_offset) =
            Self::find_active_chunk(&blocks_dir)?;

        let chunk_path = Self::chunk_path(&blocks_dir, active_chunk_id);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&chunk_path)?;
        let active_offset = if active_offset == 0 {
            // freshly created file
            0
        } else {
            active_offset
        };

        Ok(Self {
            blocks_dir,
            active_chunk_id,
            active_offset,
            writer: BufWriter::new(file),
        })
    }

    /// Appends `data` to the active chunk (rolling to a new chunk when the
    /// size limit is exceeded).
    ///
    /// Returns `(chunk_id, offset, length)` so the caller can record a WAL
    /// entry with the exact location.
    pub fn append(&mut self, data: &[u8]) -> Result<(u32, u64, u64), MemMapError> {
        if self.active_offset + data.len() as u64 > CHUNK_SIZE_LIMIT {
            self.roll_chunk()?;
        }

        let offset = self.active_offset;
        self.writer.write_all(data)?;
        self.writer.flush()?;
        self.active_offset += data.len() as u64;

        Ok((self.active_chunk_id, offset, data.len() as u64))
    }

    /// Reads `length` bytes from `chunk_id` at `offset` using a zero-copy
    /// memory mapping and returns the bytes as an owned `Vec<u8>`.
    pub fn read(&self, chunk_id: u32, offset: u64, length: u64) -> Result<Vec<u8>, MemMapError> {
        let path = Self::chunk_path(&self.blocks_dir, chunk_id);
        if !path.exists() {
            return Err(MemMapError::BlockNotFound { chunk_id, offset });
        }

        let file = File::open(&path)?;
        // SAFETY: The file is opened read-only and no other thread holds a
        // mutable mapping to the same range.  We copy out of the mapping
        // immediately, which is safe even if the underlying file is later
        // appended to.
        let mmap = unsafe { Mmap::map(&file)? };
        let start = offset as usize;
        let end = start + length as usize;
        if end > mmap.len() {
            return Err(MemMapError::BlockNotFound { chunk_id, offset });
        }
        Ok(mmap[start..end].to_vec())
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    fn roll_chunk(&mut self) -> Result<(), MemMapError> {
        self.writer.flush()?;
        self.active_chunk_id += 1;
        self.active_offset = 0;
        let path = Self::chunk_path(&self.blocks_dir, self.active_chunk_id);
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        self.writer = BufWriter::new(file);
        Ok(())
    }

    fn find_active_chunk(blocks_dir: &Path) -> Result<(u32, u64), MemMapError> {
        let mut max_id: Option<u32> = None;
        for entry in std::fs::read_dir(blocks_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(rest) = name.strip_prefix(CHUNK_NAME_PREFIX) {
                let stem = rest.trim_end_matches(".bin");
                if let Ok(id) = stem.parse::<u32>() {
                    max_id = Some(max_id.map_or(id, |m: u32| m.max(id)));
                }
            }
        }

        let chunk_id = max_id.unwrap_or(0);
        let path = Self::chunk_path(blocks_dir, chunk_id);
        let offset = if path.exists() {
            std::fs::metadata(&path)?.len()
        } else {
            0
        };
        Ok((chunk_id, offset))
    }

    fn chunk_path(blocks_dir: &Path, id: u32) -> PathBuf {
        blocks_dir.join(format!("{}{:04}.bin", CHUNK_NAME_PREFIX, id))
    }
}
