use memmap_fs::{engine::AgentState, MemMapFS};
use tempfile::TempDir;

/// Helper: create a fresh MemMapFS backed by a temporary directory.
fn make_fs() -> (MemMapFS, TempDir) {
    let dir = TempDir::new().expect("tempdir");
    let fs = MemMapFS::init(dir.path()).expect("init");
    (fs, dir)
}

#[test]
fn test_append_and_query() {
    let (fs, _dir) = make_fs();

    let id = fs
        .append_memory("The quick brown fox", vec!["animals".into()])
        .expect("append");
    assert!(id > 0);

    let results = fs.query_memory("quick fox").expect("query");
    assert!(
        results.iter().any(|r| r.contains("quick brown fox")),
        "expected to find the appended memory, got: {results:?}"
    );
}

#[test]
fn test_multiple_memories() {
    let (fs, _dir) = make_fs();

    let id1 = fs
        .append_memory("Rust is a systems language", vec!["rust".into()])
        .expect("append 1");
    let id2 = fs
        .append_memory("Tokio is an async runtime", vec!["tokio".into()])
        .expect("append 2");

    assert_ne!(id1, id2, "IDs must be unique");

    let results = fs.query_memory("async runtime").expect("query");
    assert!(
        results.iter().any(|r| r.contains("async runtime")),
        "expected Tokio result, got: {results:?}"
    );
}

#[test]
fn test_update_state() {
    let (fs, _dir) = make_fs();

    fs.update_state(AgentState {
        status: "running".into(),
        active_memory_count: 42,
        last_updated_id: 7,
        extra: Default::default(),
    })
    .expect("update_state");
}

#[test]
fn test_wal_recovery() {
    let dir = TempDir::new().expect("tempdir");

    // First session: write some data.
    {
        let fs = MemMapFS::init(dir.path()).expect("init session 1");
        fs.append_memory("Persistent memory", vec!["wal".into()])
            .expect("append");
    }

    // Second session: the WAL should be replayed and the memory re-indexed.
    {
        let fs = MemMapFS::init(dir.path()).expect("init session 2");
        // The block bytes should still be readable.
        let results = fs.query_memory("Persistent").expect("query");
        assert!(
            results.iter().any(|r| r.contains("Persistent memory")),
            "WAL recovery failed, got: {results:?}"
        );
    }
}

#[test]
fn test_kv_store() {
    let dir = TempDir::new().expect("tempdir");

    // First session: write KV data.
    {
        let fs = MemMapFS::init(dir.path()).expect("init session 1");
        
        // Non-existent key
        assert_eq!(fs.get_kv("foo"), None);

        // Set and get
        fs.set_kv("foo".into(), b"bar".to_vec()).expect("set_kv foo");
        assert_eq!(fs.get_kv("foo"), Some(b"bar".to_vec()));

        // Set another
        fs.set_kv("hello".into(), b"world".to_vec()).expect("set_kv hello");
        assert_eq!(fs.get_kv("hello"), Some(b"world".to_vec()));

        // Delete
        fs.delete_kv("foo".into()).expect("delete_kv foo");
        assert_eq!(fs.get_kv("foo"), None);
    }

    // Second session: recover and check WAL replay for KV store
    {
        let fs = MemMapFS::init(dir.path()).expect("init session 2");
        
        // 'foo' was deleted, should still be None
        assert_eq!(fs.get_kv("foo"), None);

        // 'hello' was not deleted, should be 'world'
        assert_eq!(fs.get_kv("hello"), Some(b"world".to_vec()));
    }
}

#[test]
fn test_streaming_read_write() {
    let (fs, _dir) = make_fs();

    let key = "sessions/session_123/stdout";

    // Write streaming segments
    fs.append_stream(key, b"hello ").expect("append 1");
    fs.append_stream(key, b"world!").expect("append 2");
    fs.append_stream(key, b" this is a stream.").expect("append 3");

    // Open stream for reading
    use std::io::Read;
    let mut reader = fs.open_read(key).expect("open_read");
    let mut content = String::new();
    reader.read_to_string(&mut content).expect("read_to_string");

    assert_eq!(content, "hello world! this is a stream.");
}

#[test]
fn test_key_indexing_search() {
    let (fs, _dir) = make_fs();

    fs.index("key_1", "The quick brown fox jumps over the lazy dog").expect("index 1");
    fs.index("key_2", "Rust is a fast systems programming language").expect("index 2");
    fs.index("key_3", "Tantivy is written in Rust").expect("index 3");

    // Search for "quick fox"
    let hits1 = fs.search("quick fox", 5).expect("search 1");
    assert_eq!(hits1.len(), 1);
    assert_eq!(hits1[0].key, "key_1");

    // Search for "Rust" (should match key_2 and key_3)
    let hits2 = fs.search("Rust", 5).expect("search 2");
    assert_eq!(hits2.len(), 2);
    let keys: Vec<String> = hits2.iter().map(|h| h.key.clone()).collect();
    assert!(keys.contains(&"key_2".to_string()));
    assert!(keys.contains(&"key_3".to_string()));
}
// Force rebuild comment
