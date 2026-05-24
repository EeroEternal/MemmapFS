use memmap_fs::{engine::AgentState, MemMapFS, Session};
use tempfile::TempDir;

/// Helper: create a fresh MemMapFS backed by a temporary directory.
async fn make_fs() -> (MemMapFS, TempDir) {
    let dir = TempDir::new().expect("tempdir");
    let fs = MemMapFS::init(dir.path()).await.expect("init");
    (fs, dir)
}

#[tokio::test]
async fn test_append_and_query() {
    let (fs, _dir) = make_fs().await;

    let id = fs
        .append_memory("The quick brown fox", vec!["animals".into()])
        .await
        .expect("append");
    assert!(id > 0);

    let results = fs.query_memory("quick fox").await.expect("query");
    assert!(
        results.iter().any(|r| r.contains("quick brown fox")),
        "expected to find the appended memory, got: {results:?}"
    );
}

#[tokio::test]
async fn test_multiple_memories() {
    let (fs, _dir) = make_fs().await;

    let id1 = fs
        .append_memory("Rust is a systems language", vec!["rust".into()])
        .await
        .expect("append 1");
    let id2 = fs
        .append_memory("Tokio is an async runtime", vec!["tokio".into()])
        .await
        .expect("append 2");

    assert_ne!(id1, id2, "IDs must be unique");

    let results = fs.query_memory("async runtime").await.expect("query");
    assert!(
        results.iter().any(|r| r.contains("async runtime")),
        "expected Tokio result, got: {results:?}"
    );
}

#[tokio::test]
async fn test_update_state() {
    let (fs, _dir) = make_fs().await;

    fs.update_state(AgentState {
        status: "running".into(),
        active_memory_count: 42,
        last_updated_id: 7,
        extra: Default::default(),
    })
    .await
    .expect("update_state");
}

#[tokio::test]
async fn test_wal_recovery() {
    let dir = TempDir::new().expect("tempdir");

    // First session: write some data.
    {
        let fs = MemMapFS::init(dir.path()).await.expect("init session 1");
        fs.append_memory("Persistent memory", vec!["wal".into()])
            .await
            .expect("append");
    }

    // Second session: the WAL should be replayed and the memory re-indexed.
    {
        let fs = MemMapFS::init(dir.path()).await.expect("init session 2");
        // The block bytes should still be readable.
        let results = fs.query_memory("Persistent").await.expect("query");
        assert!(
            results.iter().any(|r| r.contains("Persistent memory")),
            "WAL recovery failed, got: {results:?}"
        );
    }
}

#[tokio::test]
async fn test_kv_store() {
    let dir = TempDir::new().expect("tempdir");

    // First session: write KV data.
    {
        let fs = MemMapFS::init(dir.path()).await.expect("init session 1");
        
        // Non-existent key
        assert_eq!(fs.get_kv("foo"), None);

        // Set and get
        fs.set_kv("foo".into(), b"bar".to_vec()).await.expect("set_kv foo");
        assert_eq!(fs.get_kv("foo"), Some(b"bar".to_vec()));

        // Set another
        fs.set_kv("hello".into(), b"world".to_vec()).await.expect("set_kv hello");
        assert_eq!(fs.get_kv("hello"), Some(b"world".to_vec()));

        // Delete
        fs.delete_kv("foo".into()).await.expect("delete_kv foo");
        assert_eq!(fs.get_kv("foo"), None);
    }

    // Second session: recover and check WAL replay for KV store
    {
        let fs = MemMapFS::init(dir.path()).await.expect("init session 2");
        
        // 'foo' was deleted, should still be None
        assert_eq!(fs.get_kv("foo"), None);

        // 'hello' was not deleted, should be 'world'
        assert_eq!(fs.get_kv("hello"), Some(b"world".to_vec()));
    }
}

#[tokio::test]
async fn test_session_metadata() {
    let (fs, _dir) = make_fs().await;

    let s1 = Session {
        id: "session_1".into(),
        intent_id: "intent_a".into(),
        created_at: 1000,
        payload: b"data 1".to_vec(),
    };
    let s2 = Session {
        id: "session_2".into(),
        intent_id: "intent_b".into(),
        created_at: 2000,
        payload: b"data 2".to_vec(),
    };
    let s3 = Session {
        id: "session_3".into(),
        intent_id: "intent_a".into(),
        created_at: 1500,
        payload: b"data 3".to_vec(),
    };

    fs.put_session(&s1).await.expect("put s1");
    fs.put_session(&s2).await.expect("put s2");
    fs.put_session(&s3).await.expect("put s3");

    // Test get_session
    assert_eq!(fs.get_session("session_1").expect("get s1").as_ref(), Some(&s1));
    assert_eq!(fs.get_session("session_2").expect("get s2").as_ref(), Some(&s2));
    assert_eq!(fs.get_session("non_existent").expect("get none").as_ref(), None);

    // Test list_sessions (should be sorted by created_at desc: s2 (2000), s3 (1500), s1 (1000))
    let all = fs.list_sessions(10).expect("list all");
    assert_eq!(all.len(), 3);
    assert_eq!(all[0].id, "session_2");
    assert_eq!(all[1].id, "session_3");
    assert_eq!(all[2].id, "session_1");

    // Test list_sessions limit
    let limited = fs.list_sessions(2).expect("list limited");
    assert_eq!(limited.len(), 2);
    assert_eq!(limited[0].id, "session_2");
    assert_eq!(limited[1].id, "session_3");

    // Test list_by_intent for "intent_a" (should be sorted by created_at desc: s3 (1500), s1 (1000))
    let filtered = fs.list_by_intent("intent_a").expect("list intent_a");
    assert_eq!(filtered.len(), 2);
    assert_eq!(filtered[0].id, "session_3");
    assert_eq!(filtered[1].id, "session_1");
}

#[tokio::test]
async fn test_streaming_read_write() {
    let (fs, _dir) = make_fs().await;

    let key = "sessions/session_123/stdout";

    // Write streaming segments
    fs.append_stream(key, b"hello ").await.expect("append 1");
    fs.append_stream(key, b"world!").await.expect("append 2");
    fs.append_stream(key, b" this is a stream.").await.expect("append 3");

    // Open stream for reading
    use std::io::Read;
    let mut reader = fs.open_read(key).expect("open_read");
    let mut content = String::new();
    reader.read_to_string(&mut content).expect("read_to_string");

    assert_eq!(content, "hello world! this is a stream.");
}

#[tokio::test]
async fn test_key_indexing_search() {
    let (fs, _dir) = make_fs().await;

    fs.index("key_1", "The quick brown fox jumps over the lazy dog").await.expect("index 1");
    fs.index("key_2", "Rust is a fast systems programming language").await.expect("index 2");
    fs.index("key_3", "Tantivy is written in Rust").await.expect("index 3");

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
