use memmap_fs::{engine::AgentState, MemMapFS};
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
