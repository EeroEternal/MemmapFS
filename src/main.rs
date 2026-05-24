use memmap_fs::{engine::AgentState, MemMapFS};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let fs = MemMapFS::init(".memmap_fs_root").await?;

    // Append a memory entry.
    let id = fs
        .append_memory("Hello from MemMapFS!", vec!["demo".into()])
        .await?;
    println!("Stored memory id={id}");

    // Query it back.
    let results = fs.query_memory("MemMapFS").await?;
    println!("Query results: {results:?}");

    // Update agent state.
    fs.update_state(AgentState {
        status: "running".into(),
        active_memory_count: 1,
        last_updated_id: id,
        extra: Default::default(),
    })
    .await?;

    println!("MemMapFS initialized successfully at {:?}", fs.root_dir());
    Ok(())
}
