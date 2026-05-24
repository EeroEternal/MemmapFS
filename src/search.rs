use std::path::Path;
use std::sync::Arc;

use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::QueryParser;
use tantivy::schema::{Field, Schema, STORED, TEXT, STRING};
use tantivy::{doc, Index, IndexWriter, ReloadPolicy, TantivyDocument};
use tantivy::schema::Value;
use std::sync::Mutex;

use crate::error::MemMapError;

/// A full-text search result hit.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Hit {
    pub key: String,
    pub score: f32,
}

/// Wraps a Tantivy full-text index.
///
/// The `IndexWriter` is protected by a `Mutex` so that the single-writer
/// constraint imposed by Tantivy is enforced at runtime.  Readers are created
/// on demand from a shared `IndexReader` that auto-reloads after each commit.
pub struct SearchProvider {
    index: Index,
    writer: Arc<Mutex<IndexWriter>>,
    field_id: Field,
    field_key: Field,
    field_text: Field,
    field_tags: Field,
}

impl SearchProvider {
    /// Opens (or creates) the Tantivy index stored in `<root>/index/`.
    pub fn open(root: &Path) -> Result<Self, MemMapError> {
        let index_dir = root.join("index");
        std::fs::create_dir_all(&index_dir)?;

        let mut schema_builder = Schema::builder();
        let field_id = schema_builder.add_u64_field("id", STORED);
        let field_key = schema_builder.add_text_field("key", STRING | STORED);
        let field_text = schema_builder.add_text_field("text", TEXT | STORED);
        let field_tags = schema_builder.add_text_field("tags", TEXT | STORED);
        let schema = schema_builder.build();

        let dir = MmapDirectory::open(&index_dir)?;
        let index = Index::open_or_create(dir, schema)?;

        // 50 MB write heap – enough for typical agent workloads.
        let writer: IndexWriter = index.writer(50_000_000)?;

        Ok(Self {
            index,
            writer: Arc::new(Mutex::new(writer)),
            field_id,
            field_key,
            field_text,
            field_tags,
        })
    }

    /// Adds a document to the Tantivy index and commits immediately.
    ///
    /// Commits after every document is not optimal for bulk ingestion but
    /// guarantees that the document is searchable straight away, which matches
    /// the low-latency requirements of the MemMapFS write path.
    pub async fn index_memory(
        &self,
        id: u64,
        text: &str,
        tags: &[String],
    ) -> Result<(), MemMapError> {
        let tags_str = tags.join(" ");
        let mut writer = self.writer.lock().unwrap();
        writer.add_document(doc!(
            self.field_id => id,
            self.field_text => text,
            self.field_tags => tags_str,
        ))?;
        writer.commit()?;
        Ok(())
    }

    /// Executes a full-text query against the index and returns the IDs of
    /// matching documents in relevance order (up to `limit` results).
    pub fn search(&self, query_str: &str, limit: usize) -> Result<Vec<u64>, MemMapError> {
        let reader = self
            .index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        let searcher = reader.searcher();
        let query_parser =
            QueryParser::for_index(&self.index, vec![self.field_text, self.field_tags]);
        let query = query_parser.parse_query(query_str)?;

        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;

        let mut ids = Vec::with_capacity(top_docs.len());
        for (_score, doc_addr) in top_docs {
            let retrieved: TantivyDocument = searcher.doc(doc_addr)?;
            if let Some(id_val) = retrieved.get_first(self.field_id) {
                if let Some(id) = id_val.as_u64() {
                    ids.push(id);
                }
            }
        }
        Ok(ids)
    }

    /// Deletes all documents for `id` from the index and commits.
    pub async fn delete_memory(&self, id: u64) -> Result<(), MemMapError> {
        use tantivy::Term;
        let mut writer = self.writer.lock().unwrap();
        let term = Term::from_field_u64(self.field_id, id);
        writer.delete_term(term);
        writer.commit()?;
        Ok(())
    }

    /// Indexes a document with a custom key and text.
    pub async fn index(&self, key: &str, text: &str) -> Result<(), MemMapError> {
        let mut writer = self.writer.lock().unwrap();
        writer.add_document(doc!(
            self.field_key => key,
            self.field_text => text,
        ))?;
        writer.commit()?;
        Ok(())
    }

    /// Searches the index and returns Hits.
    pub fn search_hits(&self, query_str: &str, limit: usize) -> Result<Vec<Hit>, MemMapError> {
        let reader = self
            .index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        let searcher = reader.searcher();
        let query_parser =
            QueryParser::for_index(&self.index, vec![self.field_text, self.field_tags]);
        let query = query_parser.parse_query(query_str)?;

        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;

        let mut hits = Vec::with_capacity(top_docs.len());
        for (score, doc_addr) in top_docs {
            let retrieved: TantivyDocument = searcher.doc(doc_addr)?;
            if let Some(key_val) = retrieved.get_first(self.field_key) {
                if let Some(key) = key_val.as_str() {
                    hits.push(Hit {
                        key: key.to_string(),
                        score,
                    });
                }
            } else if let Some(id_val) = retrieved.get_first(self.field_id) {
                if let Some(id) = id_val.as_u64() {
                    hits.push(Hit {
                        key: id.to_string(),
                        score,
                    });
                }
            }
        }
        Ok(hits)
    }
}
