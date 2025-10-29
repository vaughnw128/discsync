use anyhow::Result;
use sbux::embedder::{new_embedder, VECTOR_DIM};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize basic logging for info-level messages
    let _ = tracing_subscriber::fmt::try_init();

    let model_dir = PathBuf::from("model/minilm");
    let embedder = new_embedder(&model_dir).await?;

    let texts = vec!["hello world", "another one"];
    let refs: Vec<&str> = texts.iter().map(|&s| s).collect();
    let vecs = embedder.embed_batch(&refs).await?;

    println!(
        "embedded {} texts; first vector dim = {} (expected {})",
        vecs.len(),
        vecs.get(0).map(|v| v.len()).unwrap_or(0),
        VECTOR_DIM
    );
    Ok(())
}
