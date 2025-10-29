use anyhow::Result;
use sqlx::{sqlite::SqlitePoolOptions, Row};

#[tokio::main]
async fn main() -> Result<()> {
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://messages.sqlite".to_string());
    println!("Using DATABASE_URL={}", database_url);
    let pool = SqlitePoolOptions::new().connect(&database_url).await?;

    let total: i64 = sqlx::query("SELECT COUNT(*) as c FROM messages")
        .fetch_one(&pool)
        .await?
        .get::<i64, _>(0);

    let embedded: i64 = sqlx::query("SELECT COUNT(*) as c FROM messages WHERE embedded = 1")
        .fetch_one(&pool)
        .await?
        .get::<i64, _>(0);

    let checkpoints: i64 = sqlx::query("SELECT COUNT(*) as c FROM channel_checkpoints")
        .fetch_one(&pool)
        .await?
        .get::<i64, _>(0);

    println!("messages: {} (embedded: {})", total, embedded);
    println!("channel_checkpoints: {}", checkpoints);

    Ok(())
}

