// external
use anyhow::Result;
use sqlx::{Pool, Sqlite};

pub async fn init_db(pool: &Pool<Sqlite>) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS messages (
            id TEXT PRIMARY KEY,
            content TEXT NOT NULL,
            author_id TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            embedded INTEGER DEFAULT 0
        );
        "#
    ).execute(pool).await?;

    sqlx::query(
        r#"CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp);"#
    ).execute(pool).await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS channel_checkpoints (
            channel_id TEXT PRIMARY KEY,
            last_message_id TEXT,
            last_timestamp INTEGER
        );
        "#
    ).execute(pool).await?;

    Ok(())
}

