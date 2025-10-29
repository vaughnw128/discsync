// built-in
use std::path::{Path, PathBuf};

// external
use anyhow::Result;
use clap::Parser;
use tracing::info;

// project
use discsync::archiver::{Archiver, ArchiverConfig};

#[derive(Parser, Debug, Clone)]
#[command(name = "discsync")]
#[command(about = "Discord archival: mirror all guilds/channels into SQL (SQLite/Postgres)")]
pub struct Args {
    /// Discord bot token
    #[arg(long, env = "DISCORD_TOKEN")]
    pub discord_token: String,

    /// Database URL. Defaults to local SQLite file messages.sqlite
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: Option<String>,

    /// Poll interval seconds between sync passes
    #[arg(long, env = "POLL_INTERVAL_SECS", default_value_t = 300)]
    pub poll_interval_secs: u64,

    /// SQLx slow statement log threshold in seconds (e.g. 5 -> "5s").
    #[arg(long, env = "SQLX_LOG_SLOW_STATEMENTS_SECS", default_value_t = 5)]
    pub sqlx_log_slow_statements_secs: u64,
}

fn ensure_sqlite_file(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }
    if !path.exists() {
        std::fs::File::create(path)?;
    }
    Ok(())
}

impl Args {
    pub fn resolve(self) -> Result<ArchiverConfig> {
        std::env::set_var("SQLX_LOG_SLOW_STATEMENTS", format!("{}s", self.sqlx_log_slow_statements_secs));

        let database_url = self.database_url.unwrap_or_else(|| {
            let default_path = PathBuf::from("messages.sqlite");
            ensure_sqlite_file(&default_path).unwrap();
            format!("sqlite://{}", default_path.display())
        });
        Ok(ArchiverConfig {
            discord_token: self.discord_token,
            database_url,
            poll_interval_secs: self.poll_interval_secs,
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();

    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        tracing_subscriber::EnvFilter::new("info,sea_orm=warn,sqlx=warn,sqlx::query=warn")
    });

    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();

    let cli_args = Args::parse();
    let cfg = cli_args.resolve()?;

    info!("starting discsync archiver");
    let archiver = Archiver::new(cfg).await?;
    archiver.run_forever().await
}
