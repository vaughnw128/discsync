// built-in
use std::path::{Path, PathBuf};

// external
use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing::info;

// project
use sbux::pipeline;
use sbux::pipeline::PipelineConfig;

#[derive(Subcommand, Debug, Clone)]
pub enum Command {
    /// Run the live pipeline (gateway consumer + embedding + upsert)
    Run,
    /// Backfill historical messages into SQLite and Qdrant
    Backfill {
        /// Guild ID to backfill
        #[arg(long, env = "GUILD_ID")]
        guild_id: u64,

        /// Specific channel IDs to backfill (defaults to all text channels)
        #[arg(long, num_args=1.., value_delimiter=',')]
        channels: Option<Vec<u64>>,

        /// Start time (inclusive), RFC3339 (e.g., 2024-01-01T00:00:00Z)
        #[arg(long)]
        start: Option<String>,

        /// End time (exclusive), RFC3339 (e.g., 2024-02-01T00:00:00Z)
        #[arg(long)]
        end: Option<String>,

        /// Batch size for embedding/upsert
        #[arg(long, default_value_t = 100)]
        batch_size: usize,

        /// Maximum number of messages to process (per channel); unlimited if omitted
        #[arg(long)]
        max_messages: Option<usize>,

        /// Dry run (list counts only, no DB/Qdrant writes)
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
}

#[derive(Parser, Debug, Clone)]
#[command(name = "sbux")]
#[command(about = "Discord -> embeddings -> Qdrant (with SQLite backup)")]
pub struct Args {
    /// Discord bot token
    #[arg(long, env = "DISCORD_TOKEN")]
    pub discord_token: String,

    /// Database URL or path. If omitted, a local SQLite DB will be created (messages.sqlite)
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: Option<String>,

    /// Qdrant endpoint URL, e.g. http://localhost:6334 (gRPC)
    #[arg(long, env = "QDRANT_URL", default_value = "http://localhost:6334")]
    pub qdrant_url: String,

    /// Qdrant API key (for Qdrant Cloud or secured instances)
    #[arg(long, env = "QDRANT_API_KEY")]
    pub qdrant_api_key: Option<String>,

    /// Guild ID used by the live pipeline (gateway) and startup catch-up
    #[arg(long, env = "GUILD_ID")]
    pub guild_id: Option<u64>,

    /// Local directory for model + tokenizer files
    #[arg(long, env = "MODEL_DIR")]
    pub model_dir: Option<PathBuf>,

    /// Run a one-time startup catch-up using checkpoints (default: true)
    #[arg(long, env = "STARTUP_CATCHUP", default_value_t = true)]
    pub startup_catchup: bool,

    /// Enable periodic checker (hourly) to close small gaps (default: false)
    #[arg(long, env = "ENABLE_PERIODIC_CHECK", default_value_t = false)]
    pub enable_periodic_check: bool,

    #[command(subcommand)]
    pub command: Option<Command>,
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
    pub fn resolve(self) -> Result<PipelineConfig> {
        let model_dir = self
            .model_dir
            .unwrap_or_else(|| PathBuf::from("model/minilm"));

        let database_url = self.database_url.unwrap_or_else(|| {
            let default_path = PathBuf::from("messages.sqlite");
            ensure_sqlite_file(&default_path).unwrap();
            format!("sqlite://{}", default_path.display())
        });

        Ok(PipelineConfig {
            database_url,
            qdrant_url: self.qdrant_url,
            qdrant_api_key: self.qdrant_api_key,
            discord_token: self.discord_token,
            guild_id: self.guild_id,
            model_dir,
            startup_catchup: self.startup_catchup,
            enable_periodic_check: self.enable_periodic_check,
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();

    let cli_args = Args::parse();

    let cmd = cli_args.command.clone();
    match cmd {
        Some(Command::Backfill { guild_id, channels, start, end, batch_size, max_messages, dry_run }) => {
            let cfg = cli_args.clone().resolve()?;
            let opts = pipeline::BackfillOptions {
                guild_id,
                channels,
                start,
                end,
                batch_size,
                max_messages,
                dry_run,
            };
            info!("Starting backfill subcommand");
            pipeline::backfill(cfg, opts).await
        }
        _ => {
            let cfg = cli_args.resolve()?;
            info!("Starting sbux pipeline");
            pipeline::run(cfg).await
        }
    }
}
