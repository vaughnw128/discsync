// built-in
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

// external
use anyhow::Result;
use chrono::{DateTime, Utc};
use qdrant_client::qdrant::{CreateCollectionBuilder, Distance, PointStruct, VectorParams};
use qdrant_client::Qdrant;
use serde_json::{Map as JsonMap, Value as JsonValue};
use sqlx::{sqlite::SqlitePoolOptions, Pool, Sqlite};
use tokio::select;
use tokio::sync::mpsc;
use tokio::time::{interval, Instant};
use tracing::{debug, info, warn, error};
use twilight_gateway::{Event, EventTypeFlags, Shard, ShardId, StreamExt as GwStreamExt};
use twilight_http::Client as HttpClient;
use twilight_model::channel::ChannelType;
use twilight_model::gateway::Intents;
use twilight_model::id::marker::{GuildMarker, MessageMarker};
use twilight_model::id::Id;

// project
use crate::db::init_db;
use crate::embedder::{new_embedder, Embedder, VECTOR_DIM};

const COLLECTION: &str = "discord_messages";

pub struct PipelineConfig {
    pub discord_token: String,
    pub database_url: String,
    pub qdrant_url: String,
    pub qdrant_api_key: Option<String>,
    pub guild_id: Option<u64>,
    pub model_dir: PathBuf,
    pub startup_catchup: bool,
    pub enable_periodic_check: bool,
}

pub struct BackfillOptions {
    pub guild_id: u64,
    pub channels: Option<Vec<u64>>, // if None, all text channels
    pub start: Option<String>,      // RFC3339
    pub end: Option<String>,        // RFC3339
    pub batch_size: usize,
    pub max_messages: Option<usize>,
    pub dry_run: bool,
}

// Convert RFC3339 to Discord snowflake lower/upper bounds
fn discord_snowflake_from_ts(dt: DateTime<Utc>) -> u64 {
    const DISCORD_EPOCH: i64 = 1420070400000; // ms
    let ts_ms = dt.timestamp_millis();
    let ms_since = (ts_ms - DISCORD_EPOCH).max(0) as u64;
    ms_since << 22
}

fn parse_time(s: &str) -> Result<DateTime<Utc>> {
    let dt = s.parse::<DateTime<Utc>>()?;
    Ok(dt)
}

pub async fn backfill(cfg: PipelineConfig, opts: BackfillOptions) -> Result<()> {
    info!(guild_id = opts.guild_id, "backfill starting");

    // Init DB, Qdrant, Embedder, HTTP
    let pool = SqlitePoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(30))
        .connect(&cfg.database_url)
        .await?;
    init_db(&pool).await?;

    let mut qdrant_builder = Qdrant::from_url(&cfg.qdrant_url);
    if let Some(ref key) = cfg.qdrant_api_key { qdrant_builder.api_key = Some(key.clone()); }
    let qdrant = qdrant_builder.build()?;
    ensure_qdrant_collection(&qdrant).await?;

    let embedder = new_embedder(&cfg.model_dir).await?;
    let http: Arc<HttpClient> = Arc::new(HttpClient::new(cfg.discord_token.clone()));

    // Resolve channel list
    let mut channel_ids: Vec<u64> = if let Some(list) = &opts.channels {
        list.clone()
    } else {
        http.guild_channels(Id::new(opts.guild_id)).await?.models().await?
            .into_iter()
            .filter(|c| c.kind == ChannelType::GuildText)
            .map(|c| c.id.get())
            .collect()
    };
    channel_ids.sort_unstable();

    // Time bounds
    let start_dt = match &opts.start { Some(s) => Some(parse_time(s)?), None => None };
    let end_dt = match &opts.end { Some(s) => Some(parse_time(s)?), None => None };
    let start_us = start_dt.map(|d| d.timestamp_micros());
    let end_us = end_dt.map(|d| d.timestamp_micros());
    let start_id = start_dt.map(discord_snowflake_from_ts);
    let end_id = end_dt.map(discord_snowflake_from_ts);
    let start_bound_id = start_id.map(Id::<MessageMarker>::new);

    let mut total_found = 0usize;
    let mut total_enqueued = 0usize;

    for cid in channel_ids {
        let channel_id = Id::<twilight_model::id::marker::ChannelMarker>::new(cid);
        info!(channel_id = %channel_id, "backfill: scanning");

        let mut before: Option<Id<MessageMarker>> = end_id.map(Id::new);
        let mut fetched_for_channel = 0usize;
        let mut messages_buf: VecDeque<twilight_model::channel::Message> = VecDeque::new();

        'outer: loop {
            // Build request using only `before` pagination
            let page = match before {
                Some(b) => http.channel_messages(channel_id).before(b).limit(100).await?.models().await?,
                None => {
                    // If we have an end bound, start from there; otherwise start from newest
                    if let Some(eid) = end_id {
                        before = Some(Id::new(eid));
                        continue;
                    }
                    http.channel_messages(channel_id).limit(100).await?.models().await?
                }
            };
            if page.is_empty() { break; }

            // Filter non-bot and non-empty, and within time window (if specified)
            let mut filtered = Vec::with_capacity(page.len());
            for m in page.iter() {
                total_found += 1;
                if m.author.bot { continue; }
                if m.content.trim().is_empty() { continue; }
                let msg_us = m.timestamp.as_micros() as i64;
                if let Some(sd) = start_us { if msg_us < sd { continue; } }
                if let Some(ed) = end_us { if msg_us >= ed { continue; } }
                filtered.push(m.clone());
            }

            // Oldest->newest order for stable batching
            filtered.sort_by_key(|m| m.id);

            for m in filtered {
                messages_buf.push_back(m);
                fetched_for_channel += 1;
                total_enqueued += 1;
                if let Some(max) = opts.max_messages { if fetched_for_channel >= max { break 'outer; } }
                if messages_buf.len() >= opts.batch_size {
                    if !opts.dry_run { flush(&pool, &qdrant, &embedder, &http, &mut messages_buf).await?; }
                    else { messages_buf.clear(); }
                }
            }

            // Determine next 'before' as the minimum id of this page to paginate older
            let min_id_in_page = page.iter().map(|m| m.id).min().unwrap();
            // If we've reached or passed the start bound, stop after processing this page
            if let Some(start_bound) = start_bound_id {
                if min_id_in_page <= start_bound { break; }
            }
            before = Some(min_id_in_page);
        }

        // Flush remaining
        if !messages_buf.is_empty() {
            if !opts.dry_run { flush(&pool, &qdrant, &embedder, &http, &mut messages_buf).await?; }
            else { messages_buf.clear(); }
        }
        info!(channel_id = %channel_id, found = fetched_for_channel, "backfill: done channel");
    }

    info!(total_found, total_enqueued, "backfill complete");
    Ok(())
}

pub async fn run(config: PipelineConfig) -> Result<()> {
    info!(
        database_url = %config.database_url,
        qdrant_url = %config.qdrant_url,
        guild_id = ?config.guild_id,
        model_dir = %config.model_dir.display(),
        "sbux starting with config"
    );

    let pool = SqlitePoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(30))
        .connect(&config.database_url)
        .await?;
    init_db(&pool).await?;

    // Prepare qdrant
    if config.qdrant_url.contains(":6333") {
        warn!("QDRANT_URL appears to point to REST port 6333; qdrant-client uses gRPC (port 6334). Consider using http://localhost:6334");
    }
    let mut qdrant_builder = Qdrant::from_url(&config.qdrant_url);
    if let Some(ref key) = config.qdrant_api_key {
        qdrant_builder.api_key = Some(key.clone());
    }
    let qdrant = qdrant_builder.build()?;
    ensure_qdrant_collection(&qdrant).await?;

    // Prepare embedder
    info!("Loading embedder and tokenizer");
    let embedder = new_embedder(&config.model_dir).await?;
    info!("Embedder ready");

    // Prepare Discord http client
    let http: Arc<HttpClient> = Arc::new(HttpClient::new(config.discord_token.clone()));

    // Channel for message ingestion -> batching/processing
    let (tx, mut rx) = mpsc::channel::<twilight_model::channel::Message>(1000);

    // Startup catch-up: if enabled and guild_id provided, enqueue since last checkpoint only
    if config.startup_catchup {
        if let Some(gid) = config.guild_id {
            info!(guild_id = gid, "startup catch-up running (since checkpoint only)");
            if let Err(e) = run_check_job(&http, &pool, Id::new(gid), &tx, true).await {
                warn!(error=?e, "startup catch-up failed");
            }
        } else {
            warn!("startup catch-up enabled but GUILD_ID is not set; skipping");
        }
    }

    // Spawn gateway consumer with reconnect loop
    let intents = Intents::GUILD_MESSAGES | Intents::MESSAGE_CONTENT;
    let token_for_shard = config.discord_token.clone();
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        let mut backoff = 1u64;
        loop {
            // Create a new shard each connect attempt
            let mut shard = Shard::new(ShardId::ONE, token_for_shard.clone(), intents);
            info!("gateway shard started");

            // Inner event loop for this shard session
            let should_reconnect;
            loop {
                match shard.next_event(EventTypeFlags::MESSAGE_CREATE).await {
                    Some(Ok(event)) => {
                        backoff = 1; // reset on success
                        if let Event::MessageCreate(mc) = event {
                            if !mc.author.bot {
                                debug!(message_id = %mc.id, channel_id = %mc.channel_id, "enqueue message");
                                if let Err(e) = tx_clone.send(mc.0).await {
                                    warn!(error=?e, "dropping message, queue closed");
                                }
                            }
                        }
                    }
                    Some(Err(err)) => {
                        warn!(error=?err, "Gateway error; will reconnect with backoff");
                        should_reconnect = true;
                        break; // exit inner loop to recreate shard
                    }
                    None => {
                        warn!("Gateway stream ended; reconnecting");
                        should_reconnect = true;
                        break; // exit inner loop to recreate shard
                    }
                }
            }

            if should_reconnect {
                tokio::time::sleep(Duration::from_secs(backoff.min(60))).await;
                backoff = (backoff * 2).min(60);
                continue; // recreate shard
            } else {
                break; // exit if no reconnect requested
            }
        }
    });

    // Optional periodic check job (bounded catch-up) using checkpoints
    if config.enable_periodic_check {
        if let Some(guild_id_raw) = config.guild_id {
            let http_check = http.clone();
            let pool_check = pool.clone();
            let tx_check = tx.clone();
            let guild_id = Id::<GuildMarker>::new(guild_id_raw);
            tokio::spawn(async move {
                let mut tick = interval(Duration::from_secs(3600));
                loop {
                    tick.tick().await;
                    info!(guild_id = %guild_id, "running periodic checkpoint catch-up job");
                    if let Err(e) = run_check_job(&http_check, &pool_check, guild_id, &tx_check, true).await {
                        warn!(error=?e, "periodic catch-up iteration failed");
                    }
                }
            });
        } else {
            warn!("ENABLE_PERIODIC_CHECK was set but GUILD_ID is not configured; skipping periodic check");
        }
    }

    // Batch and process messages
    let mut buffer: VecDeque<twilight_model::channel::Message> = VecDeque::with_capacity(100);
    let mut last_flush = Instant::now();
    let mut ticker = interval(Duration::from_secs(5));
    let http_for_process = http.clone();

    loop {
        select! {
            maybe_msg = rx.recv() => {
                if let Some(msg) = maybe_msg { buffer.push_back(msg); }
                if buffer.len() >= 100 {
                    info!(size = buffer.len(), "flushing batch due to size");
                    flush(&pool, &qdrant, &embedder, &http_for_process, &mut buffer).await?;
                    last_flush = Instant::now();
                }
            }
            _ = ticker.tick() => {
                if !buffer.is_empty() && last_flush.elapsed() >= Duration::from_secs(5) {
                    info!(size = buffer.len(), "flushing batch due to timer");
                    flush(&pool, &qdrant, &embedder, &http_for_process, &mut buffer).await?;
                    last_flush = Instant::now();
                }
            }
        }
    }
}

async fn ensure_qdrant_collection(client: &Qdrant) -> Result<()> {
    let collections = client.list_collections().await?;
    let exists = collections
        .collections
        .iter()
        .any(|c| c.name == COLLECTION);

    if !exists {
        let create = CreateCollectionBuilder::new(COLLECTION)
            .vectors_config(VectorParams {
                size: VECTOR_DIM as u64,
                distance: Distance::Cosine.into(),
                ..Default::default()
            });
        client.create_collection(create).await?;
        info!(collection=%COLLECTION, "created qdrant collection");
    }

    Ok(())
}

async fn flush(
    pool: &Pool<Sqlite>,
    qdrant: &Qdrant,
    embedder: &Embedder,
    _http: &Arc<HttpClient>,
    buffer: &mut VecDeque<twilight_model::channel::Message>,
) -> Result<()> {
    // Don't run if there is no buffer
    if buffer.is_empty() { return Ok(()); }

    let start_time = Instant::now();
    let mut batch: Vec<twilight_model::channel::Message> = Vec::with_capacity(buffer.len());
    while let Some(m) = buffer.pop_front() { batch.push(m); }

    // Filter messages with content
    batch.retain(|m| !m.content.trim().is_empty());
    if batch.is_empty() { return Ok(()); }

    // Dedup by id with SQLite PK
    let mut tx = pool.begin().await?;
    let mut inserted_rows: u64 = 0;
    for m in &batch {
        let res = sqlx::query(
            r#"INSERT OR IGNORE INTO messages (id, content, author_id, timestamp, embedded) VALUES (?, ?, ?, ?, 0)"#
        )
        .bind(m.id.to_string())
        .bind(m.content.as_str())
        .bind(m.author.id.to_string())
        .bind(m.timestamp.as_micros() as i64)
        .execute(&mut *tx)
        .await?;
        inserted_rows += res.rows_affected();
    }
    tx.commit().await?;

    // Build embeddings
    let t_embed_start = Instant::now();
    let texts: Vec<&str> = batch.iter().map(|m| m.content.as_str()).collect();
    let vectors = embedder.embed_batch(&texts).await?; // Vec<Vec<f32>>
    let embed_ms = t_embed_start.elapsed().as_millis();

    // Build points
    let mut points: Vec<PointStruct> = Vec::with_capacity(batch.len());
    for (i, m) in batch.iter().enumerate() {
        let ts = m.timestamp.as_micros() as i64;
        let mut payload = JsonMap::new();
        payload.insert("message_id".to_string(), JsonValue::String(m.id.to_string()));
        payload.insert("timestamp".to_string(), JsonValue::from(ts));
        payload.insert("author_id".to_string(), JsonValue::String(m.author.id.to_string()));

        // Use numeric point IDs
        let p = PointStruct::new(m.id.get(), vectors[i].clone(), payload);
        points.push(p);
    }

    let t_upsert_start = Instant::now();
    let upsert = qdrant_client::qdrant::UpsertPointsBuilder::new(COLLECTION, points).wait(true);
    if let Err(e) = qdrant.upsert_points(upsert).await {
        error!(error=?e, "qdrant upsert failed");
        return Err(e.into());
    }
    let upsert_ms = t_upsert_start.elapsed().as_millis();

    // Mark embedded and update checkpoints
    let mut tx2 = pool.begin().await?;
    for m in &batch {
        sqlx::query("UPDATE messages SET embedded = 1 WHERE id = ?")
            .bind(m.id.to_string())
            .execute(&mut *tx2)
            .await?;

        // Update channel checkpoint
        sqlx::query(
            r#"INSERT INTO channel_checkpoints(channel_id, last_message_id, last_timestamp)
                VALUES(?, ?, ?)
                ON CONFLICT(channel_id) DO UPDATE SET last_message_id=excluded.last_message_id, last_timestamp=excluded.last_timestamp"#
        )
        .bind(m.channel_id.to_string())
        .bind(m.id.to_string())
        .bind(m.timestamp.as_micros() as i64)
        .execute(&mut *tx2)
        .await?;
    }
    tx2.commit().await?;

    let total_ms = start_time.elapsed().as_millis();
    info!(
        count = batch.len(),
        inserted = inserted_rows,
        embed_ms = embed_ms as u64,
        upsert_ms = upsert_ms as u64,
        total_ms = total_ms as u64,
        "flushed batch to SQLite+Qdrant"
    );
    Ok(())
}

// Modify run_check_job to support "only_since_checkpoint": if no checkpoint, set it to newest and return
async fn run_check_job(
    http: &Arc<HttpClient>,
    pool: &Pool<Sqlite>,
    guild_id: Id<GuildMarker>,
    tx: &mpsc::Sender<twilight_model::channel::Message>,
    only_since_checkpoint: bool,
) -> Result<()> {
    let channels = http.guild_channels(guild_id).await?.models().await?;
    info!(guild_id = %guild_id, channels = channels.len(), "check job: scanning channels");
    for ch in channels {
        if ch.kind != ChannelType::GuildText { continue; }
        let channel_id = ch.id;

        let rec: Option<(String, i64)> = sqlx::query_as(
            "SELECT last_message_id, COALESCE(last_timestamp,0) FROM channel_checkpoints WHERE channel_id = ?"
        )
        .bind(channel_id.to_string())
        .fetch_optional(pool)
        .await?;

        let checkpoint_id_opt: Option<Id<MessageMarker>> = rec
            .as_ref()
            .and_then(|(id, _)| id.parse::<u64>().ok())
            .map(Id::new);

        let mut before: Option<Id<MessageMarker>> = None;
        loop {
            let page = if let Some(b) = before {
                http.channel_messages(channel_id).before(b).limit(100).await?.models().await?
            } else {
                http.channel_messages(channel_id).limit(100).await?.models().await?
            };
            if page.is_empty() { break; }

            let min_id_in_page = page.iter().map(|m| m.id).min().unwrap();
            let max_id_in_page = page.iter().map(|m| m.id).max().unwrap();

            let mut enqueued = 0usize;
            for m in page.iter().rev() {
                let newer_than_checkpoint = checkpoint_id_opt.map_or(true, |cp| m.id > cp);
                if newer_than_checkpoint && !m.author.bot {
                    if tx.send(m.clone()).await.is_ok() { enqueued += 1; }
                }
            }

            // Advance checkpoint to newest in this page
            if let Some(newest) = page.iter().map(|m| (m.id, m.timestamp.as_micros() as i64)).max_by_key(|(id, _)| *id) {
                let (newest_id, newest_ts) = newest;
                sqlx::query(
                    r#"INSERT INTO channel_checkpoints(channel_id, last_message_id, last_timestamp)
                        VALUES(?, ?, ?)
                        ON CONFLICT(channel_id) DO UPDATE SET last_message_id=excluded.last_message_id, last_timestamp=excluded.last_timestamp"#
                )
                .bind(channel_id.to_string())
                .bind(newest_id.to_string())
                .bind(newest_ts)
                .execute(pool)
                .await?;
            }

            if only_since_checkpoint {
                if let Some(cp) = checkpoint_id_opt {
                    if min_id_in_page <= cp { break; }
                }
            }

            before = Some(min_id_in_page);
            info!(channel_id = %channel_id, enqueued, page_min = %min_id_in_page, page_max = %max_id_in_page, "checkpoint catch-up: paged older");
        }
    }
    Ok(())
}
