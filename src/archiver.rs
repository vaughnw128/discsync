// external
use anyhow::{Context, Result};
use async_trait::async_trait;
use sea_orm::{Database, DatabaseConnection, Set};
use serde_json::Value as JsonValue;
use serenity::Client;
use serenity::all::{ChannelType, GatewayIntents, Message, MessagePagination};
use serenity::http::{GuildPagination, Http};
use serenity::model::id::{ChannelId, GuildId, MessageId};
use serenity::prelude::{Context as SerenityContext, EventHandler};
use tokio::time::{Duration, sleep};
use tracing::{info, warn};

// project
use crate::db;
use crate::entity; // for ActiveModel types

// Event handler for real-time gateway messages
struct GatewayHandler {
    db: DatabaseConnection,
}

#[async_trait]
impl EventHandler for GatewayHandler {
    async fn message(&self, _ctx: SerenityContext, m: Message) {
        // Skip bot/webhook messages
        if m.author.bot || m.webhook_id.is_some() {
            return;
        }
        if let Err(err) = self.persist_realtime_message(m).await {
            warn!(error=?err, "gateway message persist failed");
        }
    }
}

impl GatewayHandler {
    async fn persist_realtime_message(&self, m: Message) -> Result<()> {
        // Upsert user first
        let u = &m.author;
        let discr_str = u.discriminator.map(|d| d.get().to_string());
        let avatar_str = u.avatar.as_ref().map(|h| h.to_string());
        db::upsert_user(
            &self.db,
            u.id.get() as i64,
            Some(u.name.as_str()),
            u.global_name.as_deref(),
            discr_str.as_deref(),
            avatar_str.as_deref(),
            u.bot,
            &serde_json::to_value(u).unwrap_or(JsonValue::Null),
        )
        .await?;

        // Insert message (ignore duplicates at DB layer)
        let created_ms = snowflake_ms(m.id);
        let edited_ms = m.edited_timestamp.map(|t| t.unix_timestamp() * 1000);
        let kind_str = format!("{:?}", m.kind);
        let guild_id_i64 = m.guild_id.map(|g| g.get() as i64);

        db::insert_message(
            &self.db,
            m.id.get() as i64,
            guild_id_i64,
            m.channel_id.get() as i64,
            m.author.id.get() as i64,
            created_ms,
            edited_ms,
            if m.content.is_empty() {
                None
            } else {
                Some(m.content.as_str())
            },
            m.tts,
            m.pinned,
            &kind_str,
            m.flags.map(|f| f.bits() as i64),
            &serde_json::to_value(&m.mentions).unwrap_or(JsonValue::Array(vec![])),
            &serde_json::to_value(&m.attachments).unwrap_or(JsonValue::Array(vec![])),
            &serde_json::to_value(&m.embeds).unwrap_or(JsonValue::Array(vec![])),
            &serde_json::to_value(&m.components).unwrap_or(JsonValue::Array(vec![])),
            &serde_json::to_value(&m.reactions).unwrap_or(JsonValue::Array(vec![])),
            &serde_json::to_value(&m.message_reference).unwrap_or(JsonValue::Null),
            &serde_json::to_value(&m).unwrap_or(JsonValue::Null),
        )
        .await?;

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ArchiverConfig {
    pub discord_token: String,
    pub database_url: String,
    pub poll_interval_secs: u64,
}

pub struct Archiver {
    http: Http,
    db: DatabaseConnection,
    poll_interval: Duration,
}

impl Archiver {
    pub async fn new(cfg: ArchiverConfig) -> Result<Self> {
        let http = Http::new(&cfg.discord_token);
        let db = Database::connect(&cfg.database_url).await?;
        db::init_db(&db).await?;

        // Start a background gateway client for real-time message intake
        let intents = GatewayIntents::GUILDS
            | GatewayIntents::GUILD_MESSAGES
            | GatewayIntents::MESSAGE_CONTENT;
        let handler = GatewayHandler { db: db.clone() };
        let token = cfg.discord_token.clone();
        tokio::spawn(async move {
            match Client::builder(token, intents).event_handler(handler).await {
                Ok(mut client) => {
                    if let Err(err) = client.start_autosharded().await {
                        warn!(error=?err, "gateway client exited with error");
                    }
                }
                Err(err) => {
                    warn!(error=?err, "failed to construct gateway client");
                }
            }
        });

        Ok(Self {
            http,
            db,
            poll_interval: Duration::from_secs(cfg.poll_interval_secs),
        })
    }

    pub async fn run_forever(&self) -> Result<()> {
        loop {
            if let Err(err) = self.sync_all_guilds().await {
                warn!(error=?err, "sync pass failed");
            }
            sleep(self.poll_interval).await;
        }
    }

    async fn sync_all_guilds(&self) -> Result<()> {
        info!("sync pass started");
        let mut after: Option<GuildId> = None;
        let mut guilds_processed: usize = 0;
        loop {
            let page = self
                .http
                .get_guilds(after.map(GuildPagination::After), Some(200))
                .await?;
            if page.is_empty() {
                break;
            }
            for g in &page {
                let gid = g.id;
                info!(guild=?gid.get(), "syncing guild");

                // Guild isolate async pass
                if let Err(err) = async {
                    let guild = self.http.get_guild(gid).await?;
                    let features_val: JsonValue =
                        serde_json::to_value(&guild.features).unwrap_or(JsonValue::Array(vec![]));
                    let raw_guild: JsonValue = serde_json::to_value(&guild).unwrap_or(JsonValue::Null);
                    let icon_str = guild.icon.as_ref().map(|h| h.to_string());
                    db::upsert_guild(
                        &self.db,
                        gid.get() as i64,
                        Some(guild.name.as_str()),
                        Some(guild.owner_id.get() as i64),
                        icon_str.as_deref(),
                        &features_val,
                        &raw_guild,
                    )
                    .await?;

                    self.sync_guild_channels(gid).await
                }
                .await
                {
                    warn!(guild=?gid.get(), error=?err, "guild sync failed; continuing");
                    continue;
                }

                guilds_processed += 1;
            }
            after = page.last().map(|g| g.id);
        }
        info!(guilds = guilds_processed, "sync pass finished");

        Ok(())
    }

    async fn sync_guild_channels(&self, guild_id: GuildId) -> Result<()> {
        let channels = self.http.get_channels(guild_id).await?; // guild channels
        let total = channels.len();
        let message_bearing = channels
            .iter()
            .filter(|ch| is_message_bearing_channel(&ch.kind))
            .count();
        info!(guild=?guild_id.get(), channels=total, message_bearing, "syncing guild channels");
        for ch in channels {
            if !is_message_bearing_channel(&ch.kind) {
                continue;
            }

            // Channel sync isolate pass
            if let Err(err) = async {
                let kind_str = format!("{:?}", ch.kind);
                let pos = Some(ch.position as i32);
                db::upsert_channel(
                    &self.db,
                    ch.id.get() as i64,
                    Some(guild_id.get() as i64),
                    Some(ch.name.as_str()),
                    &kind_str,
                    ch.topic.as_deref(),
                    ch.nsfw,
                    ch.parent_id.map(|id| id.get() as i64),
                    pos,
                    &serde_json::to_value(&ch).unwrap_or(JsonValue::Null),
                )
                .await?;

                // Full sweep to ensure completeness
                self.sync_channel_full(guild_id, ch.id).await
            }
            .await
            {
                warn!(guild=?guild_id.get(), channel=?ch.id.get(), error=?err, "channel sync failed; continuing");
                continue;
            }
        }
        Ok(())
    }

    async fn sync_channel_full(&self, guild_id: GuildId, channel_id: ChannelId) -> Result<()> {
        let mut total_processed: usize = 0;
        let mut batches: usize = 0;
        // Backfill older than current min
        loop {
            let min_id = db::min_message_id_in_channel(&self.db, channel_id.get() as i64)
                .await?
                .map(|v| MessageId::new(v as u64));
            let batch = if let Some(mid) = min_id {
                self.fetch_messages_before(channel_id, mid, 100).await?
            } else {
                // Channel not in DB yet: start from newest page without a cursor
                self.fetch_messages_latest(channel_id, 100).await?
            };
            if batch.is_empty() {
                break;
            }
            let n = self.persist_messages(guild_id, channel_id, &batch).await?;
            total_processed += n;
            batches += 1;
            if batches % 10 == 0 {
                info!(guild=?guild_id.get(), channel=?channel_id.get(), batches, messages=total_processed, "channel sync progress");
            }
            if batch.len() < 100 {
                break;
            }
        }

        // Catch up newer than current max
        loop {
            let max_id = db::max_message_id_in_channel(&self.db, channel_id.get() as i64)
                .await?
                .map(|v| MessageId::new(v as u64));
            let Some(max_id) = max_id else { break };
            let batch = self.fetch_messages_after(channel_id, max_id, 100).await?;
            if batch.is_empty() {
                break;
            }
            let n = self.persist_messages(guild_id, channel_id, &batch).await?;
            total_processed += n;
            batches += 1;
            if batches % 10 == 0 {
                info!(guild=?guild_id.get(), channel=?channel_id.get(), batches, messages=total_processed, "channel sync progress");
            }
            if batch.len() < 100 {
                break;
            }
        }

        // Interior gap sweep: walk downward once more and insert any missing messages
        let mut before = None;
        loop {
            let page = match before {
                Some(mid) => self.fetch_messages_before(channel_id, mid, 100).await?,
                None => self.fetch_messages_latest(channel_id, 100).await?,
            };
            if page.is_empty() {
                break;
            }
            let n = self.persist_messages(guild_id, channel_id, &page).await?;
            total_processed += n;
            batches += 1;
            if batches % 10 == 0 {
                info!(guild=?guild_id.get(), channel=?channel_id.get(), batches, messages=total_processed, "channel sync progress");
            }
            before = page.last().map(|m| m.id);
            if page.len() < 100 {
                break;
            }
        }

        info!(guild=?guild_id.get(), channel=?channel_id.get(), batches, messages=total_processed, "channel sync complete");

        Ok(())
    }

    async fn fetch_messages_before(
        &self,
        channel_id: ChannelId,
        before: MessageId,
        limit: u16,
    ) -> Result<Vec<Message>> {
        let msgs = self
            .http
            .get_messages(
                channel_id,
                Some(MessagePagination::Before(before)),
                Some(limit as u8),
            )
            .await
            .with_context(|| format!("fetch before failed for channel {}", channel_id))?;
        Ok(msgs)
    }

    async fn fetch_messages_after(
        &self,
        channel_id: ChannelId,
        after: MessageId,
        limit: u16,
    ) -> Result<Vec<Message>> {
        let msgs = self
            .http
            .get_messages(
                channel_id,
                Some(MessagePagination::After(after)),
                Some(limit as u8),
            )
            .await
            .with_context(|| format!("fetch after failed for channel {}", channel_id))?;
        Ok(msgs)
    }

    async fn fetch_messages_latest(
        &self,
        channel_id: ChannelId,
        limit: u16,
    ) -> Result<Vec<Message>> {
        let msgs = self
            .http
            .get_messages(channel_id, None, Some(limit as u8))
            .await
            .with_context(|| format!("fetch latest failed for channel {}", channel_id))?;
        Ok(msgs)
    }

    async fn persist_messages(
        &self,
        guild_id: GuildId,
        channel_id: ChannelId,
        msgs: &[Message],
    ) -> Result<usize> {
        // Prepare batch models, skipping bot/webhook messages
        let mut users_map: std::collections::BTreeMap<i64, entity::user::ActiveModel> =
            std::collections::BTreeMap::new();
        let mut message_models: Vec<entity::message::ActiveModel> = Vec::with_capacity(msgs.len());

        for m in msgs {
            if m.author.bot || m.webhook_id.is_some() {
                continue;
            }

            // User upsert model (dedup by id)
            let u = &m.author;
            let discr_str = u.discriminator.map(|d| d.get().to_string());
            let avatar_str = u.avatar.as_ref().map(|h| h.to_string());
            let user_model = entity::user::ActiveModel {
                id: Set(u.id.get() as i64),
                username: Set(u.name.clone().into()),
                global_name: Set(u.global_name.clone()),
                discriminator: Set(discr_str),
                avatar: Set(avatar_str),
                bot: Set(u.bot),
                raw_json: Set(serde_json::to_value(u).unwrap_or(JsonValue::Null)),
            };
            users_map.insert(u.id.get() as i64, user_model);

            // Message insert model
            let created_ms = snowflake_ms(m.id);
            let edited_ms = m.edited_timestamp.map(|t| t.unix_timestamp() * 1000);
            let kind_str = format!("{:?}", m.kind);

            let msg_model = entity::message::ActiveModel {
                id: Set(m.id.get() as i64),
                guild_id: Set(Some(guild_id.get() as i64)),
                channel_id: Set(channel_id.get() as i64),
                author_id: Set(m.author.id.get() as i64),
                created_at_ms: Set(created_ms),
                edited_at_ms: Set(edited_ms),
                content: Set(if m.content.is_empty() {
                    None
                } else {
                    Some(m.content.clone())
                }),
                tts: Set(m.tts),
                pinned: Set(m.pinned),
                kind: Set(Some(kind_str)),
                flags: Set(m.flags.map(|f| f.bits() as i64)),
                mentions: Set(Some(
                    serde_json::to_value(&m.mentions).unwrap_or(JsonValue::Array(vec![])),
                )),
                attachments: Set(Some(
                    serde_json::to_value(&m.attachments).unwrap_or(JsonValue::Array(vec![])),
                )),
                embeds: Set(Some(
                    serde_json::to_value(&m.embeds).unwrap_or(JsonValue::Array(vec![])),
                )),
                components: Set(Some(
                    serde_json::to_value(&m.components).unwrap_or(JsonValue::Array(vec![])),
                )),
                reactions: Set(Some(
                    serde_json::to_value(&m.reactions).unwrap_or(JsonValue::Array(vec![])),
                )),
                message_reference: Set(Some(
                    serde_json::to_value(&m.message_reference).unwrap_or(JsonValue::Null),
                )),
                raw_json: Set(serde_json::to_value(m).unwrap_or(JsonValue::Null)),
            };
            message_models.push(msg_model);
        }

        // Execute batched writes
        let users_vec = users_map.into_values().collect::<Vec<_>>();
        db::upsert_users_batch(&self.db, users_vec).await?;
        db::insert_messages_batch(&self.db, message_models).await?;

        Ok(msgs
            .iter()
            .filter(|m| !m.author.bot && m.webhook_id.is_none())
            .count())
    }
}

fn is_message_bearing_channel(kind: &ChannelType) -> bool {
    matches!(
        kind,
        ChannelType::Text
            | ChannelType::News
            | ChannelType::PublicThread
            | ChannelType::PrivateThread
            | ChannelType::NewsThread
            | ChannelType::Forum
    )
}

fn snowflake_ms(id: MessageId) -> i64 {
    let discord_epoch = 1420070400000i64; // 2015-01-01T00:00:00.000Z

    ((id.get() >> 22) as i64) + discord_epoch
}
