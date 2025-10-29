// external
use anyhow::{Context, Result};
use serenity::http::{GuildPagination, Http};
use serenity::all::{ChannelType, Message, MessagePagination};
use serenity::model::id::{ChannelId, GuildId, MessageId};
use sea_orm::{Database, DatabaseConnection};
use serde_json::Value as JsonValue;
use tokio::time::{sleep, Duration};
use tracing::warn;

// project
use crate::db;

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
        let mut after: Option<GuildId> = None;
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
                let guild = self.http.get_guild(gid).await?;
                let features_val: JsonValue = serde_json::to_value(&guild.features).unwrap_or(JsonValue::Array(vec![]));
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

                self.sync_guild_channels(gid).await?;
            }
            after = page.last().map(|g| g.id);
        }
        Ok(())
    }

    async fn sync_guild_channels(&self, guild_id: GuildId) -> Result<()> {
        let channels = self.http.get_channels(guild_id).await?; // guild channels
        for ch in channels {
            if !is_message_bearing_channel(&ch.kind) { continue; }
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
            self.sync_channel_full(ch.id).await?;
        }
        Ok(())
    }

    async fn sync_channel_full(&self, channel_id: ChannelId) -> Result<()> {
        // Backfill older than current min
        loop {
            let min_id = db::min_message_id_in_channel(&self.db, channel_id.get() as i64)
                .await?
                .map(|v| MessageId::new(v as u64));
            let batch = if let Some(mid) = min_id {
                self.fetch_messages_before(channel_id, mid, 100).await?
            } else {
                // Channel not in DB yet: start from newest page
                self.fetch_messages_before(channel_id, MessageId::new(u64::MAX), 100)
                    .await?
            };
            if batch.is_empty() {
                break;
            }
            self.persist_messages(channel_id, &batch).await?;
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
            self.persist_messages(channel_id, &batch).await?;
            if batch.len() < 100 {
                break;
            }
        }

        // Interior gap sweep: walk downward once more and insert any missing messages
        let mut before = None;
        loop {
            let page = match before {
                Some(mid) => self.fetch_messages_before(channel_id, mid, 100).await?,
                None => self.fetch_messages_before(channel_id, MessageId::new(u64::MAX), 100).await?,
            };
            if page.is_empty() { break; }
            self.persist_messages(channel_id, &page).await?;
            before = page.last().map(|m| m.id);
            if page.len() < 100 { break; }
        }

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
            .get_messages(channel_id, Some(MessagePagination::Before(before)), Some(limit as u8))
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
            .get_messages(channel_id, Some(MessagePagination::After(after)), Some(limit as u8))
            .await
            .with_context(|| format!("fetch after failed for channel {}", channel_id))?;
        Ok(msgs)
    }

    async fn persist_messages(&self, channel_id: ChannelId, msgs: &[Message]) -> Result<()> {
        for m in msgs {
            // Avoid bot/webhook messages
            if m.author.bot || m.webhook_id.is_some() {
                continue;
            }

            // Upsert author (non-bot only)
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

            let created_ms = snowflake_ms(m.id);
            let edited_ms = m.edited_timestamp.map(|t| t.unix_timestamp() * 1000);

            let kind_str = format!("{:?}", m.kind);

            db::insert_message(
                &self.db,
                m.id.get() as i64,
                m.guild_id.map(|gid| gid.get() as i64),
                channel_id.get() as i64,
                m.author.id.get() as i64,
                created_ms,
                edited_ms,
                if m.content.is_empty() { None } else { Some(m.content.as_str()) },
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
        }
        Ok(())
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
    let discord_epoch = 1420070400000i64;
    let timestamp = ((id.get() >> 22) as i64) + discord_epoch;
    timestamp
}
