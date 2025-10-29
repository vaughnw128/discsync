// external
use anyhow::Result;
use sea_orm::sea_query::{Alias, Index, OnConflict};
use sea_orm::JsonValue;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder, Schema, Set};
use sea_orm::{ConnectionTrait, DatabaseConnection};

// project
use crate::entity::{
    self, Channel as ChannelEntity, Guild as GuildEntity, Message as MessageEntity,
    User as UserEntity,
};

pub async fn init_db(db: &DatabaseConnection) -> Result<()> {
    let backend = db.get_database_backend();
    let schema = Schema::new(backend);

    // Create tables from entity definitions (portable across backends)
    let mut stmt = schema.create_table_from_entity(GuildEntity);
    stmt.if_not_exists();
    db.execute(backend.build(&stmt)).await?;

    let mut stmt = schema.create_table_from_entity(ChannelEntity);
    stmt.if_not_exists();
    db.execute(backend.build(&stmt)).await?;

    let mut stmt = schema.create_table_from_entity(UserEntity);
    stmt.if_not_exists();
    db.execute(backend.build(&stmt)).await?;

    let mut stmt = schema.create_table_from_entity(MessageEntity);
    stmt.if_not_exists();
    db.execute(backend.build(&stmt)).await?;

    // Indexes
    let idx = Index::create()
        .if_not_exists()
        .name("idx_messages_channel_id_created")
        .table(Alias::new("messages"))
        .col(Alias::new("channel_id"))
        .col(Alias::new("created_at_ms"))
        .to_owned();
    db.execute(backend.build(&idx)).await?;

    let idx = Index::create()
        .if_not_exists()
        .name("idx_messages_channel_id_id")
        .table(Alias::new("messages"))
        .col(Alias::new("channel_id"))
        .col(Alias::new("id"))
        .to_owned();
    db.execute(backend.build(&idx)).await?;

    let idx = Index::create()
        .if_not_exists()
        .name("idx_messages_author_id")
        .table(Alias::new("messages"))
        .col(Alias::new("author_id"))
        .to_owned();
    db.execute(backend.build(&idx)).await?;

    Ok(())
}

pub async fn upsert_guild(
    db: &DatabaseConnection,
    id: i64,
    name: Option<&str>,
    owner_id: Option<i64>,
    icon: Option<&str>,
    features_json: &JsonValue,
    raw: &JsonValue,
) -> Result<()> {
    let am = entity::guild::ActiveModel {
        id: Set(id),
        name: Set(name.map(|s| s.to_owned())),
        owner_id: Set(owner_id),
        icon: Set(icon.map(|s| s.to_owned())),
        features: Set(Some(features_json.clone())),
        raw_json: Set(raw.clone()),
    };

    GuildEntity::insert(am)
        .on_conflict(
            OnConflict::column(entity::guild::Column::Id)
                .update_columns([
                    entity::guild::Column::Name,
                    entity::guild::Column::OwnerId,
                    entity::guild::Column::Icon,
                    entity::guild::Column::Features,
                    entity::guild::Column::RawJson,
                ])
                .to_owned(),
        )
        .exec(db)
        .await?;
    Ok(())
}

pub async fn upsert_channel(
    db: &DatabaseConnection,
    id: i64,
    guild_id: Option<i64>,
    name: Option<&str>,
    kind: &str,
    topic: Option<&str>,
    nsfw: bool,
    parent_id: Option<i64>,
    position: Option<i32>,
    raw: &JsonValue,
) -> Result<()> {
    let am = entity::channel::ActiveModel {
        id: Set(id),
        guild_id: Set(guild_id),
        name: Set(name.map(|s| s.to_owned())),
        kind: Set(Some(kind.to_owned())),
        topic: Set(topic.map(|s| s.to_owned())),
        nsfw: Set(nsfw),
        parent_id: Set(parent_id),
        position: Set(position),
        raw_json: Set(raw.clone()),
    };

    ChannelEntity::insert(am)
        .on_conflict(
            OnConflict::column(entity::channel::Column::Id)
                .update_columns([
                    entity::channel::Column::GuildId,
                    entity::channel::Column::Name,
                    entity::channel::Column::Kind,
                    entity::channel::Column::Topic,
                    entity::channel::Column::Nsfw,
                    entity::channel::Column::ParentId,
                    entity::channel::Column::Position,
                    entity::channel::Column::RawJson,
                ])
                .to_owned(),
        )
        .exec(db)
        .await?;
    Ok(())
}

pub async fn upsert_user(
    db: &DatabaseConnection,
    id: i64,
    username: Option<&str>,
    global_name: Option<&str>,
    discriminator: Option<&str>,
    avatar: Option<&str>,
    bot: bool,
    raw: &JsonValue,
) -> Result<()> {
    let am = entity::user::ActiveModel {
        id: Set(id),
        username: Set(username.map(|s| s.to_owned())),
        global_name: Set(global_name.map(|s| s.to_owned())),
        discriminator: Set(discriminator.map(|s| s.to_owned())),
        avatar: Set(avatar.map(|s| s.to_owned())),
        bot: Set(bot),
        raw_json: Set(raw.clone()),
    };

    UserEntity::insert(am)
        .on_conflict(
            OnConflict::column(entity::user::Column::Id)
                .update_columns([
                    entity::user::Column::Username,
                    entity::user::Column::GlobalName,
                    entity::user::Column::Discriminator,
                    entity::user::Column::Avatar,
                    entity::user::Column::Bot,
                    entity::user::Column::RawJson,
                ])
                .to_owned(),
        )
        .exec(db)
        .await?;
    Ok(())
}

pub async fn insert_message(
    db: &DatabaseConnection,
    id: i64,
    guild_id: Option<i64>,
    channel_id: i64,
    author_id: i64,
    created_at_ms: i64,
    edited_at_ms: Option<i64>,
    content: Option<&str>,
    tts: bool,
    pinned: bool,
    kind: &str,
    flags: Option<i64>,
    mentions_json: &JsonValue,
    attachments_json: &JsonValue,
    embeds_json: &JsonValue,
    components_json: &JsonValue,
    reactions_json: &JsonValue,
    reference_json: &JsonValue,
    raw: &JsonValue,
) -> Result<()> {
    let am = entity::message::ActiveModel {
        id: Set(id),
        guild_id: Set(guild_id),
        channel_id: Set(channel_id),
        author_id: Set(author_id),
        created_at_ms: Set(created_at_ms),
        edited_at_ms: Set(edited_at_ms),
        content: Set(content.map(|s| s.to_owned())),
        tts: Set(tts),
        pinned: Set(pinned),
        kind: Set(Some(kind.to_owned())),
        flags: Set(flags),
        mentions: Set(Some(mentions_json.clone())),
        attachments: Set(Some(attachments_json.clone())),
        embeds: Set(Some(embeds_json.clone())),
        components: Set(Some(components_json.clone())),
        reactions: Set(Some(reactions_json.clone())),
        message_reference: Set(Some(reference_json.clone())),
        raw_json: Set(raw.clone()),
    };

    MessageEntity::insert(am)
        .on_conflict(
            OnConflict::column(entity::message::Column::Id)
                .do_nothing()
                .to_owned(),
        )
        .exec(db)
        .await?;
    Ok(())
}

pub async fn max_message_id_in_channel(
    db: &DatabaseConnection,
    channel_id: i64,
) -> Result<Option<i64>> {
    let model = MessageEntity::find()
        .filter(entity::message::Column::ChannelId.eq(channel_id))
        .order_by_desc(entity::message::Column::Id)
        .one(db)
        .await?;
    Ok(model.map(|m| m.id))
}

pub async fn min_message_id_in_channel(
    db: &DatabaseConnection,
    channel_id: i64,
) -> Result<Option<i64>> {
    let model = MessageEntity::find()
        .filter(entity::message::Column::ChannelId.eq(channel_id))
        .order_by_asc(entity::message::Column::Id)
        .one(db)
        .await?;
    Ok(model.map(|m| m.id))
}
