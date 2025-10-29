// external
use sea_orm::entity::prelude::*;
use sea_orm::JsonValue;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "messages")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub guild_id: Option<i64>,
    pub channel_id: i64,
    pub author_id: i64,
    pub created_at_ms: i64,
    pub edited_at_ms: Option<i64>,
    pub content: Option<String>,
    pub tts: bool,
    pub pinned: bool,
    pub kind: Option<String>,
    pub flags: Option<i64>,
    pub mentions: Option<JsonValue>,
    pub attachments: Option<JsonValue>,
    pub embeds: Option<JsonValue>,
    pub components: Option<JsonValue>,
    pub reactions: Option<JsonValue>,
    pub message_reference: Option<JsonValue>,
    pub raw_json: JsonValue,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
