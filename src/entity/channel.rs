// external
use sea_orm::entity::prelude::*;
use sea_orm::JsonValue;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "channels")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub guild_id: Option<i64>,
    pub name: Option<String>,
    pub kind: Option<String>,
    pub topic: Option<String>,
    pub nsfw: bool,
    pub parent_id: Option<i64>,
    pub position: Option<i32>,
    pub raw_json: JsonValue,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
