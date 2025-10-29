// external
use sea_orm::entity::prelude::*;
use sea_orm::JsonValue;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "guilds")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub name: Option<String>,
    pub owner_id: Option<i64>,
    pub icon: Option<String>,
    pub features: Option<JsonValue>,
    pub raw_json: JsonValue,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
