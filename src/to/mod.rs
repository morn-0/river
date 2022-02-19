mod elasticsearch;
mod mysql;
mod postgresql;

use anyhow::{Error, Result};
use serde_json::Value;
use tokio_stream::Stream;
use {self::elasticsearch::Elasticsearch, mysql::MySQL, postgresql::PostgreSQL};

pub async fn write(
    key: &String,
    value: &Value,
    reader: Box<dyn Stream<Item = Vec<String>>>,
) -> Result<()> {
    if "mysql".eq(key) {
        return MySQL::new(value).write(reader).await;
    } else if "postgresql".eq(key) {
        return PostgreSQL::new(value).write(reader).await;
    } else if "elasticsearch".eq(key) {
        return Elasticsearch::new(value).write(reader).await;
    }

    Err(Error::msg(format!("No support for `{}`", key)))
}
