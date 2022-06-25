mod csv;
mod elasticsearch;
mod mysql;
mod oracle;
mod postgresql;

use self::{csv::Csv, oracle::Oracle};
use anyhow::{Error, Result};
use serde_json::Value;
use tokio_stream::Stream;
use {self::elasticsearch::Elasticsearch, mysql::MySQL, postgresql::PostgreSQL};

pub async fn write(
    key: &String,
    value: &Value,
    reader: Box<dyn Stream<Item = Vec<String>>>,
) -> Result<()> {
    if "postgresql".eq(key) {
        return PostgreSQL::new(value).write(reader).await;
    } else if "elasticsearch".eq(key) {
        return Elasticsearch::new(value).write(reader).await;
    } else if "oracle".eq(key) {
        return Oracle::new(value).write(reader).await;
    } else if "mysql".eq(key) {
        return MySQL::new(value).write(reader).await;
    } else if "csv".eq(key) {
        return Csv::new(value).write(reader).await;
    }

    Err(Error::msg(format!("No support for `{}`", key)))
}
