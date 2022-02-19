mod csv;
mod json;
mod mysql;
mod postgresql;

use anyhow::{Error, Result};
use serde_json::Value;
use tokio_stream::Stream;
use {csv::Csv, json::Json, mysql::MySQL, postgresql::PostgreSQL};

pub async fn get(key: &String, value: &Value) -> Result<Box<dyn Stream<Item = Vec<String>>>> {
    if "csv".eq(key) {
        let csv = Csv::new(value);
        return Ok(Box::new(csv.reader()?));
    } else if "json".eq(key) {
        let json = Json::new(value);
        return Ok(Box::new(json.reader()?));
    } else if "mysql".eq(key) {
        let mysql = MySQL::new(value);
        return Ok(Box::new(mysql.reader().await?));
    } else if "postgresql".eq(key) {
        let postgresql = PostgreSQL::new(value);
        return Ok(Box::new(postgresql.reader().await?));
    }

    Err(Error::msg(format!("No support for `{}`", key)))
}
