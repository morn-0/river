pub(crate) mod csv;
pub(crate) mod json;
pub(crate) mod mysql;
pub(crate) mod oracle;
pub(crate) mod postgresql;

use anyhow::{Error, Result};
use serde_json::Value;
use tokio_stream::Stream;

use self::oracle::Oracle;
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
    } else if "oracle".eq(key) {
        let oracle = Oracle::new(value);
        return Ok(Box::new(oracle.reader().await?));
    }

    Err(Error::msg(format!("No support for `{}`", key)))
}
