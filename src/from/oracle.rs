use anyhow::Result;
use async_stream::stream;
use log::{error, info};
use oracle::Connection;
use serde::{Deserialize, Serialize};
use simdutf8::basic::from_utf8;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio_stream::{Stream, StreamExt};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Oracle {
    url: String,
    table: String,
    columns: Option<Vec<String>>,

    #[serde(default = "default_parallel")]
    parallel: usize,
}

fn default_parallel() -> usize {
    1
}

impl Oracle {
    pub fn new(value: &serde_json::Value) -> Self {
        let oracle: Oracle = serde_json::from_value(value.clone()).unwrap();

        info!("{:?}", oracle);

        oracle
    }
}

#[tokio::test]
async fn test() -> Result<()> {
    let conn = Connection::connect("river", "TY5Hyz", "//10.0.2.2/XE")?;

    conn.execute(
        "create table person (id number(38), name varchar2(40))",
        &[],
    )?;
    conn.execute("insert into person values (:1, :2)", &[&1, &"John"])?;

    conn.commit()?;
    Ok(())
}
