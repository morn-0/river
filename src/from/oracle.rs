use anyhow::Result;
use async_stream::{stream, AsyncStream};
use log::info;
use oracle::Connection;
use serde::{Deserialize, Serialize};
use std::{future::Future, vec};

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

    pub async fn reader(&self) -> Result<AsyncStream<Vec<String>, impl Future<Output = ()>>> {
        let stream = stream! {
            yield vec![];
        };
        Ok(stream)
    }
}

async fn get_columns_by_table(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let columns = conn.query_as::<String>(
        "SELECT COLUMN_NAME FROM all_tab_columns WHERE TABLE_NAME = :1 ORDER BY COLUMN_ID",
        &[&table],
    )?;

    let columns = columns.filter_map(|row| row.ok()).collect::<Vec<String>>();
    Ok(columns)
}

#[tokio::test]
async fn test() -> Result<()> {
    let conn = Connection::connect("river", "TY5Hyz", "127.0.0.1/XE")?;
    let columns = get_columns_by_table(&conn, "PERSON").await?;
    println!("{:#?}", columns);

    conn.execute("drop table person", &[])?;
    conn.execute(
        "create table person (id number(38), name varchar2(40))",
        &[],
    )?;
    conn.execute("insert into person values (:1, :2)", &[&1, &"John"])?;

    conn.commit()?;
    Ok(())
}
