use anyhow::Result;
use futures::stream::FuturesUnordered;
use log::info;
use mysql_async::{prelude::Queryable, Conn, Opts, Pool};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::pin::Pin;
use tokio_stream::{Stream, StreamExt};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MySQL {
    url: String,
    table: String,
    columns: Option<Vec<String>>,

    #[serde(default = "default_batch")]
    batch: usize,
}

fn default_batch() -> usize {
    10000
}

impl MySQL {
    pub fn new(value: &Value) -> Self {
        let mysql: MySQL = serde_json::from_value(value.clone()).unwrap();

        info!("{:?}", mysql);

        mysql
    }

    pub async fn write(&self, reader: Box<dyn Stream<Item = Vec<String>>>) -> Result<()> {
        let pool = Pool::new(Opts::from_url(&self.url).unwrap());

        let columns = match &self.columns {
            Some(columns) => columns.clone(),
            None => get_columns_by_table(&mut pool.get_conn().await?, &self.table).await?,
        };
        let columns_len = columns.len();

        let sql = {
            let mut pre = format!("INSERT INTO `{}` (", self.table);
            let mut suf = String::from("VALUES (");

            for (index, column) in columns.iter().enumerate() {
                pre.push('`');
                pre.push_str(column);
                pre.push('`');
                suf.push('?');

                if index < columns_len - 1 {
                    pre.push_str(", ");
                    suf.push_str(", ");
                }
            }

            pre.push_str(") ");
            suf.push(')');

            format!("{}{}", pre, suf)
        };
        info!("sql : {}", sql);

        let futures = FuturesUnordered::new();

        let mut reader = unsafe { Pin::new_unchecked(reader) };

        'out: loop {
            let mut rows = Vec::with_capacity(self.batch);

            loop {
                if let Some(row) = reader.next().await {
                    rows.push(row);

                    if rows.len() == self.batch {
                        let (mut conn, sql) = (pool.get_conn().await?, sql.clone());
                        futures.push(tokio::spawn(async move {
                            conn.exec_batch(sql, rows).await.unwrap();
                        }));

                        rows = Vec::with_capacity(self.batch);
                    }
                } else {
                    let (mut conn, sql) = (pool.get_conn().await?, sql.clone());
                    futures.push(tokio::spawn(async move {
                        conn.exec_batch(sql, rows).await.unwrap();
                    }));

                    break 'out;
                }
            }
        }

        {
            use futures::StreamExt;
            let _ = futures.into_future().await;
        }

        Ok(())
    }
}

async fn get_columns_by_table(conn: &mut Conn, table: &str) -> Result<Vec<String>> {
    let columns: Vec<String> = conn.query(format!("SELECT `COLUMN_NAME` FROM information_schema.`COLUMNS` WHERE `TABLE_NAME` = '{}' AND `TABLE_SCHEMA` = (SELECT DATABASE()) ORDER BY `ORDINAL_POSITION`", table)).await?;
    Ok(columns)
}
