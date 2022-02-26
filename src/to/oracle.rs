use anyhow::Result;
use log::info;
use r2d2::PooledConnection;
use r2d2_oracle::{oracle::sql_type::ToSql, OracleConnectionManager};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::pin::Pin;
use tokio_stream::{Stream, StreamExt};
use url::Url;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Oracle {
    url: String,
    table: String,
    columns: Option<Vec<String>>,

    #[serde(default = "default_batch")]
    batch: usize,
}

fn default_batch() -> usize {
    10000
}

impl Oracle {
    pub fn new(value: &Value) -> Self {
        let oracle: Oracle = serde_json::from_value(value.clone()).unwrap();

        match Url::parse(&oracle.url) {
            Ok(url) => {
                if !"oracle".eq(url.scheme()) {
                    panic!("Wrong oracle connection address");
                }
            }
            Err(e) => panic!("{:#?}", e),
        }

        info!("{:?}", oracle);

        oracle
    }

    pub async fn write(&self, reader: Box<dyn Stream<Item = Vec<String>>>) -> Result<()> {
        let url = Url::parse(&self.url)?;
        let manager = if let Some(host) = url.host_str() {
            if let Some(password) = url.password() {
                let username = url.username();
                let host = format!("{}{}", host, url.path());

                OracleConnectionManager::new(username, password, &host)
            } else {
                panic!("The `password` in the `url` cannot be empty");
            }
        } else {
            panic!("`url` is wrong");
        };
        let pool = r2d2::Pool::builder()
            .min_idle(Some(1))
            .max_size(15)
            .build(manager)?;

        let columns = match &self.columns {
            Some(columns) => columns.clone(),
            None => get_columns_by_table(&pool.get()?, &self.table).await?,
        };
        let columns_len = columns.len();

        let sql = {
            let mut sql = format!("INSERT INTO \"{}\" VALUES (", self.table);

            for (index, column) in columns.iter().enumerate() {
                sql.push(':');
                sql.push_str(column);

                if index < columns_len - 1 {
                    sql.push_str(", ");
                }
            }

            sql.push_str(")");
            sql
        };
        info!("sql : {}", sql);

        let mut reader = unsafe { Pin::new_unchecked(reader) };

        'out: loop {
            let mut conn = pool.get()?;
            conn.set_autocommit(true);

            let mut batch = conn.batch(&sql, self.batch).build()?;
            let mut batch_size = 0;

            loop {
                if let Some(row) = reader.next().await {
                    let row = row
                        .iter()
                        .map(|field| field as &dyn ToSql)
                        .collect::<Vec<&dyn ToSql>>();
                    if let Ok(()) = batch.append_row(&row) {
                        batch_size = batch_size + 1;
                    }

                    if batch_size == self.batch {
                        batch.execute()?;
                        break;
                    }
                } else {
                    batch.execute()?;
                    break 'out;
                }
            }
        }

        Ok(())
    }
}

async fn get_columns_by_table(
    conn: &PooledConnection<OracleConnectionManager>,
    table: &str,
) -> Result<Vec<String>> {
    let columns = conn.query_as::<String>(
        "SELECT COLUMN_NAME FROM all_tab_columns WHERE TABLE_NAME = :1 ORDER BY COLUMN_ID",
        &[&table],
    )?;

    let columns = columns.filter_map(|row| row.ok()).collect::<Vec<String>>();
    Ok(columns)
}
