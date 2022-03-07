use anyhow::Result;
use async_stream::stream;
use futures::Stream;
use log::info;
use r2d2::PooledConnection;
use r2d2_oracle::OracleConnectionManager;
use serde::{Deserialize, Serialize};
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
    pub fn new(value: &serde_json::Value) -> Self {
        let oracle: Oracle = serde_json::from_value(value.clone()).unwrap();

        info!("{:?}", oracle);

        oracle
    }

    pub async fn reader(&self) -> Result<impl Stream<Item = Vec<String>>> {
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
        let batch = *&self.batch as u32;

        let columns = match &self.columns {
            Some(columns) => columns.clone(),
            None => get_columns_by_table(&pool.get()?, &self.table).await?,
        };
        let columns_len = columns.len();

        let sql = {
            let mut sql = String::from("SELECT ");
            for (index, column) in columns.iter().enumerate() {
                sql.push('"');
                sql.push_str(column);
                sql.push('"');
                if index < (columns_len - 1) {
                    sql.push_str(", ");
                }
            }
            sql.push_str(" FROM \"");
            sql.push_str(&self.table);
            sql.push('"');
            sql
        };
        info!("sql : {}", sql);

        let conn = pool.get()?;

        let stream = stream! {
            if let Ok(mut stmt) = conn.statement(&sql).fetch_array_size(batch).build() {
                if let Ok(rows) = stmt.query(&[]) {
                    for row in rows {
                        if let Ok(row) = row {
                            yield row
                                .sql_values()
                                .iter()
                                .filter_map(|value| value.get::<String>().ok())
                                .collect::<Vec<String>>();
                        }
                    }
                }
            }
        };

        Ok(stream)
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
