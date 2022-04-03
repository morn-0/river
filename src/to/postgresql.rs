use anyhow::Result;
use bytes::Bytes;
use futures::SinkExt;
use log::{error, info};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::pin::Pin;
use tokio::pin;
use tokio_postgres::{binary_copy::BinaryCopyInWriter, types::Type, Client, NoTls};
use tokio_stream::{Stream, StreamExt};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PostgreSQL {
    url: String,
    table: String,
    columns: Option<Vec<String>>,

    #[serde(default)]
    fixnul: bool,
    #[serde(default)]
    fast_binary: bool,
}

impl PostgreSQL {
    pub fn new(value: &Value) -> Self {
        let postgresql: PostgreSQL = serde_json::from_value(value.clone()).unwrap();

        info!("{:?}", postgresql);

        postgresql
    }

    pub async fn write(&self, reader: Box<dyn Stream<Item = Vec<String>>>) -> Result<()> {
        let (client, connection) = tokio_postgres::connect(&self.url, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                panic!("{:#?}", e);
            }
        });

        let columns = match &self.columns {
            Some(columns) => columns.clone(),
            None => get_columns_by_table(&client, &self.table).await?,
        };
        let types = get_types_by_table(&client, &self.table).await?;
        let columns_len = columns.len();

        let sql = {
            let mut sql = format!("COPY \"{}\" (", self.table);

            for (index, column) in columns.iter().enumerate() {
                sql.push('"');
                sql.push_str(column);
                sql.push('"');

                if index < columns.len() - 1 {
                    sql.push_str(", ");
                }
            }

            sql.push_str(") FROM STDIN CSV DELIMITER ',' quote '\"' ESCAPE '\\'");
            sql
        };
        info!("sql : {}", sql);

        let sink = client.copy_in(&sql).await?;

        let mut reader = unsafe { Pin::new_unchecked(reader) };

        if self.fast_binary {
            let writer = BinaryCopyInWriter::new(sink, &types);
            pin!(writer);

            if self.fixnul {
                let ac = aho_corasick::AhoCorasick::new(&["\u{0000}"]);

                while let Some(mut row) = reader.next().await {
                    if row.len() != columns_len {
                        error!(
                            "UnequalError({:#?}) : expected {} values but got {}",
                            row,
                            columns_len,
                            row.len(),
                        );
                        continue;
                    }

                    let row = row
                        .iter_mut()
                        .map(|x| ac.replace_all(x, &[""]))
                        .collect::<Vec<String>>();

                    if let Err(e) = writer.as_mut().write_raw(row).await {
                        error!("{:#?}", e);
                    }
                }
            } else {
                while let Some(row) = reader.next().await {
                    if row.len() != columns_len {
                        error!(
                            "UnequalError({:#?}) : expected {} values but got {}",
                            row,
                            columns_len,
                            row.len(),
                        );
                        continue;
                    }

                    if let Err(e) = writer.as_mut().write_raw(row).await {
                        error!("{:#?}", e);
                    }
                }
            }

            if let Err(e) = writer.finish().await {
                error!("{:#?}", e);
            };
        } else {
            pin!(sink);

            if self.fixnul {
                let ac = aho_corasick::AhoCorasick::new(&["\u{0000}"]);

                while let Some(mut row) = reader.next().await {
                    if row.len() != columns_len {
                        error!(
                            "UnequalError({:#?}) : expected {} values but got {}",
                            row,
                            columns_len,
                            row.len(),
                        );
                        continue;
                    }

                    let row_str = row
                        .iter_mut()
                        .map(|x| ac.replace_all(x, &[""]))
                        .map(|field| format!("\"{}\"", field))
                        .collect::<Vec<String>>()
                        .join(",");

                    if let Err(e) = sink.send(Bytes::from(row_str)).await {
                        error!("{:#?}", e);
                    }
                }
            } else {
                while let Some(row) = reader.next().await {
                    if row.len() != columns_len {
                        error!(
                            "UnequalError({:#?}) : expected {} values but got {}",
                            row,
                            columns_len,
                            row.len(),
                        );
                        continue;
                    }

                    let row_str = row
                        .iter()
                        .map(|field| format!("\"{}\"", field))
                        .collect::<Vec<String>>()
                        .join(",");

                    if let Err(e) = sink.send(Bytes::from(row_str)).await {
                        error!("{:#?}", e);
                    }
                }
            }

            if let Err(e) = sink.finish().await {
                error!("{:#?}", e);
            };
        }

        Ok(())
    }
}

async fn get_columns_by_table(client: &Client, table: &str) -> Result<Vec<String>> {
    let rows = client.query(&format!("SELECT attname FROM pg_attribute, information_schema.columns WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = '{}')  AND table_name = '{}' AND column_name = attname AND attnum > 0 AND attisdropped = FALSE ORDER BY ordinal_position", table, table), &[]).await?;

    Ok(rows
        .iter()
        .map(|v| v.get::<usize, String>(0))
        .collect::<Vec<String>>())
}

async fn get_types_by_table(client: &Client, table: &str) -> Result<Vec<Type>> {
    let rows = client.query(&format!("SELECT atttypid FROM pg_attribute, information_schema.columns WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = '{}')  AND table_name = '{}' AND column_name = attname AND attnum > 0 AND attisdropped = FALSE ORDER BY ordinal_position", table, table), &[]).await?;

    Ok(rows
        .iter()
        .filter_map(|v| Type::from_oid(v.get::<usize, u32>(0)))
        .collect::<Vec<Type>>())
}
