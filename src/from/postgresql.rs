use anyhow::Result;
use async_stream::{stream, AsyncStream};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use log::{error, info};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{future::Future, sync::Arc};
use tokio::{pin, sync::Semaphore};
use tokio_postgres::{
    binary_copy::{BinaryCopyOutRow, BinaryCopyOutStream},
    types::Type,
    Client, NoTls,
};
use tokio_stream::StreamExt;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PostgreSQL {
    url: String,
    table: String,
    columns: Option<Vec<String>>,

    #[serde(default = "default_parallel")]
    parallel: usize,
}

fn default_parallel() -> usize {
    1
}

impl PostgreSQL {
    pub fn new(value: &Value) -> Self {
        let postgresql: PostgreSQL = serde_json::from_value(value.clone()).unwrap();

        info!("{:?}", postgresql);

        postgresql
    }

    pub async fn reader(&self) -> Result<AsyncStream<Vec<String>, impl Future<Output = ()>>> {
        let (client, connection) = tokio_postgres::connect(&self.url, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("{:#?}", e);
            }
        });

        let columns = match &self.columns {
            Some(columns) => columns.clone(),
            None => get_columns_by_table(&client, &self.table).await?,
        };
        let types = get_types_by_table(&client, &self.table).await?;

        let sql = {
            let mut sql = format!("COPY \"{}\" (", self.table);
            for (index, column) in columns.iter().enumerate() {
                sql.push('"');
                sql.push_str(column);
                sql.push('"');
                if index < (columns.len() - 1) {
                    sql.push_str(", ");
                }
            }
            sql.push_str(") TO STDOUT BINARY");
            sql
        };
        info!("sql : {}", sql);

        let reader = {
            let stream = client.copy_out(&sql).await.unwrap();
            BinaryCopyOutStream::new(stream, &types)
        };

        let semaphore = Arc::new(Semaphore::new(self.parallel));
        let (tx, rx) = crossbeam_channel::unbounded();

        let types = Arc::new(types);
        tokio::spawn(async move {
            pin!(reader);

            while let Some(row) = reader.next().await {
                let acquire = match semaphore.clone().acquire_owned().await {
                    Ok(acquire) => acquire,
                    Err(e) => panic!("{:#?}", e),
                };

                let types = types.clone();
                let tx = tx.clone();

                tokio::spawn(async move {
                    let row = match row {
                        Ok(row) => row,
                        Err(e) => {
                            error!("{:#?}", e);
                            return;
                        }
                    };

                    let row = types
                        .iter()
                        .enumerate()
                        .map(|(index, r#type)| to_string(index, &row, r#type))
                        .collect::<Vec<String>>();

                    if let Err(e) = tx.send((row, acquire)) {
                        error!("{:#?}", e);
                    }
                });
            }
        });

        let stream = stream! {
            while let Ok((row, acquire)) = rx.recv() {
                yield row;

                drop(acquire);
            }
        };

        Ok(stream)
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

#[inline(always)]
fn to_string(index: usize, row: &BinaryCopyOutRow, r#type: &Type) -> String {
    match r#type {
        &Type::BOOL => row.get::<bool>(index).to_string(),
        &Type::CHAR => row.get::<i8>(index).to_string(),
        &Type::INT2 => row.get::<i16>(index).to_string(),
        &Type::INT4 => row.get::<i32>(index).to_string(),
        &Type::OID => row.get::<u32>(index).to_string(),
        &Type::TIMESTAMP => {
            let value = row.get::<NaiveDateTime>(index);
            format!("{}", value.format("%Y-%m-%d %H:%M:%S"))
        }
        &Type::TIMESTAMPTZ => {
            let value = row.get::<DateTime<Utc>>(index);
            format!("{}", value.format("%Y-%m-%d %H:%M:%S"))
        }
        &Type::DATE => {
            let value = row.get::<NaiveDate>(index);
            format!("{}", value.format("%Y-%m-%d"))
        }
        &Type::TIME => {
            let value = row.get::<NaiveTime>(index);
            format!("{}", value.format("%H:%M:%S"))
        }
        &Type::FLOAT4 => row.get::<f32>(index).to_string(),
        &Type::FLOAT8 => row.get::<f64>(index).to_string(),
        &Type::VARCHAR | &Type::TEXT | &Type::NAME => row.get::<String>(index),
        &Type::JSON | &Type::JSONB => row.get::<Value>(index).to_string(),
        &Type::UUID => {
            let uuid = row.get::<Uuid>(index);
            uuid.to_string()
        }
        &Type::BYTEA => {
            let bytes = row.get::<&[u8]>(index);
            let mut string = String::from("0x");
            bytes
                .iter()
                .for_each(|byte| string.extend(format!("{:02X}", *byte).chars()));
            string
        }
        _ => panic!("The `{}` type is not supported", r#type.name()),
    }
}
