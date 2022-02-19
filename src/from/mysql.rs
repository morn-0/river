use anyhow::Result;
use async_stream::stream;
use log::{error, info};
use mysql_async::{prelude::Queryable, Conn, Opts, Pool, Row, Value};
use serde::{Deserialize, Serialize};
use simdutf8::basic::from_utf8;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio_stream::{Stream, StreamExt};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MySQL {
    url: String,
    table: String,
    columns: Option<Vec<String>>,

    #[serde(default = "default_parallel")]
    parallel: usize,
}

fn default_parallel() -> usize {
    1
}

impl MySQL {
    pub fn new(value: &serde_json::Value) -> Self {
        let mysql: MySQL = serde_json::from_value(value.clone()).unwrap();

        info!("{:?}", mysql);

        mysql
    }

    pub async fn reader(&self) -> Result<impl Stream<Item = Vec<String>>> {
        let pool = Pool::new(Opts::from_url(&self.url)?);

        let columns = match &self.columns {
            Some(columns) => columns.clone(),
            None => get_columns_by_table(&mut pool.get_conn().await?, &self.table).await?,
        };
        let columns_len = columns.len();

        let sql = {
            let mut sql = String::from("SELECT ");
            for (index, column) in columns.iter().enumerate() {
                sql.push('`');
                sql.push_str(column);
                sql.push('`');
                if index < (columns_len - 1) {
                    sql.push_str(", ");
                }
            }
            sql.push_str(" FROM `");
            sql.push_str(&self.table);
            sql.push('`');
            sql
        };
        info!("sql : {}", sql);

        let semaphore = Arc::new(Semaphore::new(self.parallel));
        let (tx, rx) = flume::unbounded();

        let columns = Arc::new(columns);
        let mut conn = pool.get_conn().await?;
        tokio::spawn(async move {
            let mut reader = match conn.query_stream::<Row, String>(sql).await {
                Ok(reader) => reader,
                Err(e) => {
                    panic!("{:#?}", e)
                }
            };

            while let Some(row) = reader.next().await {
                let acquire = match semaphore.clone().acquire_owned().await {
                    Ok(acquire) => acquire,
                    Err(e) => panic!("{:#?}", e),
                };

                let columns = columns.clone();
                let tx = tx.clone();

                tokio::spawn(async move {
                    let row = match row {
                        Ok(row) => row,
                        Err(e) => {
                            error!("{:#?}", e);
                            return;
                        }
                    };

                    let row = columns
                        .iter()
                        .enumerate()
                        .map(|(index, _column)| {
                            as_sql(row.get::<Value, usize>(index).unwrap(), true)
                        })
                        .collect::<Vec<String>>();

                    if let Err(e) = tx.send_async((row, acquire)).await {
                        error!("{:#?}", e);
                    }
                });
            }
        });

        let stream = stream! {
            while let Ok((row, acquire)) = rx.recv_async().await {
                yield row;

                drop(acquire);
            }
        };

        Ok(stream)
    }
}

async fn get_columns_by_table(conn: &mut Conn, table: &str) -> Result<Vec<String>> {
    let columns: Vec<String> = conn.query(format!("SELECT `COLUMN_NAME` FROM information_schema.`COLUMNS` WHERE `TABLE_NAME` = '{}' AND `TABLE_SCHEMA` = (SELECT DATABASE()) ORDER BY `ORDINAL_POSITION`", table)).await?;
    Ok(columns)
}

#[inline(always)]
fn as_sql(value: Value, no_backslash_escape: bool) -> String {
    match value {
        Value::NULL => "NULL".into(),
        Value::Int(x) => format!("{}", x),
        Value::UInt(x) => format!("{}", x),
        Value::Float(x) => format!("{}", x),
        Value::Double(x) => format!("{}", x),
        Value::Date(y, m, d, 0, 0, 0, 0) => format!("{:04}-{:02}-{:02}", y, m, d),
        Value::Date(year, month, day, hour, minute, second, 0) => format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            year, month, day, hour, minute, second
        ),
        Value::Date(year, month, day, hour, minute, second, micros) => format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
            year, month, day, hour, minute, second, micros
        ),
        Value::Time(neg, d, h, i, s, 0) => {
            if neg {
                format!("-{:03}:{:02}:{:02}", d * 24 + u32::from(h), i, s)
            } else {
                format!("{:03}:{:02}:{:02}", d * 24 + u32::from(h), i, s)
            }
        }
        Value::Time(neg, days, hours, minutes, seconds, micros) => {
            if neg {
                format!(
                    "-{:03}:{:02}:{:02}.{:06}",
                    days * 24 + u32::from(hours),
                    minutes,
                    seconds,
                    micros
                )
            } else {
                format!(
                    "{:03}:{:02}:{:02}.{:06}",
                    days * 24 + u32::from(hours),
                    minutes,
                    seconds,
                    micros
                )
            }
        }
        Value::Bytes(ref bytes) => match from_utf8(&*bytes) {
            Ok(string) => escaped(string, no_backslash_escape),
            Err(_) => {
                let mut s = String::from("0x");
                for c in bytes.iter() {
                    s.extend(format!("{:02X}", *c).chars())
                }
                s
            }
        },
    }
}

#[inline(always)]
fn escaped(input: &str, no_backslash_escape: bool) -> String {
    let mut output = String::with_capacity(input.len());

    if no_backslash_escape {
        for c in input.chars() {
            if c == '\'' {
                output.push('\'');
                output.push('\'');
            } else {
                output.push(c);
            }
        }
    } else {
        for c in input.chars() {
            if c == '\x00' {
                output.push('\\');
                output.push('0');
            } else if c == '\n' {
                output.push('\\');
                output.push('n');
            } else if c == '\r' {
                output.push('\\');
                output.push('r');
            } else if c == '\\' || c == '\'' || c == '"' {
                output.push('\\');
                output.push(c);
            } else if c == '\x1a' {
                output.push('\\');
                output.push('Z');
            } else {
                output.push(c);
            }
        }
    }

    output
}
