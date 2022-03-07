use crate::from;
use anyhow::Result;
use async_stream::stream;
use bytes::Bytes;
use futures::stream::FuturesUnordered;
use log::info;
use mysql_async::{
    prelude::{LocalInfileHandler, Queryable},
    Conn, Opts, OptsBuilder, Pool,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_stream::StreamExt;
use tokio_util::io::StreamReader;

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

    pub async fn write(&self, from_key: &String, from_value: &Value) -> Result<()> {
        let opts = OptsBuilder::from_opts(Opts::from_url(&self.url)?);

        let opts = if "csv".eq(from_key) {
            let csv = from::csv::Csv::new(from_value);
            opts.local_infile_handler(Some(CsvHandler(csv)))
        } else if "json".eq(from_key) {
            let json = from::json::Json::new(from_value);
            opts.local_infile_handler(Some(JsonHandler(json)))
        } else if "mysql".eq(from_key) {
            let mysql = from::mysql::MySQL::new(from_value);
            opts.local_infile_handler(Some(MySQLHandler(mysql)))
        } else if "postgresql".eq(from_key) {
            let postgresql = from::postgresql::PostgreSQL::new(from_value);
            opts.local_infile_handler(Some(PostgreSQLHandler(postgresql)))
        } else if "oracle".eq(from_key) {
            let pool = Pool::new(opts);

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

            let oracle = from::oracle::Oracle::new(from_value);
            let mut reader = Box::pin(oracle.reader().await?);

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
            return Ok(());
        } else {
            panic!("No support for `{}`", from_key);
        };

        let pool = Pool::new(opts);

        let columns = match &self.columns {
            Some(columns) => columns.clone(),
            None => get_columns_by_table(&mut pool.get_conn().await?, &self.table).await?,
        };
        let columns_len = columns.len();

        let sql = {
            let mut sql = format!("LOAD DATA LOCAL INFILE 'tmp' INTO TABLE `{}` FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\r\n' (", self.table);

            for (index, column) in columns.iter().enumerate() {
                sql.push('`');
                sql.push_str(column);
                sql.push('`');

                if index < columns_len - 1 {
                    sql.push_str(", ");
                }
            }

            sql.push(')');

            sql
        };
        info!("sql : {:#?}", sql);

        pool.get_conn().await?.query_drop(sql).await?;

        Ok(())
    }
}

async fn get_columns_by_table(conn: &mut Conn, table: &str) -> Result<Vec<String>> {
    let columns: Vec<String> = conn.query(format!("SELECT `COLUMN_NAME` FROM information_schema.`COLUMNS` WHERE `TABLE_NAME` = '{}' AND `TABLE_SCHEMA` = (SELECT DATABASE()) ORDER BY `ORDINAL_POSITION`", table)).await?;
    Ok(columns)
}

struct CsvHandler(from::csv::Csv);

impl LocalInfileHandler for CsvHandler {
    fn handle(&self, _file_name: &[u8]) -> mysql_async::InfileHandlerFuture {
        let csv = self.0.clone();

        Box::pin(async move {
            let reader = csv.reader().unwrap();
            let mut reader = Box::pin(reader);

            let reader = stream! {
                while let Some(row) = reader.next().await {
                    let mut row_str = row.join(",");
                    row_str.push_str("\r\n");

                    yield std::io::Result::Ok(Bytes::from(row_str));
                }
            };

            Ok(reader)
                .map(StreamReader::new)
                .map(|x| Box::new(Box::pin(x)) as Box<_>)
                .map_err(|e: anyhow::Error| mysql_async::Error::Other(e.to_string().into()))
        })
    }
}

struct JsonHandler(from::json::Json);

impl LocalInfileHandler for JsonHandler {
    fn handle(&self, _file_name: &[u8]) -> mysql_async::InfileHandlerFuture {
        let json = self.0.clone();

        Box::pin(async move {
            let reader = json.reader().unwrap();
            let mut reader = Box::pin(reader);

            let reader = stream! {
                while let Some(row) = reader.next().await {
                    let mut row_str = row.join(",");
                    row_str.push_str("\r\n");

                    yield std::io::Result::Ok(Bytes::from(row_str));
                }
            };

            Ok(reader)
                .map(StreamReader::new)
                .map(|x| Box::new(Box::pin(x)) as Box<_>)
                .map_err(|e: anyhow::Error| mysql_async::Error::Other(e.to_string().into()))
        })
    }
}

struct MySQLHandler(from::mysql::MySQL);

impl LocalInfileHandler for MySQLHandler {
    fn handle(&self, _file_name: &[u8]) -> mysql_async::InfileHandlerFuture {
        let mysql = self.0.clone();

        Box::pin(async move {
            let reader = mysql.reader().await.unwrap();
            let mut reader = Box::pin(reader);

            let reader = stream! {
                while let Some(row) = reader.next().await {
                    let mut row_str = row.join(",");
                    row_str.push_str("\r\n");

                    yield std::io::Result::Ok(Bytes::from(row_str));
                }
            };

            Ok(reader)
                .map(StreamReader::new)
                .map(|x| Box::new(Box::pin(x)) as Box<_>)
                .map_err(|e: anyhow::Error| mysql_async::Error::Other(e.to_string().into()))
        })
    }
}

struct PostgreSQLHandler(from::postgresql::PostgreSQL);

impl LocalInfileHandler for PostgreSQLHandler {
    fn handle(&self, _file_name: &[u8]) -> mysql_async::InfileHandlerFuture {
        let postgresql = self.0.clone();

        Box::pin(async move {
            let reader = postgresql.reader().await.unwrap();
            let mut reader = Box::pin(reader);

            let reader = stream! {
                while let Some(row) = reader.next().await {
                    let mut row_str = row.join(",");
                    row_str.push_str("\r\n");

                    yield std::io::Result::Ok(Bytes::from(row_str));
                }
            };

            Ok(reader)
                .map(StreamReader::new)
                .map(|x| Box::new(Box::pin(x)) as Box<_>)
                .map_err(|e: anyhow::Error| mysql_async::Error::Other(e.to_string().into()))
        })
    }
}
