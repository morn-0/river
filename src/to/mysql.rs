use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt;
use log::info;
use mysql_async::{
    prelude::{Query, Queryable},
    Conn, Opts, Pool,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::UnboundedReceiverStream, Stream};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MySQL {
    url: String,
    table: String,
    columns: Option<Vec<String>>,
}

impl MySQL {
    pub fn new(value: &Value) -> Self {
        let mysql: MySQL = serde_json::from_value(value.clone()).unwrap();

        info!("{:?}", mysql);

        mysql
    }

    pub async fn write(&self, reader: Box<dyn Stream<Item = Vec<String>>>) -> Result<()> {
        let pool = Pool::new(Opts::from_url(&self.url)?);

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

        let (sender, recv) = mpsc::unbounded_channel();

        let mut conn = pool.get_conn().await?;
        conn.set_infile_handler(
            async move { Ok(UnboundedReceiverStream::new(recv).map(Ok).boxed()) },
        );

        let conn_task = tokio::spawn(async move {
            let _ = sql.ignore(&mut conn).await;
        });

        let mut reader = unsafe { Pin::new_unchecked(reader) };
        while let Some(row) = reader.next().await {
            let mut row_str = row
                .iter()
                .map(|field| format!("\"{}\"", field))
                .collect::<Vec<String>>()
                .join(",");
            row_str.push_str("\r\n");

            sender.send(Bytes::from(row_str))?;
        }

        drop(sender);
        conn_task.await?;

        Ok(())
    }
}

async fn get_columns_by_table(conn: &mut Conn, table: &str) -> Result<Vec<String>> {
    let columns: Vec<String> = conn.query(format!("SELECT `COLUMN_NAME` FROM information_schema.`COLUMNS` WHERE `TABLE_NAME` = '{}' AND `TABLE_SCHEMA` = (SELECT DATABASE()) ORDER BY `ORDINAL_POSITION`", table)).await?;
    Ok(columns)
}
