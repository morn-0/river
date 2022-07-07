use anyhow::Result;
use async_stream::stream;
use log::{error, info};
use mongodb::{bson::Document, options::FindOptions};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio_stream::{Stream, StreamExt};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MongoDB {
    url: String,
    collection: String,

    #[serde(default = "default_parallel")]
    parallel: usize,
    #[serde(default = "default_batch")]
    batch: usize,
}

fn default_batch() -> usize {
    10000
}

fn default_parallel() -> usize {
    1
}

impl MongoDB {
    pub fn new(value: &serde_json::Value) -> Self {
        let mongodb: MongoDB = serde_json::from_value(value.clone()).unwrap();

        info!("{:?}", mongodb);

        mongodb
    }

    pub async fn reader(&self) -> Result<impl Stream<Item = Vec<String>>> {
        let client = mongodb::Client::with_uri_str(&self.url).await?;
        let coll = match client.default_database() {
            Some(db) => db,
            None => panic!(""),
        }
        .collection::<Document>(&self.collection);

        let find_opt = FindOptions::builder()
            .batch_size(Some(self.batch as u32))
            .limit(None)
            .build();
        let mut cursor = coll.find(None, Some(find_opt)).await?;

        let semaphore = Arc::new(Semaphore::new(self.parallel));
        let (tx, rx) = crossbeam_channel::unbounded();

        tokio::spawn(async move {
            while let Some(row) = cursor.next().await {
                let acquire = match semaphore.clone().acquire_owned().await {
                    Ok(acquire) => acquire,
                    Err(e) => panic!("{:#?}", e),
                };
                let tx = tx.clone();

                tokio::spawn(async move {
                    let row = match row {
                        Ok(row) => row,
                        Err(e) => {
                            error!("{:#?}", e);
                            return;
                        }
                    };
                    let row = row.values().map(|v| v.to_string()).collect::<Vec<String>>();

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
