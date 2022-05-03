use anyhow::Result;
use elasticsearch::{
    auth::Credentials,
    cert::CertificateValidation,
    http::{
        transport::{SingleNodeConnectionPool, TransportBuilder},
        Url,
    },
    BulkOperation, BulkParts,
};
use log::{error, info};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{pin::Pin, sync::Arc};
use tokio::sync::Semaphore;
use tokio_stream::{Stream, StreamExt};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Elasticsearch {
    url: String,
    index: String,
    r#type: String,
    columns: Vec<String>,

    #[serde(default = "default_batch")]
    batch: usize,
    #[serde(default = "default_parallel")]
    parallel: usize,
}

fn default_batch() -> usize {
    10000
}

fn default_parallel() -> usize {
    1
}

impl Elasticsearch {
    pub fn new(value: &Value) -> Self {
        let elasticsearch: Elasticsearch = serde_json::from_value(value.clone()).unwrap();

        info!("{:?}", elasticsearch);

        elasticsearch
    }

    pub async fn write(&self, reader: Box<dyn Stream<Item = Vec<String>>>) -> Result<()> {
        let url = Box::leak(self.url.clone().into_boxed_str());
        let index = Box::leak(self.index.clone().into_boxed_str());
        let r#type = Box::leak(self.r#type.clone().into_boxed_str());
        let columns = Arc::new(self.columns.clone());
        let batch = self.batch;

        let semaphore = Arc::new(Semaphore::new(self.parallel));
        let (tx, rx) = crossbeam_channel::unbounded();

        let recv_task = tokio::spawn(async move {
            let client = match client(url).await {
                Ok(client) => client,
                Err(e) => panic!("{:#?}", e),
            };
            let mut ops = Vec::with_capacity(batch);

            while let Ok((op, acquire)) = rx.recv() {
                ops.push(op);
                if ops.len() == batch {
                    bulk(&client, index, r#type, ops).await;
                    ops = Vec::with_capacity(batch);
                }

                drop(acquire);
            }

            bulk(&client, index, r#type, ops).await;
        });

        let mut reader = unsafe { Pin::new_unchecked(reader) };

        while let Some(mut row) = reader.next().await {
            let acquire = match semaphore.clone().acquire_owned().await {
                Ok(acquire) => acquire,
                Err(e) => panic!("{:#?}", e),
            };

            let columns = columns.clone();
            let tx = tx.clone();

            tokio::spawn(async move {
                let mut map = Map::with_capacity(row.len());

                columns.iter().enumerate().for_each(|(index, column)| {
                    let value = unsafe { row.get_unchecked_mut(index) };
                    #[cfg(target_arch = "x86_64")]
                    map.insert(column.to_string(), simd_json::from_str(value).unwrap());
                    #[cfg(target_arch = "aarch64")]
                    map.insert(column.to_string(), serde_json::from_str(value).unwrap());
                });

                let op = BulkOperation::create(Value::Object(map)).into();

                if let Err(e) = tx.send((op, acquire)) {
                    error!("{:#?}", e);
                }
            });
        }
        drop(tx);
        recv_task.await?;

        Ok(())
    }
}

async fn client(url: &str) -> Result<elasticsearch::Elasticsearch> {
    let mut url = Url::parse(url).unwrap();

    let credentials = if url.scheme() == "https" {
        let credentials = Some(Credentials::Basic(
            url.username().to_string(),
            url.password().unwrap().to_string(),
        ));

        url.set_username("").unwrap();
        url.set_password(None).unwrap();

        credentials
    } else {
        None
    };

    let pool = SingleNodeConnectionPool::new(url);
    let transport = match credentials {
        Some(credentials) => TransportBuilder::new(pool)
            .auth(credentials)
            .cert_validation(CertificateValidation::None)
            .build()?,
        None => TransportBuilder::new(pool).build()?,
    };

    Ok(elasticsearch::Elasticsearch::new(transport))
}

async fn bulk(
    client: &elasticsearch::Elasticsearch,
    index: &'static str,
    r#type: &'static str,
    ops: Vec<BulkOperation<Value>>,
) {
    match client
        .bulk(BulkParts::IndexType(index, r#type))
        .body(ops)
        .send()
        .await
    {
        Ok(response) => match response.json::<Value>().await {
            Ok(body) => match body.get("errors") {
                Some(errors) if errors.as_bool().is_some() => {
                    if errors.as_bool().unwrap() {
                        error!("{:#?}", body);
                    }
                }
                _ => error!("{:#?}", body),
            },
            Err(e) => error!("{:#?}", e),
        },
        Err(e) => error!("{:#?}", e),
    }
}
