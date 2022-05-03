use anyhow::Result;
use async_stream::stream;
use log::{error, info};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{path::PathBuf, sync::Arc};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, BufReader},
    sync::Semaphore,
};
use tokio_stream::Stream;
use walkdir::{DirEntry, WalkDir};

static DELIMITER: u8 = b'\n';

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Json {
    path: Vec<String>,
    columns: Vec<String>,

    #[serde(default = "default_parallel")]
    parallel: usize,
}

fn default_parallel() -> usize {
    1
}

impl Json {
    pub fn new(value: &Value) -> Self {
        let json: Json = serde_json::from_value(value.clone()).unwrap();

        for path in &json.path {
            if !PathBuf::from(path).exists() {
                panic!("`path`.`{}` does not exist", path);
            }
        }

        info!("{:?}", json);

        json
    }

    pub fn reader(&self) -> Result<impl Stream<Item = Vec<String>>> {
        let mut paths = Vec::with_capacity(self.path.len());

        for path in &self.path {
            let path = PathBuf::from(path);

            if path.is_file() {
                paths.push(path);
            } else {
                let walker = WalkDir::new(path).into_iter();
                for entry in walker.filter_entry(|e| !is_hidden(e)) {
                    let entry = entry?;
                    if entry.metadata()?.is_file() {
                        paths.push(PathBuf::from(entry.path()));
                    }
                }
            }
        }

        let semaphore = Arc::new(Semaphore::new(self.parallel));
        let (tx, rx) = crossbeam_channel::unbounded();

        let columns = Arc::new(self.columns.clone());
        tokio::spawn(async move {
            for path in paths {
                let path_str = path.as_os_str().to_string_lossy().to_string();
                info!("Start processing `{}`", path_str);

                let file = match File::open(path).await {
                    Ok(file) => file,
                    Err(e) => {
                        error!("{:#?}", e);
                        continue;
                    }
                };
                let mut reader = BufReader::with_capacity(1024 * 1024 * 16, file);

                let mut buf = Vec::new();
                while let Ok(num) = reader.read_until(DELIMITER, &mut buf).await {
                    if num == 0 {
                        break;
                    }

                    let acquire = match semaphore.clone().acquire_owned().await {
                        Ok(acquire) => acquire,
                        Err(e) => panic!("{:#?}", e),
                    };

                    let columns = columns.clone();
                    let tx = tx.clone();

                    tokio::spawn(async move {
                        #[cfg(target_arch = "x86_64")]
                        let json = simd_json::from_slice::<Value>(buf.as_mut_slice());
                        #[cfg(target_arch = "aarch64")]
                        let json = serde_json::from_slice::<Value>(buf.as_mut_slice());
                        match json {
                            Ok(json) => {
                                let iter = columns.iter().map(|column| column.as_str());

                                let row = iter
                                    .filter_map(|column| {
                                        json.get(column).map(|value| value.to_string())
                                    })
                                    .collect::<Vec<String>>();

                                if let Err(e) = tx.send((row, acquire)) {
                                    error!("{:#?}", e);
                                }
                            }
                            Err(e) => {
                                error!("{:#?}", e);
                            }
                        }
                    });

                    buf = Vec::new();
                }
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

fn is_hidden(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.starts_with("."))
        .unwrap_or(false)
}
