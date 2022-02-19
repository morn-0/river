use anyhow::Result;
use async_stream::stream;
use csv_async::{AsyncReader, AsyncReaderBuilder, Terminator, Trim};
use futures::{stream::FuturesUnordered, StreamExt};
use log::{error, info};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::{serde_as, skip_serializing_none, BytesOrString};
use simdutf8::basic::from_utf8;
use std::{path::PathBuf, sync::Arc};
use tokio::{fs::File, sync::Semaphore};
use tokio_stream::Stream;
use walkdir::{DirEntry, WalkDir};

#[serde_as]
#[skip_serializing_none]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Csv {
    path: Vec<String>,

    #[serde_as(as = "BytesOrString")]
    #[serde(default = "default_delimiter")]
    delimiter: Vec<u8>,
    #[serde(skip)]
    _delimiter: u8,

    #[serde_as(as = "BytesOrString")]
    #[serde(default = "default_quote")]
    quote: Vec<u8>,
    #[serde(skip)]
    _quote: u8,

    #[serde(default)]
    #[serde_as(as = "Option<BytesOrString>")]
    escape: Option<Vec<u8>>,
    #[serde(skip)]
    _escape: Option<u8>,

    #[serde(default)]
    #[serde_as(as = "Option<BytesOrString>")]
    comment: Option<Vec<u8>>,
    #[serde(skip)]
    _comment: Option<u8>,

    #[serde(skip)]
    trim: Trim,
    #[serde(skip)]
    terminator: Terminator,

    #[serde(default)]
    ascii: bool,
    #[serde(default)]
    flexible: bool,
    #[serde(default)]
    has_headers: bool,
    #[serde(default)]
    quoting: bool,
    #[serde(default)]
    double_quote: bool,

    #[serde(default = "default_parallel")]
    parallel: usize,
}

fn default_delimiter() -> Vec<u8> {
    vec![b',']
}

fn default_quote() -> Vec<u8> {
    vec![b'"']
}

fn default_parallel() -> usize {
    1
}

impl Csv {
    pub fn new(value: &Value) -> Self {
        let mut csv: Csv = serde_json::from_value(value.clone()).unwrap();

        for path in &csv.path {
            if !PathBuf::from(path).exists() {
                panic!("`path`.`{}` does not exist", path);
            }
        }

        let csv_clone = csv.clone();
        csv._delimiter = csv_clone.delimiter.get(0).map(|v| *v).unwrap();
        csv._quote = csv_clone.quote.get(0).map(|v| *v).unwrap();
        if let Some(escape) = csv_clone.escape {
            csv._escape = escape.get(0).map(|v| *v);
        }
        if let Some(comment) = csv_clone.comment {
            csv._comment = comment.get(0).map(|v| *v);
        }

        csv.trim = match value.get("trim") {
            Some(value) => match value.as_str() {
                Some(value) => match value {
                    "all" => Trim::All,
                    "fields" => Trim::Fields,
                    "headers" => Trim::Headers,
                    "none" => Trim::None,
                    _ => panic!("`trim` only supports 'all', 'fields', 'headers', 'none'"),
                },
                None => panic!("`trim` only supports 'all', 'fields', 'headers', 'none'"),
            },
            None => Trim::None,
        };

        csv.terminator = match value.get("terminator") {
            Some(value) => match value.as_str() {
                Some(value) => {
                    let chars = value.chars().collect::<Vec<char>>();
                    if chars.len() > 1 {
                        panic!("`terminator` supports only single characters")
                    } else {
                        Terminator::Any(*chars.iter().next().unwrap() as u8)
                    }
                }
                None => panic!("`terminator` supports only single characters"),
            },
            None => Terminator::CRLF,
        };

        info!("{:?}", csv);

        csv
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
        let (tx, rx) = flume::unbounded();

        let self_clone = self.clone();
        tokio::spawn(async move {
            let futures = FuturesUnordered::new();

            for path in paths {
                let semaphore = semaphore.clone();
                let tx = tx.clone();
                let self_clone = self_clone.clone();

                let join = tokio::spawn(async move {
                    let path_str = Arc::new(path.as_os_str().to_string_lossy().to_string());
                    info!("Start processing `{}`", path_str);

                    let file = match File::open(path).await {
                        Ok(file) => file,
                        Err(e) => {
                            error!("`{}` : {:#?}", path_str, e);
                            return;
                        }
                    };
                    let mut reader = reader(&self_clone, file).await;

                    loop {
                        let mut bytes = csv_async::ByteRecord::new();
                        if !match reader.read_byte_record(&mut bytes).await {
                            Ok(bool) => bool,
                            Err(e) => {
                                error!("`{}` : {:#?}", path_str, e);
                                continue;
                            }
                        } {
                            break;
                        }

                        let acquire = match semaphore.clone().acquire_owned().await {
                            Ok(acquire) => acquire,
                            Err(e) => panic!("`{}` : {:#?}", path_str, e),
                        };

                        let tx = tx.clone();
                        let path_str = path_str.clone();

                        tokio::spawn(async move {
                            let position = bytes.position();

                            let mut row = Vec::with_capacity(bytes.len());
                            bytes.iter().for_each(|byte| {
                                if let Ok(str) = from_utf8(byte) {
                                    row.push(str.to_string());
                                }
                            });

                            if row.len() == bytes.len() {
                                if let Err(e) = tx.send_async((row, acquire)).await {
                                    error!("`{}` : {:#?} , {:#?}", path_str, e, position);
                                }
                            } else {
                                error!("`{}` : FromUtf8Error({:#?})", path_str, position);
                            }
                        });
                    }
                });

                futures.push(join);
            }

            let _ = futures.into_future().await;
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

async fn reader(csv: &Csv, file: File) -> AsyncReader<File> {
    if csv.ascii {
        AsyncReaderBuilder::new()
            .delimiter(csv._delimiter)
            .quote(csv._quote)
            .escape(csv._escape)
            .comment(csv._comment)
            .trim(csv.trim)
            .terminator(csv.terminator)
            .flexible(csv.flexible)
            .has_headers(csv.has_headers)
            .quoting(csv.quoting)
            .double_quote(csv.double_quote)
            .buffer_capacity(1024 * 1024 * 16)
            .ascii()
            .create_reader(file)
    } else {
        AsyncReaderBuilder::new()
            .delimiter(csv._delimiter)
            .quote(csv._quote)
            .escape(csv._escape)
            .comment(csv._comment)
            .trim(csv.trim)
            .terminator(csv.terminator)
            .flexible(csv.flexible)
            .has_headers(csv.has_headers)
            .quoting(csv.quoting)
            .double_quote(csv.double_quote)
            .buffer_capacity(1024 * 1024 * 16)
            .create_reader(file)
    }
}

fn is_hidden(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.starts_with("."))
        .unwrap_or(false)
}
