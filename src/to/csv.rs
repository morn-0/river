use anyhow::Result;
use csv_async::{AsyncWriter, AsyncWriterBuilder, ByteRecord, QuoteStyle, Terminator};
use futures::StreamExt;
use log::info;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::{serde_as, skip_serializing_none, BytesOrString};
use std::{pin::Pin, sync::Arc};
use tokio::fs::File;
use tokio_stream::Stream;

#[serde_as]
#[skip_serializing_none]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Csv {
    path: String,

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
    _escape: u8,

    #[serde(skip)]
    quote_style: QuoteStyle,
    #[serde(skip)]
    terminator: Terminator,
    #[serde(default)]
    flexible: bool,
    #[serde(default)]
    has_headers: bool,
    #[serde(default)]
    double_quote: bool,
}

fn default_delimiter() -> Vec<u8> {
    vec![b',']
}

fn default_quote() -> Vec<u8> {
    vec![b'"']
}

impl Csv {
    pub fn new(value: &Value) -> Self {
        let mut csv: Csv = serde_json::from_value(value.clone()).unwrap();

        let csv_clone = csv.clone();
        csv._delimiter = csv_clone.delimiter.get(0).map(|v| *v).unwrap();
        csv._quote = csv_clone.quote.get(0).map(|v| *v).unwrap();
        if let Some(escape) = csv_clone.escape {
            csv._escape = escape.get(0).map(|v| *v).unwrap();
        }

        csv.quote_style = match value.get("quote_style") {
            Some(value) => match value.as_str() {
                Some(value) => match value {
                    "always" => QuoteStyle::Always,
                    "necessary" => QuoteStyle::Necessary,
                    "never" => QuoteStyle::Never,
                    "non_numeric" => QuoteStyle::NonNumeric,
                    _ => panic!(
                        "`quote_style` only supports 'always', 'necessary', 'never', 'non_numeric'"
                    ),
                },
                None => panic!(
                    "`quote_style` only supports 'always', 'necessary', 'never', 'non_numeric'"
                ),
            },
            None => QuoteStyle::Necessary,
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

    pub async fn write(&self, reader: Box<dyn Stream<Item = Vec<String>>>) -> Result<()> {
        let path_str = Arc::new(&self.path);
        info!("Start processing `{}`", path_str);

        let file = match File::create(&self.path).await {
            Ok(file) => file,
            Err(e) => {
                panic!("`{}` : {:#?}", path_str, e);
            }
        };
        let mut writer = writer(&self, file).await;
        let mut reader = unsafe { Pin::new_unchecked(reader) };

        while let Some(row) = reader.next().await {
            writer
                .write_byte_record(&ByteRecord::from_iter(row))
                .await?;
        }
        writer.flush().await?;

        Ok(())
    }
}

async fn writer(csv: &Csv, file: File) -> AsyncWriter<File> {
    AsyncWriterBuilder::new()
        .delimiter(csv._delimiter)
        .quote(csv._quote)
        .escape(csv._escape)
        .terminator(csv.terminator)
        .flexible(csv.flexible)
        .has_headers(csv.has_headers)
        .double_quote(csv.double_quote)
        .buffer_capacity(1024 * 1024 * 16)
        .quote_style(csv.quote_style)
        .create_writer(file)
}
