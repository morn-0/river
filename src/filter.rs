use anyhow::Result;
use async_stream::stream;
use log::error;
use mlua::Lua;
use std::{fs::File, io::Read, pin::Pin, sync::Arc};
use tokio_stream::{Stream, StreamExt};

pub struct Filter {
    script: Vec<u8>,
}

impl Filter {
    pub fn new(path: &str) -> Self {
        let mut file = File::open(path).unwrap();

        let mut script = Vec::new();
        file.read_to_end(&mut script).unwrap();

        Filter { script }
    }

    pub fn filter(
        &self,
        reader: Box<dyn Stream<Item = Vec<String>>>,
    ) -> Result<Box<dyn Stream<Item = Vec<String>>>> {
        let lua = unsafe { Lua::unsafe_new() }.into_static();
        let func = Arc::new(match lua.load(&self.script).into_function() {
            Ok(func) => func,
            Err(_) => panic!("Failed to create function"),
        });

        let mut reader = unsafe { Pin::new_unchecked(reader) };

        let stream = stream! {
            while let Some(row) = reader.next().await {
                let row = match func.call_async::<Vec<String>, Vec<String>>(row).await {
                    Ok(row) => row,
                    Err(e) => {
                        error!("{:#?}", e);
                        return;
                    }
                };

                if row.len() > 0 {
                    yield row;
                }
            }
        };

        Ok(Box::new(stream))
    }
}
