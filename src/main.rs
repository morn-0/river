mod filter;
mod from;
mod to;

use anyhow::Result;
use clap::Parser;
use env_logger::{Builder, Env};
use filter::Filter;
#[cfg(target_os = "unix")]
use mimalloc::MiMalloc;
use serde_json::Value;
#[cfg(target_os = "windows")]
use snmalloc_rs::SnMalloc;
use std::{fs::File, io::Read, path::PathBuf, thread};
use tokio::runtime;

#[cfg(target_os = "windows")]
#[global_allocator]
static ALLOC: SnMalloc = SnMalloc;

#[cfg(target_os = "unix")]
#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
#[clap(
    version = "0.0.1",
    author = "morning",
    about = "High Performance Data Migration Tools."
)]
struct Args {
    #[clap(required = true, value_name = "FILE", parse(from_os_str))]
    config: PathBuf,
}

fn main() -> Result<()> {
    let mut builder = Builder::new();
    builder.parse_env(Env::new().filter("LOG")).init();

    let args = Args::parse();

    let buf = {
        let mut buf = Vec::new();
        let mut config = File::open(&args.config)?;
        config.read_to_end(&mut buf)?;
        buf
    };
    let value: Value = toml::from_slice::<Value>(buf.as_slice())?;

    let tasks = match value.as_object() {
        Some(array) => array,
        None => panic!("This configuration is empty"),
    };
    let mut joins = Vec::with_capacity(tasks.len());

    for task in tasks {
        let task = (task.0.clone(), task.1.clone());

        let join = thread::spawn(move || {
            let thread = match task.1.get("thread") {
                Some(value) => match value.as_u64() {
                    Some(value) => value as usize,
                    None => panic!("`{}`.`thread` supports only one unsigned integer", task.0),
                },
                None => 1,
            };
            let runtime = runtime::Builder::new_multi_thread()
                .worker_threads(thread)
                .enable_all()
                .build();

            let runtime = match runtime {
                Ok(runtime) => runtime,
                Err(err) => panic!("{:#?}", err),
            };

            runtime.block_on(async {
                let froms = match task.1.get("from") {
                    Some(value) => value.as_object().unwrap(),
                    None => panic!("`{}`.`from` is empty", task.0),
                };
                let tos = match task.1.get("to") {
                    Some(value) => value.as_object().unwrap(),
                    None => panic!("`{}`.`to` is empty", task.0),
                };
                if froms.len() != 1 || tos.len() != 1 {
                    panic!("Only one from and one to are supported in a single task");
                }

                let filter = match task.1.get("filter") {
                    Some(value) => value.as_str(),
                    None => None,
                };

                let from = {
                    let from = froms.iter().next().unwrap();
                    from::get(from.0, from.1)
                        .await
                        .expect(&format!("Failed to get `{}`", from.0))
                };
                let to = tos.iter().next().unwrap();

                let from = if let Some(filter) = filter {
                    if !PathBuf::from(filter).exists() {
                        panic!("`{}`.`filter` does not exist", task.0);
                    }

                    let filter = Filter::new(filter);
                    filter.filter(from).expect("Failed to create filter")
                } else {
                    from
                };

                if let Err(err) = to::write(to.0, to.1, from).await {
                    panic!("{:#?}", err);
                }
            });
        });

        joins.push(join);
    }

    for join in joins {
        let _ = join.join();
    }

    Ok(())
}
