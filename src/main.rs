mod cli_options;
mod common;
mod error;
mod op;

use tokio::select;

use clap::Parser;
use cli_options::{CliOpts, OpMode};
use error::PulsarCatError;

use crate::op::{run_consume, run_list, run_produce};

#[tokio::main]
async fn main() -> Result<(), PulsarCatError> {
    let cli_opts = CliOpts::parse();
    run(&cli_opts).await?;
    Ok(())
}

async fn run(cli_opts: &CliOpts) -> Result<(), PulsarCatError> {
    let mut work_join_handle = match &cli_opts.command {
        OpMode::List(list_opts) => {
            let broker = cli_opts.broker.clone();
            let list_opts = list_opts.clone();
            tokio::spawn(async move { run_list(broker, list_opts).await })
        }
        OpMode::Producer(produce_opts) => {
            let broker = cli_opts.broker.clone();
            let produce_opts = produce_opts.clone();
            tokio::spawn(async move { run_produce(broker, &produce_opts).await })
        }
        OpMode::Consumer(consume_opts) => {
            let broker = cli_opts.broker.clone();
            let consume_opts = consume_opts.clone();
            tokio::spawn(async move { run_consume(broker, &consume_opts).await })
        }
    };

    select! {
        result = &mut work_join_handle => {
            if result.is_err() {
                return Err(anyhow::anyhow!(result.unwrap_err()).into());
            }

            match result.unwrap() {
                Ok(_) => Ok(()),
                Err(e) => Err(e)
            }
        }
        _ = tokio::signal::ctrl_c() => {
            println!("Ctrl-C pressed");
            work_join_handle.abort();
            Ok(())
        }
    }
}
