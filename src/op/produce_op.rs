use crate::{cli_options::ProducerOpts, error::PulsarCatError};

use crate::common::get_base_client;

use crate::op::OpValidate;
use flate2::Compression as Flate2Compression;
use pulsar::compression::{
    Compression, CompressionLz4, CompressionSnappy, CompressionZlib, CompressionZstd,
};
use std::{
    io::{self, BufRead},
    sync::Arc,
};
use tokio::{
    sync::{Mutex, broadcast, mpsc, oneshot},
    task::JoinSet,
};

pub async fn run_produce(broker: String, opts: &ProducerOpts) -> Result<(), PulsarCatError> {
    opts.validate()?;

    let client = get_base_client(&broker, &opts.auth).await?;

    // Configure producer builder with options
    let mut producer_builder = client.producer().with_topic(&opts.topic);

    // Set compression if specified
    if let crate::cli_options::CompressionOpt::None = opts.compression {
        // No compression
    } else {
        let compression = match opts.compression {
            crate::cli_options::CompressionOpt::Lz4 => Compression::Lz4(CompressionLz4::default()),
            crate::cli_options::CompressionOpt::Zlib => Compression::Zlib(CompressionZlib {
                level: Flate2Compression::default(),
            }),
            crate::cli_options::CompressionOpt::Zstd => {
                Compression::Zstd(CompressionZstd::default())
            }
            crate::cli_options::CompressionOpt::Snappy => Compression::Snappy(CompressionSnappy {}),
            crate::cli_options::CompressionOpt::None => unreachable!(),
        };
        producer_builder = producer_builder.with_options(pulsar::ProducerOptions {
            compression: Some(compression),
            ..Default::default()
        });
    }

    let producer = producer_builder.build().await?;
    // Wrap producer in Arc<Mutex<>> for sharing between tasks
    let producer = Arc::new(Mutex::new(producer));

    // Create channels for message processing
    let (line_sender, line_receiver) = mpsc::channel(100);
    let (shutdown_sender, _) = broadcast::channel::<()>(1);
    let (input_done_tx, input_done_rx) = oneshot::channel();
    let mut shutdown_receiver = shutdown_sender.subscribe();

    // Spawn a task to read from stdin
    let stdin_reader = tokio::task::spawn_blocking(move || {
        let stdin = io::stdin();
        let reader = io::BufReader::new(stdin);
        let lines = reader.lines();

        // Read lines until EOF
        for line_result in lines {
            match line_result {
                Ok(line) => {
                    if line.is_empty() {
                        continue;
                    }

                    // Try to send the line, if the channel is closed, stop reading
                    if line_sender.blocking_send(line).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("Error reading from stdin: {}", e);
                    break;
                }
            }
        }

        // Signal that we've reached EOF (all input has been read)
        let _ = input_done_tx.send(());
    });

    // Create a JoinSet to manage message processing tasks
    let mut join_set = JoinSet::new();

    // Clone references for the message processor
    let producer_ref = producer.clone();
    let key_delimiter = opts.key.clone();
    let enforce_key = opts.enforce_key;

    // Clone line_receiver for the message processor
    let mut processor_line_receiver = line_receiver;

    // Spawn message processor task
    let message_processor = tokio::spawn(async move {
        while let Some(line) = processor_line_receiver.recv().await {
            let producer_task = producer_ref.clone();
            let key_delim = key_delimiter.clone();

            // Spawn a task for each message
            join_set.spawn(async move {
                // Get locked producer for this task
                let mut producer = producer_task.lock().await;

                // Parse key and value based on delimiter if provided
                let (message_key, message_data) = if let Some(delimiter) = key_delim {
                    // Split at the first occurrence of the delimiter
                    if let Some(delimiter_pos) = line.find(&delimiter) {
                        let (k, v) = line.split_at(delimiter_pos);
                        let v = &v[delimiter.len()..]; // Skip the delimiter
                        (Some(k.to_string()), v.to_string())
                    } else if enforce_key {
                        // If key is enforced but delimiter not found
                        return Err(PulsarCatError::Application(anyhow::anyhow!(
                            "Key is enforced but delimiter '{}' not found in the message", delimiter
                        )));
                    } else {
                        // No delimiter found, use whole line as data
                        (None, line)
                    }
                } else {
                    // No delimiter specified
                    (None, line)
                };

                // Create message builder
                let mut message_builder = producer.create_message();

                // Add key if available
                if let Some(key) = message_key {
                    message_builder = message_builder.with_key(key);
                } else if enforce_key {
                    return Err(PulsarCatError::Application(anyhow::anyhow!(
                        "Message key is required but not provided, please use --key to set the delimiter."
                    )));
                }

                // Set message content and send
                let message = message_builder
                    .with_content(message_data.as_bytes()).send_non_blocking()
                    .await?;

                // Wait for message to be acknowledged
                message.await?;
                Ok::<_, PulsarCatError>(())
            });
        }

        // Wait for all message processing tasks to complete
        while let Some(result) = join_set.join_next().await {
            if let Err(e) = result {
                eprintln!("Error in message processing task: {}", e);
            }
        }
    });

    // Set up handler to wait for either:
    // 1. Ctrl-C signal
    // 2. Input done (EOF reached)
    // 3. Shutdown signal
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("Received Ctrl-C, shutting down gracefully...");
        }
        _ = input_done_rx => {
            println!("Finished reading input, waiting for messages to be sent...");
        }
        _ = shutdown_receiver.recv() => {
            println!("Shutdown signal received from task, shutting down...");
        }
    }

    // Trigger shutdown to clean up processing
    let _ = shutdown_sender.send(());

    // Wait for stdin reader to finish
    if let Err(e) = stdin_reader.await {
        eprintln!("Error joining stdin reader: {}", e);
    }

    // Wait for message processor to finish
    if let Err(e) = message_processor.await {
        eprintln!("Error joining message processor: {}", e);
    }

    println!("All tasks completed, shutting down");
    Ok(())
}
