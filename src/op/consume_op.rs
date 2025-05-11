use crate::common::get_base_client;
use crate::op::OpValidate;
use crate::{
    cli_options::{ConsumerOpts, OffsetPosition},
    error::PulsarCatError,
};

use futures::TryStreamExt;
use pulsar::proto::KeyValue;
use pulsar::{SubType, consumer::ConsumerOptions, consumer::InitialPosition};
use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use std::str;
use tokio::time::timeout;

pub async fn run_consume(broker: String, opts: &ConsumerOpts) -> Result<(), PulsarCatError> {
    // Create Pulsar client
    let client = get_base_client(&broker, &opts.auth).await?;

    // Prepare consumer options with initial position
    let consumer_options = if let Some(offset) = &opts.offset {
        match offset {
            OffsetPosition::Beginning => {
                ConsumerOptions::default().with_initial_position(InitialPosition::Earliest)
            }
            OffsetPosition::End => {
                ConsumerOptions::default().with_initial_position(InitialPosition::Latest)
            }
        }
    } else {
        ConsumerOptions::default()
    };

    // Create consumer with topic and options
    let mut consumer = client
        .consumer()
        .with_topic(&opts.topic)
        .with_subscription_type(SubType::Exclusive)
        .with_subscription(format!("pulsar-cat-consumer-{}", generate_consumer_id()))
        .with_consumer_name(format!("pulsar-cat-{}", generate_consumer_id()))
        .with_options(consumer_options)
        .build::<Vec<u8>>()
        .await?;

    if !opts.display.json {
        println!("Started consuming from topic: {}", opts.topic);
        println!("Press Ctrl+C to exit");
    }

    let early_exit = opts.exit;

    // Add time tracking for early exit detection
    let mut last_message_time = SystemTime::now();
    let mut got_at_least_one_message = false;

    // Use a shorter timeout for faster detection of end of stream
    const TIMEOUT_DURATION: Duration = Duration::from_millis(300);
    // Define the maximum idle time before considering the stream finished
    const MAX_IDLE_TIME: Duration = Duration::from_millis(1000);

    // For empty topics with early exit, check for messages immediately with a timeout
    if early_exit {
        // Use a short timeout for the first message check
        match timeout(TIMEOUT_DURATION, consumer.try_next()).await {
            // Timeout on first attempt indicates an empty topic
            Err(_) => {
                if !opts.display.json {
                    println!("No messages available in topic (empty topic), exiting...");
                }

                // Try to close consumer gracefully
                if let Err(e) = consumer.close().await {
                    eprintln!("Error closing consumer: {}", e);
                }

                if !opts.display.json {
                    println!("Consumer shut down");
                }
                return Ok(());
            }
            // Got a result from the first attempt
            Ok(result) => match result {
                // Found a message, process normally
                Ok(Some(msg)) => {
                    got_at_least_one_message = true;
                    last_message_time = SystemTime::now();

                    // Process the message
                    let payload = msg.payload.data.as_ref();
                    let message_id = msg.message_id.clone();
                    let topic = msg.topic.clone();
                    let key = msg.key().map(|k| k.to_string());
                    let publish_time = msg.metadata().publish_time;
                    let headers = msg.metadata().properties.clone();

                    // Format message according to options
                    if opts.display.json {
                        // Output in JSON format
                        let json_output = json!({
                            "topic": topic,
                            "message_id": format!("{:?}", message_id),
                            "key": key,
                            "payload": str::from_utf8(payload).unwrap_or("<binary data>"),
                            "payload_size": payload.len(),
                            "publish_time": publish_time,
                        });
                        println!("{}", serde_json::to_string(&json_output).unwrap());
                    } else if let Some(format_str) = &opts.display.format {
                        // Custom format
                        let formatted = format_message(
                            format_str,
                            &topic,
                            format!("{:?}", message_id).as_str(),
                            key.as_deref(),
                            payload,
                            publish_time,
                            &headers,
                        );
                        println!("{}", formatted);
                    } else {
                        // Default format - just the payload
                        let content = String::from_utf8_lossy(payload);
                        println!("{}", content);
                    }

                    // Acknowledge the message
                    if let Err(e) = consumer.ack(&msg).await {
                        eprintln!("Failed to acknowledge message: {}", e);
                    }
                }
                // No messages (empty topic) or end of stream
                Ok(None) => {
                    if !opts.display.json {
                        println!("No messages in topic (empty topic), exiting...");
                    }

                    // Try to close consumer gracefully
                    if let Err(e) = consumer.close().await {
                        eprintln!("Error closing consumer: {}", e);
                    }

                    if !opts.display.json {
                        println!("Consumer shut down");
                    }
                    return Ok(());
                }
                // Error consuming
                Err(e) => {
                    eprintln!("Error receiving message: {}", e);

                    // Try to close consumer gracefully
                    if let Err(e) = consumer.close().await {
                        eprintln!("Error closing consumer: {}", e);
                    }

                    return Err(e.into());
                }
            },
        }
    }

    // Main consumption loop with a way to exit on Ctrl+C
    loop {
        // Always use a timeout when early_exit is enabled to detect end of stream
        let next_message = if early_exit {
            timeout(TIMEOUT_DURATION, consumer.try_next()).await
        } else {
            Ok(consumer.try_next().await)
        };

        // Check if we've been idle too long and should exit
        if early_exit && got_at_least_one_message {
            let idle_time = SystemTime::now()
                .duration_since(last_message_time)
                .unwrap_or(Duration::from_secs(0));
            if idle_time > MAX_IDLE_TIME {
                if !opts.display.json {
                    println!(
                        "No new messages received for {} ms, exiting...",
                        idle_time.as_millis()
                    );
                }
                break;
            }
        }

        tokio::select! {
            // Handle the next message (potentially with timeout)
            result = async { next_message } => {
                match result {
                    // Timeout occurred - check if we should exit
                    Err(_) if early_exit => {
                        // If we've already received at least one message and hit a timeout,
                        // it's likely we've reached the end
                        if got_at_least_one_message {
                            let idle_time = SystemTime::now().duration_since(last_message_time).unwrap_or(Duration::from_secs(0));
                            if idle_time > MAX_IDLE_TIME / 2 {
                                if !opts.display.json {
                                    println!("No more messages available after timeout, exiting...");
                                }
                                break;
                            }
                        } else {
                            // If we haven't received any messages yet, exit after a few timeouts
                            if !opts.display.json {
                                println!("No messages available after timeout, exiting...");
                            }
                            break;
                        }
                    },
                    // Got a result from the consumer
                    Ok(consumer_result) => {
                        match consumer_result {
                            Ok(Some(msg)) => {
                                // Update time tracking for early exit detection
                                got_at_least_one_message = true;
                                last_message_time = SystemTime::now();

                                // Access message data
                                let payload = msg.payload.data.as_ref();
                                let message_id = msg.message_id.clone();
                                let topic = msg.topic.clone();
                                let headers = msg.metadata().properties.clone();
                                let key = msg.key().map(|k| k.to_string());
                                // Get publish time - may need to use event time or other timestamp
                                let publish_time = msg.metadata().publish_time;

                                // Format message according to options
                                if opts.display.json {
                                    // Output in JSON format
                                    let json_output = json!({
                                        "topic": topic,
                                        "message_id": format!("{:?}", message_id),
                                        "key": key,
                                        "payload": str::from_utf8(payload).unwrap_or("<binary data>"),
                                        "payload_size": payload.len(),
                                        "publish_time": publish_time,
                                    });
                                    println!("{}", serde_json::to_string(&json_output).unwrap());
                                } else if let Some(format_str) = &opts.display.format {
                                    // Custom format
                                    let formatted = format_message(
                                        format_str,
                                        &topic,
                                        format!("{:?}", message_id).as_str(),
                                        key.as_deref(),
                                        payload,
                                        publish_time,
                                        &headers
                                    );
                                    println!("{}", formatted);
                                } else {
                                    // Default format - just the payload
                                    let content = String::from_utf8_lossy(payload);
                                    println!("{}", content);
                                }

                                // Acknowledge the message
                                if let Err(e) = consumer.ack(&msg).await {
                                    eprintln!("Failed to acknowledge message: {}", e);
                                }
                            },
                            Ok(None) => {
                                if !opts.display.json {
                                    println!("End of stream");
                                }
                                if early_exit {
                                    break;
                                }
                            },
                            Err(e) => {
                                eprintln!("Error receiving message: {}", e);
                                break;
                            }
                        }
                    },
                    // This shouldn't happen since select will choose available branches
                    _ => {
                        if early_exit {
                            break;
                        }
                    },
                }
            },

            _ = tokio::signal::ctrl_c() => {
                if !opts.display.json {
                    println!("Received Ctrl+C, shutting down consumer...");
                }
                break;
            }

            // Add a periodic timeout check to ensure we exit if no progress
            _ = tokio::time::sleep(Duration::from_millis(500)), if early_exit && got_at_least_one_message => {
                let idle_time = SystemTime::now().duration_since(last_message_time).unwrap_or(Duration::from_secs(0));
                if idle_time > MAX_IDLE_TIME {
                    if !opts.display.json {
                        println!("No new messages received for {} ms, exiting...", idle_time.as_millis());
                    }
                    break;
                }
            }
        }
    }

    // Try to close consumer gracefully
    if let Err(e) = consumer.close().await {
        eprintln!("Error closing consumer: {}", e);
    }

    if !opts.display.json {
        println!("Consumer shut down");
    }
    Ok(())
}

// Format a message according to the format string
// Placeholders: %t=topic, %p=partition, %o=offset, %k=key, %s=payload, %S=size, %h=headers, %T=timestamp
fn format_message(
    format_str: &str,
    topic: &str,
    message_id: &str,
    key: Option<&str>,
    payload: &[u8],
    timestamp: u64,
    headers: &[KeyValue],
) -> String {
    let mut result = String::new();
    let mut in_placeholder = false;

    for c in format_str.chars() {
        if in_placeholder {
            match c {
                't' => result.push_str(topic),
                'p' => result.push_str(message_id), // Using message_id as the partition equivalent
                'o' => result.push_str(message_id), // Using message_id as the offset equivalent
                'k' => result.push_str(key.unwrap_or("")),
                's' => result.push_str(&String::from_utf8_lossy(payload)),
                'S' => result.push_str(&payload.len().to_string()),
                'h' => result.push_str(
                    &headers
                        .iter()
                        .map(|h| format!("{}={}", h.key, h.value))
                        .collect::<Vec<String>>()
                        .join(", "),
                ),
                'T' => result.push_str(&timestamp.to_string()),
                '%' => result.push('%'),
                _ => {
                    result.push('%');
                    result.push(c);
                }
            }
            in_placeholder = false;
        } else if c == '%' {
            in_placeholder = true;
        } else {
            result.push(c);
        }
    }

    // Handle trailing % if any
    if in_placeholder {
        result.push('%');
    }

    result
}

// Generate a unique consumer ID based on the current timestamp
fn generate_consumer_id() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    format!("{}", now)
}

impl OpValidate for ConsumerOpts {
    fn validate(&self) -> Result<(), PulsarCatError> {
        Ok(())
    }
}
