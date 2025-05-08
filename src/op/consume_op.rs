use crate::{cli_options::{ConsumerOpts, OffsetPosition}, error::PulsarCatError};
use crate::common::get_base_client;
use crate::op::OpValidate;

use pulsar::{SubType, consumer::ConsumerOptions, consumer::InitialPosition};
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use futures::TryStreamExt;
use serde_json::json;
use std::str;

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

    // Keep track of message count for early exit
    let mut message_count = 0;
    let early_exit = opts.exit;

    // Main consumption loop with a way to exit on Ctrl+C
    loop {
        tokio::select! {
            result = consumer.try_next() => {
                match result {
                    Ok(Some(msg)) => {
                        message_count += 1;
                        
                        // Access message data
                        let payload = msg.payload.data.as_ref();
                        let message_id = msg.message_id.clone();
                        let topic = msg.topic.clone();
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
                                publish_time
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
                    Ok(None) => {
                        if !opts.display.json {
                            println!("End of stream");
                        }
                        if early_exit {
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("Error receiving message: {}", e);
                        break;
                    }
                }
            }
            
            _ = tokio::signal::ctrl_c() => {
                if !opts.display.json {
                    println!("Received Ctrl+C, shutting down consumer...");
                }
                break;
            }
        }

        // Check if we've reached the end and should exit early
        if early_exit && message_count > 0 {
            // Wait a short time to see if more messages arrive
            tokio::time::sleep(Duration::from_millis(100)).await;
            
            // Check if any more messages are available by attempting to receive
            match consumer.try_next().await {
                Ok(Some(msg)) => {
                    // Got another message, process it
                    message_count += 1;
                    
                    // Access message data
                    let payload = msg.payload.data.as_ref();
                    let message_id = msg.message_id.clone();
                    let topic = msg.topic.clone();
                    let key = msg.key().map(|k| k.to_string());
                    let publish_time = msg.metadata().publish_time;

                    // Format message according to options (same logic as above)
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
                        let formatted = format_message(
                            format_str, 
                            &topic, 
                            format!("{:?}", message_id).as_str(), 
                            key.as_deref(), 
                            payload, 
                            publish_time
                        );
                        println!("{}", formatted);
                    } else {
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
                        println!("No more messages available, exiting...");
                    }
                    break;
                },
                Err(e) => {
                    eprintln!("Error receiving message: {}", e);
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
fn format_message(format_str: &str, topic: &str, message_id: &str, key: Option<&str>, payload: &[u8], timestamp: u64) -> String {
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
                'h' => result.push_str(""), // Pulsar doesn't have headers in the same way Kafka does
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
