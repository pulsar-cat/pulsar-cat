use crate::{cli_options::{ConsumerOpts, OffsetPosition}, error::PulsarCatError};
use crate::common::get_base_client;
use crate::op::OpValidate;

use pulsar::{SubType, consumer::ConsumerOptions, consumer::InitialPosition};
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use futures::TryStreamExt;
use serde_json::json;
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

    // Keep track of message count for early exit
    let early_exit = opts.exit;
    // Keep track of time since last message for early exit
    let mut no_messages_count = 0;
    // Track if this is the first message attempt (for empty topic check)
    let mut is_first_attempt = true;

    // For empty topics with early exit, check for messages immediately with a timeout
    if early_exit {
        // Use a short timeout for the first message check
        match timeout(Duration::from_millis(1000), consumer.try_next()).await {
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
            },
            // Got a result from the first attempt
            Ok(result) => match result {
                // Found a message, process normally
                Ok(Some(msg)) => {
                    // Process the message
                    let payload = msg.payload.data.as_ref();
                    let message_id = msg.message_id.clone();
                    let topic = msg.topic.clone();
                    let key = msg.key().map(|k| k.to_string());
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
                },
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
                },
                // Error consuming
                Err(e) => {
                    eprintln!("Error receiving message: {}", e);
                    
                    // Try to close consumer gracefully
                    if let Err(e) = consumer.close().await {
                        eprintln!("Error closing consumer: {}", e);
                    }
                    
                    return Err(e.into());
                }
            }
        }
        
        // First attempt handled, continue with normal loop
        is_first_attempt = false;
    }

    // Main consumption loop with a way to exit on Ctrl+C
    loop {
        // If early exit is enabled, use a timeout to detect when no more messages are available
        let next_message = if early_exit && !is_first_attempt {
            // Use a shorter timeout after we've seen some messages
            timeout(Duration::from_millis(500), consumer.try_next()).await
        } else {
            // Normal operation without timeout (wrapped to match type)
            Ok(consumer.try_next().await)
        };
        
        // No longer the first attempt
        is_first_attempt = false;

        tokio::select! {
            // Handle the next message (potentially with timeout)
            result = async { next_message }, if next_message.is_ok() => {
                match result {
                    // Timeout occurred - no more messages available in timeout period
                    Err(_) if early_exit => {
                        no_messages_count += 1;
                        // Exit after a few timeouts to ensure we're really at the end
                        if no_messages_count >= 2 {
                            if !opts.display.json {
                                println!("No more messages available, exiting...");
                            }
                            break;
                        }
                    },
                    // Got a result from the consumer
                    Ok(consumer_result) => {
                        match consumer_result {
                            Ok(Some(msg)) => {
                                // Reset the no messages counter since we received a message
                                no_messages_count = 0;
                                
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
                    // This shouldn't happen since we guard against it in the select condition
                    _ => unreachable!(),
                }
            },
            
            _ = tokio::signal::ctrl_c() => {
                if !opts.display.json {
                    println!("Received Ctrl+C, shutting down consumer...");
                }
                break;
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
