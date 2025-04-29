use crate::cli_options::ListOpts;
use crate::common::get_base_client;
use crate::error::PulsarCatError;
use pulsar::BrokerAddress;
use pulsar::proto::command_get_topics_of_namespace::Mode;

use crate::op::OpValidate;

pub async fn run_list(broker: String, list_opts: ListOpts) -> Result<(), PulsarCatError> {
    list_opts.validate()?;
    let pulsar = get_base_client(&broker, &list_opts.auth).await?;

    match (list_opts.topic, list_opts.namespace) {
        (Some(topic), None) => {
            println!(
                "Printing partition info for topic: {}, service url: {}",
                &topic, &broker
            );
            let partitions = pulsar.lookup_partitioned_topic(&topic).await?;
            let format_partition_info = |broker_address: &BrokerAddress| -> String {
                format!(
                    "{}:{}",
                    broker_address.url,
                    if broker_address.proxy {
                        "(via proxy)"
                    } else {
                        ""
                    }
                )
            };
            match partitions.len() {
                0 => println!("Topic {} not found", topic),
                1 => {
                    println!("Topic {} is a non-partitioned topic", topic);
                    println!(
                        "    Partition 0: {}",
                        format_partition_info(&partitions[0].1)
                    );
                }
                _ => {
                    println!("Topic {} has {} partitions", topic, partitions.len());
                    for (i, partition) in partitions.iter().enumerate() {
                        println!(
                            "    Partition {}: {}",
                            i,
                            format_partition_info(&partition.1)
                        );
                    }
                }
            }
        }
        (None, Some(namespace)) => {
            let topics = pulsar
                .get_topics_of_namespace(namespace.clone(), Mode::All)
                .await?;
            println!(
                "Printing topics in namespace: {}, service url: {}",
                namespace, broker
            );
            for topic in topics {
                println!("    {}", topic);
            }
        }
        _ => {
            return Err(anyhow::anyhow!(
                "
            You must provide either a topic or a namespace.
            If you want to list all topics in a namespace, use the --namespace flag.
            If you want to list all partitions in a topic, use the --topic flag.
        "
            )
            .into());
        }
    }

    Ok(())
}
