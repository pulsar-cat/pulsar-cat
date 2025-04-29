use clap::ValueEnum;
use clap::{Args, Parser, Subcommand};

use crate::PulsarCatError;
use crate::op::OpValidate;

#[derive(Parser, Debug, Clone)]
#[clap(version, about = "The DevOps tool that provides Kcat like experience for Pulsar", long_about = None)]
#[clap(propagate_version = true)]
#[clap(infer_subcommands = true)]
pub struct CliOpts {
    /// Pulsar broker URL
    #[arg(
        short = 'b',
        long = "broker",
        required = true,
        help = "Pulsar broker URL"
    )]
    pub broker: String,

    #[command(subcommand)]
    pub command: OpMode,
}

#[derive(Subcommand, Debug, Clone)]
pub enum OpMode {
    /// Producer mode: send messages to a topic
    #[command(name = "produce", alias = "P")]
    Producer(ProducerOpts),

    /// Consumer mode: read messages from a topic
    #[command(name = "consume", alias = "C")]
    Consumer(ConsumerOpts),

    /// List mode: view metadata about clusters, brokers, and topics
    #[command(name = "list", alias = "L")]
    List(ListOpts),
}

#[derive(ValueEnum, Debug, Clone)]
enum AuthMethod {
    UserPassword,
    Token,
}

#[derive(Args, Debug, Clone)]
pub struct DisplayOpts {
    #[arg(
        short = 'f',
        long = "format",
        required = false,
        help = "Format to display messages in"
    )]
    format: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub struct AuthOpts {
    #[arg(
        long = "auth_token",
        required = false,
        help = "Token for authentication"
    )]
    pub token: Option<String>,
}

#[derive(ValueEnum, Debug, Clone)]
pub enum CompressionOpt {
    #[value(alias = "none")]
    None,
    #[value(alias = "lz4")]
    Lz4,
    #[value(alias = "zlib")]
    Zlib,
    #[value(alias = "zstd")]
    Zstd,
    #[value(alias = "snappy")]
    Snappy,
}
#[derive(Args, Debug, Clone)]
pub struct ProducerOpts {
    #[arg(
        short = 't',
        long = "topic",
        required = true,
        help = "Topic to produce messages to, should be in the format of 'tenant/namespace/topic'"
    )]
    pub topic: String,

    #[arg(
        short = 'p',
        long = "partition",
        required = false,
        help = "Partition to produce messages to, should be a number.
            Return error if partition is not a number or is not in the range of partitions."
    )]
    pub partition: Option<u32>,

    #[arg(
        long = "compression",
        short = 'z',
        required = false,
        help = "Compression to use for the messages, should be one of 'none', 'lz4', 'zlib', 'zstd', 'snappy'",
        default_value = "none"
    )]
    pub compression: CompressionOpt,

    #[arg(
        long = "key",
        short = 'K',
        required = false,
        help = "Key for delimiting messages, should be a string.
            If not provided, the message will not be sent with a key."
    )]
    pub key: Option<String>,

    #[arg(
        long = "enforce_key",
        short = 'k',
        help = "Enforce a key for the messages. When provided, messages must have a key.",
        default_value = "false"
    )]
    pub enforce_key: bool,

    #[command(flatten)]
    pub auth: AuthOpts,
}

impl OpValidate for ProducerOpts {
    fn validate(&self) -> Result<(), PulsarCatError> {
        Ok(())
    }
}

#[derive(Args, Debug, Clone)]
pub struct ConsumerOpts {
    #[arg(
        short = 't',
        long = "topic",
        required = true,
        help = "Topic to consume messages from, should be in the format of 'tenant/namespace/topic'"
    )]
    topic: String,

    #[command(flatten)]
    pub auth: AuthOpts,

    #[command(flatten)]
    pub display: DisplayOpts,
}

#[derive(Args, Debug, Clone)]
pub struct ListOpts {
    #[command(flatten)]
    pub auth: AuthOpts,

    #[arg(
        long = "namespace",
        required = false,
        help = "Namespace to list topics from, should be in the format of 'tenant/namespace'"
    )]
    pub namespace: Option<String>,

    #[arg(
        short = 't',
        long = "topic",
        required = false,
        help = "Topic to list messages from, should be in the format of 'tenant/namespace/topic'"
    )]
    pub topic: Option<String>,
}

impl OpValidate for ListOpts {
    fn validate(&self) -> Result<(), PulsarCatError> {
        if !(self.namespace.is_some() || self.topic.is_some()) {
            return Err(PulsarCatError::Application(anyhow::anyhow!(
                "You must provide either a topic or a namespace.
                If you want to list all topics in a namespace, use the --namespace flag.
                If you want to list all partitons in a topic, use the --topic flag."
            )));
        }
        Ok(())
    }
}
