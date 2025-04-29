# Pulsar-Cat

A versatile command-line tool for Apache Pulsar

## Overview

Pulsar-Cat is a command-line utility for Apache Pulsar that lets you produce, consume, and inspect topics easily. It's designed to be simple yet powerful, making it ideal for Pulsar administrators, developers, and for debugging Pulsar deployments.

Think of it as a "netcat for Pulsar" - a swiss-army knife for interacting with your Pulsar deployment.

## Features

- **Producer mode**: Send messages to Pulsar topics from stdin or files
- **Consumer mode**: Read messages from Pulsar topics and print to stdout
- **List mode**: View information about clusters, brokers, topics, and partitions
- **Format control**: Customize output format for messages
- **Support for compression**: Control message compression
- **Authentication support**: Connect to secured Pulsar clusters

## Installation

### Building from source

```bash
git clone https://github.com/user/pulsar-cat.git
cd pulsar-cat
./build.sh
```

## Usage

Pulsar-Cat operates in one of three modes:

- `produce` or `P`: Producer mode
- `consume` or `C`: Consumer mode
- `list` or `L`: List mode (metadata)

### Basic Usage

```bash
pulsar-cat --broker <BROKER_URL> <COMMAND> [OPTIONS]
```

The broker URL is always required:

```bash
pulsar-cat --broker pulsar://localhost:6650 <COMMAND> [OPTIONS]
```

### Producer Mode

Send messages to a topic:

```bash
pulsar-cat --broker pulsar://localhost:6650 produce --topic my-topic
message1
message2
message3
```

Send messages with a specific partition:

```bash
pulsar-cat --broker pulsar://localhost:6650 produce --topic my-topic --partition 0
```

Send messages with keys:

```bash
pulsar-cat --broker pulsar://localhost:6650 produce --topic my-topic --key ":"
key1:message1
key2:message2
```

Enforce keys for all messages:

```bash
pulsar-cat --broker pulsar://localhost:6650 produce --topic my-topic --key ":" --enforce-key
```

Use compression:

```bash
pulsar-cat --broker pulsar://localhost:6650 produce --topic my-topic --compression zstd
```

### Consumer Mode

Read messages from a topic:

```bash
pulsar-cat --broker pulsar://localhost:6650 consume --topic my-topic
```

Format output:

```bash
pulsar-cat --broker pulsar://localhost:6650 consume --topic my-topic --format 'Key: %k, Value: %s'
```

### List Mode

List topics in a namespace:

```bash
pulsar-cat --broker pulsar://localhost:6650 list --namespace tenant/namespace
```

View information about a specific topic:

```bash
pulsar-cat --broker pulsar://localhost:6650 list --topic tenant/namespace/topic
```

### Authentication

Connect to a secured Pulsar cluster:

```bash
pulsar-cat --broker pulsar+ssl://localhost:6651 consume --topic my-topic --auth_token "your-token"
```

## Format String Options

When using the `--format` option in consumer mode, the following placeholders are available:

- `%t`: Topic name
- `%p`: Partition
- `%o`: Message offset
- `%k`: Message key
- `%s`: Message payload (string)
- `%S`: Message payload size in bytes
- `%h`: Message headers
- `%T`: Message timestamp

## Compression Options

Available compression algorithms:
- `none`: No compression (default)
- `lz4`: LZ4 compression
- `zlib`: ZLIB compression
- `zstd`: ZSTD compression
- `snappy`: Snappy compression

## Examples

### Consume messages from a topic

```bash
pulsar-cat --broker pulsar://localhost:6650 consume --topic my-topic
```

### Produce messages with a key

```bash
pulsar-cat --broker pulsar://localhost:6650 produce --topic my-topic --key ":"
user1:{"name":"Alice","action":"login"}
user2:{"name":"Bob","action":"search"}
```

### Display formatted messages

```bash
pulsar-cat --broker pulsar://localhost:6650 consume --topic my-topic \
  --format "Offset: %o, Key: %k, Value: %s, Time: %T"
```

### List topics in a namespace

```bash
pulsar-cat --broker pulsar://localhost:6650 list --namespace my-tenant/my-namespace
```

## License

Pulsar-Cat is licensed under the Apache License 2.0 - see LICENSE file for details.
