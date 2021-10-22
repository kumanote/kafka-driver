# kafka-driver

> Rust implementation of [rust-rdkafka - Apache kafka client](https://github.com/fede1024/rust-rdkafka) wrapper.

This package provides [Apache kafka](https://kafka.apache.org/) client wrapper.

## Installation

#### Dependencies

- [Rust with Cargo](http://rust-lang.org)

**rust-toolchain**

```text
1.54.0
```

#### Importing

**Cargo.toml**

```toml
[dependencies]
kafka-driver = { version = "0.1.0-beta", git = "https://github.com/kumanote/kafka-driver", branch = "main" }
```

**rust files**

```rust
use kafka_driver::{KafkaConsumer, KafkaBrokers, KafkaBroker, KafkaProducer};
use kafka_driver::config::{ConsumerConfig, ProducerConfig};
use kafka_driver::consumer::{Consumer, CommitMode};
use kafka_driver::message::{Message, Headers};
```

## Examples

Here's a basic example:

*please note that you need to run kafka and zookeeper service on localhost in advance.*

You can start your local kafka and zookeeper service by using [docker-compose.yaml](./docker-compose.yaml).
...and you can check messages inside a specific topic by login to the kafka docker container. 

```bash
% docker exec -it kafka-driver-kafka-1 bash
# show all topics
$ kafka-topics --zookeeper zookeeper:2181 --list
# show all messages in "test" topic
$ kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning
```

```rust
use kafka_driver::{KafkaConsumer, KafkaBrokers, KafkaBroker, KafkaProducer};
use kafka_driver::config::{ConsumerConfig, ProducerConfig};
use kafka_driver::consumer::{Consumer, CommitMode};
use kafka_driver::message::{Message, Headers};
use futures::StreamExt;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let rt = tokio::runtime::Runtime::new().unwrap();
    // consume
    let task1 = async {
        let topics = vec!["test"];
        let group_id = String::from("001");
        let brokers = KafkaBrokers::new(vec![KafkaBroker::new("localhost".into(), 9092)]);
        let config = ConsumerConfig::new_with_defaults(brokers, group_id);
        let consumer = KafkaConsumer::new(config).expect("consumer must be configured...");
        consumer.subscribe(topics.as_slice()).unwrap();
        println!("subscribed!");
        let mut message_stream = consumer.start();
        println!("subscription started!");
        while let Some(message) = message_stream.next().await {
            println!("Got a message from server!");
            match message {
                Err(e) => println!("Kafka error: {}", e),
                Ok(m) => {
                    let key = match m.key() {
                        Some(k) => std::str::from_utf8(k).unwrap(),
                        None => "",
                    };
                    let payload = match m.payload_view::<str>() {
                        None => "",
                        Some(Ok(s)) => s,
                        Some(Err(e)) => {
                            println!("Error while deserializing message payload: {:?}", e);
                            ""
                        }
                    };
                    println!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                             key, payload, m.topic(), m.partition(), m.offset(), m.timestamp());
                    if let Some(headers) = m.headers() {
                        for i in 0..headers.count() {
                            let header = headers.get(i).unwrap();
                            println!("  Header {:#?}: {:?}", header.0, std::str::from_utf8(header.1).unwrap());
                        }
                    }
                    consumer.commit_message(&m, CommitMode::Sync).unwrap();
                }
            };
        }
    };
    rt.spawn(task1);

    // produce
    let task2 = async {
        let brokers = KafkaBrokers::new(vec![KafkaBroker::new("localhost".into(), 9092)]);
        let topic = "test";
        let config = ProducerConfig::new_with_defaults(brokers);
        let producer = KafkaProducer::new(config).expect("producer must be configured...");
        let mut headers: HashMap<&str, &str> = HashMap::new();
        headers.insert("header_key", "header_value");
        let key = format!("This is the key");
        let value = format!("This is the message");
        println!("Let's produce message!");
        let result = producer.produce(topic, &key, &value, Some(headers)).await;
        println!("Produce completed. Result: {:?}", result);
    };
    rt.spawn(task2);

    tokio::signal::ctrl_c().await?;
    println!("ctrl-c received!");
    Ok(())
}
```
