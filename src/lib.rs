mod types;
pub use types::*;

pub mod config;

use rdkafka::client::Client;
use std::collections::HashMap;
use std::sync::Arc;

use crate::config::{ConsumerConfig, ProducerConfig};
use crate::consumer::{
    CommitMode, Consumer, DefaultConsumerContext, MessageStream, StreamConsumer,
};
use crate::message::{BorrowedMessage, ToBytes};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumerContext;
use rdkafka::consumer::ConsumerGroupMetadata;
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::groups::GroupList;
use rdkafka::message::OwnedHeaders;
use rdkafka::metadata::Metadata;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{Offset, TopicPartitionList};

pub mod metadata {
    pub use rdkafka::metadata::Metadata;
}

pub mod message {
    pub use rdkafka::message::{BorrowedHeaders, BorrowedMessage, Headers, Message, ToBytes};
}

pub mod consumer {
    pub use rdkafka::consumer::{
        BaseConsumer, CommitMode, Consumer, DefaultConsumerContext, MessageStream, StreamConsumer,
    };
}

pub type Result<T> = std::result::Result<T, Error>;
pub type Error = KafkaError;

pub struct KafkaProducer {
    producer: Arc<FutureProducer>,
}

impl KafkaProducer {
    pub fn new(config: ProducerConfig) -> Result<Self> {
        let client_config: ClientConfig = config.into();
        let producer: FutureProducer = client_config.create()?;
        Ok(Self {
            producer: Arc::new(producer),
        })
    }

    pub async fn produce<K, V, HV>(
        &self,
        topic_name: &str,
        key: &K,
        value: &V,
        headers: Option<HashMap<&str, &HV>>,
    ) -> Result<ProduceResult>
    where
        K: ToBytes + ?Sized,
        V: ToBytes + ?Sized,
        HV: ToBytes + ?Sized,
    {
        let mut record = FutureRecord::to(topic_name).payload(value).key(key);
        let mut own_headers = OwnedHeaders::new();
        if let Some(headers) = headers {
            for (k, v) in headers {
                own_headers = own_headers.add(k, v);
            }
        }
        record = record.headers(own_headers);

        match self.producer.send(record, Timeout::Never).await {
            Ok((partition, offset)) => Ok((partition, offset)),
            Err((kafka_error, _owned_message)) => Err(kafka_error),
        }
    }
}

pub struct KafkaConsumer {
    consumer: Arc<StreamConsumer<DefaultConsumerContext>>,
}

impl KafkaConsumer {
    pub fn new(config: ConsumerConfig) -> Result<Self> {
        let context = DefaultConsumerContext;
        let client_config: ClientConfig = config.into();
        let consumer = client_config.create_with_context(context)?;
        Ok(Self {
            consumer: Arc::new(consumer),
        })
    }
    pub fn start(&self) -> MessageStream<DefaultConsumerContext> {
        self.consumer.stream()
    }
}

impl Consumer<StreamConsumerContext<DefaultConsumerContext>> for KafkaConsumer {
    fn client(&self) -> &Client<StreamConsumerContext<DefaultConsumerContext>> {
        self.consumer.client()
    }
    fn group_metadata(&self) -> Option<ConsumerGroupMetadata> {
        self.consumer.group_metadata()
    }
    fn subscribe(&self, topics: &[&str]) -> KafkaResult<()> {
        self.consumer.subscribe(topics)
    }
    fn unsubscribe(&self) {
        self.consumer.unsubscribe()
    }
    fn assign(&self, assignment: &TopicPartitionList) -> KafkaResult<()> {
        self.consumer.assign(assignment)
    }
    fn seek<T: Into<Timeout>>(
        &self,
        topic: &str,
        partition: i32,
        offset: Offset,
        timeout: T,
    ) -> KafkaResult<()> {
        self.consumer.seek(topic, partition, offset, timeout)
    }
    fn commit(
        &self,
        topic_partition_list: &TopicPartitionList,
        mode: CommitMode,
    ) -> KafkaResult<()> {
        self.consumer.commit(topic_partition_list, mode)
    }
    fn commit_consumer_state(&self, mode: CommitMode) -> KafkaResult<()> {
        self.consumer.commit_consumer_state(mode)
    }
    fn commit_message(&self, message: &BorrowedMessage<'_>, mode: CommitMode) -> KafkaResult<()> {
        self.consumer.commit_message(message, mode)
    }
    fn store_offset(&self, message: &BorrowedMessage<'_>) -> KafkaResult<()> {
        self.consumer.store_offset(message)
    }
    fn store_offsets(&self, tpl: &TopicPartitionList) -> KafkaResult<()> {
        self.consumer.store_offsets(tpl)
    }
    fn subscription(&self) -> KafkaResult<TopicPartitionList> {
        self.consumer.subscription()
    }
    fn assignment(&self) -> KafkaResult<TopicPartitionList> {
        self.consumer.assignment()
    }
    fn committed<T>(&self, timeout: T) -> KafkaResult<TopicPartitionList>
    where
        T: Into<Timeout>,
        Self: Sized,
    {
        self.consumer.committed(timeout)
    }
    fn committed_offsets<T>(
        &self,
        tpl: TopicPartitionList,
        timeout: T,
    ) -> KafkaResult<TopicPartitionList>
    where
        T: Into<Timeout>,
    {
        self.consumer.committed_offsets(tpl, timeout)
    }
    fn offsets_for_timestamp<T>(
        &self,
        timestamp: i64,
        timeout: T,
    ) -> KafkaResult<TopicPartitionList>
    where
        T: Into<Timeout>,
        Self: Sized,
    {
        self.consumer.offsets_for_timestamp(timestamp, timeout)
    }
    fn offsets_for_times<T>(
        &self,
        timestamps: TopicPartitionList,
        timeout: T,
    ) -> KafkaResult<TopicPartitionList>
    where
        T: Into<Timeout>,
        Self: Sized,
    {
        self.consumer.offsets_for_times(timestamps, timeout)
    }
    fn position(&self) -> KafkaResult<TopicPartitionList> {
        self.consumer.position()
    }
    fn fetch_metadata<T>(&self, topic: Option<&str>, timeout: T) -> KafkaResult<Metadata>
    where
        T: Into<Timeout>,
        Self: Sized,
    {
        self.consumer.fetch_metadata(topic, timeout)
    }
    fn fetch_watermarks<T>(
        &self,
        topic: &str,
        partition: i32,
        timeout: T,
    ) -> KafkaResult<(i64, i64)>
    where
        T: Into<Timeout>,
        Self: Sized,
    {
        self.consumer.fetch_watermarks(topic, partition, timeout)
    }
    fn fetch_group_list<T>(&self, group: Option<&str>, timeout: T) -> KafkaResult<GroupList>
    where
        T: Into<Timeout>,
        Self: Sized,
    {
        self.consumer.fetch_group_list(group, timeout)
    }
    fn pause(&self, partitions: &TopicPartitionList) -> KafkaResult<()> {
        self.consumer.pause(partitions)
    }
    fn resume(&self, partitions: &TopicPartitionList) -> KafkaResult<()> {
        self.consumer.resume(partitions)
    }
}
