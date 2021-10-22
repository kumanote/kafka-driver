use crate::{AutoOffsetReset, IsolationLevel, KafkaBrokers, SecurityProtocol};
use rdkafka::ClientConfig;
pub type KafkaLogLevel = rdkafka::config::RDKafkaLogLevel;

/// librdkafka producer client configurations
/// see https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md for more detail.
pub struct ProducerConfig {
    /// Initial list of brokers as a CSV list of broker host or host:port.
    pub metadata_broker_list: KafkaBrokers,

    /// librdkafka statistics emit interval.
    /// range: 0 .. 86400000
    /// default: 0
    /// A value of 0 disables statistics.
    pub statistics_interval_ms: Option<u32>,

    /// Request broker's supported API versions to adjust functionality to available protocol features.
    /// default: true
    pub api_version_request: Option<bool>,

    /// Protocol used to communicate with brokers.
    pub security_protocol: Option<SecurityProtocol>,

    /// When set to true, the producer will ensure that messages are successfully produced exactly once and in the original produce order.
    /// The following configuration properties are adjusted automatically (if not modified by the user) when idempotence is enabled
    ///   max.in.flight.requests.per.connection=5 (must be less than or equal to 5)
    ///   retries=INT32_MAX (must be greater than 0)
    ///   acks=all
    ///   queuing.strategy=fifo.
    /// Producer instantation will fail if user-supplied configuration is incompatible.
    /// default: false
    pub enable_idempotence: Option<bool>,

    /// Maximum number of messages allowed on the producer queue. This queue is shared by all topics and partitions.
    /// range: 1 .. 10000000
    /// default: 100000
    pub queue_buffering_max_messages: Option<u32>,

    /// Maximum total message size sum allowed on the producer queue. This queue is shared by all topics and partitions.
    /// This property has higher priority than queue.buffering.max.messages.
    /// range: 1 .. 2147483647
    /// default: 1048576
    pub queue_buffering_max_kbytes: Option<u32>,

    /// Delay in milliseconds to wait for messages in the producer queue to accumulate before constructing message batches (MessageSets) to transmit to brokers.
    /// range: 0 .. 900000
    /// default: 0.5
    pub queue_buffering_max_ms: Option<f64>,

    /// How many times to retry sending a failing Message.
    /// retrying may cause reordering unless enable.idempotence is set to true.
    /// range: 0 .. 10000000
    /// default: 2
    pub message_send_max_retries: Option<u32>,

    /// Local message timeout. This value is only enforced locally and limits the time a produced message waits for successful delivery.
    /// A time of 0 is infinite.
    /// range: 0 .. 2147483647
    /// default: 300000
    pub message_timeout_ms: Option<u32>,
}

impl ProducerConfig {
    pub fn new_with_defaults(brokers: KafkaBrokers) -> Self {
        Self {
            metadata_broker_list: brokers,
            statistics_interval_ms: None,
            api_version_request: None,
            security_protocol: None,
            enable_idempotence: None,
            queue_buffering_max_messages: None,
            queue_buffering_max_kbytes: None,
            queue_buffering_max_ms: None,
            message_send_max_retries: None,
            message_timeout_ms: None,
        }
    }
    pub fn set_bootstrap_servers(&mut self, brokers: KafkaBrokers) {
        self.metadata_broker_list = brokers
    }
    pub fn set_linger_ms(&mut self, linger_ms: Option<f64>) {
        self.queue_buffering_max_ms = linger_ms
    }
    pub fn set_retries(&mut self, retries: Option<u32>) {
        self.message_send_max_retries = retries
    }
    pub fn set_delivery_timeout_ms(&mut self, delivery_timeout_ms: Option<u32>) {
        self.message_timeout_ms = delivery_timeout_ms
    }
}

impl Into<ClientConfig> for ProducerConfig {
    fn into(self) -> ClientConfig {
        let mut client_config = ClientConfig::new();
        client_config.set(
            "bootstrap.servers",
            self.metadata_broker_list.socket_addresses().as_str(),
        );
        if let Some(statistics_interval_ms) = self.statistics_interval_ms {
            client_config.set(
                "statistics.interval.ms",
                statistics_interval_ms.to_string().as_str(),
            );
        }
        if let Some(api_version_request) = self.api_version_request {
            client_config.set(
                "api.version.request",
                api_version_request.to_string().as_str(),
            );
        }
        if let Some(security_protocol) = self.security_protocol {
            client_config.set("security.protocol", security_protocol.to_string().as_str());
        }
        if let Some(enable_idempotence) = self.enable_idempotence {
            client_config.set(
                "enable.idempotence",
                enable_idempotence.to_string().as_str(),
            );
        }
        if let Some(queue_buffering_max_messages) = self.queue_buffering_max_messages {
            client_config.set(
                "queue.buffering.max.messages",
                queue_buffering_max_messages.to_string().as_str(),
            );
        }
        if let Some(queue_buffering_max_kbytes) = self.queue_buffering_max_kbytes {
            client_config.set(
                "queue.buffering.max.kbytes",
                queue_buffering_max_kbytes.to_string().as_str(),
            );
        }
        if let Some(queue_buffering_max_ms) = self.queue_buffering_max_ms {
            client_config.set(
                "queue.buffering.max.ms",
                queue_buffering_max_ms.to_string().as_str(),
            );
        }
        if let Some(message_send_max_retries) = self.message_send_max_retries {
            client_config.set(
                "message.send.max.retries",
                message_send_max_retries.to_string().as_str(),
            );
        }
        if let Some(message_timeout_ms) = self.message_timeout_ms {
            client_config.set(
                "message.timeout.ms",
                message_timeout_ms.to_string().as_str(),
            );
        }
        client_config
    }
}

/// librdkafka consumer client configurations
/// see https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md for more detail.
pub struct ConsumerConfig {
    /// Initial list of brokers as a CSV list of broker host or host:port.
    pub metadata_broker_list: KafkaBrokers,

    /// librdkafka statistics emit interval.
    /// range: 0 .. 86400000
    /// default: 0
    /// A value of 0 disables statistics.
    pub statistics_interval_ms: Option<u32>,

    /// Request broker's supported API versions to adjust functionality to available protocol features.
    /// default: true
    pub api_version_request: Option<bool>,

    /// Protocol used to communicate with brokers.
    pub security_protocol: Option<SecurityProtocol>,

    /// Client group id string.
    pub group_id: String,

    /// Client group session and failure detection timeout.
    /// range: 1 .. 3600000
    /// default: 10000
    pub session_timeout_ms: Option<u32>,

    /// Maximum allowed time between calls to consume messages (e.g., rd_kafka_consumer_poll()) for high-level consumers.
    /// range: 1 .. 86400000
    /// default: 300000
    pub max_poll_interval_ms: Option<u32>,

    /// Automatically and periodically commit offsets in the background.
    /// default: true
    pub enable_auto_commit: Option<bool>,

    /// Automatically store offset of last message provided to application.
    /// default: true
    pub enable_auto_offset_store: Option<bool>,

    /// Controls how to read messages written transactionally
    /// read_committed - only return transactional messages which have been committed.
    /// read_uncommitted - return all messages, even transactional messages which have been aborted.
    /// default: read_committed
    pub isolation_level: Option<IsolationLevel>,

    /// Action to take when there is no initial offset in offset store or the desired offset is out of range
    /// 'smallest','earliest' - automatically reset the offset to the smallest offset
    /// 'largest','latest' - automatically reset the offset to the largest offset
    /// 'error' - trigger an error which is retrieved by consuming messages and checking 'message->err'.
    pub auto_offset_reset: Option<AutoOffsetReset>,
}

impl ConsumerConfig {
    pub fn new_with_defaults(brokers: KafkaBrokers, group_id: String) -> Self {
        Self {
            metadata_broker_list: brokers,
            statistics_interval_ms: None,
            api_version_request: None,
            security_protocol: None,
            group_id,
            session_timeout_ms: None,
            max_poll_interval_ms: None,
            enable_auto_commit: None,
            enable_auto_offset_store: None,
            isolation_level: None,
            auto_offset_reset: None,
        }
    }
}

impl Into<ClientConfig> for ConsumerConfig {
    fn into(self) -> ClientConfig {
        let mut client_config = ClientConfig::new();
        client_config.set(
            "bootstrap.servers",
            self.metadata_broker_list.socket_addresses().as_str(),
        );
        client_config.set("group.id", self.group_id.to_string().as_str());
        if let Some(statistics_interval_ms) = self.statistics_interval_ms {
            client_config.set(
                "statistics.interval.ms",
                statistics_interval_ms.to_string().as_str(),
            );
        }
        if let Some(api_version_request) = self.api_version_request {
            client_config.set(
                "api.version.request",
                api_version_request.to_string().as_str(),
            );
        }
        if let Some(security_protocol) = self.security_protocol {
            client_config.set("security.protocol", security_protocol.to_string().as_str());
        }
        if let Some(session_timeout_ms) = self.session_timeout_ms {
            client_config.set(
                "session.timeout.ms",
                session_timeout_ms.to_string().as_str(),
            );
        }
        if let Some(max_poll_interval_ms) = self.max_poll_interval_ms {
            client_config.set(
                "max.poll.interval.ms",
                max_poll_interval_ms.to_string().as_str(),
            );
        }
        if let Some(enable_auto_commit) = self.enable_auto_commit {
            client_config.set(
                "enable.auto.commit",
                enable_auto_commit.to_string().as_str(),
            );
        }
        if let Some(enable_auto_offset_store) = self.enable_auto_offset_store {
            client_config.set(
                "enable.auto.offset.store",
                enable_auto_offset_store.to_string().as_str(),
            );
        }
        if let Some(isolation_level) = self.isolation_level {
            client_config.set("isolation.level", isolation_level.to_string().as_str());
        }
        if let Some(auto_offset_reset) = self.auto_offset_reset {
            client_config.set("auto.offset.reset", auto_offset_reset.to_string().as_str());
        }
        client_config
    }
}
