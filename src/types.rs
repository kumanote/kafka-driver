use std::fmt::{Display, Error, Formatter};

pub type ProduceResult = (i32, i64);

#[derive(Clone, Debug)]
pub struct KafkaBrokers {
    pub brokers: Vec<KafkaBroker>,
}

impl KafkaBrokers {
    pub fn new(brokers: Vec<KafkaBroker>) -> Self {
        Self { brokers }
    }
    pub fn socket_addresses(&self) -> String {
        let socket_addresses: Vec<String> = self.brokers.iter().map(|x| format!("{}", x)).collect();
        socket_addresses.join(",")
    }
}

#[derive(Clone, Debug)]
pub struct KafkaBroker {
    pub host: String,
    pub port: u16,
}

impl KafkaBroker {
    pub fn new(host: String, port: u16) -> Self {
        Self { host, port }
    }
    pub fn socket_address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

impl Display for KafkaBroker {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}", self.socket_address())
    }
}

impl Default for KafkaBroker {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 9092,
        }
    }
}

pub enum SecurityProtocol {
    PlainText,
    SSL,
    SaslPlainText,
    SaslSSL,
}

impl ToString for SecurityProtocol {
    fn to_string(&self) -> String {
        match *self {
            SecurityProtocol::PlainText => "plaintext".into(),
            SecurityProtocol::SSL => "ssl".into(),
            SecurityProtocol::SaslPlainText => "sasl_plaintext".into(),
            SecurityProtocol::SaslSSL => "sasl_ssl".into(),
        }
    }
}

impl Default for SecurityProtocol {
    fn default() -> Self {
        SecurityProtocol::PlainText
    }
}

pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
}

impl ToString for IsolationLevel {
    fn to_string(&self) -> String {
        match *self {
            IsolationLevel::ReadUncommitted => "read_uncommitted".into(),
            IsolationLevel::ReadCommitted => "read_committed".into(),
        }
    }
}

impl Default for IsolationLevel {
    fn default() -> Self {
        IsolationLevel::ReadUncommitted
    }
}

pub enum AutoOffsetReset {
    Smallest,
    Earliest,
    Beginning,
    Largest,
    Latest,
    End,
    Error,
}

impl ToString for AutoOffsetReset {
    fn to_string(&self) -> String {
        match *self {
            AutoOffsetReset::Smallest => "smallest".into(),
            AutoOffsetReset::Earliest => "earliest".into(),
            AutoOffsetReset::Beginning => "beginning".into(),
            AutoOffsetReset::Largest => "largest".into(),
            AutoOffsetReset::Latest => "latest".into(),
            AutoOffsetReset::End => "end".into(),
            AutoOffsetReset::Error => "error".into(),
        }
    }
}

impl Default for AutoOffsetReset {
    fn default() -> Self {
        AutoOffsetReset::Largest
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_brokers() {
        let brokers: KafkaBrokers = KafkaBrokers::new(vec![KafkaBroker::default()]);
        let socket_addresses = brokers.socket_addresses();
        assert_eq!(socket_addresses, "localhost:9092");

        let brokers: KafkaBrokers = KafkaBrokers::new(vec![
            KafkaBroker::new("localhost".into(), 9092),
            KafkaBroker::new("localhost".into(), 9093),
        ]);
        let socket_addresses = brokers.socket_addresses();
        assert_eq!(socket_addresses, "localhost:9092,localhost:9093");
    }
}
