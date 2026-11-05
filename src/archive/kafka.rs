/*
Copyright 2019-2024 Andy Georges <itkovian+sarchive@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

use super::Archive;
use crate::scheduler::job::JobInfo;
use chrono::{DateTime, Utc};
use clap::{Args, ValueEnum};
use log::{debug, info};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseRecord, DefaultProducerContext, ThreadedProducer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use strum::Display;

/// Command line options for the kafka archiver subcommand
#[derive(Args, Debug)]
pub struct KafkaArgs {
    #[arg(
        long,
        env = "SARCHIVE_KAFKA_BROKERS",
        help = "Comma-separated list of brokers"
    )]
    brokers: String,

    #[arg(long, env = "SARCHIVE_KAFKA_TOPIC", help = "Topic under which to send messages to Kafka", default_value_t = String::from("sarchive"))]
    topic: String,

    #[arg(long, env = "SARCHIVE_KAFKA_MESSAGE_TIMEOUT", help = "Message timeout in ms", default_value_t = String::from("5000"))]
    message_timeout: String,

    #[arg(long, env = "SARCHIVE_KAFKA_SECURITY_PROTOCOL", help = "Protocol used to communicate with Kafka", default_value_t = SecurityProtocol::Plaintext)]
    security_protocol: SecurityProtocol,

    #[arg(
        long,
        env = "SARCHIVE_KAFKA_SSL",
        help = "SSL options for the underlying Kafka lib, as comma-separated key=value pairs"
    )]
    ssl: Option<String>,

    #[arg(
        long,
        env = "SARCHIVE_KAFKA_SASL",
        help = "SASL options for the underlying Kafka lib, as comma-separated key=value pairs"
    )]
    sasl: Option<String>,

    #[arg(
        long,
        env = "SARCHIVE_KAFKA_SASL_PASSWORD",
        hide_env_values = true,
        help = "SASL password (sets sasl.password). Prefer the environment variable over the command line, which is visible to other users."
    )]
    sasl_password: Option<String>,
}

/// Parses a comma-separated list of `key=value` pairs. Values may themselves
/// contain `=`. Malformed entries are an error rather than being dropped.
fn parse_props(s: &str) -> Result<Vec<(&str, &str)>, Error> {
    s.split(',')
        .filter(|e| !e.trim().is_empty())
        .map(|e| {
            e.split_once('=')
                .map(|(k, v)| (k.trim(), v))
                .filter(|(k, _)| !k.is_empty())
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidInput,
                        // never echo the entry: it may contain a secret
                        "Malformed Kafka option, expected comma-separated key=value pairs",
                    )
                })
        })
        .collect()
}

#[allow(non_camel_case_types)]
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Display, ValueEnum, Debug)]
// Keep Display in sync with the spelling clap's ValueEnum accepts (kebab-case), so
// `default_value_t` round-trips. KafkaArchive::new maps it to librdkafka's SASL_SSL etc.
#[strum(serialize_all = "kebab-case")]
pub enum SecurityProtocol {
    Plaintext,
    Ssl,
    Sasl_plaintext,
    Sasl_ssl,
}

/// Returns true if the librdkafka property is likely to hold a secret
/// (passwords, private keys, tokens, ...) and must never be logged.
fn is_sensitive(key: &str) -> bool {
    let k = key.to_lowercase();
    [
        "password",
        "secret",
        "key",
        "token",
        "oauthbearer",
        "credential",
    ]
    .iter()
    .any(|s| k.contains(s))
}

/// Returns the value to show in logs for the given property: the value itself
/// for non-sensitive properties, a placeholder otherwise.
fn loggable<'a>(key: &str, value: &'a str) -> &'a str {
    if is_sensitive(key) {
        "<redacted>"
    } else {
        value
    }
}

/// Returns the property list with sensitive values masked, for use in log output.
fn redacted<'a>(props: &Option<Vec<(&'a str, &'a str)>>) -> Option<Vec<(&'a str, &'a str)>> {
    props
        .as_ref()
        .map(|v| v.iter().map(|(k, val)| (*k, loggable(k, val))).collect())
}

pub struct KafkaArchive {
    producer: ThreadedProducer<DefaultProducerContext>,
    topic: String,
}

impl KafkaArchive {
    /// Creates a new `KafkaArchive` instance with the specified Kafka configuration.
    ///
    /// # Arguments
    ///
    /// * `brokers` - A reference to the Kafka brokers (comma-separated list).
    /// * `topic` - A reference to the Kafka topic for archiving.
    /// * `message_timeout` - A reference to the message timeout duration.
    /// * `security_protocol` - A reference to the `SecurityProtocol` enum indicating the security protocol.
    /// * `ssl` - An optional reference to SSL configuration.
    /// * `sasl` - An optional reference to SASL configuration.
    ///
    /// # Returns
    ///
    /// A new `KafkaArchive` instance.
    ///
    /// # Panics
    ///
    /// Panics if there is an error creating the Kafka producer.
    pub fn new(
        brokers: &String,
        topic: &String,
        message_timeout: &String,
        security_protocol: &SecurityProtocol,
        ssl: &Option<Vec<(&str, &str)>>,
        sasl: &Option<Vec<(&str, &str)>>,
    ) -> Self {
        let mut p = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", message_timeout)
            .set(
                "security.protocol",
                security_protocol
                    .to_string()
                    .to_uppercase()
                    .replace('-', "_"),
            )
            .to_owned();

        if let Some(ssl) = ssl {
            for (k, v) in ssl.iter() {
                debug!(
                    "Setting kafka ssl property {k} with value {}",
                    loggable(k, v)
                );
                p.set(*k, *v);
            }
        }

        if let Some(sasl) = sasl {
            for (k, v) in sasl.iter() {
                debug!(
                    "Setting kafka sasl property {k} with value {}",
                    loggable(k, v)
                );
                p.set(*k, *v);
            }
        }

        KafkaArchive {
            producer: p.create().expect("Cannot create Kafka producer. Aborting."),
            topic: topic.to_owned(),
        }
    }

    /// Builds a `KafkaArchive` instance based on the provided `KafkaArgs`.
    ///
    /// # Arguments
    ///
    /// * `args` - A reference to the `KafkaArgs` struct containing Kafka configuration.
    ///
    /// # Returns
    ///
    /// A `Result` containing the created `KafkaArchive` instance or an error.
    pub fn build(args: KafkaArgs) -> Result<Self, Error> {
        info!(
            "Using Kafka archival, talking to {} on topic {} using protocol {}",
            args.brokers, args.topic, args.security_protocol
        );

        let ssl = args.ssl.as_deref().map(parse_props).transpose()?;

        let mut sasl = args.sasl.as_deref().map(parse_props).transpose()?;
        if let Some(pw) = args.sasl_password.as_deref() {
            sasl.get_or_insert_with(Vec::new)
                .push(("sasl.password", pw));
        }

        debug!("Using ssl options {:?}", redacted(&ssl));
        debug!("Using sasl options {:?}", redacted(&sasl));

        Ok(KafkaArchive::new(
            &args.brokers,
            &args.topic,
            &args.message_timeout,
            &args.security_protocol,
            &ssl,
            &sasl,
        ))
    }
}

#[cfg(feature = "kafka")]
#[derive(Serialize, Deserialize)]
struct JobMessage {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub cluster: String,
    pub script: String,
    pub hostname: String,
    pub environment: Option<HashMap<String, String>>,
}

impl Archive for KafkaArchive {
    fn archive(&self, job_entry: &Box<dyn JobInfo>) -> Result<(), Error> {
        debug!(
            "Kafka archiver, received an entry for job ID {}",
            job_entry.jobid()
        );

        let doc = JobMessage {
            id: job_entry.jobid(),
            timestamp: Utc::now(),
            cluster: job_entry.cluster(),
            hostname: job_entry.hostname(),
            script: job_entry.script(),
            environment: job_entry.extra_info(),
        };

        if let Ok(serial) = serde_json::to_string(&doc) {
            debug!("Serialisation succeeded");
            match self
                .producer
                .send::<str, str>(BaseRecord::to(&self.topic).payload(&serial))
            {
                Ok(_) => {
                    debug!("Message produced correctly");
                    Ok(())
                }
                Err((_e, _)) => {
                    debug!("Could not produce job entry");
                    Ok(())
                }
            }
        } else {
            Err(Error::new(
                ErrorKind::InvalidData,
                "Cannot convert job info to JSON",
            ))
        }
    }
}

#[cfg(feature = "kafka")]
#[cfg(test)]
mod tests {

    use mockito::Server;
    use std::collections::HashMap;
    use std::path::PathBuf;

    use super::*;
    use crate::scheduler::job::JobInfo;

    #[test]
    fn test_parse_props() {
        assert_eq!(
            parse_props("a=1, b=x=y,,c=").unwrap(),
            vec![("a", "1"), ("b", "x=y"), ("c", "")]
        );
        assert!(parse_props("a=1,oops").is_err());
        assert!(parse_props("=1").is_err());
        let err = parse_props("sasl.password").unwrap_err().to_string();
        assert!(!err.contains("sasl.password"));
    }

    #[test]
    fn test_redaction_masks_secrets() {
        for k in [
            "sasl.password",
            "SASL.Password",
            "ssl.key.password",
            "ssl.key.pem",
            "ssl.keystore.password",
            "sasl.oauthbearer.config",
            "sasl.oauthbearer.client.secret",
        ] {
            assert_eq!(loggable(k, "hunter2"), "<redacted>", "{k} not redacted");
        }
        assert_eq!(loggable("sasl.username", "alice"), "alice");
        assert_eq!(loggable("sasl.mechanism", "PLAIN"), "PLAIN");
    }

    #[test]
    fn test_redacted_props_do_not_leak() {
        let props = Some(vec![
            ("sasl.username", "alice"),
            ("sasl.password", "hunter2"),
        ]);
        let out = format!("{:?}", redacted(&props));
        assert!(out.contains("alice"));
        assert!(!out.contains("hunter2"));
    }

    #[derive(Debug)]
    struct DummyJobInfo;

    impl JobInfo for DummyJobInfo {
        fn path(&self) -> PathBuf {
            PathBuf::from("/tmp/test")
        }

        fn jobid(&self) -> String {
            "123".to_string()
        }

        fn moment(&self) -> std::time::Instant {
            std::time::Instant::now()
        }

        fn cluster(&self) -> String {
            "test_cluster".to_string()
        }

        fn hostname(&self) -> String {
            "master".to_string()
        }

        fn read_job_info(&mut self) -> Result<(), std::io::Error> {
            Ok(())
        }

        fn files(&self) -> Vec<(String, Vec<u8>)> {
            vec![
                ("file1.txt".to_string(), b"contents1".to_vec()),
                ("file2.txt".to_string(), b"contents2".to_vec()),
            ]
        }

        fn script(&self) -> String {
            "echo 'Hello, World!'".to_string()
        }

        fn extra_info(&self) -> Option<HashMap<String, String>> {
            Some(HashMap::new())
        }
    }

    #[test]
    fn test_kafka_archive_new() {
        // Mock the Kafka broker
        let mut s = Server::new();
        let _m = s.mock("GET", "/").with_status(200).create();

        let brokers = format!("http://{}", s.url());
        let topic = "test_topic".to_string();
        let message_timeout = "5000".to_string();
        let security_protocol = SecurityProtocol::Plaintext;
        let ssl = None;
        let sasl = None;

        let kafka_archive = KafkaArchive::new(
            &brokers,
            &topic,
            &message_timeout,
            &security_protocol,
            &ssl,
            &sasl,
        );

        // Assert that the KafkaArchive was created successfully
        assert_eq!(kafka_archive.topic, topic);
    }

    #[test]
    fn test_kafka_archive_build() {
        // Mock the Kafka broker
        let mut s = mockito::Server::new();
        let _m = s.mock("GET", "/").with_status(200).create();

        let brokers = format!("http://{}", s.url());
        let topic = "test_topic".to_string();
        let message_timeout = "5000".to_string();
        let security_protocol = SecurityProtocol::Plaintext;
        let ssl = None;
        let sasl = None;

        let kafka_args = KafkaArgs {
            brokers,
            topic: topic.clone(),
            message_timeout,
            security_protocol,
            ssl,
            sasl,
            sasl_password: None,
        };

        let kafka_archive = KafkaArchive::build(kafka_args).unwrap();

        // Assert that the KafkaArchive was built successfully
        assert_eq!(kafka_archive.topic, topic);
    }
}
