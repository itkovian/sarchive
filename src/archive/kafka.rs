/*
Copyright 2019-2020 Andy Georges <itkovian+sarchive@gmail.com>

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
use clap::{App, Arg, ArgMatches, SubCommand};
use log::{debug, info};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseRecord, DefaultProducerContext, ThreadedProducer};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};

pub fn clap_subcommand(command: &str) -> App {
    SubCommand::with_name(command)
        .about("Archive to Kafka")
        .arg(
            Arg::with_name("brokers")
                .long("brokers")
                .takes_value(true)
                .default_value("localhost:9092")
                .help("Comma-separated list of brokers"),
        )
        .arg(
            Arg::with_name("topic")
                .long("topic")
                .takes_value(true)
                .default_value("sarchive")
                .help("Topic under which to send messages to Kafka"),
        )
        .arg(
            Arg::with_name("message_timeout")
                .long("message.timeout")
                .takes_value(true)
                .default_value("5000")
                .help("Message timout in ms"),
        )
}

/// An archiver to Kafka
pub struct KafkaArchive {
    /// The producer sending messages
    producer: ThreadedProducer<DefaultProducerContext>,
    /// The topic to produce to
    topic: String,
}

impl KafkaArchive {

    /// Returns a new archiver that produces messagesto Kafka
    ///
    /// # Arguments
    ///
    /// * `brokers` - a str slice that contains a comma-separated
    ///               list of brokers to talk to. A single broker
    ///               usually suffices, as the first information
    ///               that is retrieved is the full list of brokers
    ///               that comprise the Kafka service
    /// * `topic` - a str slice representing the topic to produce to
    /// * `message_timeout` - timeout in ms for sent messages
    pub fn new(brokers: &str, topic: &str, message_timeout: &str) -> Self {
        KafkaArchive {
            producer: ClientConfig::new()
                .set("bootstrap.servers", brokers)
                .set("message.timeout.ms", message_timeout)
                .create()
                .expect("Cannot create Kafka producer. Aborting."),
            topic: topic.to_owned(),
        }
    }

    pub fn build(matches: &ArgMatches) -> Result<Self, Error> {
        info!(
            "Using Kafka archival, talking to {} on topic {}",
            matches.value_of("brokers").unwrap(),
            matches.value_of("topic").unwrap()
        );
        Ok(KafkaArchive::new(
            matches.value_of("brokers").unwrap(),
            matches.value_of("topic").unwrap(),
            matches.value_of("message_timeout").unwrap(),
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

#[cfg(test)]
mod tests {}
