/*
Copyright 2019 Andy Georges <itkovian+sarchive@gmail.com>

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
use itertools::Itertools;
use log::{debug, info};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseRecord, DefaultProducerContext, ThreadedProducer};
use serde::{Deserialize, Serialize};
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
            Arg::with_name("message.timeout")
                .long("message.timeout")
                .takes_value(true)
                .default_value("5000")
                .help("Message timout in ms"),
        )
        .arg(
            Arg::with_name("security.protocol")
                .long("security.protocol")
                .takes_value(true)
                .default_value("plaintext")
                .possible_value("plaintext")
                .possible_value("ssl")
                .possible_value("sasl_plaintext")
                .possible_value("sasl_ssl")
                .help("Protocol used to communicate with Kafka"),
        )
        .arg(
            Arg::with_name("ssl")
                .long("ssl")
                .takes_value(true)
                .help("Comma separated list of librdkafka ssl options"),
        )
        .arg(
            Arg::with_name("sasl")
                .long("sasl")
                .takes_value(true)
                .help("Comma separated list of librdkafka sasl options"),
        )
}

pub struct KafkaArchive {
    producer: ThreadedProducer<DefaultProducerContext>,
    topic: String,
}

impl KafkaArchive {
    pub fn new(
        brokers: &str,
        topic: &str,
        message_timeout: &str,
        ssl: &Option<Vec<(&str, &str)>>,
        sasl: &Option<Vec<(&str, &str)>>,
    ) -> Self {
        let mut p = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", message_timeout)
            .to_owned();

        if let Some(ssl) = ssl {
            for (k, v) in ssl.iter() {
                p.set(k, v);
            }
        }

        if let Some(sasl) = sasl {
            for (k, v) in sasl.iter() {
                p.set(k, v);
            }
        }

        KafkaArchive {
            producer: p.create().expect("Cannot create Kafka producer. Aborting."),
            topic: topic.to_owned(),
        }
    }

    pub fn build(matches: &ArgMatches) -> Result<Self, Error> {
        info!(
            "Using Kafka archival, talking to {} on topic {} using protocol {}",
            matches.value_of("brokers").unwrap(),
            matches.value_of("topic").unwrap(),
            matches.value_of("security.protocol").unwrap()
        );

        let ssl = matches.value_of("ssl").map(|ssl| {
            ssl.split(',')
                .map(|s| s.split('='))
                .flatten()
                .tuples()
                .collect()
        });
        let sasl = matches.value_of("sasl").map(|sasl| {
            sasl.split(',')
                .map(|s| s.split('='))
                .flatten()
                .tuples()
                .collect()
        });

        debug!("Using ssl options {:?}", ssl);
        debug!("Using sasl options {:?}", sasl);

        Ok(KafkaArchive::new(
            matches.value_of("brokers").unwrap(),
            matches.value_of("topic").unwrap(),
            matches.value_of("message_timeout").unwrap(),
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
