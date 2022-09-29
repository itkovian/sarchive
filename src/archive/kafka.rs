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
use clap::{Args, ValueEnum};
use enum_display_derive::Display;
use itertools::Itertools;
use log::{debug, info};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseRecord, DefaultProducerContext, ThreadedProducer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;
use std::io::{Error, ErrorKind};

#[derive(Args)]
pub struct KafkaArgs {
    #[arg(long, help = "Comma-separated list of brokers")]
    brokers: String,

    #[arg(long, help = "Topic under which to send messages to Kafka", default_value_t = String::from("sarchive"))]
    topic: String,

    #[arg(long, help = "Message timeout in ms", default_value_t = String::from("5000"))]
    message_timeout: String,

    #[arg(long, help = "Protocol used to communicate with Kafka", default_value_t = SecurityProtocol::Plaintext)]
    security_protocol: SecurityProtocol,

    #[arg(long, help = "SSL options for the underlying Kafka lib")]
    ssl: Option<String>,

    #[arg(long, help = "SASL options for the underlying Kafka lib")]
    sasl: Option<String>,
}

#[allow(non_camel_case_types)]
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Display, ValueEnum)]
pub enum SecurityProtocol {
    Plaintext,
    Ssl,
    Sasl_plaintext,
    Sasl_ssl,
}

pub struct KafkaArchive {
    producer: ThreadedProducer<DefaultProducerContext>,
    topic: String,
}

impl KafkaArchive {
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
                debug!("Setting kafka ssl property {k} with value {v}");
                p.set(*k, *v);
            }
        }

        if let Some(sasl) = sasl {
            for (k, v) in sasl.iter() {
                debug!("Setting kafka sasl property {k} with value {v}");
                p.set(*k, *v);
            }
        }

        KafkaArchive {
            producer: p.create().expect("Cannot create Kafka producer. Aborting."),
            topic: topic.to_owned(),
        }
    }

    pub fn build(args: &KafkaArgs) -> Result<Self, Error> {
        info!(
            "Using Kafka archival, talking to {} on topic {} using protocol {}",
            args.brokers,
            args.topic,
            args.security_protocol
        );

        let ssl = args
            .ssl
            .as_ref()
            .map(|s| s.split(',').flat_map(|s| s.split('=')).tuples().collect());

        let sasl = args
            .sasl
            .as_ref()
            .map(|s| s.split(',').flat_map(|s| s.split('=')).tuples().collect());

        debug!("Using ssl options {ssl:?}");
        debug!("Using sasl options {sasl:?}");

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
