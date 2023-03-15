SArchive
========

[![crates.io](https://img.shields.io/crates/v/sarchive.svg)](https://crates.io/crates/sarchive)
[![Travis Build Status](https://travis-ci.org/itkovian/sarchive.svg?branch=master)](https://travis-ci.org/itkovian/sarchive)
[![Actions Status](https://github.com/itkovian/sarchive/workflows/sarchive%20tests/badge.svg)](https://github.com/itkovian/sarchive/actions)
[![Coverage Status](https://coveralls.io/repos/github/itkovian/sarchive/badge.svg)](https://coveralls.io/github/itkovian/sarchive)
[![License](https://img.shields.io/github/license/itkovian/sarchive)](https://opensource.org/licenses/MIT)
[![LoC](https://tokei.rs/b1/github/itkovian/sarchive?category=code)](https://github.com/XAMPPRocky/tokei).


Archival tool for scheduler job scripts and accompanying files.

Note that the master branch here may be running ahead of the latest release
on crates.io. During development, we sometimes rely on dependencies
that have not yet released a version with the features we use.

## Minimum supported `rustc`

`1.64.0`

This version is what we test against in CI. We also test on
  - stable
  - nightly

If you do not have [Rust](https://rustlang.org), please see
[Rustup](https://rustup.rs) for installation instructions.

## Usage

`sarchive` requires that the path to the scheduler's main spool directory is
specified. It also requires a `cluster` (name) to be set.

`sarchive` supports multiple schedulers, the one to be used must be specified
on the command line. Right now, there is support for [Slurm](https://slurm.schedmd.com)
and [Torque](https://adaptivecomputing.com).

For Slurm, the directory to watch is defined as the `StateSaveLocation` in the slurm config.

Furthermore, `sarchive` offers various backends. The basic `file` backend
writes a copy of the job scripts and associated files to a directory on a
mounted filesystem. We also have limited support for sending job information
to [Elasticsearch](https://elastic.co) or produce to a
[Kafka](https://kafka.apache.org) topic. We briefly discuss these backends
below.

### File archival

Activated using the `file` subcommand. Note that we do not support using
multiple subcommands (i.e., backends) at this moment.

For file archival, `sarchive` requires the path to the archive's top
directory, i.e., where you want to store the backup scripts and accompanying
files.

The archive can be further divided into subdirectories per
  - year: YYYY, by provinding `--period=yearly`
  - month: YYYYMM, by providing `--period=monthly`
  - day: YYYYMMDD, by providing `--period=daily`
Each of these directories are also created upon file archival if they do
not exist. This allows for easily tarring old(er) directories you still
wish to keep around, but probably no longer immediately need for user support.

For example,

`sarchive --cluster huppel -s /var/spool/slurm file --archive /var/backups/slurm/job-archive`

### Elasticsearch archival (removed)

The Elasticsearch backend will be revamped, as using the elastic crate is subject to a
vulnerability through its hyper dependency (https://rustsec.org/advisories/RUSTSEC-2021-0078)

This will be added again once we can move to the official Elastic.co crate.

### Kafka archival

You can ship the job scripts as messages to Kafka.

For example,

`./sarchive --cluster huppel -l /var/log/sarchive.log -s /var/spool/slurm/ kafka --brokers mykafka.mydomain:9092 --topic slurm-job-archival`

Support for SSL and SASL is available, through the `--ssl` and `--sasl` options. Both of these expect a comma-separated
list of options to pass to the underlying kafka library.

## Features

- Multithreaded, watching one dir per thread, so no need for hierarchical watching.
- Separate processing thread to ensure swift draining of the inotify event queues.
- Clean log rotation when SIGHUP is received.
- Experimental support for clean termination on receipt of SIGTERM or SIGINT, where
  job events that have already been seen are processed, to minimise potential loss
  when restarting the service.
- Output to a file in  a hierarchical directory structure
- Output to Elasticsearch
- Output to Kafka

## RPMs

We provide a build script to generate an RPM using the cargo-rpm tool. You may tailor the spec
file (listed under the `.rpm` directory) to fit your needs. The RPM includes a unit file so
`sarchive` can be started as a service by systemd. This file should also be changed to fit your
requirements and local configuration.

