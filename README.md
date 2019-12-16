SArchive
========

[![crates.io](https://img.shields.io/crates/v/sarchive.svg)](https://crates.io/crates/sarchive)
[![Travis Build Status](https://travis-ci.org/itkovian/sarchive.svg?branch=master)](https://travis-ci.org/itkovian/sarchive)
[![Actions Status](https://github.com/itkovian/sarchive/workflows/sarchive%20tests/badge.svg)](https://github.com/itkovian/sarchive/actions)
[![Coverage Status](https://coveralls.io/repos/github/itkovian/sarchive/badge.svg)](https://coveralls.io/github/itkovian/sarchive)
[![License](https://img.shields.io/github/license/itkovian/sarchive)](https://opensource.org/licenses/MIT)


Archival tool for scheduler job scripts and accompanying files.

Note that the version on crates.io is older and only supports Slurm and archival to files. The reason is that we rely on several 3rd party crates that need to be published before we can publish a new version.

## Minimum supported `rustc`

`1.36`

This version is what we test against in CI. We also test on
  - stable
  - beta
  - nightly

for both Linux and MacOS.

If you do not have [Rust](https://rustlang.org), please see [Rustup](https://rustup.rs) for installation instructions.

## Usage

`sarchive` requires that the paths to the scheduler's main spool directory is specified.

`sarchive` supports multiple schedulers, the one to use should also be specified
on the command line. Right now, there is support for [Slurm](https://slurm.schedmd.com)
and [Torque](https://adaptivecomputing.com).

Furthermore, `sarchive` offers various backends. The basic backend writes a copy of the job scripts
and associated files to a directory on a mounted filesystem. We also have limited support for
sending job information to [Elasticsearch](https://elastic.co) or produce to
a [Kafka](https://kafka.apache.org) topic. We briefly discuss these backends below.

### File archival

For file archival, `sarchive` requires the path to the archive's top directory.

The archive can be further divided into subdirectories per
  - year: YYYY, by provinding `--period=yearly`
  - month: YYYYMM, by providing `--period=monthly`
  - day: YYYYMMDD, by providing `--period=daily`
This allows for easily tarring old(er) directories you still wish to keep around,
but probably no longer immediately need for user support. Each of these directories
are also created upon file archival if they do not exist.

For example, `sarchive -s /var/spool/slurm -a /var/backups/slurm/job-archive`

### Elasticsearch archival

If you want to maintain the job script archive on another machine and/or make it easily searchable,
the Elasticsearch backend. The shipped data structure contains a timestamp along with the job script
and potentially other relevant information (at the scheduler's discretion).

We do not yet support SSL/TLS or authentication with the ES backend.

### Kafka archival

Similar to ES archival, no SSL/TLS support at this moment. Data is shipped in the same manner.

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

