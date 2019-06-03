SArchive
========

[![Build Status](https://travis-ci.org/itkovian/sarchive.svg?branch=master)](https://travis-ci.org/itkovian/sarchive)
[![Coverage Status](https://coveralls.io/repos/github/itkovian/sarchive/badge.svg)](https://coveralls.io/github/itkovian/sarchive)

[![crates.io](https://img.shields.io/crates/v/sarchive.svg)](https://crates.io/crates/sarchive)

Archival tool for Slurm job scripts and accompanying environments.

## Minimum supported `rustc`

`1.34.2+`

This version is what we test against in CI. We also test on 
    - stable
    - beta
    - nightly
for both Linux and MaxOS.:w


## Usage

`sarchive` requires that two paths are provided:
    - The Slurm spool directory where the `hash.[0-9]` directories can be found
    - The archive directory, where the copied scripts and environments will be 
      stored. This directory is created, if it does not exist.

The archive can be further divided into subdirectories per
    - year: YYYY, by provinging `--period=yearly`
    - month: YYYYMM, by providing `--period=montly`
    - day: YYYYMMDD, by providing `--period=daily`
This allows for easily tarring old(er) directories you still wish to keep around, 
but probably no longer immediately need for user support.

`sarchive -s /var/spool/slurm -a /var/backups/slurm/job-archive`

## RPMs

We provide a build script to generate an RPM using the cargo-rpm tool. You may tailor the spec 
file (listed under the `.rpm` directory) to fit your needs. The RPM includes a unit file so
`sarchive` can be started as a service by systemd. This file should also be changed to fit your
requirements and local configuration.

