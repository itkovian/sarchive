SArchive
========

[![Build Status](https://travis-ci.org/itkovian/sarchive.svg?branch=master)](https://travis-ci.org/itkovian/sarchive)
[![Coverage Status](https://coveralls.io/repos/github/itkovian/sarchive/badge.svg)](https://coveralls.io/github/itkovian/sarchive)

[![crates.io](https://img.shields.io/crates/v/sarchive.svg)](https://crates.io/crates/sarchive)
Archival tool for Slurm job scripts and accompanying environments.

## Minimum supported `rustc`

`1.34.2+`

This version is what we test against in CI.

## Usage

`sarchive` requires that two paths are provided:
    - The Slurm spool directory where the `hash.[0-9]` directories can be found
    - The archive directory, which should exist and be accesible to the user running sarchive.

`sarchive -s /var/spool/slurm -a /var/backups/slurm/job-archive`

## RPMs

We provide a build script to generate an RPM using the cargo-rpm tool. You should tailor the spec file (under `.rpm`) to your needs.

