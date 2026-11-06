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

use clap::Parser;

use crossbeam_channel::{bounded, unbounded};
use crossbeam_utils::sync::Parker;
use crossbeam_utils::thread::scope;
use gethostname::gethostname;
use log::{error, info};
use regex::Regex;
use std::path::PathBuf;
use std::process::exit;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

mod archive;
mod monitor;
mod scheduler;
mod utils;

use archive::{archive_builder, process, Archive, Archiver};

use monitor::monitor;
use scheduler::{create, SchedulerKind};
use utils::{register_signal_handler, signal_handler_atomic};

fn setup_logging(debug: bool, logfile: Option<PathBuf>) -> Result<(), log::SetLoggerError> {
    let level_filter = if debug {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Info
    };

    let base_config = fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{}][{}][{}] {}",
                chrono::Local::now().to_rfc3339(),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(level_filter);

    match logfile {
        Some(filename) => {
            let r = fern::log_reopen1(&filename, std::iter::once(libc::SIGHUP)).unwrap();
            base_config.chain(r)
        }
        None => base_config.chain(std::io::stdout()),
    }
    .apply()
}

#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    #[arg(
        long,
        help = "Name of the cluster where the jobs have been submitted to."
    )]
    cluster: String,

    #[arg(long)]
    debug: bool,

    #[arg(
        long,
        help = "[Experimental] Process already received events when the program is terminated with SIGINT or SIGTERM"
    )]
    cleanup: bool,

    #[arg(long, help = "Log file name.")]
    logfile: Option<PathBuf>,

    #[arg(long)]
    torque_subdirs: bool,

    #[arg(long)]
    spool: PathBuf,

    #[arg(long, required = true)]
    scheduler: SchedulerKind,

    #[arg(
        long,
        help = "Regex; job environment variables whose names match it are left out of the archive (Slurm only)."
    )]
    filter_regex: Option<String>,

    #[command(subcommand)]
    archiver: Archiver,
}

/// Compiles the optional filter regex. An invalid pattern is an error, never ignored.
fn parse_filter_regex(pattern: Option<&str>) -> Result<Option<Regex>, regex::Error> {
    pattern
        .map(|r| {
            info!("Setting filter regex to {}", r);
            Regex::new(r)
        })
        .transpose()
}

fn main() -> Result<(), std::io::Error> {
    //let matches = args();
    let cli = Cli::parse();

    match setup_logging(cli.debug, cli.logfile) {
        Ok(_) => (),
        Err(e) => panic!("Cannot set up logging: {e:?}"),
    };
    let base = cli.spool.to_owned();

    // FIXME: Check for permissions to read directory contents
    if !base.is_dir() {
        error!("Provided spool {:?} is not a valid directory", base);
        exit(1);
    }

    let scheduler = cli.scheduler;
    // Fail before doing anything else: a filter that silently does not apply would let
    // environment variables that must not be archived through.
    let filter_regex = match parse_filter_regex(cli.filter_regex.as_deref()) {
        Ok(r) => r,
        Err(e) => {
            error!("Invalid --filter-regex: {}", e);
            exit(1);
        }
    };
    let archiver: Box<dyn Archive> = match archive_builder(cli.archiver) {
        Ok(a) => a,
        Err(e) => {
            error!("Cannot set up the archiver: {}", e);
            exit(1);
        }
    };
    let cluster = cli.cluster;
    let hostname = gethostname().to_string_lossy().to_string();

    info!("sarchive starting. Watching spool {:?}.", base);

    let notification = Arc::new(AtomicBool::new(false));
    let parker = Parker::new();
    let unparker = parker.unparker();

    register_signal_handler(signal_hook::consts::SIGTERM, unparker, &notification);
    register_signal_handler(signal_hook::consts::SIGINT, unparker, &notification);

    let (sig_sender, sig_receiver) = bounded(20);
    let cleanup = cli.cleanup;

    // we will watch the locations provided by the scheduler
    let (sender, receiver) = unbounded();
    let sched = create(&scheduler, &base, &cluster, &hostname, &filter_regex);
    if let Err(e) = scope(|s| {
        let ss = &sig_sender;
        s.spawn(move |_| {
            signal_handler_atomic(ss, notification, &parker);
            info!("Signal handled");
        });

        for loc in sched.watch_locations() {
            let t = &sender;
            let sr = &sig_receiver;
            let sl = &sched;
            let b = &base;
            s.spawn(move |_| match monitor(sl, &loc, t, sr) {
                Ok(_) => info!("Stopped watching location {:?}", loc),
                Err(e) => {
                    error!("{:?}", e);
                    panic!("Error watching {:?}", b);
                }
            });
        }

        let r = &receiver;
        let sr = &sig_receiver;
        s.spawn(move |_| {
            match process(archiver, r, sr, cleanup) {
                Ok(()) => info!("Processing completed successfully"),
                Err(e) => error!("processing failed: {:?}", e),
            };
        });
    }) {
        error!("sarchive stopping due to error: {:?}", e);
        exit(1);
    };

    info!("sarchive finished");
    exit(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_filter_regex_none() {
        assert!(parse_filter_regex(None).unwrap().is_none());
    }

    #[test]
    fn test_parse_filter_regex_valid() {
        let r = parse_filter_regex(Some("^EBROOT|^EBVERSION"))
            .unwrap()
            .unwrap();
        assert!(r.is_match("EBROOTGCC"));
        assert!(!r.is_match("HOME"));
    }

    #[test]
    fn test_parse_filter_regex_invalid_is_an_error() {
        assert!(parse_filter_regex(Some("([unclosed")).is_err());
    }
}
