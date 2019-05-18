extern crate clap;
extern crate crossbeam_queue;
extern crate crossbeam_utils;
extern crate notify;

use clap::{App, Arg};
use crossbeam_queue::SegQueue;
use crossbeam_utils::thread::scope;
use std::path::Path;

mod lib;
use lib::{monitor, process};

fn main() {
    let matches = App::new("SArchive")
        .version("0.1.0")
        .author("Andy Georges <itkovian+sarchive@gmail.com>")
        .about("Archive slurm user job scripts.")
        .arg(
            Arg::with_name("archive")
                .long("archive")
                .short("a")
                .takes_value(true)
                .help("Location of the job scripts' archive."),
        )
        .arg(
            Arg::with_name("cluster")
                .long("cluster")
                .short("c")
                .takes_value(true)
                .help("Name of the cluster where the jobs have been submitted to."),
        )
        .arg(
            Arg::with_name("spool")
                .long("spool")
                .short("s")
                .takes_value(true)
                .help(
                    "Location of the Slurm StateSaveLocation (where the job hash dirs are kept).",
                ),
        )
        .get_matches();

    let base = Path::new(
        matches
            .value_of("spool")
            .expect("You must provide the location of the hash dirs."),
    );
    let archive = Path::new(
        matches
            .value_of("archive")
            .expect("You must provide the location of the archive"),
    );

    // TODO: check the base exists
    let jobscript_q = SegQueue::new();
    let hash_range = 0..10;

    scope(|s| {
        for hash in hash_range {
            println!("Watching hash.{}", &hash);
            let h = hash.clone(); 
            let q = &jobscript_q;
            s.spawn(move |_| {
                match monitor(base, h, q) {
                    Ok(_) => println!("Stopped watching hash.{}", h),
                    Err(e) => panic!(format!("Oops: {:?}", e)),
                }});
        }
        let q = &jobscript_q;
        s.spawn(move |_| process(q));
    });
}
