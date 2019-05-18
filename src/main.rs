extern crate clap;
extern crate notify;
extern crate rayon;

use clap::{App, Arg};
use rayon::ThreadPoolBuilder;
use std::path::Path;

mod lib;
use lib::watch_and_archive;

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
    let pool = ThreadPoolBuilder::new().num_threads(10).build().unwrap();
    pool.scope(|s| {
        for hash in 0..10 {
            println!("Watching hash.{}", &hash);
            let b = &base;
            let a = &archive;
            let h = hash.clone();
            s.spawn(move |_| match watch_and_archive(a, b, &h) {
                Ok(_) => println!("Stopped watching hash.{}", &hash),
                Err(e) => panic!(format!("Oops: {:?}", e)),
            });
        }
    });
}
