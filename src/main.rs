extern crate clap;
extern crate inotify;

use clap::{Arg, App};


fn main() {

    let matches = App::new("SArchive")
        .version("0.1.0")
        .author("Andy Georges <itkovian+sarchive@gmail.com>")
        .about("Archive slurm user job scripts")
        .arg(Arg::with_name("spool")
            .long("spool")
            .short("s")
            .help("Location of the Slurm StateSaveLocation (where the job hash dirs are kept)")
        )
        .get_matches();

}
