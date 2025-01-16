use core::{fmt, option::Option};
use std::{collections::HashSet, fs::File, path::PathBuf};

use clap::Parser;
use libbtrfsrs::{
    tree_search::{Item, Tree},
    Subvolume, TreeSearch,
};

#[derive(Debug)]
struct Error {
    message: String,
    source: Option<Box<dyn std::error::Error>>,
}

#[derive(clap::Parser)]
struct Args {
    #[arg(long)]
    subvolume: PathBuf,
    #[arg(long)]
    root: PathBuf,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for Error {}

fn main() {
    match run() {
        Ok(_) => (),
        Err(e) => {
            eprintln!("{e}");
        }
    }
}

fn run() -> Result<(), Error> {
    let args = Args::parse();

    let root_file = File::open(args.root.as_path()).map_err(|e| Error {
        message: "failed to open root".to_string(),
        source: Some(Box::new(e)),
    })?;

    let target_file = File::open(args.subvolume.as_path()).map_err(|e| Error {
        message: "failed to open target subvol".to_string(),
        source: Some(Box::new(e)),
    })?;

    let target_subvol = match Subvolume::new(&target_file) {
        Ok(Some(subvol)) => subvol,
        Ok(Option::None) => {
            return Err(Error {
                message: "target parameter is not a subvolume".to_string(),
                source: None,
            })
        }
        Err(e) => {
            return Err(Error {
                message: "target while opening root subvolume".to_string(),
                source: Some(Box::new(e)),
            })
        }
    };

    let target_info = target_subvol.info().map_err(|e| Error {
        message: "failed to fetch subvolume info".to_string(),
        source: Some(Box::new(e)),
    })?;

    let mut extents: HashSet<(u64, u64)> = HashSet::new();

    eprintln!("collecting target subvolume extents");

    for item in TreeSearch::search_all(&target_file, Tree::Subvol(target_info.tree_id)) {
        match item {
            Ok((_, Item::FileExtentReg(extent))) => {
                extents.insert((extent.disk_bytenr.get(), extent.disk_num_bytes.get()));
            }
            Ok(_) => continue,
            Err(e) => {
                return Err(Error {
                    message: "error walking target subvolume tree".to_string(),
                    source: Some(Box::new(e)),
                })
            }
        }
    }

    let total_use = extents.iter().map(|(_, count)| *count).sum::<u64>();

    for item in TreeSearch::search_all(&root_file, Tree::Root) {
        match item {
            Ok((key, Item::RootBackRef(back_ref))) if key.objectid != target_info.tree_id => {
                eprintln!(
                    "removing subvolume {}s extents from the set",
                    back_ref.name.as_path().to_str().unwrap()
                );
                for item in TreeSearch::search_all(&root_file, Tree::Subvol(key.objectid)) {
                    match item {
                        Ok((_, Item::FileExtentReg(extent))) => {
                            if extents
                                .contains(&(extent.disk_bytenr.get(), extent.disk_num_bytes.get()))
                            {
                                extents.remove(&(
                                    extent.disk_bytenr.get(),
                                    extent.disk_num_bytes.get(),
                                ));
                            }
                        }
                        Ok(_) => continue,
                        Err(e) => {
                            return Err(Error {
                                message: "error walking target subvolume tree".to_string(),
                                source: Some(Box::new(e)),
                            })
                        }
                    }
                }
            }
            Ok(_) => continue,
            Err(e) => {
                return Err(Error {
                    message: "error walking root tree".to_string(),
                    source: Some(Box::new(e)),
                })
            }
        }
    }

    let exclusive_use = extents.iter().map(|(_, count)| *count).sum::<u64>();

    println!(
        "total: {}\nexclusive: {}",
        to_mb(total_use),
        to_mb(exclusive_use)
    );

    Ok(())
}

fn to_mb(i: u64) -> u64 {
    i / 1024u64.pow(2)
}
