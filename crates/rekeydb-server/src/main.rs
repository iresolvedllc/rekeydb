use std::process::ExitCode;

use rekeydb_server::Config;
use rekeydb_server::config::{HELP, Startup};

fn main() -> ExitCode {
    let startup = match Config::from_args(std::env::args()) {
        Ok(startup) => startup,
        Err(err) => {
            eprintln!("rekeydb: {err}");
            return ExitCode::FAILURE;
        }
    };
    match startup {
        Startup::Help => {
            print!("{HELP}");
            ExitCode::SUCCESS
        }
        Startup::Version => {
            println!("rekeydb {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Startup::Run(config) => match rekeydb_server::run(config) {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("rekeydb: {err}");
                ExitCode::FAILURE
            }
        },
    }
}
