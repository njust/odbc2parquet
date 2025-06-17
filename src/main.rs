mod connection;
mod enum_args;
#[cfg(feature = "unfinished")]
mod execute;
mod insert;
pub mod model;
mod parquet_buffer;
mod query;
use anyhow::{bail, Error};
use io_arg::IoArg;
use model::{InsertOpt, QueryOpt};
use odbc_api::environment;
use stderrlog::ColorChoice;

use clap::{ArgAction, CommandFactory, Parser};
use clap_complete::{generate, Shell};

/// Query an ODBC data source at store the result in a Parquet file.
#[derive(Parser)]
#[clap(version)]
struct Cli {
    /// Only print errors to standard error stream. Suppresses warnings and all other log levels
    /// independent of the verbose mode.
    #[arg(short = 'q', long)]
    quiet: bool,
    /// Verbose mode (-v, -vv, -vvv, etc)
    ///
    /// 'v': Info level logging
    /// 'vv': Debug level logging
    /// 'vvv': Trace level logging
    #[arg(short = 'v', long, action = ArgAction::Count)]
    verbose: u8,
    #[arg(long)]
    /// Never emit colors.
    ///
    /// Controls the colors of the log output. If specified the log output will never be colored.
    /// If not specified the tool will try to emit Colors, but not force it. If `TERM=dumb` or
    /// `NO_COLOR` is defined, then colors will not be used.
    no_color: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Parser)]
enum Command {
    /// Query a data source and write the result as parquet.
    Query {
        #[clap(flatten)]
        query_opt: QueryOpt,
    },
    /// List available drivers and their attributes.
    ListDrivers,
    /// List preconfigured data sources. Useful to find data source name to connect to database.
    ListDataSources,
    /// Read the content of a parquet and insert it into a table.
    Insert {
        #[clap(flatten)]
        insert_opt: InsertOpt,
    },
    /// Generate shell completions
    Completions {
        #[arg(long, short = 'o', default_value = "-")]
        /// Output file. Defaults to `-` which means standard output.
        output: IoArg,
        /// Name of the shell to generate completions for.
        shell: Shell,
    },
    #[cfg(feature = "unfinished")]
    /// Executes an arbitrary SQL statement using the contents of an parquet file as input arrays.
    Exec {
        #[clap(flatten)]
        exec_opt: ExecOpt,
    },
}

impl Cli {
    /// Perform some validation logic, beyond what is possible (or sensible) to verify directly with
    /// clap.
    pub fn perform_extra_validation(&self) -> Result<(), Error> {
        if let Command::Query { query_opt } = &self.command {
            if !query_opt.output.is_file() {
                if query_opt.file_size_threshold.is_some() {
                    bail!("file-size-threshold conflicts with specifying stdout ('-') as output.")
                }
                if query_opt.row_groups_per_file != 0 {
                    bail!("row-groups-per-file conflicts with specifying stdout ('-') as output.")
                }
            }
        }
        Ok(())
    }
}

fn main() -> Result<(), Error> {
    let opt = Cli::parse();
    opt.perform_extra_validation()?;

    let verbose = if opt.quiet {
        // Log errors, but nothing else
        0
    } else {
        // Log warnings and one additional log level for each `-v` passed in the command line.
        opt.verbose as usize + 1
    };

    let color_choice = if opt.no_color {
        ColorChoice::Never
    } else {
        ColorChoice::Auto
    };

    // Initialize logging
    stderrlog::new()
        .module(module_path!())
        .module("odbc_api")
        .quiet(false) // Even if `opt.quiet` is true, we still want to print errors
        .verbosity(verbose)
        .color(color_choice)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    // Initialize ODBC environment used to create the connection to the Database. We now use the
    // singleton pattern with `environment`. This makes our life easier if using concurrent fetching
    // since it allows us to create an environment with a 'static lifetime. From this point forward
    // in the application, we may assume that calls to environment are succesful, since any error
    // creating the environment must occur now.
    let odbc_env = environment()?;

    match opt.command {
        Command::Query { query_opt } => {
            query::query(query_opt)?;
        }
        Command::Insert { insert_opt } => {
            insert::insert(&insert_opt)?;
        }
        Command::ListDrivers => {
            for driver_info in odbc_env.drivers()? {
                println!("{}", driver_info.description);
                for (key, value) in &driver_info.attributes {
                    println!("\t{key}={value}");
                }
                println!()
            }
        }
        Command::ListDataSources => {
            let mut first = true;
            for data_source_info in odbc_env.data_sources()? {
                // After first item, always place an additional newline in between.
                if first {
                    first = false;
                } else {
                    println!()
                }
                println!("Server name: {}", data_source_info.server_name);
                println!("Driver: {}", data_source_info.driver);
            }
        }
        Command::Completions { shell, output } => {
            let output = output.open_as_output()?;
            let mut output = output.into_write();
            generate(shell, &mut Cli::command(), "odbc2parquet", &mut output);
        }
        #[cfg(feature = "unfinished")]
        Command::Exec { exec_opt } => {
            execute::execute(&exec_opt)?;
        }
    }

    Ok(())
}
