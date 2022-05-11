use std::io::Write;

use log::{LevelFilter, SetLoggerError};
use simplelog::{ColorChoice, ConfigBuilder, TermLogger, TerminalMode, WriteLogger};

pub fn init_write_logger<W: Write + Send + 'static>(writer: W) -> Result<(), SetLoggerError> {
    let config = ConfigBuilder::default()
        .set_max_level(LevelFilter::Info)
        .set_time_level(LevelFilter::Info)
        .set_time_format_rfc3339()
        .build();
    WriteLogger::init(LevelFilter::Info, config, writer)
}

pub fn init_term_logger() -> Result<(), SetLoggerError> {
    let config = ConfigBuilder::default()
        .set_max_level(LevelFilter::Info)
        .set_time_level(LevelFilter::Info)
        .set_time_format_rfc3339()
        .build();
    TermLogger::init(
        LevelFilter::Info,
        config,
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )
}
