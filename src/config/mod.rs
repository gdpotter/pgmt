pub mod builder;
pub mod defaults;
pub mod filter;
pub mod merge;
pub mod types;

#[cfg(test)]
mod tests;

pub use builder::ConfigBuilder;
pub use filter::ObjectFilter;
pub use types::*;

use anyhow::Result;
use std::path::Path;

/// Main configuration loading function
pub fn load_config(config_file: &str) -> Result<(ConfigInput, std::path::PathBuf)> {
    let config_dir = Path::new(config_file)
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();

    let config_input = if Path::new(config_file).exists() {
        let contents = std::fs::read_to_string(config_file)?;
        serde_yaml::from_str(&contents)?
    } else {
        ConfigInput::default()
    };

    Ok((config_input, config_dir))
}
