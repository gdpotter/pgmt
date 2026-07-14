use anyhow::{Context, Result, anyhow};
use std::path::Path;
use std::time::Duration;

/// A section within a migration file
#[derive(Debug, Clone, PartialEq)]
pub struct MigrationSection {
    /// Unique name for this section within the migration
    pub name: String,

    /// Optional human-readable description for logging
    pub description: Option<String>,

    /// Transaction mode for this section
    pub mode: TransactionMode,

    /// Maximum execution time for this section (statement_timeout)
    pub timeout: Duration,

    /// Maximum time to wait for locks (lock_timeout)
    pub lock_timeout: Option<Duration>,

    /// Retry configuration (optional)
    pub retry_config: Option<RetryConfig>,

    /// The SQL to execute for this section
    pub sql: String,

    /// The raw header line(s) of this section (the `-- pgmt:section` line plus
    /// any `-- pgmt:` attribute continuation lines), exactly as written.
    /// Empty for the implicit header-less `default` section. It participates
    /// in the per-section checksum because `module=`/`remaps=` attributes are
    /// attribution facts pinned alongside the body.
    pub raw_header: String,

    /// Owning module (modules feature). `None` = the unmoduled base — also
    /// what every pre-modules migration parses as.
    pub module: Option<String>,

    /// Prior owners of this section's objects (re-anchoring baselines only):
    /// module names, or "(unmoduled)" for the base. Establishment derivation
    /// reads these; ordinary migrations never carry them.
    pub remaps: Vec<String>,

    /// Line number where this section starts (for error reporting)
    pub start_line: usize,
}

impl MigrationSection {
    /// The exact content hashed for this section's checksum: the raw header
    /// line(s) followed by the body SQL. The header carries `module=`/`remaps=`
    /// attribution, so it is part of the immutable unit. The header-less
    /// `default` section hashes its body alone.
    pub fn checksum_content(&self) -> String {
        if self.raw_header.is_empty() {
            self.sql.clone()
        } else {
            format!("{}\n{}", self.raw_header, self.sql)
        }
    }
}

/// Transaction execution mode for a section
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionMode {
    /// Wrap section in BEGIN/COMMIT, rollback on error
    Transactional,

    /// Execute without transaction wrapper (for CONCURRENTLY, etc.)
    NonTransactional,

    /// Execute each statement individually with auto-commit
    Autocommit,
}

impl TransactionMode {
    /// The stored/parsed string form of the mode.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Transactional => "transactional",
            Self::NonTransactional => "non-transactional",
            Self::Autocommit => "autocommit",
        }
    }
}

/// Retry configuration for a section
#[derive(Debug, Clone, PartialEq)]
pub struct RetryConfig {
    /// Number of retry attempts (1 = no retry, execute once)
    pub attempts: u32,

    /// Base delay between retry attempts
    pub delay: Duration,

    /// Backoff strategy for retries
    pub backoff: BackoffStrategy,

    /// Action to take on lock timeout specifically
    pub on_lock_timeout: LockTimeoutAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackoffStrategy {
    /// Use constant delay between retries
    None,

    /// Exponential backoff: delay * 2^(attempt - 1)
    Exponential,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockTimeoutAction {
    /// Fail immediately on lock timeout
    Fail,

    /// Retry according to retry_config
    Retry,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            attempts: 1,
            delay: Duration::from_secs(0),
            backoff: BackoffStrategy::None,
            on_lock_timeout: LockTimeoutAction::Fail,
        }
    }
}

/// Parse a migration file into sections
pub fn parse_migration_sections(_file_path: &Path, sql: &str) -> Result<Vec<MigrationSection>> {
    let mut sections = Vec::new();
    let lines: Vec<&str> = sql.lines().collect();
    let mut current_section: Option<SectionBuilder> = None;
    let mut current_sql = String::new();

    for (line_num, line) in lines.iter().enumerate() {
        if line.trim_start().starts_with("-- pgmt:section") {
            // Save previous section if exists
            if let Some(builder) = current_section.take() {
                sections.push(builder.build(current_sql.clone())?);
                current_sql.clear();
            }

            // Start new section
            let mut builder = SectionBuilder::new(line_num + 1);
            builder.raw_header.push_str(line);

            // Parse attributes on the same line (e.g., name="test")
            let rest = line
                .trim_start()
                .trim_start_matches("-- pgmt:section")
                .trim();
            if !rest.is_empty() {
                // Create a pseudo-line for parsing
                let attr_line = format!("-- pgmt:{}", rest);
                parse_section_attribute(&attr_line, &mut builder)?;
            }

            current_section = Some(builder);
        } else if line.trim_start().starts_with("-- pgmt:") {
            // Parse section attribute
            if let Some(builder) = current_section.as_mut() {
                if !builder.raw_header.is_empty() {
                    builder.raw_header.push('\n');
                }
                builder.raw_header.push_str(line);
                parse_section_attribute(line, builder)?;
            }
        } else {
            // Accumulate SQL
            current_sql.push_str(line);
            current_sql.push('\n');
        }
    }

    // Save final section
    if let Some(builder) = current_section {
        sections.push(builder.build(current_sql)?);
    }

    // If no sections found, treat entire file as single section
    if sections.is_empty() {
        sections.push(MigrationSection {
            name: "default".to_string(),
            description: None,
            mode: TransactionMode::Transactional,
            timeout: Duration::from_secs(600), // 10 minute default
            lock_timeout: None,
            retry_config: None,
            sql: sql.to_string(),
            raw_header: String::new(),
            module: None,
            remaps: Vec::new(),
            start_line: 1,
        });
    }

    Ok(sections)
}

/// Parse section attributes from a line (supports multiple key-value pairs)
fn parse_section_attribute(line: &str, builder: &mut SectionBuilder) -> Result<()> {
    let line = line.trim_start().trim_start_matches("-- pgmt:");
    let line = line.trim();

    // Parse multiple key="value" pairs from the line
    for (key, value) in parse_key_value_pairs(line)? {
        match key.as_str() {
            "name" => builder.name = Some(value),
            "mode" => builder.mode = Some(parse_transaction_mode(&value)?),
            "timeout" => builder.timeout = Some(parse_duration(&value)?),
            "description" => builder.description = Some(value),
            "retry_attempts" => builder.retry_attempts = Some(value.parse()?),
            "retry_delay" => builder.retry_delay = Some(parse_duration(&value)?),
            "retry_backoff" => builder.retry_backoff = Some(parse_backoff(&value)?),
            "on_lock_timeout" => builder.on_lock_timeout = Some(parse_lock_action(&value)?),
            "lock_timeout" => builder.lock_timeout = Some(parse_duration(&value)?),
            "module" => builder.module = Some(value),
            "remaps" => {
                // Provenance-cut sections (modules.md §12): a remap section is
                // acquired from exactly ONE prior owner, so `remaps` carries a
                // single source. The comma-list form is retired — a baseline
                // stamped under the old multi-source rule must be regenerated.
                let sources: Vec<String> = value
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                if sources.len() > 1 {
                    return Err(anyhow!(
                        "section 'remaps' at line {} lists {} sources ('{}'); a provenance-cut \
                         remap section is acquired from exactly one prior owner (modules.md §12). \
                         Regenerate this baseline with a current pgmt.",
                        builder.start_line,
                        sources.len(),
                        value
                    ));
                }
                builder.remaps = Some(sources);
            }
            _ => {
                return Err(anyhow!(
                    "Unknown section attribute '{}' at line {}",
                    key,
                    builder.start_line
                ));
            }
        }
    }

    Ok(())
}

/// Parse multiple key="value" pairs from a string
/// Supports: name="table" mode="transactional" timeout="30s"
fn parse_key_value_pairs(input: &str) -> Result<Vec<(String, String)>> {
    let mut pairs = Vec::new();
    let mut chars = input.chars().peekable();

    while chars.peek().is_some() {
        // Skip whitespace
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }

        if chars.peek().is_none() {
            break;
        }

        // Parse key (everything until '=')
        let mut key = String::new();
        while let Some(&ch) = chars.peek() {
            if ch == '=' {
                chars.next(); // consume '='
                break;
            }
            if ch.is_whitespace() {
                return Err(anyhow!("Expected '=' after key '{}'", key));
            }
            key.push(ch);
            chars.next();
        }

        if key.is_empty() {
            break;
        }

        // Expect opening quote
        if chars.next() != Some('"') {
            return Err(anyhow!("Expected '\"' after '{}='", key));
        }

        // Parse value (everything until closing quote)
        let mut value = String::new();
        let mut found_closing_quote = false;
        for ch in chars.by_ref() {
            if ch == '"' {
                found_closing_quote = true;
                break;
            }
            value.push(ch);
        }

        if !found_closing_quote {
            return Err(anyhow!("Missing closing quote for value of '{}'", key));
        }

        pairs.push((key, value));
    }

    Ok(pairs)
}

/// Parse transaction mode from string
fn parse_transaction_mode(s: &str) -> Result<TransactionMode> {
    match s.to_lowercase().as_str() {
        "transactional" => Ok(TransactionMode::Transactional),
        "non-transactional" => Ok(TransactionMode::NonTransactional),
        "autocommit" => Ok(TransactionMode::Autocommit),
        _ => Err(anyhow!("Unknown transaction mode: {}", s)),
    }
}

/// Parse backoff strategy from string
fn parse_backoff(s: &str) -> Result<BackoffStrategy> {
    match s.to_lowercase().as_str() {
        "none" => Ok(BackoffStrategy::None),
        "exponential" => Ok(BackoffStrategy::Exponential),
        _ => Err(anyhow!("Unknown backoff strategy: {}", s)),
    }
}

/// Parse lock timeout action from string
fn parse_lock_action(s: &str) -> Result<LockTimeoutAction> {
    match s.to_lowercase().as_str() {
        "fail" => Ok(LockTimeoutAction::Fail),
        "retry" => Ok(LockTimeoutAction::Retry),
        _ => Err(anyhow!("Unknown lock timeout action: {}", s)),
    }
}

/// Parse duration strings like "30s", "5m", "2h", "500ms", "1m30s"
pub fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    let mut total = Duration::ZERO;
    let mut num_str = String::new();
    let mut chars = s.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch.is_ascii_digit() {
            num_str.push(ch);
        } else if ch.is_alphabetic() {
            // Check for "ms" unit
            let is_ms = ch == 'm' && chars.peek() == Some(&'s');

            let num: u64 = num_str
                .parse()
                .with_context(|| format!("Invalid duration number: {}", num_str))?;

            let unit_duration = if is_ms {
                chars.next(); // consume 's'
                Duration::from_millis(num)
            } else {
                match ch {
                    's' => Duration::from_secs(num),
                    'm' => Duration::from_secs(num * 60),
                    'h' => Duration::from_secs(num * 3600),
                    _ => return Err(anyhow!("Unknown duration unit: {}", ch)),
                }
            };

            total += unit_duration;
            num_str.clear();
        } else if !ch.is_whitespace() {
            return Err(anyhow!("Invalid character in duration: {}", ch));
        }
    }

    if !num_str.is_empty() {
        return Err(anyhow!("Duration missing unit: {}", num_str));
    }

    if total.is_zero() {
        return Err(anyhow!("Invalid duration: {}", s));
    }

    Ok(total)
}

/// Helper struct for building sections
struct SectionBuilder {
    start_line: usize,
    raw_header: String,
    name: Option<String>,
    description: Option<String>,
    mode: Option<TransactionMode>,
    timeout: Option<Duration>,
    lock_timeout: Option<Duration>,
    retry_attempts: Option<u32>,
    retry_delay: Option<Duration>,
    retry_backoff: Option<BackoffStrategy>,
    on_lock_timeout: Option<LockTimeoutAction>,
    module: Option<String>,
    remaps: Option<Vec<String>>,
}

impl SectionBuilder {
    fn new(start_line: usize) -> Self {
        Self {
            start_line,
            raw_header: String::new(),
            name: None,
            description: None,
            mode: None,
            timeout: None,
            lock_timeout: None,
            retry_attempts: None,
            retry_delay: None,
            retry_backoff: None,
            on_lock_timeout: None,
            module: None,
            remaps: None,
        }
    }

    fn build(self, sql: String) -> Result<MigrationSection> {
        let name = self.name.ok_or_else(|| {
            anyhow!(
                "Section at line {} missing 'name' attribute",
                self.start_line
            )
        })?;

        // Apply defaults (same as legacy migrations)
        let mode = self.mode.unwrap_or(TransactionMode::Transactional);
        let timeout = self.timeout.unwrap_or(Duration::from_secs(600)); // 10 minutes

        // Build retry config if any retry attributes specified
        let retry_config = if self.retry_attempts.is_some()
            || self.retry_delay.is_some()
            || self.retry_backoff.is_some()
            || self.on_lock_timeout.is_some()
        {
            Some(RetryConfig {
                attempts: self.retry_attempts.unwrap_or(1),
                delay: self.retry_delay.unwrap_or(Duration::ZERO),
                backoff: self.retry_backoff.unwrap_or(BackoffStrategy::None),
                on_lock_timeout: self.on_lock_timeout.unwrap_or(LockTimeoutAction::Fail),
            })
        } else {
            None
        };

        Ok(MigrationSection {
            name,
            description: self.description,
            module: self.module,
            remaps: self.remaps.unwrap_or_default(),
            mode,
            timeout,
            lock_timeout: self.lock_timeout,
            retry_config,
            sql: sql.trim().to_string(),
            raw_header: self.raw_header,
            start_line: self.start_line,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration_seconds() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("1s").unwrap(), Duration::from_secs(1));
    }

    #[test]
    fn test_parse_duration_minutes() {
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("1m").unwrap(), Duration::from_secs(60));
    }

    #[test]
    fn test_parse_duration_hours() {
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
    }

    #[test]
    fn test_parse_duration_milliseconds() {
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("100ms").unwrap(), Duration::from_millis(100));
    }

    #[test]
    fn test_parse_duration_composite() {
        assert_eq!(parse_duration("1m30s").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("1h30m").unwrap(), Duration::from_secs(5400));
        assert_eq!(
            parse_duration("2h15m30s").unwrap(),
            Duration::from_secs(8130)
        );
    }

    #[test]
    fn test_parse_duration_with_spaces() {
        assert_eq!(parse_duration("30 s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("1m 30s").unwrap(), Duration::from_secs(90));
    }

    #[test]
    fn test_parse_duration_invalid() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("30").is_err());
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("30x").is_err());
    }

    #[test]
    fn test_parse_basic_section() {
        let sql = r#"
-- pgmt:section name="test_section"
-- pgmt:  mode="transactional"
-- pgmt:  timeout="30s"
ALTER TABLE users ADD COLUMN test TEXT;
"#;

        let sections = parse_migration_sections(Path::new("test.sql"), sql).unwrap();

        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].name, "test_section");
        assert_eq!(sections[0].mode, TransactionMode::Transactional);
        assert_eq!(sections[0].timeout, Duration::from_secs(30));
        assert!(sections[0].sql.contains("ALTER TABLE users"));
    }

    #[test]
    fn test_parse_section_with_retry() {
        let sql = r#"
-- pgmt:section name="concurrent_index"
-- pgmt:  mode="non-transactional"
-- pgmt:  timeout="2s"
-- pgmt:  retry_attempts="10"
-- pgmt:  retry_delay="5s"
-- pgmt:  retry_backoff="exponential"
CREATE INDEX CONCURRENTLY idx_test ON users(email);
"#;

        let sections = parse_migration_sections(Path::new("test.sql"), sql).unwrap();

        assert_eq!(sections.len(), 1);
        let section = &sections[0];
        assert_eq!(section.name, "concurrent_index");
        assert_eq!(section.mode, TransactionMode::NonTransactional);

        let retry = section.retry_config.as_ref().unwrap();
        assert_eq!(retry.attempts, 10);
        assert_eq!(retry.delay, Duration::from_secs(5));
        assert_eq!(retry.backoff, BackoffStrategy::Exponential);
    }

    #[test]
    fn test_parse_multiple_sections() {
        let sql = r#"
-- pgmt:section name="section1"
-- pgmt:  mode="transactional"
-- pgmt:  timeout="30s"
ALTER TABLE users ADD COLUMN col1 TEXT;

-- pgmt:section name="section2"
-- pgmt:  mode="non-transactional"
-- pgmt:  timeout="2s"
-- pgmt:  retry_attempts="5"
CREATE INDEX CONCURRENTLY idx ON users(col1);

-- pgmt:section name="section3"
-- pgmt:  mode="transactional"
-- pgmt:  timeout="10s"
ALTER TABLE users ALTER COLUMN col1 SET NOT NULL;
"#;

        let sections = parse_migration_sections(Path::new("test.sql"), sql).unwrap();

        assert_eq!(sections.len(), 3);
        assert_eq!(sections[0].name, "section1");
        assert_eq!(sections[1].name, "section2");
        assert_eq!(sections[2].name, "section3");
    }

    #[test]
    fn test_parse_legacy_migration_without_sections() {
        let sql = "ALTER TABLE users ADD COLUMN email TEXT;";

        let sections = parse_migration_sections(Path::new("test.sql"), sql).unwrap();

        // Legacy migrations get wrapped in a default section
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].name, "default");
        assert_eq!(sections[0].mode, TransactionMode::Transactional);
        assert_eq!(sections[0].timeout, Duration::from_secs(600));
    }

    #[test]
    fn test_parse_section_with_description() {
        let sql = r#"
-- pgmt:section name="test"
-- pgmt:  mode="transactional"
-- pgmt:  timeout="30s"
-- pgmt:  description="This is a test section"
ALTER TABLE users ADD COLUMN test TEXT;
"#;

        let sections = parse_migration_sections(Path::new("test.sql"), sql).unwrap();
        assert_eq!(
            sections[0].description,
            Some("This is a test section".to_string())
        );
    }

    #[test]
    fn test_parse_section_with_lock_timeout() {
        let sql = r#"
-- pgmt:section name="add_column"
-- pgmt:  mode="transactional"
-- pgmt:  timeout="30s"
-- pgmt:  lock_timeout="2s"
ALTER TABLE users ADD COLUMN status TEXT;
"#;

        let sections = parse_migration_sections(Path::new("test.sql"), sql).unwrap();
        assert_eq!(sections[0].lock_timeout, Some(Duration::from_secs(2)));
    }

    #[test]
    fn test_parse_section_missing_name() {
        // Only 'name' is required; mode and timeout have defaults
        let sql = r#"
-- pgmt:section
-- pgmt:  mode="transactional"
ALTER TABLE users ADD COLUMN test TEXT;
"#;

        let result = parse_migration_sections(Path::new("test.sql"), sql);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("name"));
    }

    #[test]
    fn test_parse_section_with_defaults() {
        // Only name specified, mode and timeout use defaults
        let sql = r#"
-- pgmt:section name="test"
ALTER TABLE users ADD COLUMN test TEXT;
"#;

        let sections = parse_migration_sections(Path::new("test.sql"), sql).unwrap();
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].name, "test");
        assert_eq!(sections[0].mode, TransactionMode::Transactional);
        assert_eq!(sections[0].timeout, Duration::from_secs(600)); // 10 minute default
    }

    #[test]
    fn test_parse_single_line_section_minimal() {
        let sql = r#"
-- pgmt:section name="table" mode="transactional" timeout="30s"
ALTER TABLE users ADD COLUMN email TEXT;
"#;

        let sections = parse_migration_sections(Path::new("test.sql"), sql).unwrap();
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].name, "table");
        assert_eq!(sections[0].mode, TransactionMode::Transactional);
        assert_eq!(sections[0].timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_parse_single_line_section_with_retry() {
        let sql = r#"
-- pgmt:section name="index" mode="non-transactional" timeout="2s" retry_attempts="5" retry_delay="1s"
CREATE INDEX CONCURRENTLY idx_users_email ON users(email);
"#;

        let sections = parse_migration_sections(Path::new("test.sql"), sql).unwrap();
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].name, "index");
        assert_eq!(sections[0].mode, TransactionMode::NonTransactional);
        assert_eq!(sections[0].timeout, Duration::from_secs(2));

        let retry = sections[0].retry_config.as_ref().unwrap();
        assert_eq!(retry.attempts, 5);
        assert_eq!(retry.delay, Duration::from_secs(1));
    }

    #[test]
    fn test_parse_single_line_with_description() {
        let sql = r#"
-- pgmt:section name="my_table" mode="transactional" timeout="10s" description="Create user table"
CREATE TABLE users (id SERIAL PRIMARY KEY);
"#;

        let sections = parse_migration_sections(Path::new("test.sql"), sql).unwrap();
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].name, "my_table");
        assert_eq!(
            sections[0].description,
            Some("Create user table".to_string())
        );
    }

    #[test]
    fn test_parse_mixed_single_and_multiline() {
        let sql = r#"
-- pgmt:section name="schema" mode="transactional" timeout="30s"
CREATE TABLE users (id SERIAL);

-- pgmt:section name="index"
-- pgmt:  mode="non-transactional"
-- pgmt:  timeout="2s"
-- pgmt:  retry_attempts="10"
CREATE INDEX CONCURRENTLY idx ON users(id);

-- pgmt:section name="constraint" mode="transactional" timeout="10s"
ALTER TABLE users ADD CONSTRAINT check_id CHECK (id > 0);
"#;

        let sections = parse_migration_sections(Path::new("test.sql"), sql).unwrap();
        assert_eq!(sections.len(), 3);

        // First section (single line)
        assert_eq!(sections[0].name, "schema");
        assert_eq!(sections[0].mode, TransactionMode::Transactional);

        // Second section (multi-line)
        assert_eq!(sections[1].name, "index");
        assert_eq!(sections[1].mode, TransactionMode::NonTransactional);
        assert_eq!(sections[1].retry_config.as_ref().unwrap().attempts, 10);

        // Third section (single line)
        assert_eq!(sections[2].name, "constraint");
        assert_eq!(sections[2].mode, TransactionMode::Transactional);
    }

    #[test]
    fn test_parse_single_line_with_spaces_in_values() {
        let sql = r#"
-- pgmt:section name="my complex section" mode="transactional" timeout="5m" description="This has spaces"
CREATE TABLE test (id INT);
"#;

        let sections = parse_migration_sections(Path::new("test.sql"), sql).unwrap();
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].name, "my complex section");
        assert_eq!(sections[0].description, Some("This has spaces".to_string()));
    }

    /// `remaps` is accepted on MIGRATION sections too (modules.md §11):
    /// acquisition sections carry the same attribute, same single-source
    /// grammar as baseline remap sections.
    #[test]
    fn test_parse_migration_remap_section() {
        let sql = r#"
-- objects moved from module 'a'; runs only on targets without it.
-- pgmt:section name="b" module="b" remaps="a"
CREATE TABLE y (id SERIAL PRIMARY KEY);

-- pgmt:section name="default" remaps="a"
CREATE TABLE x (id SERIAL PRIMARY KEY);
"#;
        let sections = parse_migration_sections(Path::new("100_move.sql"), sql).unwrap();
        assert_eq!(sections.len(), 2);
        assert_eq!(sections[0].module.as_deref(), Some("b"));
        assert_eq!(sections[0].remaps, vec!["a".to_string()]);
        assert_eq!(sections[1].module, None);
        assert_eq!(sections[1].remaps, vec!["a".to_string()]);
    }

    /// The comma-list remaps form is retired (§12): a provenance-cut remap
    /// section has exactly one source, in migrations and baselines alike.
    #[test]
    fn test_parse_multi_source_remaps_is_error() {
        let sql = r#"
-- pgmt:section name="c" module="c" remaps="a,b"
CREATE TABLE z (id SERIAL PRIMARY KEY);
"#;
        let err = parse_migration_sections(Path::new("bad.sql"), sql)
            .unwrap_err()
            .to_string();
        assert!(err.contains("exactly one prior owner"), "{err}");
    }

    #[test]
    fn test_parse_key_value_pairs() {
        use super::parse_key_value_pairs;

        let pairs = parse_key_value_pairs(r#"name="test" mode="transactional""#).unwrap();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0], ("name".to_string(), "test".to_string()));
        assert_eq!(pairs[1], ("mode".to_string(), "transactional".to_string()));
    }

    #[test]
    fn test_parse_key_value_pairs_with_spaces() {
        use super::parse_key_value_pairs;

        let pairs =
            parse_key_value_pairs(r#"name="my table" mode="non-transactional" timeout="30s""#)
                .unwrap();
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], ("name".to_string(), "my table".to_string()));
        assert_eq!(
            pairs[1],
            ("mode".to_string(), "non-transactional".to_string())
        );
        assert_eq!(pairs[2], ("timeout".to_string(), "30s".to_string()));
    }

    #[test]
    fn test_parse_key_value_pairs_empty() {
        use super::parse_key_value_pairs;

        let pairs = parse_key_value_pairs("").unwrap();
        assert_eq!(pairs.len(), 0);
    }

    #[test]
    fn test_parse_key_value_pairs_missing_quote() {
        use super::parse_key_value_pairs;

        let result = parse_key_value_pairs(r#"name=test"#);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Expected '\"'"));
    }

    #[test]
    fn test_parse_key_value_pairs_unclosed_quote() {
        use super::parse_key_value_pairs;

        let result = parse_key_value_pairs(r#"name="test"#);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Missing closing quote")
        );
    }
}
