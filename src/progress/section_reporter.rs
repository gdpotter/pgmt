use console::style;
use std::time::Duration;

pub struct SectionReporter {
    total_sections: usize,
    current_section: usize,
    verbose: bool,
}

impl SectionReporter {
    pub fn new(total_sections: usize, verbose: bool) -> Self {
        Self {
            total_sections,
            current_section: 0,
            verbose,
        }
    }

    pub fn start_section(&mut self, name: &str, _description: Option<&str>) {
        self.current_section += 1;

        // Only print section header if there are multiple sections
        if self.total_sections > 1 {
            println!(
                "  Section {}/{}: {}",
                self.current_section, self.total_sections, name
            );
        }
    }

    pub fn skip_section(&self, name: &str) {
        if self.total_sections > 1 {
            println!("  Section '{}' already completed (skipping)", name);
        }
    }

    pub fn attempt(&self, current: u32, total: u32) {
        if total > 1 {
            println!("    Attempt {}/{}", current, total);
        }
    }

    pub fn retry(&self, _name: &str, attempt: u32, error: &anyhow::Error, delay: Duration) {
        let delay_str = format_duration(delay);
        println!(
            "    {} Lock timeout on attempt {} (retrying in {}...)",
            style("⚠").yellow(),
            attempt,
            style(&delay_str).yellow()
        );

        if self.verbose {
            println!("      {}", style(error.to_string()).dim());
        }
    }

    pub fn complete_section(&self, name: &str, duration: Duration, rows: Option<usize>) {
        let duration_str = format_duration(duration);
        let rows_str = rows.map(|r| format!(", {} rows", r)).unwrap_or_default();

        // Only print section completion if there are multiple sections
        if self.total_sections > 1 {
            println!(
                "  Section {}/{}: {} ({}{})",
                self.current_section, self.total_sections, name, duration_str, rows_str
            );
        }
    }

    pub fn complete_section_with_retry(
        &self,
        name: &str,
        duration: Duration,
        rows: Option<usize>,
        attempts: u32,
        total_attempts: u32,
    ) {
        let duration_str = format_duration(duration);
        let rows_str = rows.map(|r| format!(", {} rows", r)).unwrap_or_default();
        let attempts_str = if attempts > 1 {
            format!(", succeeded on attempt {}/{}", attempts, total_attempts)
        } else {
            String::new()
        };

        // Only print section completion if there are multiple sections
        if self.total_sections > 1 {
            println!(
                "  Section {}/{}: {} ({}{}{})",
                self.current_section,
                self.total_sections,
                name,
                duration_str,
                rows_str,
                attempts_str
            );
        }
    }

    pub fn fail_section(&self, _name: &str, error: &anyhow::Error) {
        let err_str = error.to_string();
        println!("{} Failed: {}", style("✗").red(), style(&err_str).red());
    }

    #[allow(dead_code)]
    pub fn batch_progress(&self, batch_num: usize, rows: usize, elapsed: Duration) {
        let elapsed_str = format_duration(elapsed);
        println!(
            "    Batch {}: {:>6} rows (elapsed: {})",
            batch_num,
            rows,
            style(&elapsed_str).dim()
        );
    }

    pub fn migration_summary(&self, total_duration: Duration, _sections_completed: usize) {
        let duration_str = format_duration(total_duration);
        println!(
            "{} Completed in {}",
            style("✓").green(),
            style(&duration_str).green()
        );
    }
}

fn format_duration(d: Duration) -> String {
    let total_secs = d.as_secs();
    let millis = d.subsec_millis();

    if total_secs == 0 {
        format!("{}ms", millis)
    } else if total_secs < 60 {
        if millis > 0 {
            format!("{}.{}s", total_secs, millis / 100)
        } else {
            format!("{}s", total_secs)
        }
    } else if total_secs < 3600 {
        let mins = total_secs / 60;
        let secs = total_secs % 60;
        if secs > 0 {
            format!("{}m{}s", mins, secs)
        } else {
            format!("{}m", mins)
        }
    } else {
        let hours = total_secs / 3600;
        let mins = (total_secs % 3600) / 60;
        if mins > 0 {
            format!("{}h{}m", hours, mins)
        } else {
            format!("{}h", hours)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_duration_milliseconds() {
        assert_eq!(format_duration(Duration::from_millis(5)), "5ms");
        assert_eq!(format_duration(Duration::from_millis(50)), "50ms");
        assert_eq!(format_duration(Duration::from_millis(500)), "500ms");
    }

    #[test]
    fn test_format_duration_seconds() {
        assert_eq!(format_duration(Duration::from_secs(1)), "1s");
        assert_eq!(format_duration(Duration::from_secs(30)), "30s");
        assert_eq!(format_duration(Duration::from_millis(1500)), "1.5s");
        assert_eq!(format_duration(Duration::from_millis(2300)), "2.3s");
    }

    #[test]
    fn test_format_duration_minutes() {
        assert_eq!(format_duration(Duration::from_secs(60)), "1m");
        assert_eq!(format_duration(Duration::from_secs(90)), "1m30s");
        assert_eq!(format_duration(Duration::from_secs(300)), "5m");
        assert_eq!(format_duration(Duration::from_secs(3599)), "59m59s");
    }

    #[test]
    fn test_format_duration_hours() {
        assert_eq!(format_duration(Duration::from_secs(3600)), "1h");
        assert_eq!(format_duration(Duration::from_secs(3660)), "1h1m");
        assert_eq!(format_duration(Duration::from_secs(7200)), "2h");
        assert_eq!(format_duration(Duration::from_secs(5400)), "1h30m");
    }
}
