use crate::helpers::cli::with_cli_helper;
use anyhow::Result;

#[cfg(not(windows))]
use expectrl::Eof;

#[cfg(not(windows))]
#[tokio::test]
async fn test_simple_help_interactive() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        // First try a simple help command that should work
        let mut session = helper.interactive_command(&["--help"]).await?;

        // Wait for the help output
        session.expect("PostgreSQL Migration Tool")?;
        session.expect(Eof)?;

        Ok(())
    })
    .await
}

#[cfg(not(windows))]
#[tokio::test]
async fn test_simple_migrate_help_interactive() -> Result<()> {
    with_cli_helper(async |helper| {
        helper.init_project()?;

        // Try migrate help which should work without any prompting
        let mut session = helper.interactive_command(&["migrate", "--help"]).await?;

        // Wait for the help output
        session.expect("Migration commands")?;
        session.expect(Eof)?;

        Ok(())
    })
    .await
}
