//! Pins the command ↔ connection-flag matrix.
//!
//! Connection values are typed (`DevUrl`/`ShadowDatabase`/`TargetUrl`) and
//! only obtainable through their clap args structs, so the compiler already
//! enforces "command connects ⇒ flag declared". This test pins the reverse
//! direction the compiler can't see: no stale connection flags on commands
//! that don't touch that database. Each command's `--help` is documentation
//! of which databases it connects to — keep it honest.

const DEV: &str = "--dev-url";
const SHADOW: &str = "--shadow-url";
const TARGET: &str = "--target-url";

fn help_for(args: &[&str]) -> String {
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("pgmt");
    let output = cmd.args(args).arg("--help").output().unwrap();
    assert!(output.status.success(), "--help failed for {:?}", args);
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn test_connection_flags_match_databases_each_command_uses() {
    // (command, flags that must appear, flags that must NOT appear)
    let matrix: &[(&[&str], &[&str], &[&str])] = &[
        (&["apply"], &[DEV, SHADOW], &[TARGET]),
        (&["diff"], &[DEV, SHADOW], &[TARGET]),
        (&["validate"], &[DEV, SHADOW], &[TARGET]),
        (&["migrate", "new"], &[SHADOW], &[DEV, TARGET]),
        (&["migrate", "update"], &[SHADOW], &[DEV, TARGET]),
        (&["migrate", "apply"], &[TARGET], &[DEV, SHADOW]),
        (&["migrate", "status"], &[DEV], &[SHADOW, TARGET]),
        (&["migrate", "validate"], &[SHADOW], &[DEV, TARGET]),
        (&["migrate", "diff"], &[TARGET, SHADOW], &[DEV]),
        (&["migrate", "baseline"], &[SHADOW], &[DEV, TARGET]),
        (&["migrate", "resolve"], &[TARGET], &[DEV, SHADOW]),
        (&["debug", "dependencies"], &[SHADOW], &[DEV, TARGET]),
    ];

    for (args, expected, forbidden) in matrix {
        let help = help_for(args);
        for flag in *expected {
            assert!(
                help.contains(flag),
                "`pgmt {}` should accept {} (it connects to that database)",
                args.join(" "),
                flag
            );
        }
        for flag in *forbidden {
            assert!(
                !help.contains(flag),
                "`pgmt {}` should NOT accept {} (it never connects to that database)",
                args.join(" "),
                flag
            );
        }
    }
}
