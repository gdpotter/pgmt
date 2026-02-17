# pgmt demo recording
#
# Records: git diff → pgmt apply → pgmt migrate new
# Setup:   Run demos/record.sh (handles postgres, setup, and recording)
#
# Usage:
#   ./demos/record.sh

Output demo.gif

Set Shell "bash"
Set Theme "Catppuccin Mocha"
Set FontSize 16
Set Width 1000
Set Height 600
Set Padding 20
Set TypingSpeed 40ms

# 1. Show what changed — one parameter added to a function
Type "git diff schema/functions/calculate_score.sql"
Enter
Sleep 4s

# 2. Apply the change — pgmt resolves the entire dependency chain
Type "pgmt apply"
Enter
Sleep 6s

# 3. Generate a production migration
Type `pgmt migrate new "add bonus scoring"`
Enter
Sleep 6s

# Hold for viewers
Sleep 4s
