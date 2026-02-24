#!/bin/sh
# pgmt installer — https://pgmt.dev
#
# Usage:
#   curl -fsSL https://pgmt.dev/install.sh | sh
#   curl -fsSL https://pgmt.dev/install.sh | sh -s -- v0.4.5
#
# Environment variables:
#   PGMT_INSTALL  — installation directory (default: ~/.pgmt)

set -e

REPO="gdpotter/pgmt"
INSTALL_DIR="${PGMT_INSTALL:-$HOME/.pgmt}"
BIN_DIR="$INSTALL_DIR/bin"

main() {
  need_cmd curl
  need_cmd tar

  version="${1:-}"

  platform="$(detect_platform)"
  arch="$(detect_arch)"

  if [ -z "$version" ]; then
    version="$(get_latest_version)"
  fi

  # Strip leading 'v' for the archive name comparison, keep it for the tag
  tag="$version"
  case "$version" in
    v*) ;;
    *) tag="v$version" ;;
  esac

  archive="$(get_archive_name "$platform" "$arch")"
  url="https://github.com/$REPO/releases/download/$tag/$archive"

  printf "  Installing pgmt %s (%s-%s)\n" "$tag" "$platform" "$arch"
  printf "  from %s\n\n" "$url"

  ensure mkdir -p "$BIN_DIR"

  # Download and extract
  if [ "$platform" = "windows" ]; then
    need_cmd unzip
    tmp="$(mktemp)"
    ensure curl -fsSL -o "$tmp" "$url"
    ensure unzip -o "$tmp" -d "$BIN_DIR"
    rm -f "$tmp"
  else
    ensure curl -fsSL "$url" | tar xz -C "$BIN_DIR"
  fi

  ensure chmod +x "$BIN_DIR/pgmt"

  printf "  pgmt installed to %s/pgmt\n\n" "$BIN_DIR"

  add_path_instructions
}

detect_platform() {
  os="$(uname -s)"
  case "$os" in
    Linux*)  echo "linux" ;;
    Darwin*) echo "macos" ;;
    MINGW* | MSYS* | CYGWIN*) echo "windows" ;;
    *)
      printf "Error: unsupported operating system: %s\n" "$os" >&2
      exit 1
      ;;
  esac
}

detect_arch() {
  arch="$(uname -m)"
  case "$arch" in
    x86_64 | amd64)  echo "x86_64" ;;
    aarch64 | arm64)  echo "aarch64" ;;
    *)
      printf "Error: unsupported architecture: %s\n" "$arch" >&2
      exit 1
      ;;
  esac
}

get_latest_version() {
  response="$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest")" || {
    printf "Error: failed to fetch latest release from GitHub\n" >&2
    exit 1
  }
  # Parse version from JSON without jq
  version="$(printf '%s' "$response" | grep '"tag_name"' | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/')"
  if [ -z "$version" ]; then
    printf "Error: could not determine latest version\n" >&2
    exit 1
  fi
  printf '%s' "$version"
}

get_archive_name() {
  platform="$1"
  arch="$2"
  if [ "$platform" = "windows" ]; then
    printf "pgmt-%s-%s.zip" "$platform" "$arch"
  else
    printf "pgmt-%s-%s.tar.gz" "$platform" "$arch"
  fi
}

add_path_instructions() {
  case ":$PATH:" in
    *:"$BIN_DIR":*)
      printf "  Run 'pgmt --version' to verify.\n"
      return
      ;;
  esac

  printf "  Add pgmt to your PATH:\n\n"

  shell_name="$(basename "${SHELL:-/bin/sh}")"
  case "$shell_name" in
    zsh)
      printf "    echo 'export PATH=\"%s:\$PATH\"' >> ~/.zshrc\n" "$BIN_DIR"
      printf "    source ~/.zshrc\n"
      ;;
    fish)
      printf "    fish_add_path %s\n" "$BIN_DIR"
      ;;
    *)
      printf "    echo 'export PATH=\"%s:\$PATH\"' >> ~/.bashrc\n" "$BIN_DIR"
      printf "    source ~/.bashrc\n"
      ;;
  esac

  printf "\n  Then run 'pgmt --version' to verify.\n"
}

need_cmd() {
  if ! command -v "$1" > /dev/null 2>&1; then
    printf "Error: '%s' is required but not found\n" "$1" >&2
    exit 1
  fi
}

ensure() {
  if ! "$@"; then
    printf "Error: command failed: %s\n" "$*" >&2
    exit 1
  fi
}

main "$@"
