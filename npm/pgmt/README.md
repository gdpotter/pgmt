# pgmt — PostgreSQL Schema-as-Code

Edit database objects like code. pgmt tracks dependencies, applies changes instantly, and generates production-ready migrations.

## Install

```bash
npm install -g @pgmt/pgmt
```

Or use without installing:

```bash
npx @pgmt/pgmt --version
```

## Quick Start

```bash
pgmt init --dev-url postgres://localhost/mydb --defaults
pgmt apply
pgmt migrate new "add user analytics"
```

## Documentation

- [Quick Start](https://pgmt.dev/docs/getting-started/quick-start)
- [CLI Reference](https://pgmt.dev/docs/cli/)
- [GitHub](https://github.com/gdpotter/pgmt)

## Other Install Methods

```bash
# Shell script
curl -fsSL https://pgmt.dev/install.sh | sh

# Cargo (from source)
cargo install pgmt
```

## License

MIT
