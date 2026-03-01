# DevContainer for Infrarust

This DevContainer provides a complete development environment for working on Infrarust.

## Features

### Development Tools
- **Rust 1.75.0** with stable toolchain
- **Cargo tools**: clippy, rustfmt, cargo-watch
- **VS Code extensions**: rust-analyzer, lldb debugger, Docker, YAML support
- **GitHub CLI** for repository management

### Pre-configured Environment
- **Environment variables**:
  - `CARGO_TERM_COLOR=always` - Colored cargo output
  - `RUST_LOG=debug` - Debug logging by default
  - `RUST_BACKTRACE=1` - Full backtraces for debugging

### Network Configuration
- **Host networking** for easy testing of Minecraft proxy functionality
- **Forwarded ports**:
  - `25565` - Minecraft proxy port
  - `3000` - Grafana dashboard (if monitoring enabled)
  - `9090` - Prometheus metrics (if monitoring enabled)

### Optional Services
- **PostgreSQL** - Available with `testing` profile
- **Redis** - Available with `testing` profile

## Usage

### Quick Start
1. Open the project in VS Code
2. When prompted, click "Reopen in Container"
3. Wait for container to build and start

### Manual Commands
```bash
# Build the project
cargo build --release

# Run tests
cargo test

# Run with clippy checks
cargo clippy -- -D warnings

# Format code
cargo fmt

# Watch for changes and auto-rebuild
cargo watch -x check -x test

# Run the application
cargo run -- --config-path config.yaml --no-interactive
```

### Testing Setup
```bash
# Start with optional testing services
docker-compose --profile testing up -d

# Run tests with database
cargo test

# Stop testing services
docker-compose --profile testing down
```

### Development Workflow
1. Make your changes
2. Use `cargo fmt` to format code
3. Use `cargo clippy` to check for issues
4. Use `cargo test` to run tests
5. Use `cargo build --release` to create release build

### Debugging
- VS Code debugger is pre-configured with lldb
- Set breakpoints in your Rust code
- Use "Run > Start Debugging" or F5

### Port Forwarding
The container automatically forwards common development ports. You can access:
- Minecraft proxy: `localhost:25565`
- Grafana: `localhost:3000` (when monitoring is enabled)
- Prometheus: `localhost:9090` (when monitoring is enabled)

## Troubleshooting

### Container Won't Start
- Check Docker Desktop is running
- Verify you have enough disk space
- Try rebuilding: `Dev Containers: Rebuild Container`

### Build Issues
- Run `cargo clean` then try again
- Check that all dependencies are available
- Look at the terminal output for specific errors

### Network Issues
- Ensure port 25565 isn't already in use
- Try using different ports in your config
- Check firewall settings

### Performance
- The container uses `host` networking for best performance
- Volume mounts are cached for faster file access
- Rust toolchain is cached between sessions
