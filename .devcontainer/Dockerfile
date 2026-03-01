FROM mcr.microsoft.com/devcontainers/rust:1

# Install additional tools
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
        # System tools
        curl \
        wget \
        git \
        vim \
        nano \
        htop \
        # Network tools for testing
        netcat-openbsd \
        telnet \
        # Build dependencies
        pkg-config \
        libssl-dev \
        # For debugging
        gdb \
        lldb \
    && apt-get clean -y && rm -rf /var/lib/apt/lists/*

# Install Docker CLI (for Docker integration testing)
RUN curl -fsSL https://get.docker.com -o get-docker.sh \
    && sh get-docker.sh \
    && rm get-docker.sh

# Install Docker Compose
RUN curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose \
    && chmod +x /usr/local/bin/docker-compose

# Set up Rust environment
ENV CARGO_TERM_COLOR=always
ENV RUST_LOG=info
ENV RUST_BACKTRACE=1

# Create workspace directory
WORKDIR /workspace

# Add useful aliases to bash profile
RUN echo 'alias ll="ls -la"' >> /root/.bashrc \
    && echo 'alias cargo-watch="cargo watch -x check -x test"' >> /root/.bashrc \
    && echo 'alias build-release="cargo build --release"' >> /root/.bashrc

# Expose common ports for development
EXPOSE 25565 3000 9090

# Set default command
CMD ["/bin/bash"]
