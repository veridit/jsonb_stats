FROM postgres:18-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates build-essential pkg-config libclang-dev clang postgresql-server-dev-18 gcc-x86-64-linux-gnu libc6-dev-amd64-cross && rm -rf /var/lib/apt/lists/*
ENV RUSTUP_HOME=/opt/rustup CARGO_HOME=/opt/cargo PATH=/opt/cargo/bin:$PATH
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal && rustup target add x86_64-unknown-linux-gnu
RUN cargo install --locked cargo-pgrx --version 0.16.1
RUN cargo pgrx init --pg18 /usr/lib/postgresql/18/bin/pg_config
WORKDIR /src
