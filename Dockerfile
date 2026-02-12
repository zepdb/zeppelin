# syntax=docker/dockerfile:1

# ---- Builder stage ----
FROM rust:1.84-bookworm AS builder

ARG FEATURES=""

WORKDIR /app

# Install build dependencies (protobuf-compiler needed by pprof's prost-codec)
RUN apt-get update && \
    apt-get install -y --no-install-recommends pkg-config libssl-dev protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

# Copy manifests first for dependency caching
COPY Cargo.toml Cargo.lock ./

# Create dummy sources to build dependencies as a cached layer
RUN mkdir -p src benches && \
    echo "fn main() {}" > src/main.rs && \
    echo "" > src/lib.rs && \
    echo "fn main() {}" > benches/core_benchmarks.rs && \
    cargo build --release ${FEATURES:+--features $FEATURES} && \
    rm -rf src benches

# Copy real source code
COPY src/ src/
COPY benches/ benches/

# Touch files so cargo detects changes from the dummy build
RUN touch src/main.rs src/lib.rs && \
    cargo build --release ${FEATURES:+--features $FEATURES}

# ---- Runtime stage ----
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

# Non-root user
RUN groupadd --system zeppelin && \
    useradd --system --gid zeppelin --create-home zeppelin

# Cache directory
RUN mkdir -p /var/cache/zeppelin && \
    chown zeppelin:zeppelin /var/cache/zeppelin

COPY --from=builder /app/target/release/zeppelin /usr/local/bin/zeppelin

USER zeppelin

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/healthz || exit 1

ENTRYPOINT ["zeppelin"]
