# Rust multi-stage Dockerfile
FROM rust:1.94 AS builder

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

COPY . .
RUN touch src/main.rs && cargo build --release

# Runtime
FROM cgr.dev/chainguard/glibc-dynamic:latest AS runtime

COPY --from=builder /build/target/release/discsync /usr/local/bin/app

ENTRYPOINT ["/usr/local/bin/app"]
