FROM rust:1.88-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
RUN cargo build --locked --release --bin ringstore

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /build/target/release/ringstore /usr/local/bin/ringstore
COPY config.example.json /app/config.example.json
USER 65534:65534
ENTRYPOINT ["ringstore"]
