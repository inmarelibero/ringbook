FROM rust:1.93-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY analysis analysis
RUN cargo build --release --locked -p ringbook-app -p ringbook-ctl

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/ringbook /usr/local/bin/ringbook
COPY --from=build /src/target/release/ringbookctl /usr/local/bin/ringbookctl
COPY configs /etc/ringbook
COPY infra/engine-entrypoint.sh /usr/local/bin/engine-entrypoint.sh
RUN useradd --create-home ringbook && mkdir -p /data /run/ringbook && chown ringbook /data /run/ringbook
USER ringbook
