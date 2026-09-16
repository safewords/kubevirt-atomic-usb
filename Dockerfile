# syntax=docker/dockerfile:1

# Local builds: `docker build .` compiles from source.
# CI builds: `docker buildx build --target prebuilt` packages binaries from dist/<arch>/.

FROM rust:1-trixie AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked && cp target/release/atomic-usb /atomic-usb

# trixie ships usbredir 0.15; selecting devices by BUS-DEVICE is only reliable from 0.14 on.
FROM debian:trixie-slim AS runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends usbredirect \
 && rm -rf /var/lib/apt/lists/*
ENTRYPOINT ["/usr/local/bin/atomic-usb"]

FROM runtime AS prebuilt
ARG TARGETARCH
COPY dist/${TARGETARCH}/atomic-usb /usr/local/bin/atomic-usb

FROM runtime
COPY --from=build /atomic-usb /usr/local/bin/atomic-usb
