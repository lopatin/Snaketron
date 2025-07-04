# Build stage for Rust server
FROM rust:latest as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /usr/src/app

# Copy workspace files
COPY Cargo.toml ./
COPY Cargo.lock ./
COPY macros ./macros
COPY common ./common
COPY server ./server
COPY client ./client
COPY terminal ./terminal

# Build the server
RUN cargo build --release --bin server

# Build stage for WASM client
FROM rust:latest as wasm-builder

# Install wasm-pack
RUN curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh

# Create app directory
WORKDIR /usr/src/app

# Copy workspace files
COPY Cargo.toml ./
COPY Cargo.lock ./
COPY macros ./macros
COPY common ./common
COPY client ./client
# Copy server Cargo.toml to satisfy workspace requirements
COPY server/Cargo.toml ./server/
# Copy terminal Cargo.toml as well
COPY terminal/Cargo.toml ./terminal/

# Build WASM package
RUN cd client && wasm-pack build --target web --out-dir pkg

# Build stage for web client
FROM node:20 as web-builder

# Create app directory structure to match the expected paths
WORKDIR /app

# First copy the WASM package to the expected location
COPY --from=wasm-builder /usr/src/app/client/pkg /client/pkg

# Then copy package files
COPY client/web/package.json client/web/package-lock.json ./

# Install dependencies
RUN npm ci

# Create symlink for local wasm-snaketron package
RUN cd node_modules && ln -s /client/pkg wasm-snaketron

# Copy web source files
COPY client/web ./

# Build production bundle
RUN npm run build

# Runtime stage
FROM ubuntu:22.04

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 snaketron

# Copy the binary from builder
COPY --from=builder /usr/src/app/target/release/server /usr/local/bin/server

# Create directories
RUN mkdir -p /var/lib/snaketron/replays /app/web && \
    chown -R snaketron:snaketron /var/lib/snaketron /app

# Copy web assets from web-builder
COPY --from=web-builder --chown=snaketron:snaketron /app/dist /app/web

# Switch to non-root user
USER snaketron

# Set working directory
WORKDIR /home/snaketron

# Expose ports
EXPOSE 8080 3001 50051 50052

# Set default environment variables
ENV SNAKETRON_WS_PORT=8080
ENV SNAKETRON_API_PORT=3001
ENV SNAKETRON_GRPC_PORT=50051
ENV SNAKETRON_RAFT_PORT=50052
ENV SNAKETRON_WEB_DIR=/app/web
ENV SNAKETRON_REPLAY_DIR=/var/lib/snaketron/replays
ENV RUST_LOG=info

# Run the server
CMD ["server"]