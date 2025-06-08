# Build stage
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

# Create replay directory
RUN mkdir -p /var/lib/snaketron/replays && \
    chown -R snaketron:snaketron /var/lib/snaketron

# Switch to non-root user
USER snaketron

# Set working directory
WORKDIR /home/snaketron

# Expose ports
EXPOSE 8080 50051

# Set default environment variables for local development
ENV SNAKETRON_WS_PORT=8080
ENV SNAKETRON_GRPC_PORT=50051
ENV SNAKETRON_REPLAY_DIR=/var/lib/snaketron/replays
ENV RUST_LOG=info

# Run the server
CMD ["server"]