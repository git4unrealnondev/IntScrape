FROM ubuntu:24.04

# Install sqlite + ffmpeg + certs (common requirement)
RUN apt-get update && apt-get install -y \
    sqlite3 \
    ffmpeg \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*
