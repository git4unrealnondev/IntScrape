FROM archlinux:base AS chef

WORKDIR /src

# Install the Rust toolchain and native dependencies required by the workspace.
RUN pacman -Syu --noconfirm \
    base-devel \
    ca-certificates \
    cargo \
    clang \
    cmake \
    ffmpeg \
    git \
    openssl \
    pkgconf \
    rust \
    sqlite \
    && pacman -Scc --noconfirm \
    && cargo install cargo-chef --locked

FROM chef AS planner
COPY . .

RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /src/recipe.json recipe.json

RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
ARG BUILD_REVISION=unknown

# Add these mount flags to your cargo build step
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    echo "Building revision ${BUILD_REVISION}" \
    && rm -rf compiled_plugins bin \
    && cargo build --release --workspace \
    && mkdir -p compiled_plugins bin \
    && cp target/release/intscrape bin/intscrape \
    && find target/release -type f -name '*.so' -exec cp {} compiled_plugins/ \; \
    && strip compiled_plugins/*.so \
    && test -n "$(find compiled_plugins -maxdepth 1 -type f -name '*.so' -print -quit)"

FROM archlinux:base
WORKDIR /app

ARG BUILD_REVISION=unknown
LABEL org.opencontainers.image.revision=$BUILD_REVISION

# Keep only the runtime dependencies in the final image.
RUN pacman -Syu --noconfirm \
    ca-certificates \
    ffmpeg \
    openssl \
    sqlite \
    && pacman -Scc --noconfirm

COPY --from=builder /src/bin/intscrape /app/intscrape
COPY --from=builder /src/compiled_plugins /app/compiled_plugins
# The plugins are linked against the FFmpeg libraries from the builder image.
COPY --from=builder /usr/lib/libav*.so* /usr/lib/
COPY --from=builder /usr/lib/libsw*.so* /usr/lib/

ENTRYPOINT ["/app/intscrape"]
