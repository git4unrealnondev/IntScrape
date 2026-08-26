FROM archlinux:base AS builder

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
    && pacman -Scc --noconfirm

COPY . .

# Build the main binary and every cdylib plugin in the workspace. The root
# build script stages the stripped plugin libraries in compiled_plugins.
RUN rm -rf compiled_plugins \
    && cargo build --release --workspace \
    && cargo build --release \
    && mkdir -p compiled_plugins \
    && find target/release -type f -name '*.so' -exec cp {} compiled_plugins/ \; \
    && strip compiled_plugins/*.so \
    && test -n "$(find compiled_plugins -maxdepth 1 -type f -name '*.so' -print -quit)"

FROM archlinux:base

WORKDIR /app

# Keep only the runtime dependencies in the final image.
RUN pacman -Syu --noconfirm \
    ca-certificates \
    ffmpeg \
    openssl \
    sqlite \
    && pacman -Scc --noconfirm

COPY --from=builder /src/target/release/intscrape /app/intscrape
COPY --from=builder /src/compiled_plugins /app/compiled_plugins
# The plugins are linked against the FFmpeg libraries from the builder image.
COPY --from=builder /usr/lib/libav*.so* /usr/lib/
COPY --from=builder /usr/lib/libsw*.so* /usr/lib/

ENTRYPOINT ["/app/intscrape"]
