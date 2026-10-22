# syntax=docker/dockerfile:1
FROM archlinux:base AS chef

WORKDIR /src

# Pin the Rust toolchain so image builds are reproducible instead of floating
# with the rolling `rust` pacman package. Bump this to track new releases.
#
# IMPORTANT: this must stay in sync with the cargo version that generated the
# committed Cargo.lock (locally: `cargo --version`). A newer cargo can resolve
# dependencies differently and then refuses `--locked` builds with
# "cannot update the lock file ... because --locked was passed". When bumping,
# regenerate the lock with the new toolchain (`cargo +<new> check`) in the same
# change.
#
# The `minimal` profile skips optional components, but rustfmt must be pulled
# in explicitly: the `export_ipc` proc macro shells out to `rustfmt` to format
# the generated client code, so building without it fails with
# "'rustfmt' is not installed for the toolchain ...".
ARG RUST_VERSION=1.97.1

# Install the native dependencies required by the workspace, then the pinned
# Rust toolchain and cargo-chef.
RUN pacman -Syu --noconfirm \
    base-devel \
    ca-certificates \
    clang \
    cmake \
    curl \
    ffmpeg \
    git \
    libbluray \
    openssl \
    pkgconf \
    sqlite \
    && pacman -Scc --noconfirm \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
        | sh -s -- -y --profile minimal --default-toolchain "${RUST_VERSION}" \
            --component rustfmt \
    && "$HOME/.cargo/bin/cargo" install cargo-chef --locked

ENV PATH="/root/.cargo/bin:${PATH}"

FROM chef AS planner
COPY . .

RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /src/recipe.json recipe.json

# Build dependencies only - this is the caching Docker layer! Source edits in
# the COPY below do not recompile it as long as Cargo.lock/manifests are
# unchanged.
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
ARG BUILD_REVISION=unknown

# Build the whole workspace: every dependency was already compiled by
# cargo-chef into the shared target directory, so only workspace crates compile
# here. INTSCRAPE_SKIP_PLUGIN_BUILD=1 makes build.rs a no-op (the workspace
# build already emits the plugin .so files into target/release), which avoids a
# second full dependency-graph build that a build-script-invoked cargo would
# otherwise need in its separate target directory.
#
# --locked pins the build to the committed Cargo.lock. If the build context
# ships a stale or missing lock (e.g. a server checkout with local lock
# changes), fall back to letting cargo resolve from the manifests so the image
# can still be built, and say so loudly.
RUN <<'EOF'
set -eu

echo "Building revision ${BUILD_REVISION}"
rm -rf compiled_plugins bin

if ! INTSCRAPE_SKIP_PLUGIN_BUILD=1 cargo build --release --locked --workspace; then
    echo "warning: Cargo.lock did not match the manifests in this build context; building without --locked" >&2
    INTSCRAPE_SKIP_PLUGIN_BUILD=1 cargo build --release --workspace
fi

mkdir -p bin compiled_plugins
cp target/release/intscrape bin/intscrape
for plugin_dir in plugins/*/; do
    crate_name="$(basename "$plugin_dir" | tr '-' '_')"
    if [ -f "target/release/lib${crate_name}.so" ]; then
        cp "target/release/lib${crate_name}.so" compiled_plugins/
    else
        echo "warning: expected target/release/lib${crate_name}.so" >&2
    fi
done
test -n "$(find compiled_plugins -maxdepth 1 -type f -name '*.so' -print -quit)"
EOF

FROM archlinux:base
WORKDIR /app

ARG BUILD_REVISION=unknown
LABEL org.opencontainers.image.revision=$BUILD_REVISION

# Keep only the runtime dependencies in the final image.
RUN pacman -Syu --noconfirm \
    ca-certificates \
    ffmpeg \
    libbluray \
    openssl \
    sqlite \
    && pacman -Scc --noconfirm

COPY --from=builder /src/bin/intscrape /app/intscrape
COPY --from=builder /src/compiled_plugins /app/compiled_plugins
# The plugins are linked against the FFmpeg libraries from the builder image.
COPY --from=builder /usr/lib/libav*.so* /usr/lib/
COPY --from=builder /usr/lib/libsw*.so* /usr/lib/

ENTRYPOINT ["/app/intscrape"]