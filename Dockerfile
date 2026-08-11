FROM archlinux:base

# Install sqlite + ffmpeg + certs (common requirement)
RUN pacman -Syu --noconfirm \
    sqlite3 \
    ffmpeg \
    ca-certificates \
    && pacman -Scc --noconfirm
