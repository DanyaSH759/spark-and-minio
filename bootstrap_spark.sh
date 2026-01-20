#!/usr/bin/env bash
set -euo pipefail
log() { echo -e "\n==> $*\n"; }
have() { command -v "$1" >/dev/null 2>&1; }

PIPX_BIN_DIR="${HOME}/.local/bin"
export PATH="$PATH:$PIPX_BIN_DIR"

# ---------- system update ----------
log "APT update & upgrade"
sudo apt update
sudo apt upgrade -y

# ---------- base tools ----------
log "Installing base packages"
sudo apt install -y \
  ca-certificates \
  curl \
  wget \
  git \
  unzip \
  htop \
  tree \
  build-essential \
  software-properties-common

# ---------- python ----------
log "Installing Python tooling"
sudo apt install -y python3 python3-venv python3-pip

# ---------- java ----------
log "Installing Java 17 (required for Spark)"
sudo apt install -y openjdk-17-jdk
java -version

# ---------- docker ----------
# ---------- docker ----------
if ! have docker; then
  log "Installing Docker"
  curl -fsSL https://get.docker.com | sudo sh
else
  log "Docker already installed"
fi

log "Docker version (may require sudo)"
sudo docker version
sudo docker compose version || true

# ---------- pipx ----------
log "Installing pipx"
sudo apt install -y pipx
pipx ensurepath
exec bash

# ---------- uv ----------
log "Installing uv via pipx"
pipx install uv --force
log "uv version"
uv --version

# ---------- uv project setup ----------
if [[ ! -f pyproject.toml ]]; then
  log "Initializing uv project"
  uv init
fi

log "Syncing environment"
uv sync

log "DONE ✅"
echo "NOTE: Re-login (new SSH session) so docker group applies (then docker works without sudo)."
