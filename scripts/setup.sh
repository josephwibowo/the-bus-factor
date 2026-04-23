#!/usr/bin/env bash
# scripts/setup.sh — install non-Python toolchain for The Bus Factor.
#
# What this installs (macOS + Linux):
#   - Homebrew (macOS only, if missing)
#   - Bruin CLI (via official curl installer)
#   - uv (Python toolchain + package manager)
#   - fnm (Node version manager that honors .nvmrc)
#   - Node 24 LTS (via fnm reading .nvmrc)
#   - pnpm (via corepack, pinned by packageManager in package.json)
#   - Playwright browsers (only if web/ has been installed)
#
# Python project deps are installed separately via `uv sync --locked`.
# JS project deps are installed separately via `pnpm install`.
#
# The script is idempotent. Safe to re-run.

set -euo pipefail

# ---- Helpers ------------------------------------------------------
log()   { printf "\033[1;34m[setup]\033[0m %s\n" "$*"; }
warn()  { printf "\033[1;33m[setup]\033[0m %s\n" "$*" >&2; }
fail()  { printf "\033[1;31m[setup]\033[0m %s\n" "$*" >&2; exit 1; }
have()  { command -v "$1" >/dev/null 2>&1; }

OS="$(uname -s)"
ARCH="$(uname -m)"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

log "OS=$OS  ARCH=$ARCH  ROOT=$REPO_ROOT"

# ---- 0. Git repo ---------------------------------------------------
# Bruin uses the Git root to locate the project, so initialize if needed.
if [ ! -d ".git" ]; then
  log "Initializing git repo (Bruin requires a git root)"
  git init -q
  git checkout -q -b main 2>/dev/null || true
else
  log "Git repo already initialized"
fi

# ---- 1. Homebrew (macOS only) -------------------------------------
if [ "$OS" = "Darwin" ]; then
  if ! have brew; then
    log "Installing Homebrew"
    NONINTERACTIVE=1 /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    # Add to PATH for the rest of this script
    if [ -x /opt/homebrew/bin/brew ]; then
      eval "$(/opt/homebrew/bin/brew shellenv)"
    elif [ -x /usr/local/bin/brew ]; then
      eval "$(/usr/local/bin/brew shellenv)"
    fi
  else
    log "Homebrew already installed: $(brew --version | head -n1)"
  fi
fi

# ---- 2. uv --------------------------------------------------------
if ! have uv; then
  log "Installing uv"
  curl -LsSf https://astral.sh/uv/install.sh | sh
  # uv installs to ~/.local/bin
  export PATH="$HOME/.local/bin:$PATH"
else
  log "uv already installed: $(uv --version)"
fi
have uv || fail "uv still not on PATH; add \$HOME/.local/bin to your shell rc"

# ---- 3. Bruin CLI -------------------------------------------------
# Homebrew install is deprecated per Bruin docs; curl installer is canonical.
if ! have bruin; then
  log "Installing Bruin CLI"
  curl -LsSf https://getbruin.com/install/cli | sh
  export PATH="$HOME/.local/bin:$PATH"
else
  log "Bruin CLI already installed: $(bruin --version 2>/dev/null || echo 'version unknown')"
fi
have bruin || fail "bruin still not on PATH; open a new shell or add \$HOME/.local/bin to PATH"

# ---- 4. fnm + Node (per .nvmrc) -----------------------------------
if ! have fnm; then
  if [ "$OS" = "Darwin" ] && have brew; then
    log "Installing fnm via Homebrew"
    brew install fnm
  else
    log "Installing fnm via official installer"
    curl -fsSL https://fnm.vercel.app/install | bash -s -- --skip-shell
    export PATH="$HOME/.local/share/fnm:$PATH"
  fi
fi

if have fnm; then
  # Prime fnm env for this script
  eval "$(fnm env --use-on-cd --shell bash)" 2>/dev/null || eval "$(fnm env)"
  log "fnm: $(fnm --version)"

  if [ -f ".nvmrc" ]; then
    NODE_VERSION="$(tr -d '[:space:]' < .nvmrc)"
    log "Installing Node $NODE_VERSION via fnm"
    fnm install "$NODE_VERSION"
    fnm use "$NODE_VERSION"
    fnm default "$NODE_VERSION"
  fi
else
  warn "fnm not available; falling back to whatever Node is on PATH"
fi

have node || fail "Node is not on PATH after install"
log "Node: $(node --version)  npm: $(npm --version)"

# ---- 5. pnpm via corepack -----------------------------------------
# packageManager in package.json pins the exact version; corepack honors it.
if have corepack; then
  log "Enabling corepack (for pnpm)"
  corepack enable 2>/dev/null || sudo corepack enable || warn "corepack enable needs sudo; re-run manually if pnpm is missing"
  # Prime the pnpm shim from packageManager in package.json
  corepack prepare --activate >/dev/null 2>&1 || true
fi

if ! have pnpm; then
  warn "pnpm not found after corepack enable; falling back to npm install -g pnpm"
  npm install -g pnpm
fi
log "pnpm: $(pnpm --version)"

# ---- 6. Playwright browsers (only if web node_modules exist) -----
if [ -d "web/node_modules/@playwright/test" ]; then
  log "Installing Playwright browsers"
  pnpm --filter ./web exec playwright install --with-deps chromium webkit || \
    warn "Playwright install returned non-zero; system deps may require sudo"
else
  log "Skipping Playwright browser install (run 'pnpm install' first, then re-run setup)"
fi

# ---- 7. Bruin config bootstrap -----------------------------------
if [ ! -f ".bruin.yml" ] && [ -f ".bruin.yml.example" ]; then
  log "Creating .bruin.yml from .bruin.yml.example (edit it to add real credentials later)"
  cp .bruin.yml.example .bruin.yml
else
  log ".bruin.yml already exists or template missing — skipping"
fi

# ---- Summary ------------------------------------------------------
cat <<EOF

$(printf "\033[1;32m[setup]\033[0m") All done. Installed / verified:
  - git           $(git --version)
  - uv            $(uv --version)
  - bruin         $(bruin --version 2>/dev/null || echo 'run \`bruin --version\` in a new shell')
  - node          $(node --version)
  - pnpm          $(pnpm --version)

Next steps:
  1) uv sync --locked                     # install Python deps
  2) pnpm install                          # install JS deps
  3) ./scripts/setup.sh                    # re-run to install Playwright browsers now that pnpm install ran
  4) bruin validate pipeline/pipeline.yml  # sanity-check the pipeline
  5) bruin run --workers=1 --full-refresh -e fixture pipeline/pipeline.yml

If any command above is missing from PATH, open a new shell (the installers edit ~/.zshrc / ~/.bashrc).
EOF
