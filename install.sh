#!/bin/sh
# Install hcom binary from GitHub Releases.
# Usage: curl -fsSL https://raw.githubusercontent.com/aannoo/hcom/main/install.sh | sh
set -e

REPO="aannoo/hcom"
INSTALL_DIR="${HCOM_INSTALL_DIR:-$HOME/.local/bin}"

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)
case "$ARCH" in amd64) ARCH="x86_64" ;; arm64) ARCH="aarch64" ;; esac

# Detect Termux/Android — the standard linux-aarch64 binary uses glibc which
# doesn't exist on Android (Bionic libc + different dynamic linker path).
if [ -n "$TERMUX_VERSION" ] || [ -d "/data/data/com.termux" ]; then
    OS="android"
fi

ASSET="hcom-${OS}-${ARCH}"

# Get latest release tag. Prefer git ls-remote (no rate limits), fall back to GitHub API.
# || true prevents set -e from aborting on failure before the fallback/error check.
TAG=$(git ls-remote --tags --sort=version:refname "https://github.com/$REPO.git" 2>/dev/null | grep -v '\^{}' | tail -1 | sed 's|.*refs/tags/||' || true)
if [ -z "$TAG" ]; then
    TAG=$(curl -fsSL --max-time 10 "https://api.github.com/repos/$REPO/releases/latest" 2>/dev/null | grep '"tag_name"' | cut -d'"' -f4 || true)
fi
if [ -z "$TAG" ]; then
    echo "Error: could not determine latest release. Check your network connection." >&2
    exit 1
fi

URL="https://github.com/$REPO/releases/download/$TAG/$ASSET"
echo "Installing hcom $TAG ($OS/$ARCH)..."

mkdir -p "$INSTALL_DIR"
TMP="$INSTALL_DIR/hcom.tmp.$$"
if ! curl -fSL "$URL" -o "$TMP"; then
    rm -f "$TMP"
    echo "Error: no binary available for $OS/$ARCH" >&2
    echo "Available platforms: linux/x86_64, linux/aarch64, android/aarch64, darwin/x86_64, darwin/aarch64" >&2
    exit 1
fi
chmod +x "$TMP"
mv "$TMP" "$INSTALL_DIR/hcom"

echo "Installed hcom $TAG to $INSTALL_DIR/hcom"

# Ensure INSTALL_DIR is on PATH
case ":$PATH:" in
    *":$INSTALL_DIR:"*) ;;
    *)
        LINE="export PATH=\"$INSTALL_DIR:\$PATH\""
        ADDED=false
        for RC in "$HOME/.zshrc" "$HOME/.bashrc"; do
            [ -f "$RC" ] || continue
            grep -qF "$INSTALL_DIR" "$RC" 2>/dev/null && continue
            echo "" >> "$RC"
            echo "# Added by hcom installer" >> "$RC"
            echo "$LINE" >> "$RC"
            ADDED=true
            echo "Added $INSTALL_DIR to PATH in $(basename "$RC")"
        done
        if [ "$ADDED" = false ]; then
            echo "Add to PATH: $LINE"
        fi
        export PATH="$INSTALL_DIR:$PATH"
        ;;
esac
