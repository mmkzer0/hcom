#!/bin/sh
# Install hcom binary from GitHub Releases.
# Usage: curl -fsSL https://raw.githubusercontent.com/aannoo/hcom/main/install.sh | sh
set -e

REPO="aannoo/hcom"
INSTALL_DIR="${HCOM_INSTALL_DIR:-$HOME/.local/bin}"

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)
case "$ARCH" in amd64) ARCH="x86_64" ;; arm64) ARCH="aarch64" ;; esac

ASSET="hcom-${OS}-${ARCH}"

# Get latest release tag
TAG=$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" | grep '"tag_name"' | cut -d'"' -f4)
if [ -z "$TAG" ]; then
    echo "Error: could not determine latest release" >&2
    exit 1
fi

URL="https://github.com/$REPO/releases/download/$TAG/$ASSET"
echo "Installing hcom $TAG ($OS/$ARCH)..."

mkdir -p "$INSTALL_DIR"
TMP="$INSTALL_DIR/hcom.tmp.$$"
if ! curl -fSL "$URL" -o "$TMP"; then
    rm -f "$TMP"
    echo "Error: no binary available for $OS/$ARCH" >&2
    echo "Available platforms: linux/x86_64, linux/aarch64, darwin/x86_64, darwin/aarch64" >&2
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
