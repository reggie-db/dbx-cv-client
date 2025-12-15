#!/usr/bin/env sh
set -e

if command -v uv >/dev/null 2>&1; then
  echo "uv is already installed"
  exit 0
fi

INSTALL_URL="https://astral.sh/uv/install.sh"

if command -v curl >/dev/null 2>&1; then
  echo "Installing uv with curl"
  curl -LsSf "$INSTALL_URL" | sh
elif command -v wget >/dev/null 2>&1; then
  echo "Installing uv with wget"
  wget -qO- "$INSTALL_URL" | sh
else
  echo "curl or wget is required" >&2
  exit 1
fi

UV_BIN="$HOME/.local/bin"

case ":$PATH:" in
  *":$UV_BIN:"*) ;;
  *)
    export PATH="$UV_BIN:$PATH"
    case "$SHELL" in
      */bash)
        [ -f "$HOME/.bashrc" ] && . "$HOME/.bashrc"
        ;;
      */zsh)
        [ -f "$HOME/.zshrc" ] && . "$HOME/.zshrc"
        ;;
      */fish)
        fish -c "set -Ux PATH $UV_BIN \$PATH" >/dev/null 2>&1 || true
        ;;
    esac
    ;;
esac

command -v uv >/dev/null 2>&1