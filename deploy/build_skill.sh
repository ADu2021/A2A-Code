#!/usr/bin/env bash
# Build the self-contained skill bundle (SKILL.md + tools + hooks) for
# distribution — typically published on the relay at /skill.
# Usage: bash deploy/build_skill.sh [output-dir]     (default: repo root)
set -euo pipefail
cd "$(dirname "$0")/.."
OUT="${1:-.}"
PKG="$(mktemp -d)"
cp -r skill "$PKG/a2a-code"
rm -rf "$PKG/a2a-code/.a2a" "$PKG"/a2a-code/*/__pycache__ 2>/dev/null || true
(cd "$PKG" && zip -qr a2a-code.skill a2a-code)
cp "$PKG/a2a-code.skill" "$OUT/a2a-code.skill"
rm -rf "$PKG"
echo "built $OUT/a2a-code.skill ($(du -h "$OUT/a2a-code.skill" | cut -f1))"
