#!/usr/bin/env bash
# Cross-compile areamy for embedded targets to verify std primitives we
# rely on (thread, sync, backtrace, etc.) are available there. Doesn't
# flash anything — just type-checks + emits rlibs for the target.
#
# Prereqs (one-time):
#   cargo install espup
#   espup install
#   source ~/export-esp.sh    # in every shell that runs this
set -euo pipefail

cd "$(dirname "$0")/.."

# ESP32-S3 (Xtensa, ESP-IDF + std). Requires the `esp` toolchain
# installed via espup. -Z build-std because Xtensa std isn't pre-built.
echo "==> xtensa-esp32s3-espidf"
cargo +esp build --lib \
    --target xtensa-esp32s3-espidf \
    -Z build-std=std,panic_abort

echo "==> all targets ok"
