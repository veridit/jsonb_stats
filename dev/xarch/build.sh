#!/bin/bash
# Build jsonb_stats for linux aarch64 (native) and x86_64 (cross) into /out.
set -euo pipefail
mkdir -p /work && cp -r /src_ro/. /work/ && cd /work
rm -rf .cargo   # macOS-only linker flags
export CARGO_TARGET_DIR=/cache/target

cargo pgrx package --pg-config /usr/lib/postgresql/18/bin/pg_config --out-dir /out/aarch64 >/out/build-aarch64.log 2>&1 \
  || { tail -30 /out/build-aarch64.log; exit 1; }

export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc
export CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc
export BINDGEN_EXTRA_CLANG_ARGS_x86_64_unknown_linux_gnu="--target=x86_64-linux-gnu -I/usr/x86_64-linux-gnu/include"
cargo build --release --lib --target x86_64-unknown-linux-gnu >/out/build-x86_64.log 2>&1 \
  || { tail -30 /out/build-x86_64.log; exit 1; }
mkdir -p /out/x86_64
cp /cache/target/x86_64-unknown-linux-gnu/release/libjsonb_stats.so /out/x86_64/jsonb_stats.so
# ELF e_machine at offset 18: 0x3e = x86_64
echo "x86_64 e_machine: $(od -An -tx1 -j18 -N1 /out/x86_64/jsonb_stats.so | tr -d ' ')"
find /out/aarch64 -name '*.so' -o -name '*.sql' -o -name '*.control' | sort
