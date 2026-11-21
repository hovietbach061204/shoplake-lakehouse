#!/usr/bin/env bash
# M0 sanity check: is Docker running, does it have enough memory/CPU,
# and can containers see the external SSD where this project lives?
set -u

ok()   { printf "  [ok]   %s\n" "$1"; }
warn() { printf "  [warn] %s\n" "$1"; }

echo "== Docker"
if ! docker info >/dev/null 2>&1; then
  warn "Docker is not reachable. Start Colima first: see docs/M0-setup.md step 1"
  exit 1
fi
ok "Docker daemon reachable ($(docker context show 2>/dev/null))"

mem_bytes=$(docker info --format '{{.MemTotal}}')
cpus=$(docker info --format '{{.NCPU}}')
arch=$(docker info --format '{{.Architecture}}')
mem_gb=$(( mem_bytes / 1024 / 1024 / 1024 ))
echo "   memory: ${mem_gb} GB, cpus: ${cpus}, arch: ${arch}"
[ "$mem_gb" -ge 7 ] && ok "memory >= 7 GB (full stack fits)" || warn "less than 7 GB for Docker: restart Colima with --memory 8"
[ "$cpus" -ge 4 ] && ok "cpus >= 4" || warn "fewer than 4 CPUs for Docker: restart Colima with --cpu 4"
[ "$arch" = "aarch64" ] && ok "native arm64 (no emulation)" || warn "architecture is $arch"

echo "== Bind mount from this folder"
# Colima only shares your home folder by default. This project lives on /Volumes,
# so the folder must be added as a mount, otherwise containers see an empty dir.
here="$(pwd)"
if docker run --rm -v "$here":/probe alpine:3.20 ls /probe/README.md >/dev/null 2>&1; then
  ok "containers can read $here"
else
  warn "containers cannot see $here. Restart Colima with --mount (docs/M0-setup.md step 1)"
fi

echo "== Disk"
df -h "$here" | tail -1 | awk '{print "   free on this drive: " $4}'
