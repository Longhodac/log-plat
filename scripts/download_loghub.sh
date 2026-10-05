#!/usr/bin/env bash
# Downloads one Loghub dataset (https://github.com/logpai/loghub) into data/loghub/.
# Usage: scripts/download_loghub.sh [hdfs|apache]   (default: hdfs)
set -euo pipefail

dataset="${1:-hdfs}"
base="https://zenodo.org/records/8196385/files"
case "$dataset" in
  hdfs)   archive="HDFS_v1.zip";   md5="76a24b4d9a6164d543fb275f89773260"; member="HDFS.log" ;;
  apache) archive="Apache.tar.gz"; md5="de9a42d12f9b60612631c67a5a9f8628"; member="Apache.log" ;;
  *) echo "unknown dataset '$dataset' (want hdfs or apache)" >&2; exit 2 ;;
esac

root="$(cd "$(dirname "$0")/.." && pwd)"
dest="$root/data/loghub"
mkdir -p "$dest"

if [[ -s "$dest/$member" ]]; then
  echo "$dest/$member already present ($(wc -l < "$dest/$member" | tr -d ' ') lines)"
  exit 0
fi

tmp="$dest/$archive.part"
echo "downloading $archive from Zenodo..."
curl -fsSL --retry 3 -o "$tmp" "$base/$archive?download=1"

if command -v md5sum > /dev/null; then got="$(md5sum "$tmp" | cut -d' ' -f1)"; else got="$(md5 -q "$tmp")"; fi
if [[ "$got" != "$md5" ]]; then
  echo "checksum mismatch for $archive: got $got, want $md5" >&2
  rm -f "$tmp"
  exit 1
fi

case "$archive" in
  *.zip)    unzip -o -j -q "$tmp" "$member" -d "$dest" ;;
  *.tar.gz) tar -xzf "$tmp" -C "$dest" "$member" ;;
esac
rm -f "$tmp"
echo "wrote $dest/$member ($(wc -l < "$dest/$member" | tr -d ' ') lines)"
