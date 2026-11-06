#!/bin/bash

# Build the OCI image with Podman. The resulting image can also be built
# with Docker later using the same Containerfile.
set -euo pipefail

if [ -z "${1:-}" ] ; then
    echo "Usage: $0 <image_tag>"
    exit 1
fi

IMAGE_TAG="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

podman build \
    -t "$IMAGE_TAG" \
    -f "$SCRIPT_DIR/Containerfile" \
    "$SCRIPT_DIR"
