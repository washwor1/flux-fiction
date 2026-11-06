#!/bin/bash
set -euo pipefail

/usr/local/bin/build-flux-core.sh
/usr/local/bin/build-flux-sched.sh
/usr/local/bin/build-flux-fiction.sh

echo
echo "Build complete."
