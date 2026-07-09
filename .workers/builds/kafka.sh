#!/usr/bin/env bash
set -euo pipefail

# Optional named build profile. Replace this with target-specific service setup.
# Select it from .workers/map.md with Build profile = kafka.

exec ./.workers/build.sh
