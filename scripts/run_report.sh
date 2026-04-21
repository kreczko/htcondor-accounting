#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-archive}"

# placeholder for future report render command(s)
pixi run htcondor-accounting show-config
