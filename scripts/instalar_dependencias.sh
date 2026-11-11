#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
command -v python3 >/dev/null || { echo 'ERROR: falta Python3.' >&2; exit 1; }
if [[ ! -x .venv/bin/python ]]; then
  python3 -m venv .venv || { echo 'Instala python3-venv: sudo apt install -y python3-venv' >&2; exit 1; }
fi
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m py_compile app/nodo.py legacy/NODO.py
printf 'Dependencias listas en %s/.venv\n' "$ROOT"
