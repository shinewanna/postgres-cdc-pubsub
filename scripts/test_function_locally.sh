#!/usr/bin/env bash
# Tests function/main.py on a throwaway local Postgres with a fake Pub/Sub.
# Needs Homebrew postgres (initdb, pg_ctl, psql) and python3. Nothing touches GCP.
#
#   scripts/test_function_locally.sh
set -euo pipefail
cd "$(dirname "$0")/.."
export LC_ALL=C LANG=C

# python deps for the function, in a local venv (ignored by git)
if [ ! -d .venv ]; then
    python3 -m venv .venv
    .venv/bin/pip install -q -r function/requirements.txt
fi

PORT="${LOCAL_TEST_PORT:-55434}"
WORKDIR="$(mktemp -d)"
cleanup() { pg_ctl -D "$WORKDIR/data" -m immediate stop >/dev/null 2>&1 || true; rm -rf "$WORKDIR"; }
trap cleanup EXIT

initdb -D "$WORKDIR/data" -U postgres --auth=trust --locale=C --encoding=UTF8 >/dev/null
pg_ctl -D "$WORKDIR/data" -o "-p $PORT -c unix_socket_directories='' -c listen_addresses=127.0.0.1" \
    -l "$WORKDIR/pg.log" -w start >/dev/null
createdb -h 127.0.0.1 -p "$PORT" -U postgres cdc_pubsub_test

LOCAL_TEST_DSN="postgresql://postgres@127.0.0.1:$PORT/cdc_pubsub_test" .venv/bin/python function/test_main.py
