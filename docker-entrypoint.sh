#!/bin/sh
set -e

case "$1" in
  node)
    exec uvicorn cdn.node_app:app --host 0.0.0.0 --port "${PORT:-8000}"
    ;;
  gateway)
    exec uvicorn cdn.gateway:app --host 0.0.0.0 --port "${PORT:-8080}" --proxy-headers --forwarded-allow-ips='*'
    ;;
  all-in-one)
    export SUPERVISE=1 DATA_ROOT="${DATA_ROOT:-/tmp/cdn}"
    exec uvicorn cdn.gateway:app --host 0.0.0.0 --port "${PORT:-8080}" --proxy-headers --forwarded-allow-ips='*'
    ;;
  *)
    exec "$@"
    ;;
esac
