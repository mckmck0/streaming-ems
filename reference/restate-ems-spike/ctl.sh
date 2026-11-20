#!/usr/bin/env bash
# Helper for the spike: start / kill -9 restate-server and the two Python deployments.
set -u
cd "$(dirname "$0")"
BIN=./node_modules/.bin
export RESTATE_BIND_IP=127.0.0.1          # sandbox had no IPv6; drop this on a normal laptop
export RESTATE_ADMIN_URL=http://127.0.0.1:9070

bg() { if command -v setsid >/dev/null; then setsid nohup "$@"; else nohup "$@"; fi; }
killport() {                               # kills master AND worker processes of hypercorn
  if command -v lsof >/dev/null; then lsof -ti "tcp:$1" -sTCP:LISTEN | xargs kill -9 2>/dev/null
  else python3 tools/killport.py "$1"; fi
}

case "${1:-}" in
  server)      bg $BIN/restate-server --no-logo --base-dir ./restate-data >> server.log 2>&1 < /dev/null & ;;
  kill-server) pkill -9 -f restate-server ;;
  svc1)        bg python3 -m hypercorn ems_spike.restate_app:app --bind 127.0.0.1:9080 >> svc1.log 2>&1 < /dev/null & ;;
  kill-svc1)   killport 9080 ;;
  svc2)        bg python3 -m hypercorn ems_spike.extra_app:app --bind 127.0.0.1:9081 >> svc2.log 2>&1 < /dev/null & ;;
  kill-svc2)   killport 9081 ;;
  register)    $BIN/restate -y deployments register "$2" ;;
  register-force) $BIN/restate -y deployments register --force "$2" ;;   # dev only: breaks immutability
  sql)         $BIN/restate sql "$2" ;;
  wipe)        pkill -9 -f restate-server; killport 9080; killport 9081; sleep 1; rm -rf restate-data *.log ;;
  *) echo "usage: $0 server|kill-server|svc1|kill-svc1|svc2|kill-svc2|register URL|register-force URL|sql QUERY|wipe" ;;
esac
