#!/usr/bin/env bash
# The reference spike's ctl.sh verbs for `make spike` (spec 010, AC-010-23). jobs/spike_repro.py copies this file
# into the run directory, where the unedited reference/restate-ems-spike/experiments.py calls it as ./ctl.sh.
# Differences from the spike's own ctl.sh, which ran a native restate-server from npm:
# - the server is the Restate image pinned in infra/docker-compose.yml, run as the container ems-spike-restate
#   with a volume of its own (never the stack's), on 127.0.0.1:8080 and :9070; kill-server sends it SIGKILL;
# - the SDK services run on the virtualenv the job puts first on PATH, with the spike's directory on PYTHONPATH;
# - register posts to the admin API, with 127.0.0.1 rewritten to host.docker.internal (reachable from the
#   container), force false like the spike's CLI call; register-force sets force true.
# Every verb appends one line to ctl.log in the working directory: "<UTC> <verb> <key=value ...>".
set -u
: "${SPIKE_DIR:?set by jobs/spike_repro.py}"
: "${SPIKE_RESTATE_IMAGE:?set by jobs/spike_repro.py}"
NAME=ems-spike-restate
VOLUME=ems-spike-restate-data
ADMIN=http://127.0.0.1:9070

log() { printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >> ctl.log; }

listeners() { lsof -ti "tcp:$1" -sTCP:LISTEN 2>/dev/null | sort -u | paste -sd, -; }

killport() {  # SIGKILL to every process listening on the port: hypercorn serves from a child process
  local pids
  pids=$(listeners "$1")
  if [ -n "$pids" ]; then
    echo "$pids" | tr ',' '\n' | xargs kill -9 2>/dev/null
  fi
  log "$2 port=$1 pids=$pids"
}

svc() {  # module port logfile verb
  PYTHONPATH="$SPIKE_DIR" nohup python -m hypercorn "ems_spike.$1:app" --bind "127.0.0.1:$2" \
    >> "$3" 2>&1 < /dev/null &
  log "$4 port=$2 pid=$!"
}

case "${1:-}" in
  server)
    if docker container inspect "$NAME" >/dev/null 2>&1; then
      docker start "$NAME" >/dev/null
    else
      docker run -d --name "$NAME" --add-host host.docker.internal:host-gateway \
        -p 127.0.0.1:8080:8080 -p 127.0.0.1:9070:9070 -v "$VOLUME:/restate-data" \
        -e RESTATE_NODE_NAME=spike-1 "$SPIKE_RESTATE_IMAGE" >/dev/null
    fi
    log "server container=$NAME status=$(docker inspect -f '{{.State.Status}}' "$NAME" 2>/dev/null)"
    ;;
  kill-server)
    docker kill --signal KILL "$NAME" >/dev/null 2>&1
    log "kill-server container=$NAME exit=$(docker wait "$NAME" 2>/dev/null)"
    ;;
  svc1) svc restate_app 9080 svc1.log svc1 ;;
  kill-svc1) killport 9080 kill-svc1 ;;
  svc2) svc extra_app 9081 svc2.log svc2 ;;
  kill-svc2) killport 9081 kill-svc2 ;;
  register | register-force)
    uri=${2/127.0.0.1/host.docker.internal}
    force=false
    [ "$1" = register-force ] && force=true
    code=$(curl -s -o register.out -w '%{http_code}' -X POST "$ADMIN/deployments" \
      -H 'content-type: application/json' -d "{\"uri\": \"$uri\", \"force\": $force}")
    log "$1 uri=$uri force=$force http=$code"
    ;;
  sql)
    body=$(python -c 'import json, sys; print(json.dumps({"query": sys.argv[1]}))' "$2")
    curl -s -X POST "$ADMIN/query" -H 'content-type: application/json' -H 'accept: application/json' \
      -d "$body"
    ;;
  wipe)
    killport 9080 wipe-svc1
    killport 9081 wipe-svc2
    docker rm -f "$NAME" >/dev/null 2>&1
    docker volume rm "$VOLUME" >/dev/null 2>&1
    log "wipe container=$NAME volume=$VOLUME"
    ;;
  *)
    echo "usage: $0 server|kill-server|svc1|kill-svc1|svc2|kill-svc2|register URL|register-force URL|sql QUERY|wipe"
    exit 2
    ;;
esac
