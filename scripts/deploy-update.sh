#!/usr/bin/env bash
# Nightly deploy: pull from git, back up postgres, rebuild & restart if anything
# changed. Designed to be invoked from a systemd user timer. Safe to run
# manually. No-op if origin/main has no new commits.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKUP_DIR="${BACKUP_DIR:-${REPO_DIR}/backups}"
KEEP_BACKUPS="${KEEP_BACKUPS:-7}"
BRANCH="${DEPLOY_BRANCH:-main}"
LOCK_FILE="${LOCK_FILE:-/tmp/tedisc-deploy.lock}"

# Wrap everything so bash parses the whole script before executing — guards
# against `git pull` rewriting this file mid-run.
main() {
    cd "$REPO_DIR"

    exec 9>"$LOCK_FILE"
    if ! flock -n 9; then
        echo "another deploy is already in progress; exiting"
        return 0
    fi

    log "fetching origin/$BRANCH"
    git fetch --quiet origin "$BRANCH"

    local local_sha remote_sha
    local_sha="$(git rev-parse HEAD)"
    remote_sha="$(git rev-parse "origin/$BRANCH")"

    if [[ "$local_sha" == "$remote_sha" ]]; then
        log "already up to date at $local_sha; nothing to do"
        return 0
    fi

    log "updating $local_sha -> $remote_sha"
    backup_database
    git pull --ff-only origin "$BRANCH"

    log "rebuilding and recreating services"
    docker compose up -d --build --remove-orphans --wait --wait-timeout 300

    prune_old_backups
    log "deploy complete"
}

backup_database() {
    mkdir -p "$BACKUP_DIR"
    local ts file
    ts="$(date -u +%Y%m%dT%H%M%SZ)"
    file="${BACKUP_DIR}/postgres_${ts}.sql.gz"
    log "backing up postgres to $file"
    # Run pg_dump inside the container; expand POSTGRES_USER/DB from the
    # container's own env so this script doesn't need to know the credentials.
    docker compose exec -T dagster_postgresql \
        sh -c 'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
        | gzip > "$file"
}

prune_old_backups() {
    # Keep the newest $KEEP_BACKUPS dumps, delete the rest.
    if ! compgen -G "${BACKUP_DIR}/postgres_*.sql.gz" > /dev/null; then
        return 0
    fi
    # shellcheck disable=SC2012 # ls -t is fine here; filenames are ISO timestamps
    ls -1t "${BACKUP_DIR}"/postgres_*.sql.gz \
        | tail -n +$((KEEP_BACKUPS + 1)) \
        | xargs -r rm -f
}

log() {
    printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

main "$@"
