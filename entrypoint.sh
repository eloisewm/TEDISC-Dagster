#!/bin/sh
# Renders dagster.yaml from dagster.yaml.tmpl at container startup, substituting
# the per-machine values that Dagster's own config cannot expand (${VAR} is not
# supported in dagster.yaml; only the `env:` key form is). Then execs the given
# command (dagster-webserver / dagster-daemon, supplied via compose `command:`).
set -eu

: "${DAGSTER_HOME:?DAGSTER_HOME must be set}"
: "${DOCKER_SOCK:=/var/run/docker.sock}"

if [ -z "${WORK_DIR:-}" ]; then
  echo "entrypoint: WORK_DIR is not set (host path to the repo checkout); cannot render dagster.yaml" >&2
  exit 1
fi

export DOCKER_SOCK WORK_DIR

# Restrict substitution to these two vars so any other '$' in the file is left
# untouched (e.g. future additions); everything else is copied verbatim.
envsubst '${DOCKER_SOCK} ${WORK_DIR}' \
  < "${DAGSTER_HOME}/dagster.yaml.tmpl" \
  > "${DAGSTER_HOME}/dagster.yaml"

exec "$@"
