#!/bin/bash
set -euo pipefail

NETWORK_NAME="big-data-network"

if ! docker network inspect "${NETWORK_NAME}" >/dev/null 2>&1; then
  echo "Creating docker network: ${NETWORK_NAME}"
  docker network create "${NETWORK_NAME}"
  echo "Docker network ${NETWORK_NAME} created."
else
  echo "Docker network ${NETWORK_NAME} already exists"
fi

exit 0
