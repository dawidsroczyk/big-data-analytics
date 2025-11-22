#!/bin/bash
# quick_nifi_login.sh

CONTAINER_NAME="${1:-nifi}"

echo "=== NiFi Generated Credentials ==="
docker logs "$CONTAINER_NAME" 2>&1 | grep -E "Generated (Username|Password) \[" | tail -4

echo ""
echo "=== Most Recent Credentials ==="
docker logs "$CONTAINER_NAME" 2>&1 | grep "Generated Username" | tail -1
docker logs "$CONTAINER_NAME" 2>&1 | grep "Generated Password" | tail -1

echo ""
echo "=== Raw Values ==="
username=$(docker logs "$CONTAINER_NAME" 2>&1 | grep "Generated Username" | tail -1 | sed -E 's/.*\[([^]]+)\].*/\1/')
password=$(docker logs "$CONTAINER_NAME" 2>&1 | grep "Generated Password" | tail -1 | sed -E 's/.*\[([^]]+)\].*/\1/')
echo "Username: $username"
echo "Password: $password"