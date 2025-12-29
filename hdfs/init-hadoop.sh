#!/bin/sh
# Ensure Hadoop directories exist with proper permissions
mkdir -p /hadoop/dfs/name /hadoop/dfs/data
chown -R hadoop:hadoop /hadoop/dfs

# Execute the original command
exec "$@"
