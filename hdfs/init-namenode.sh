#!/bin/bash
# ensure storage directory exists
mkdir -p /hadoop/dfs/name
chown -R hadoop:hadoop /hadoop/dfs/name

# format the NameNode only if empty
if [ ! -f /hadoop/dfs/name/current/VERSION ]; then
    echo "Formatting NameNode..."
    hdfs namenode -format -force
fi

# start NameNode
exec hdfs namenode
