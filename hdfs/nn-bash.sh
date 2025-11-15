#!/bin/bash
docker exec -it $(docker ps -q -f name=namenode) bash