#!/bin/bash

echo "[Chaos] Starting fault injection"

echo "[Chaos] Stopping node1"
docker stop node1
sleep 10

echo "[Chaos] Starting node1"
docker start node1
sleep 10

echo "[Chaos] Stopping node3"
docker stop node3
sleep 10

echo "[Chaos] Starting node3"
docker start node3
sleep 10

echo "[Chaos] Fault injection complete"
