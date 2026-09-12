#!/usr/bin/env bash
# vagrant/provision/dc-a-setup.sh — DC-A Docker Compose 기동
set -e

cd /home/vagrant/compose
docker compose -f docker-compose.dc-a.yml up -d
