#!/usr/bin/env bash
# vagrant/provision/dc-b-setup.sh — DC-B Docker Compose 기동
set -e

cd /home/vagrant/compose
docker compose -f docker-compose.dc-b.yml up -d
