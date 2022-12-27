#!/usr/bin/sh

docker build -t inspira-crawlers:latest .

docker tag inspira-crawlers:latest us-docker.pkg.dev/inspira-registry/docker/data/crawlers:latest

kind load docker-image us-docker.pkg.dev/inspira-registry/docker/data/crawlers:latest
