#!/usr/bin/env bash

export IMAGE_TAG=${1:-latest}

for f in cronjob/*.yaml; do
    envsubst < "${f}" | kubectl apply -f -;
done

envsubst < "./celery/crawler-queue.yaml" | kubectl apply -f -;
