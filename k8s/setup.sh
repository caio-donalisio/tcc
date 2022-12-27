#!/usr/bin/sh

set -ex

helm repo add bitnami https://charts.bitnami.com/bitnami

helm repo update

kind create cluster --config local/kind-config.yaml

kubectl create ns crawlers

kubectl config set-context --current --namespace=crawlers

kubectl taint nodes --all node-role.kubernetes.io/master- || true

helm install -n crawlers redis bitnami/redis -f redis/values.yaml

kubectl create configmap  crawler-env --from-env-file=./local/.env.dev

./local/load_images.sh
