#!/bin/bash

set -eu
set -o pipefail

# Tag is not always populated correctly by the docker-image resource (ie it defaults to latest)
# so use the actual source for tag
TAG=$(cat src/.git/ref)
REPO=$(cat img/repository)

cat <<EOF > deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kinesis-to-elasticsearch
spec:
  selector:
    matchLabels:
      app: kinesis-to-elasticsearch
  replicas: 1
  template:
    metadata:
      labels:
        app: kinesis-to-elasticsearch
    spec:
      containers:
      - name: kinesis-to-elasticsearch
        image: ${REPO}:${TAG}
        resources: {limits: {memory: "1024Mi", cpu: "100m"}}
        envFrom:
        - secretRef: {name: kinesis-to-elasticsearch}
        ports:
        - name: http
          containerPort: 8080 # /metrics
---
kind: Service
apiVersion: v1
metadata:
  name: kinesis-to-elasticsearch
  labels:
    monitor: me
spec:
  selector:
    app: kinesis-to-elasticsearch
  ports:
  - name: web
    port: 8080
EOF

cat deployment.yaml

echo $KUBECONFIG > k
export KUBECONFIG=k

kubectl apply --record -f - < deployment.yaml
kubectl rollout status deployment.apps/kinesis-to-elasticsearch
