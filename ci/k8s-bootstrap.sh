#!/bin/bash

set -eu
set -o pipefail

NAMESPACE=${NAMESPACE:-kinesis-to-elasticsearch} 

ci_user="ci-user"

kubectl apply -f <(cat <<EOF
# Create our own namespace
apiVersion: v1
kind: Namespace
metadata:
  name: "${NAMESPACE}"
---
# Create a service account for deployment
apiVersion: v1
kind: ServiceAccount
metadata:
  name: "${ci_user}"
  namespace: "${NAMESPACE}"
---
# Give appropriate permissions for being able to
# deploy into this namespace
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: ci-user-can-deploy
  namespace: "${NAMESPACE}"
roleRef:
  name: edit
  kind: ClusterRole
  apiGroup: rbac.authorization.k8s.io
subjects:
- name: "${ci_user}"
  namespace: "${NAMESPACE}"
  kind: ServiceAccount
---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: "${NAMESPACE}"
  namespace: "${NAMESPACE}"
  labels:
    release: prometheus-operator
spec:
  selector:
    matchLabels:
      monitor: me
  endpoints:
  - port: web
EOF
)

secret="$(kubectl get "serviceaccount/${ci_user}" --namespace "${NAMESPACE}" -o=jsonpath='{.secrets[0].name}')"
token="$(kubectl get secret "${secret}" --namespace "${NAMESPACE}" -o=jsonpath='{.data.token}' | base64 --decode)"

cur_context="$(kubectl config view -o=jsonpath='{.current-context}' --flatten=true)"
cur_cluster="$(kubectl config view -o=jsonpath="{.contexts[?(@.name==\"${cur_context}\")].context.cluster}" --flatten=true)"
cur_api_server="$(kubectl config view -o=jsonpath="{.clusters[?(@.name==\"${cur_cluster}\")].cluster.server}" --flatten=true)"
cur_crt="$(kubectl config view -o=jsonpath="{.clusters[?(@.name==\"${cur_cluster}\")].cluster.certificate-authority-data}" --flatten=true)"

kubeconfig="$(cat <<EOF
{
  "apiVersion": "v1",
  "clusters": [
    {
      "cluster": {
        "certificate-authority-data": "${cur_crt}",
        "server": "${cur_api_server}"
      },
      "name": "kubernetes"
    }
  ],
  "contexts": [
    {
      "context": {
        "cluster": "kubernetes",
        "user": "${ci_user}",
        "namespace": "${NAMESPACE}"
      },
      "name": "kubernetes"
    }
  ],
  "current-context": "kubernetes",
  "kind": "Config",
  "users": [
    {
      "name": "${ci_user}",
      "user": {
        "token": "${token}"
      }
    }
  ]
}
EOF
)"

echo "You may wish to run the following..."
echo 'credhub set -n /concourse/apps/kinesis-to-elasticsearch/kubeconfig -t value -v "$(cat <<EOKUBECONFIG'
echo "${kubeconfig}"
echo 'EOKUBECONFIG'
echo ')"'
echo
echo "Use in concourse:"
echo "echo \$KUBECONFIG > k"
echo "export KUBECONFIG=k"
echo "kubectl get all"
