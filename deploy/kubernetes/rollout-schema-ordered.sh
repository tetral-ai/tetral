#!/usr/bin/env bash
set -euo pipefail

manifest_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
namespace="${TETRAL_KUBERNETES_NAMESPACE:-tetral-system}"
timeout="${TETRAL_API_ROLLOUT_TIMEOUT:-5m}"

kubectl apply -f "$manifest_dir/api.yaml"
kubectl --namespace "$namespace" rollout status deployment/api --timeout "$timeout"

for manifest in \
  auth.yaml \
  queue.yaml \
  sandbox.yaml \
  bridge.yaml \
  event-stream.yaml \
  cleanup.yaml \
  git-proxy.yaml \
  gateway.yaml
do
  kubectl apply -f "$manifest_dir/$manifest"
done
