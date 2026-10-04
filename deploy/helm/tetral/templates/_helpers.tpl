{{- define "tetral.imageTag" -}}
{{- $tag := toString .Values.image.tag -}}
{{- if eq $tag "" -}}
{{- .Chart.AppVersion -}}
{{- else -}}
{{- $tag -}}
{{- end -}}
{{- end -}}

{{- define "tetral.sandboxSnapshot" -}}
{{- printf "%s/sandbox:%s" .Values.image.registry (include "tetral.imageTag" .) -}}
{{- end -}}

{{- define "tetral.image" -}}
{{- $root := .root -}}
{{- $name := .name -}}
{{- $digest := index $root.Values.image.digests $name | default "" -}}
{{- if $digest -}}
{{- printf "%s/%s@%s" $root.Values.image.registry $name $digest -}}
{{- else -}}
{{- printf "%s/%s:%s" $root.Values.image.registry $name (include "tetral.imageTag" $root) -}}
{{- end -}}
{{- end -}}

{{- define "tetral.routingAnnotations" -}}
{{- $root := .root -}}
sidecar.istio.io/inject: "true"
sidecar.istio.io/nativeSidecar: "true"
{{- $proxyImage := "" }}
{{- range (($root.Files.Get "files/dependencies.lock.json" | fromJson).istio.images) }}{{ if eq .component "proxyv2" }}{{ $proxyImage = .reference }}{{ end }}{{ end }}
sidecar.istio.io/proxyImage: {{ required "locked proxyv2 image is required" $proxyImage | quote }}
{{- $proxyDrain := "5s" -}}
{{- if eq .role "agent-runtime" -}}
{{- if eq (mod (int $root.Values.lifecycle.runtimeProxyJoinMs) 1000) 0 -}}
{{- $proxyDrain = printf "%ds" (div (int $root.Values.lifecycle.runtimeProxyJoinMs) 1000) -}}
{{- else -}}
{{- $proxyDrain = printf "%.3fs" (divf (float64 $root.Values.lifecycle.runtimeProxyJoinMs) 1000) -}}
{{- end -}}{{- end }}
proxy.istio.io/config: {{ if and (eq .role "agent-runtime") (eq $root.Values.transport.profile "hardened") }}'{"holdApplicationUntilProxyStarts":true,"terminationDrainDuration":"{{ $proxyDrain }}","proxyStatsMatcher":{"inclusionPrefixes":["sds.tetral.runtime.direct_leaf.","sds.tetral.runtime.direct_validation."]}}'{{ else }}'{"holdApplicationUntilProxyStarts":true,"terminationDrainDuration":"{{ $proxyDrain }}"}'{{ end }}
{{- if eq .role "agent-runtime" }}
traffic.sidecar.istio.io/excludeInboundPorts: {{ if eq $root.Values.transport.profile "hardened" }}"19443"{{ else }}"19090"{{ end }}
{{- if eq $root.Values.transport.profile "hardened" }}
sidecar.istio.io/userVolume: {{ list (dict "name" "runtime-direct-leaf" "secret" (dict "secretName" $root.Values.transport.runtimeDirectLeafSecret)) (dict "name" "runtime-direct-trust" "configMap" (dict "name" $root.Values.transport.runtimeDirectTrustConfigMap)) (dict "name" "runtime-direct-sds" "configMap" (dict "name" "tetral-runtime-direct-sds")) | toJson | quote }}
sidecar.istio.io/userVolumeMount: '[{"name":"runtime-direct-leaf","mountPath":"/var/run/tetral/runtime-direct/leaf","readOnly":true},{"name":"runtime-direct-trust","mountPath":"/var/run/tetral/runtime-direct/trust","readOnly":true},{"name":"runtime-direct-sds","mountPath":"/var/run/tetral/runtime-direct/sds","readOnly":true}]'
{{- end }}
{{- else if eq .role "job-runner" }}
traffic.sidecar.istio.io/excludeOutboundPorts: {{ if eq $root.Values.transport.profile "hardened" }}"19443"{{ else }}"19090"{{ end }}
{{- else if and (eq .role "provider-gateway") $root.Values.preview.enabled }}
# Native NATS security and reconnect are owned by its official client.
traffic.sidecar.istio.io/excludeOutboundPorts: "4222"
{{- end }}
{{- end -}}

{{- define "tetral.storeTLSEnv" -}}
{{- if .database }}
- name: TETRAL_DATABASE_TLS_CA_PATH
  value: /var/run/tetral/store-trust/database-ca.crt
- name: TETRAL_DATABASE_TLS_SERVER_NAME
  value: {{ .root.Values.transport.databaseServerName | quote }}
- name: {{ if .bun }}TETRAL_DATABASE_POOL_MAX{{ else }}TETRAL_DB_MAX_OPEN_CONNS{{ end }}
  value: {{ if .bun }}{{ .root.Values.transport.bunPoolMax | quote }}{{ else }}{{ .root.Values.transport.goPoolMax | quote }}{{ end }}
{{- end }}
{{- if .blob }}
- name: TETRAL_BLOB_TLS_CA_PATH
  value: /var/run/tetral/store-trust/object-store-ca.crt
- name: TETRAL_BLOB_TLS_SERVER_NAME
  value: {{ .root.Values.transport.blobServerName | quote }}
{{- end }}
{{- end -}}

{{/* Resolve a Deployment's absolute or percentage surge against its bound. */}}
{{- define "tetral.surge" -}}
{{- $value := toString .root.Values.rollout.maxSurge -}}
{{- if hasSuffix "%" $value -}}
{{- div (add (mul .replicas (int (trimSuffix "%" $value))) 99) 100 -}}
{{- else -}}{{ int $value }}{{- end -}}
{{- end -}}

{{/* Go updates credentials on new connections in one pool; Bun serializes
replacement with at most two generations per process, including candidates. */}}
{{- define "tetral.databasePoolSlots" -}}
{{- $provider := int .Values.replicas.providerGateway -}}
{{/* The HPA limit does not cap the separately declared initial Deployment. */}}
{{- if .Values.autoscaling.providerGateway.enabled -}}{{- $provider = max $provider (int .Values.autoscaling.providerGateway.maxReplicas) -}}{{- end -}}
{{/* Each named consumer owns one Go pool. Sandbox and Event Stream use
alternative DSN environment names; neither can be inferred from DATABASE_URL. */}}
{{- $goProcesses := 1 -}}{{/* nonoverlapping Cleanup CronJob */}}
{{- range $key := list "bridge" "jobRunner" "api" "auth" "sandbox" -}}
{{- $replicas := int (get $.Values.replicas $key) -}}
{{- $goProcesses = add $goProcesses $replicas (int (include "tetral.surge" (dict "root" $ "replicas" $replicas))) -}}
{{- end -}}
{{- $eventStream := int .Values.replicas.eventStream -}}
{{- $goProcesses = add $goProcesses $eventStream (int (include "tetral.surge" (dict "root" . "replicas" $eventStream))) -}}
{{/* Queue remains fixed at one replica. */}}
{{- range list "queue" -}}
{{- $goProcesses = add $goProcesses 1 (int (include "tetral.surge" (dict "root" $ "replicas" 1))) -}}
{{- end -}}
{{- $git := int (include "tetral.gitProxyMaxReplicas" .) -}}
{{- $goProcesses = add $goProcesses $git (int (include "tetral.surge" (dict "root" . "replicas" $git))) -}}
{{- $mcp := int .Values.replicas.mcpConnector -}}
{{- $bunProcesses := add $provider (int (include "tetral.surge" (dict "root" . "replicas" $provider))) $mcp (int (include "tetral.surge" (dict "root" . "replicas" $mcp))) -}}
{{- add (mul $goProcesses (int .Values.transport.goPoolMax)) (mul $bunProcesses 2 (int .Values.transport.bunPoolMax)) -}}
{{- end -}}

{{/* Existing Git Proxy HPA bound, shared by the workload and SQL ledger. */}}
{{- define "tetral.gitProxyMaxReplicas" -}}10{{- end -}}

{{/* Fixed scheduling/signal margin after Runtime's four application/proxy phases. */}}
{{- define "tetral.runtimeShutdownMarginMs" -}}5000{{- end -}}

{{/* Existing Provider HPA floor, shared by its resource and validation. */}}
{{- define "tetral.providerGatewayMinReplicas" -}}2{{- end -}}
