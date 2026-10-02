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
proxy.istio.io/config: {{ if and (eq .role "agent-runtime") (eq $root.Values.transport.profile "hardened") }}'{"holdApplicationUntilProxyStarts":true,"terminationDrainDuration":"5s","proxyStatsMatcher":{"inclusionPrefixes":["sds.tetral.runtime.direct_leaf.","sds.tetral.runtime.direct_validation."]}}'{{ else }}'{"holdApplicationUntilProxyStarts":true,"terminationDrainDuration":"5s"}'{{ end }}
{{- if eq .role "agent-runtime" }}
traffic.sidecar.istio.io/excludeInboundPorts: {{ if eq $root.Values.transport.profile "hardened" }}"19443"{{ else }}"19090"{{ end }}
{{- if eq $root.Values.transport.profile "hardened" }}
sidecar.istio.io/userVolume: {{ list (dict "name" "runtime-direct-leaf" "secret" (dict "secretName" $root.Values.transport.runtimeDirectLeafSecret)) (dict "name" "runtime-direct-trust" "configMap" (dict "name" $root.Values.transport.runtimeDirectTrustConfigMap)) (dict "name" "runtime-direct-sds" "configMap" (dict "name" "tetral-runtime-direct-sds")) | toJson | quote }}
sidecar.istio.io/userVolumeMount: '[{"name":"runtime-direct-leaf","mountPath":"/var/run/tetral/runtime-direct/leaf","readOnly":true},{"name":"runtime-direct-trust","mountPath":"/var/run/tetral/runtime-direct/trust","readOnly":true},{"name":"runtime-direct-sds","mountPath":"/var/run/tetral/runtime-direct/sds","readOnly":true}]'
{{- end }}
{{- else if eq .role "job-runner" }}
traffic.sidecar.istio.io/excludeOutboundPorts: {{ if eq $root.Values.transport.profile "hardened" }}"19443"{{ else }}"19090"{{ end }}
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
{{- if .Values.autoscaling.providerGateway.enabled -}}{{- $provider = int .Values.autoscaling.providerGateway.maxReplicas -}}{{- end -}}
{{- $bridge := int .Values.replicas.bridge -}}
{{- $runner := int .Values.replicas.jobRunner -}}
{{- $mcp := int .Values.replicas.mcpConnector -}}
{{- $api := int .Values.replicas.api -}}
{{- $auth := int .Values.replicas.auth -}}
{{- $single := int (include "tetral.surge" (dict "root" . "replicas" 1)) -}}
{{/* Git Proxy retains its existing fixed HPA maximum of ten. */}}
{{- $git := int (include "tetral.gitProxyMaxReplicas" .) -}}
{{- $goProcesses := add $bridge (int (include "tetral.surge" (dict "root" . "replicas" $bridge))) $runner (int (include "tetral.surge" (dict "root" . "replicas" $runner)))  $api (int (include "tetral.surge" (dict "root" . "replicas" $api))) $auth (int (include "tetral.surge" (dict "root" . "replicas" $auth))) (mul 2 (add 1 $single)) $git (int (include "tetral.surge" (dict "root" . "replicas" $git))) 1 -}}
{{- $bunProcesses := add $provider (int (include "tetral.surge" (dict "root" . "replicas" $provider))) $mcp (int (include "tetral.surge" (dict "root" . "replicas" $mcp))) -}}
{{- add (mul $goProcesses (int .Values.transport.goPoolMax)) (mul $bunProcesses 2 (int .Values.transport.bunPoolMax)) -}}
{{- end -}}

{{/* Existing Git Proxy HPA bound, shared by the workload and SQL ledger. */}}
{{- define "tetral.gitProxyMaxReplicas" -}}10{{- end -}}
