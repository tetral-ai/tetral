{{- define "tetral.previewEnv" -}}
{{- if .Values.preview.enabled }}
- name: TETRAL_NATS_SERVERS
  value: {{ printf "%s://%s" (ternary "tls" "nats" (eq .Values.transport.profile "hardened")) .Values.preview.brokerAddress | quote }}
- name: TETRAL_NATS_USER_PATH
  value: /var/run/tetral/nats-role/user
- name: TETRAL_NATS_PASSWORD_PATH
  value: /var/run/tetral/nats-role/password
{{- if eq .Values.transport.profile "hardened" }}
- name: TETRAL_NATS_TLS_CA_PATH
  value: /var/run/tetral/nats-trust/ca.crt
- name: TETRAL_NATS_TLS_CERT_PATH
  value: /var/run/tetral/nats-leaf/tls.crt
- name: TETRAL_NATS_TLS_KEY_PATH
  value: /var/run/tetral/nats-leaf/tls.key
{{- end }}
{{- end }}
{{- end -}}

{{- define "tetral.previewMounts" -}}
{{- if .Values.preview.enabled }}
- name: nats-role
  mountPath: /var/run/tetral/nats-role
  readOnly: true
{{- if eq .Values.transport.profile "hardened" }}
- name: nats-trust
  mountPath: /var/run/tetral/nats-trust
  readOnly: true
- name: nats-leaf
  mountPath: /var/run/tetral/nats-leaf
  readOnly: true
{{- end }}
{{- end }}
{{- end -}}

{{- define "tetral.previewVolumes" -}}
{{- if .root.Values.preview.enabled }}
- name: nats-role
  secret:
    secretName: {{ index .root.Values.preview (printf "%sSecret" .role) | quote }}
    defaultMode: 0440
    items:
      - key: user
        path: user
      - key: password
        path: password
{{- if eq .root.Values.transport.profile "hardened" }}
- name: nats-trust
  configMap:
    name: {{ .root.Values.preview.trustConfigMap | quote }}
- name: nats-leaf
  secret:
    secretName: {{ index .root.Values.preview (printf "%sLeafSecret" .role) | quote }}
    defaultMode: 0440
    items:
      - key: tls.crt
        path: tls.crt
      - key: tls.key
        path: tls.key
{{- end }}
{{- end }}
{{- end -}}

{{- define "tetral.previewGatewayPolicy" -}}
{{- if .Values.preview.enabled }}
{{- range $env, $value := dict "TETRAL_GATEWAY_PREVIEW_QUEUE_BYTES" .Values.preview.gateway.queueBytes "TETRAL_GATEWAY_PREVIEW_QUEUE_FRAMES" .Values.preview.gateway.queueFrames "TETRAL_GATEWAY_PREVIEW_BATCH_BYTES" .Values.preview.gateway.batchBytes "TETRAL_GATEWAY_PREVIEW_BATCH_FRAMES" .Values.preview.gateway.batchFrames "TETRAL_GATEWAY_PREVIEW_FLUSH_TIMEOUT_MS" .Values.preview.gateway.flushTimeoutMs "TETRAL_NATS_CONNECT_TIMEOUT_MS" .Values.preview.gateway.connectTimeoutMs "TETRAL_NATS_RETRY_MAX_MS" .Values.preview.gateway.retryMaxMs "TETRAL_NATS_CREDENTIAL_POLL_MS" .Values.preview.gateway.credentialPollMs "TETRAL_NATS_PING_INTERVAL_MS" .Values.preview.gateway.pingIntervalMs "TETRAL_NATS_MAX_PING_OUT" .Values.preview.gateway.maxPingOut }}
- name: {{ $env }}
  value: {{ $value | int64 | quote }}
{{- end }}
{{- end }}
{{- end -}}

{{- define "tetral.eventStreamPolicy" -}}
{{- range $env, $value := dict "TETRAL_EVENT_STREAM_POLL_INTERVAL_MS" .Values.eventStream.pollIntervalMs "TETRAL_EVENT_STREAM_HEARTBEAT_INTERVAL_MS" .Values.eventStream.heartbeatIntervalMs "TETRAL_EVENT_STREAM_WRITE_TIMEOUT_MS" .Values.eventStream.writeTimeoutMs "TETRAL_EVENT_STREAM_PREVIEW_SETUP_TIMEOUT_MS" .Values.eventStream.previewSetupTimeoutMs "TETRAL_EVENT_STREAM_HUB_MAX_BYTES" .Values.eventStream.hubMaxBytes "TETRAL_EVENT_STREAM_HUB_MAX_FRAMES" .Values.eventStream.hubMaxFrames "TETRAL_EVENT_STREAM_VIEWER_MAX_BYTES" .Values.eventStream.viewerMaxBytes "TETRAL_EVENT_STREAM_VIEWER_MAX_FRAMES" .Values.eventStream.viewerMaxFrames "TETRAL_EVENT_STREAM_SUBSCRIPTION_MAX_BYTES" .Values.eventStream.subscriptionMaxBytes "TETRAL_EVENT_STREAM_SUBSCRIPTION_MAX_FRAMES" .Values.eventStream.subscriptionMaxFrames "TETRAL_EVENT_STREAM_ACTIVE_REQUESTS" .Values.eventStream.activeRequests }}
- name: {{ $env }}
  value: {{ $value | int64 | quote }}
{{- end }}
{{- if .Values.preview.enabled }}
- name: TETRAL_NATS_CONNECT_TIMEOUT_MS
  value: {{ .Values.preview.subscriber.connectTimeoutMs | quote }}
- name: TETRAL_NATS_RECONNECT_WAIT_MS
  value: {{ .Values.preview.subscriber.reconnectWaitMs | quote }}
- name: TETRAL_NATS_PING_INTERVAL_MS
  value: {{ .Values.preview.subscriber.pingIntervalMs | int64 | quote }}
- name: TETRAL_NATS_MAX_PING_OUT
  value: {{ .Values.preview.subscriber.maxPingOut | int64 | quote }}
{{- end }}
{{- end -}}
