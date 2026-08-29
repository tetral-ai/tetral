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
