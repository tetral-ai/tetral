{{- define "tetral.imageTag" -}}
{{- $tag := toString .Values.image.tag -}}
{{- if eq $tag "" -}}
{{- .Chart.AppVersion -}}
{{- else -}}
{{- $tag -}}
{{- end -}}
{{- end -}}
