{{- define "gpu-broker.name" -}}
{{- printf "%s-gpu-broker" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "gpu-broker.selector" -}}
app.kubernetes.io/name: gpu-broker
app.kubernetes.io/instance: {{ .Release.Name | quote }}
{{- end -}}
