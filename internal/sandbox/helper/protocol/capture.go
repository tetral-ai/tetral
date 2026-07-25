package protocol

const (
	CaptureDefaultMaxEntries   = 10_000
	CaptureDefaultMaxNameBytes = 512 * 1024
)

type CaptureEnvelope struct {
	SchemaVersion int            `json:"schema_version"`
	Status        string         `json:"status"`
	Result        *CaptureResult `json:"result,omitempty"`
	Error         *CaptureError  `json:"error,omitempty"`
}

type CaptureResult struct {
	SourcePath           string   `json:"source_path"`
	Kind                 string   `json:"kind"`
	LinkCount            uint64   `json:"link_count"`
	SizeBytes            int64    `json:"size_bytes"`
	Skipped              bool     `json:"skipped,omitempty"`
	SkipReason           string   `json:"skip_reason,omitempty"`
	SHA256               string   `json:"sha256,omitempty"`
	MIMEType             string   `json:"mime_type,omitempty"`
	DataBase64           string   `json:"data_base64,omitempty"`
	Entries              []string `json:"entries,omitempty"`
	EntriesTruncated     bool     `json:"entries_truncated,omitempty"`
	UnrepresentableNames int      `json:"unrepresentable_names,omitempty"`
}

type CaptureError struct {
	Kind    string `json:"kind"`
	Message string `json:"message"`
}
