package inference

// ImageInput is an image supplied to a multimodal inference invocation.
type ImageInput struct {
	MimeType  string `json:"mime_type"`
	Data      []byte `json:"-"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
	Source    string `json:"source,omitempty"`
	BlobID    string `json:"blob_id,omitempty"`
}

type MessagePart struct {
	Type  string      `json:"type"`
	Text  string      `json:"text,omitempty"`
	Image *ImageInput `json:"image,omitempty"`
}

type Message struct {
	Role    string        `json:"role"`
	Content string        `json:"content,omitempty"`
	Parts   []MessagePart `json:"parts,omitempty"`
}

type Usage struct {
	InputTokens      int64
	OutputTokens     int64
	TotalTokens      int64
	TokenCountSource string
}
