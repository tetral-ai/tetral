package protocol

// ReceivedToolStatus is a tool report as Engine received it: its fields, its
// compact size, and the parsed JSON it stores.
type ReceivedToolStatus struct {
	Status ToolStatus
	Size   int // bytes of compact JSON
	tree   *value
}

// DecodeReceivedToolStatus decodes a tool report as DecodeToolStatus does and
// keeps what storing it needs.
func DecodeReceivedToolStatus(data []byte) (ReceivedToolStatus, error) {
	var tree *value
	status, err := toolChecked(data, toolStatus, func(s ToolStatus, v *value) error {
		tree = v
		return checkToolStatus(s, v)
	})
	if err != nil {
		return ReceivedToolStatus{}, err
	}
	return ReceivedToolStatus{Status: status, Size: compactSize(tree), tree: tree}, nil
}

// Helper is the report's result envelope, unset for a rejection or a
// control error without a result.
func (r ReceivedToolStatus) Helper() Optional[HelperEnvelope] {
	switch s := r.Status.(type) {
	case ExecutionReport:
		return s.Result
	case ControlReport:
		return s.Result
	}
	return Optional[HelperEnvelope]{}
}

// Image is the image a view_image result holds.
func (r ReceivedToolStatus) Image() (MediaResult, bool) {
	helper := r.Helper()
	if !helper.Set || helper.Value.Tool != ToolViewImage {
		return MediaResult{}, false
	}
	media, ok := helper.Value.Result.(MediaResult)
	return media, ok
}

// WithAttachment is the report with its image's data_base64 member replaced
// by the member attachment holding reference, a JSON object. The size is
// unchanged: it stays the size the report was received with.
func (r ReceivedToolStatus) WithAttachment(reference []byte) (ReceivedToolStatus, error) {
	attachment, err := parse(reference)
	if err != nil {
		return ReceivedToolStatus{}, err
	}
	helper, _ := r.tree.get("result")
	inner, _ := helper.get("result")
	if _, ok := inner.get("data_base64"); !ok {
		return r, nil
	}
	stored := *inner
	stored.members = make([]member, 0, len(inner.members))
	for _, m := range inner.members {
		if m.key == "data_base64" {
			m = member{key: "attachment", value: attachment}
		}
		stored.members = append(stored.members, m)
	}
	r.tree = replaceMember(r.tree, "result", replaceMember(helper, "result", &stored))
	return r, nil
}

// replaceMember is a copy of object v whose member key holds replacement.
func replaceMember(v *value, key string, replacement *value) *value {
	out := *v
	out.members = append([]member(nil), v.members...)
	for i := range out.members {
		if out.members[i].key == key {
			out.members[i].value = replacement
		}
	}
	return &out
}

// Body is the report's compact JSON.
func (r ReceivedToolStatus) Body() []byte { return appendValue(nil, r.tree) }

// ResultJSON is the compact JSON of the report's result envelope, or nil
// without one.
func (r ReceivedToolStatus) ResultJSON() []byte {
	helper, ok := r.tree.get("result")
	if !ok {
		return nil
	}
	return appendValue(nil, helper)
}
