package runtimecontrol

import "bytes"

// RestoreJSONStringifySeparatorEscapes restores encoder-authored escapes only.
// JavaScript JSON.stringify leaves the Unicode line and paragraph separators
// intact. encoding/json escapes them even when HTML escaping is disabled, so
// declaration digests restore only encoder-authored separator escapes. Escaped
// backslash text such as "\\u2028" remains byte-for-byte unchanged.
func RestoreJSONStringifySeparatorEscapes(encoded []byte) []byte {
	result := make([]byte, 0, len(encoded))
	for offset := 0; offset < len(encoded); {
		next := bytes.IndexByte(encoded[offset:], '\\')
		if next < 0 {
			result = append(result, encoded[offset:]...)
			break
		}
		result = append(result, encoded[offset:offset+next]...)
		offset += next
		separator := offset+6 <= len(encoded) &&
			(string(encoded[offset:offset+6]) == `\u2028` || string(encoded[offset:offset+6]) == `\u2029`)
		if separator {
			precedingSlashes := 0
			for index := offset - 1; index >= 0 && encoded[index] == '\\'; index-- {
				precedingSlashes++
			}
			if precedingSlashes%2 == 0 {
				if encoded[offset+5] == '8' {
					result = append(result, "\u2028"...)
				} else {
					result = append(result, "\u2029"...)
				}
				offset += 6
				continue
			}
		}
		result = append(result, encoded[offset])
		offset++
	}
	return result
}
