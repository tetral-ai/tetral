package pathvalidation

import "golang.org/x/text/unicode/norm"

func IsNFC(value string) bool {
	return norm.NFC.IsNormalString(value)
}

func NormalizeNFC(value string) string {
	return norm.NFC.String(value)
}
