package pathvalidation

import "golang.org/x/text/unicode/norm"

// streamSafeNonStarters is the longest run of non-starters the Stream-Safe Text
// Format (UAX #15) allows.
const streamSafeNonStarters = 30

func IsNFC(value string) bool {
	return norm.NFC.IsNormalString(value)
}

func NormalizeNFC(value string) string {
	return norm.NFC.String(value)
}

// IsStreamSafe reports whether value's NFKD form has no run of more than 30
// non-starters.
func IsStreamSafe(value string) bool {
	run := 0
	decomposed := norm.NFKD.String(value)
	for i := 0; i < len(decomposed); {
		properties := norm.NFKD.PropertiesString(decomposed[i:])
		if properties.CCC() == 0 {
			run = 0
		} else if run++; run > streamSafeNonStarters {
			return false
		}
		i += properties.Size()
	}
	return true
}
