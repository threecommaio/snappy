package snappy

import "strings"

func Split(s, sep string) (string, string) {
	// Empty string should just return empty
	if len(s) == 0 {
		return s, s
	}

	slice := strings.SplitN(s, sep, 2)

	// Incase no separator was present
	if len(slice) == 1 {
		return slice[0], ""
	}

	return slice[0], slice[1]
}
