package utils

import (
	"strings"
)

func JoinEnumsAsString[T ~string](enumList []T, separator string) string {
	items := make([]string, len(enumList))
	for i := range enumList {
		items[i] = string(enumList[i])
	}
	return strings.Join(items, separator)
}
