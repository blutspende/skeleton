package utils

import "strings"

func JoinEnumsAsString[T ~string](enumList []T, separator string) string {
	items := make([]string, len(enumList))
	for i := range enumList {
		items[i] = string(enumList[i])
	}
	return strings.Join(items, separator)
}

func SplitStringToEnumArray[R any](value string, separator string) []R {
	stringItems := strings.Split(value, separator)
	items := make([]R, len(stringItems))
	for i := range stringItems {
		items[i] = any(stringItems[i]).(R)
	}
	return items
}
