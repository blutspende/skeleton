package utils

import "strings"

func JoinEnumsAsString[T []string](enumList T, separator string) string {
	return strings.Join(enumList, separator)
}

func SplitStringToEnumArray[R []string](value string, separator string) R {
	return strings.Split(value, separator)
}
