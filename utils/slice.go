package utils

/*
func MapSliceKeyExists[V comparable, K comparable](search V, data map[V]K) bool {
	for key := range data {
		if key == search {
			return true
		}
	}
	return false
}
*/

// SliceContains - utility-function to check wether an element is part of an array
func SliceContains[V comparable](search V, data []V) bool {
	for _, value := range data {
		if value == search {
			return true
		}
	}
	return false
}

func RemoveIndex[V comparable](s []V, index int) []V {
	return append(s[:index], s[index+1:]...)
}
