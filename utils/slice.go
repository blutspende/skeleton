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

func Partition(totalLength int, partitionLength int, consumer func(low int, high int) error) error {
	if partitionLength <= 0 || totalLength <= 0 {
		return nil
	}
	partitions := totalLength / partitionLength
	var i int
	var err error
	for i = 0; i < partitions; i++ {
		err = consumer(i*partitionLength, i*partitionLength+partitionLength)
		if err != nil {
			return err
		}
	}
	if rest := totalLength % partitionLength; rest != 0 {
		err = consumer(i*partitionLength, i*partitionLength+rest)
		if err != nil {
			return err
		}
	}
	return err
}

func RemoveIndex[V comparable](s []V, index int) []V {
	return append(s[:index], s[index+1:]...)
}
