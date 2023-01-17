package skeleton

import "strings"

// StandardizeUSDecimalValue removes all thousands-seperators and normalizes to the form 123123.00 having the "." the decimal separator
func StandardizeUSDecimalValue(in string) string {

	in = strings.ReplaceAll(in, ",", "") // remove thousdand-separator
	if strings.HasPrefix(in, ".") {
		in = "0" + in
	}
	in = strings.TrimSuffix(in, ".")

	return in
}

// StandardizeUSDecimalValue removes all thousands-seperators and normalizes to the form 123123.00 having the "." the decimal separator
func StandardizeEUDecimalValue(in string) string {

	in = strings.ReplaceAll(in, ".", "") // remove thousdand-separator

	if strings.Count(in, ",") == 1 { // if one decimal separator is included replace that with the ".", if there are more, this is an error of confusion
		in = strings.ReplaceAll(in, ",", ".")
	}
	if strings.HasPrefix(in, ".") {
		in = "0" + in
	}
	in = strings.TrimSuffix(in, ".")

	return in
}

//LookupResultMapping - Based on the Resultmappings (made in UI) translate a reulst value to its expected value
//by example "+" -> "pos" or "?" -> "inv" etc...
func LookupResultMapping(analyteMapping AnalyteMapping, valueFromInstrument string) string {

	for _, resultMappedKeyValue := range analyteMapping.ResultMappings {
		if resultMappedKeyValue.Key == valueFromInstrument {
			return resultMappedKeyValue.Value
		}
	}

	return valueFromInstrument
}

func partition(totalLength int, partitionLength int, consumer func(low int, high int)) {
	if partitionLength <= 0 || totalLength <= 0 {
		return
	}
	partitions := totalLength / partitionLength
	var i int
	for i = 0; i < partitions; i++ {
		consumer(i*partitionLength, i*partitionLength+partitionLength)
	}
	if rest := totalLength % partitionLength; rest != 0 {
		consumer(i*partitionLength, i*partitionLength+rest)
	}
}
