package skeleton

import (
	"database/sql"
	"github.com/google/uuid"
	"strconv"
	"strings"
	"time"
)

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

// LookupResultMapping - Based on the Resultmappings (made in UI) translate a reulst value to its expected value
// by example "+" -> "pos" or "?" -> "inv" etc...
func LookupResultMapping(analyteMapping AnalyteMapping, valueFromInstrument string) string {

	for _, resultMappedKeyValue := range analyteMapping.ResultMappings {
		if resultMappedKeyValue.Key == valueFromInstrument {
			return resultMappedKeyValue.Value
		}
	}

	return valueFromInstrument
}

func stringPointerToString(value *string) string {
	if value != nil {
		return *value
	}
	return ""
}

func nullStringToString(value sql.NullString) string {
	if value.Valid {
		return value.String
	}
	return ""
}

func nullStringToStringPointer(value sql.NullString) *string {
	if value.Valid {
		return &value.String
	}
	return nil
}

func nullUUIDToUUIDPointer(value uuid.NullUUID) *uuid.UUID {
	if value.Valid {
		return &value.UUID
	}
	return nil
}

func nullTimeToTimePointer(value sql.NullTime) *time.Time {
	if value.Valid {
		return &value.Time
	}
	return nil
}

func timePointerToNullTime(value *time.Time) sql.NullTime {
	if value != nil {
		return sql.NullTime{
			Time:  *value,
			Valid: true,
		}
	}
	return sql.NullTime{}
}

func isSorted(pageable Pageable) bool {
	return pageable.Sort != ""
}

func applyPagination(pageable Pageable, tableAlias, defaultSort string) string {
	var paginationQueryPart string
	if isSorted(pageable) {
		var tableAliasWithDot = ""
		if tableAlias != "" {
			tableAliasWithDot = tableAlias + "."
		}
		paginationQueryPart += " ORDER BY " + tableAliasWithDot + pageable.Sort
		if pageable.Direction != SortNone {
			paginationQueryPart += " " + strings.ToUpper(pageable.Direction.String())
		}
	} else if defaultSort != "" && pageable.IsPaged() {
		paginationQueryPart += " ORDER BY " + defaultSort
	}
	if pageable.IsPaged() {
		paginationQueryPart += " LIMIT " + strconv.Itoa(pageable.PageSize)
		if pageable.Page > 0 {
			paginationQueryPart += " OFFSET " + strconv.Itoa(pageable.PageSize*pageable.Page)
		}
	}
	return paginationQueryPart
}
