package skeleton

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"github.com/google/uuid"
	"sort"
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

func stringPointerToStringWithDefault(value *string, defaultValue string) string {
	if value != nil {
		return *value
	}
	return defaultValue
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

func ConvertUUIDsToMap(ids []uuid.UUID) map[uuid.UUID]any {
	idsMap := make(map[uuid.UUID]any)
	for _, id := range ids {
		idsMap[id] = nil
	}
	return idsMap
}

func HashInstrument(instrument Instrument) string {
	var builder strings.Builder

	// Hash basic fields
	builder.WriteString(instrument.ID.String())
	builder.WriteString(instrument.Name)
	builder.WriteString(string(instrument.Type))
	builder.WriteString(instrument.ProtocolID.String())
	builder.WriteString(string(instrument.ProtocolName))
	builder.WriteString(string(instrument.ConnectionMode))
	builder.WriteString(string(instrument.ResultMode))
	builder.WriteString(fmt.Sprintf("%t", instrument.CaptureResults))
	builder.WriteString(fmt.Sprintf("%t", instrument.CaptureDiagnostics))
	builder.WriteString(fmt.Sprintf("%t", instrument.ReplyToQuery))
	builder.WriteString(instrument.Status)
	builder.WriteString(instrument.FileEncoding)
	builder.WriteString(instrument.Timezone)
	builder.WriteString(instrument.Hostname)
	if instrument.ClientPort != nil {
		builder.WriteString(fmt.Sprintf("%d", *instrument.ClientPort))
	}
	builder.WriteString(instrument.CreatedAt.Format(time.RFC3339))
	if instrument.ModifiedAt != nil {
		builder.WriteString(instrument.ModifiedAt.Format(time.RFC3339))
	}
	if instrument.DeletedAt != nil {
		builder.WriteString(instrument.DeletedAt.Format(time.RFC3339))
	}

	// Hash nested struct FTPConfig
	if instrument.FTPConfig != nil {
		builder.WriteString(instrument.FTPConfig.ID.String())
		builder.WriteString(instrument.FTPConfig.Username)
		builder.WriteString(instrument.FTPConfig.OrderPath)
		builder.WriteString(instrument.FTPConfig.ResultPath)
	}

	// Hash slices AnalyteMappings, RequestMappings, SortingRules, and Settings
	hashSlice(&builder, instrument.AnalyteMappings, func(a AnalyteMapping) string {
		return a.ID.String() + a.InstrumentAnalyte
	})

	hashSlice(&builder, instrument.RequestMappings, func(r RequestMapping) string {
		return r.ID.String() + r.Code
	})

	hashSlice(&builder, instrument.SortingRules, func(s SortingRule) string {
		return s.ID.String() + s.Target
	})

	hashSlice(&builder, instrument.Settings, func(s InstrumentSetting) string {
		return s.ID.String() + s.Value
	})

	// Create the hash
	hasher := sha256.New()
	hasher.Write([]byte(builder.String()))
	return hex.EncodeToString(hasher.Sum(nil))
}

// Utility function to hash slices using a custom string generator
func hashSlice[T any](builder *strings.Builder, slice []T, toString func(T) string) {
	sort.SliceStable(slice, func(i, j int) bool {
		return toString(slice[i]) < toString(slice[j])
	})
	for _, item := range slice {
		builder.WriteString(toString(item))
	}
}
