package skeleton

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/blutspende/bloodlab-common/utils"
	"github.com/google/uuid"
	"sort"
	"strings"
)

func HashDeletedInstrument(instrumentID uuid.UUID) string {
	const deletedMarker = "DELETED"
	const deletedType = "INSTRUMENT"

	hasher := sha256.New()
	hasher.Write([]byte(deletedMarker))
	hasher.Write([]byte(deletedType))
	hasher.Write([]byte(instrumentID.String()))

	return hex.EncodeToString(hasher.Sum(nil))
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
	builder.WriteString(string(instrument.Encoding))
	builder.WriteString(string(instrument.TimeZone))
	builder.WriteString(instrument.Hostname)
	if instrument.ClientPort != nil {
		builder.WriteString(fmt.Sprintf("%d", *instrument.ClientPort))
	}
	builder.WriteString(fmt.Sprintf("%t", instrument.DeletedAt != nil))

	// Hash nested struct FTPConfig
	if instrument.FTPConfig != nil {
		builder.WriteString(instrument.FTPConfig.Username)
		builder.WriteString(instrument.FTPConfig.Password)
		builder.WriteString(instrument.FTPConfig.OrderPath)
		builder.WriteString(instrument.FTPConfig.OrderFileMask)
		builder.WriteString(instrument.FTPConfig.OrderFileSuffix)
		builder.WriteString(instrument.FTPConfig.ResultPath)
		builder.WriteString(instrument.FTPConfig.ResultFileMask)
		builder.WriteString(instrument.FTPConfig.ResultFileSuffix)
		builder.WriteString(instrument.FTPConfig.FtpServerType)
	}

	// Hash slices AnalyteMappings, RequestMappings, SortingRules, and Settings
	hashSlice(&builder, instrument.AnalyteMappings, func(a AnalyteMapping) string {
		var analyteBuilder strings.Builder
		analyteBuilder.WriteString(a.AnalyteID.String())
		analyteBuilder.WriteString(a.InstrumentAnalyte)
		analyteBuilder.WriteString(string(a.ResultType))
		analyteBuilder.WriteString(utils.StringPointerToString(a.ControlInstrumentAnalyte))
		analyteBuilder.WriteString(fmt.Sprintf("%t", a.ControlResultRequired))

		// Hash nested ChannelMappings
		hashSlice(&analyteBuilder, a.ChannelMappings, func(c ChannelMapping) string {
			return c.ChannelID.String() + c.InstrumentChannel
		})

		// Hash nested ResultMappings
		hashSlice(&analyteBuilder, a.ResultMappings, func(r ResultMapping) string {
			return r.Key + r.Value + fmt.Sprintf("%d", r.Index)
		})

		return analyteBuilder.String()
	})

	hashSlice(&builder, instrument.RequestMappings, func(r RequestMapping) string {
		var reqBuilder strings.Builder
		reqBuilder.WriteString(r.Code)
		reqBuilder.WriteString(fmt.Sprintf("%t", r.IsDefault))

		// Hash AnalyteIDs (sorting ensures consistent order)
		analyteIDs := make([]string, len(r.AnalyteIDs))
		for i, id := range r.AnalyteIDs {
			analyteIDs[i] = id.String()
		}
		sort.Strings(analyteIDs)
		for _, id := range analyteIDs {
			reqBuilder.WriteString(id)
		}

		return reqBuilder.String()
	})

	hashSlice(&builder, instrument.SortingRules, func(s SortingRule) string {
		var ruleBuilder strings.Builder
		ruleBuilder.WriteString(s.InstrumentID.String())
		ruleBuilder.WriteString(s.Target)
		ruleBuilder.WriteString(s.Programme)
		ruleBuilder.WriteString(fmt.Sprintf("%d", s.Priority))

		// Hash Condition if it exists
		if s.Condition != nil {
			ruleBuilder.WriteString(hashCondition(s.Condition))
		}

		return ruleBuilder.String()
	})

	hashSlice(&builder, instrument.Settings, func(s InstrumentSetting) string {
		return s.ProtocolSettingID.String() + s.Value
	})

	// Create the hash
	hasher := sha256.New()
	hasher.Write([]byte(builder.String()))
	return hex.EncodeToString(hasher.Sum(nil))
}

func HashDeletedExpectedControlResult(expectedControlResultId, userId uuid.UUID) string {
	const deletedMarker = "DELETED"
	const deletedType = "EXPECTED_CONTROL_RESULT"

	hasher := sha256.New()
	hasher.Write([]byte(deletedMarker))
	hasher.Write([]byte(deletedType))
	hasher.Write([]byte(expectedControlResultId.String()))
	hasher.Write([]byte(userId.String()))

	return hex.EncodeToString(hasher.Sum(nil))
}

func HashExpectedControlResults(expectedControlResults []ExpectedControlResult) string {
	var builder strings.Builder

	hashSlice(&builder, expectedControlResults, func(e ExpectedControlResult) string {
		var resultBuilder strings.Builder
		resultBuilder.WriteString(e.ID.String())
		resultBuilder.WriteString(e.SampleCode)
		resultBuilder.WriteString(e.AnalyteMappingId.String())
		resultBuilder.WriteString(e.ExpectedValue)
		if e.ExpectedValue2 != nil {
			resultBuilder.WriteString(*e.ExpectedValue2)
		}
		resultBuilder.WriteString(string(e.Operator))
		resultBuilder.WriteString(fmt.Sprintf("%t", e.DeletedAt != nil))
		resultBuilder.WriteString(e.CreatedBy.String())
		if e.DeletedBy.Valid {
			resultBuilder.WriteString(e.DeletedBy.UUID.String())
		}
		return resultBuilder.String()
	})

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

// Recursive function to hash Condition structs
func hashCondition(cond *Condition) string {
	if cond == nil {
		return ""
	}

	var condBuilder strings.Builder
	if cond.Name != nil {
		condBuilder.WriteString(*cond.Name)
	}
	condBuilder.WriteString(string(cond.Operator))
	condBuilder.WriteString(fmt.Sprintf("%t", cond.NegateSubCondition1))
	condBuilder.WriteString(fmt.Sprintf("%t", cond.NegateSubCondition2))

	// Recursively hash subconditions
	condBuilder.WriteString(hashCondition(cond.SubCondition1))
	condBuilder.WriteString(hashCondition(cond.SubCondition2))

	// Hash operands if present
	condBuilder.WriteString(hashOperand(cond.Operand1))
	condBuilder.WriteString(hashOperand(cond.Operand2))

	return condBuilder.String()
}

// Function to hash ConditionOperand structs
func hashOperand(operand *ConditionOperand) string {
	if operand == nil {
		return ""
	}

	var operandBuilder strings.Builder
	if operand.Name != nil {
		operandBuilder.WriteString(*operand.Name)
	}
	operandBuilder.WriteString(string(operand.Type))
	if operand.ConstantValue != nil {
		operandBuilder.WriteString(*operand.ConstantValue)
	}
	if operand.ExtraValueKey != nil {
		operandBuilder.WriteString(*operand.ExtraValueKey)
	}

	return operandBuilder.String()
}
