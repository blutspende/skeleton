package skeleton

import (
	"github.com/blutspende/bloodlab-common/encoding"
	"github.com/blutspende/bloodlab-common/timezone"
	"github.com/google/uuid"
	"strings"
)

// models

type instrumentTO struct {
	ID                 uuid.UUID             `json:"id"`
	Type               InstrumentType        `json:"instrumentType"`
	Name               string                `json:"name"`
	ProtocolID         uuid.UUID             `json:"protocolId"`
	ProtocolName       string                `json:"type"`
	Enabled            bool                  `json:"enabled"`
	ConnectionMode     ConnectionMode        `json:"connectionMode"`
	ResultMode         ResultMode            `json:"runningMode"`
	CaptureResults     bool                  `json:"captureResults"`
	CaptureDiagnostics bool                  `json:"captureDiagnostics"`
	ReplyToQuery       bool                  `json:"replyToQuery"`
	Status             string                `json:"status"`
	Encoding           encoding.Encoding     `json:"fileEncoding"`
	TimeZone           timezone.TimeZone     `json:"timezone"`
	Hostname           string                `json:"hostname"`
	ClientPort         *int                  `json:"clientPort"`
	FileServerConfig   *fileServerConfigTO   `json:"fileServerConfig"`
	AnalyteMappings    []analyteMappingTO    `json:"analyteMappings"`
	RequestMappings    []requestMappingTO    `json:"requestMappings"`
	Settings           []instrumentSettingTO `json:"instrumentSettings"`
	SortingRuleGroups  []sortingRuleGroupTO  `json:"sortingRuleGroups"`
}

type fileServerConfigTO struct {
	Username         string         `json:"userName"`
	Password         string         `json:"password"`
	OrderPath        string         `json:"orderPath"`
	OrderFileMask    string         `json:"orderFileMask"`
	OrderFileSuffix  string         `json:"orderFileSuffix"`
	ResultPath       string         `json:"resultPath"`
	ResultFileMask   string         `json:"resultFileMask"`
	ResultFileSuffix string         `json:"resultFileSuffix"`
	ServerType       FileServerType `json:"serverType"`
}

type analyteMappingTO struct {
	ID                  uuid.UUID          `json:"id"`
	InstrumentAnalyte   string             `json:"instrumentAnalyte"`
	AnalyteID           uuid.UUID          `json:"analyteId"`
	ChannelMappings     []channelMappingTO `json:"channelMappings"`
	ResultMappings      []resultMappingTO  `json:"resultMappings"`
	ResultType          ResultType         `json:"resultType"`
	ControlRequired     bool               `json:"controlRequired"`
	IsControl           bool               `json:"isControl"`
	ValidatedAnalyteIDs []uuid.UUID        `json:"validatedAnalyteIDs"`
}

type requestMappingTO struct {
	ID         uuid.UUID   `json:"id"`
	Code       string      `json:"code"`
	IsDefault  bool        `json:"isDefault"`
	AnalyteIDs []uuid.UUID `json:"requestMappingAnalyteIds"`
}

type controlMappingTO struct {
	ID                uuid.UUID   `json:"id"`
	AnalyteID         uuid.UUID   `json:"analyteId"`
	ControlAnalyteIDs []uuid.UUID `json:"controlAnalyteIds"`
}

type channelMappingTO struct {
	ID                uuid.UUID `json:"id"`
	InstrumentChannel string    `json:"instrumentChannel"`
	ChannelID         uuid.UUID `json:"channelId"`
}

type resultMappingTO struct {
	ID    uuid.UUID `json:"id"`
	Key   string    `json:"key"`
	Value string    `json:"value"`
	Index int       `json:"index"`
}

type instrumentSettingTO struct {
	ID                uuid.UUID `json:"id"`
	ProtocolSettingID uuid.UUID `json:"protocolSettingId"`
	Value             string    `json:"value"`
}

type sortingRuleGroupTO struct {
	Name         string          `json:"name"`
	SortingRules []sortingRuleTO `json:"sortingRules"`
}

type sortingRuleTO struct {
	ID        uuid.UUID    `json:"id"`
	Condition *conditionTO `json:"condition"`
	Target    string       `json:"target"`
	Priority  int          `json:"priority"`
}

type conditionTO struct {
	ID                  uuid.UUID           `json:"id"`
	Name                *string             `json:"name,omitempty"`
	Operator            ConditionOperator   `json:"operator"`
	SubCondition1       *conditionTO        `json:"subCondition1"`
	SubCondition2       *conditionTO        `json:"subCondition2"`
	SubCondition1ID     *uuid.UUID          `json:"subCondition1Id"`
	SubCondition2ID     *uuid.UUID          `json:"subCondition2Id"`
	NegateSubCondition1 bool                `json:"negateSubCondition1"`
	NegateSubCondition2 bool                `json:"negateSubCondition2"`
	Operand1            *conditionOperandTO `json:"operand1"`
	Operand2            *conditionOperandTO `json:"operand2"`
}

type conditionOperandTO struct {
	ID            uuid.UUID            `json:"id"`
	Name          *string              `json:"name"`
	Type          ConditionOperandType `json:"type"`
	ConstantValue *string              `json:"constantValue"`
	ExtraValueKey *string              `json:"extraValueKey"`
	AnalyteID     *uuid.UUID           `json:"analyteId"`
}

type supportedProtocolTO struct {
	ID                uuid.UUID           `json:"id"`
	Name              string              `json:"name"`
	Description       *string             `json:"description"`
	ProtocolAbilities []protocolAbilityTO `json:"protocolAbilities"`
	ProtocolSettings  []protocolSettingTO `json:"protocolSettings"`
}

type protocolAbilityTO struct {
	ConnectionMode          ConnectionMode `json:"connectionMode"`
	Abilities               []Ability      `json:"abilities"`
	RequestMappingAvailable bool           `json:"requestMappingAvailable"`
}

type protocolSettingTO struct {
	ID          uuid.UUID           `json:"id"`
	Key         string              `json:"key"`
	Description *string             `json:"description"`
	Type        ProtocolSettingType `json:"type"`
}

type supportedManufacturerTestTO struct {
	TestName          string   `json:"testName"`
	Channels          []string `json:"channels"`
	ValidResultValues []string `json:"validResultValues"`
}

// converters

func convertInstrumentTOToInstrument(instrumentTO instrumentTO) Instrument {
	model := Instrument{
		ID:                 instrumentTO.ID,
		Type:               instrumentTO.Type,
		Name:               instrumentTO.Name,
		ProtocolID:         instrumentTO.ProtocolID,
		ProtocolName:       instrumentTO.ProtocolName,
		Enabled:            instrumentTO.Enabled,
		ConnectionMode:     instrumentTO.ConnectionMode,
		ResultMode:         instrumentTO.ResultMode,
		CaptureResults:     instrumentTO.CaptureResults,
		CaptureDiagnostics: instrumentTO.CaptureDiagnostics,
		Status:             instrumentTO.Status,
		ReplyToQuery:       instrumentTO.ReplyToQuery,
		Encoding:           instrumentTO.Encoding,
		TimeZone:           instrumentTO.TimeZone,
		Hostname:           instrumentTO.Hostname,
		ClientPort:         instrumentTO.ClientPort,
		AnalyteMappings:    make([]AnalyteMapping, len(instrumentTO.AnalyteMappings)),
		RequestMappings:    make([]RequestMapping, len(instrumentTO.RequestMappings)),
		SortingRules:       make([]SortingRule, 0),
		Settings:           convertInstrumentSettingTOsToInstrumentSettings(instrumentTO.Settings),
	}

	if instrumentTO.ConnectionMode == FileServer {
		model.FileServerConfig = &FileServerConfig{
			InstrumentId: instrumentTO.ID,
			OrderPath:    "/",
			ResultPath:   "/",
		}
		if instrumentTO.FileServerConfig != nil {
			model.FileServerConfig = &FileServerConfig{
				InstrumentId:     instrumentTO.ID,
				Username:         instrumentTO.FileServerConfig.Username,
				Password:         instrumentTO.FileServerConfig.Password,
				OrderPath:        instrumentTO.FileServerConfig.OrderPath,
				OrderFileMask:    instrumentTO.FileServerConfig.OrderFileMask,
				OrderFileSuffix:  instrumentTO.FileServerConfig.OrderFileSuffix,
				ResultPath:       instrumentTO.FileServerConfig.ResultPath,
				ResultFileMask:   instrumentTO.FileServerConfig.ResultFileMask,
				ResultFileSuffix: instrumentTO.FileServerConfig.ResultFileSuffix,
				ServerType:       instrumentTO.FileServerConfig.ServerType,
			}
			if !strings.HasPrefix(model.FileServerConfig.OrderPath, "/") {
				model.FileServerConfig.OrderPath = "/" + model.FileServerConfig.OrderPath
			}
			if !strings.HasPrefix(model.FileServerConfig.ResultPath, "/") {
				model.FileServerConfig.ResultPath = "/" + model.FileServerConfig.ResultPath
			}
		}
	}

	if instrumentTO.Status == "" {
		model.Status = string(InstrumentReady)
	}

	for i, analyteMapping := range instrumentTO.AnalyteMappings {
		model.AnalyteMappings[i] = convertAnalyteMappingTOToAnalyteMapping(analyteMapping)
	}

	for i, requestMapping := range instrumentTO.RequestMappings {
		model.RequestMappings[i] = convertRequestMappingTOToRequestMapping(requestMapping)
	}

	model.SortingRules = convertSortingRuleGroupTOsToSortingRules(instrumentTO.SortingRuleGroups, instrumentTO.ID)

	return model
}

func convertInstrumentSettingTOsToInstrumentSettings(settingTOs []instrumentSettingTO) []InstrumentSetting {
	settings := make([]InstrumentSetting, len(settingTOs))
	for i := range settings {
		settings[i] = convertInstrumentSettingTOToInstrumentSetting(settingTOs[i])
	}
	return settings
}

func convertInstrumentSettingTOToInstrumentSetting(settingTO instrumentSettingTO) InstrumentSetting {
	return InstrumentSetting{
		ID:                settingTO.ID,
		ProtocolSettingID: settingTO.ProtocolSettingID,
		Value:             settingTO.Value,
	}
}

func convertAnalyteMappingTOToAnalyteMapping(analyteMappingTO analyteMappingTO) AnalyteMapping {
	model := AnalyteMapping{
		ID:                    analyteMappingTO.ID,
		InstrumentAnalyte:     analyteMappingTO.InstrumentAnalyte,
		AnalyteID:             analyteMappingTO.AnalyteID,
		ChannelMappings:       make([]ChannelMapping, len(analyteMappingTO.ChannelMappings)),
		ResultMappings:        make([]ResultMapping, len(analyteMappingTO.ResultMappings)),
		ResultType:            analyteMappingTO.ResultType,
		ControlResultRequired: analyteMappingTO.ControlRequired,
		IsControl:             analyteMappingTO.IsControl,
		ValidatedAnalyteIDs:   make([]uuid.UUID, len(analyteMappingTO.ValidatedAnalyteIDs)),
	}

	for i, channelMapping := range analyteMappingTO.ChannelMappings {
		model.ChannelMappings[i] = convertChannelMappingTOToChannelMapping(channelMapping)
	}

	for i, resultMapping := range analyteMappingTO.ResultMappings {
		model.ResultMappings[i] = convertResultMappingTOToResultMapping(resultMapping)
	}

	for i, validatedAnalyteID := range analyteMappingTO.ValidatedAnalyteIDs {
		model.ValidatedAnalyteIDs[i] = validatedAnalyteID
	}

	return model
}

func convertRequestMappingTOToRequestMapping(requestMappingTO requestMappingTO) RequestMapping {
	return RequestMapping{
		ID:         requestMappingTO.ID,
		Code:       requestMappingTO.Code,
		IsDefault:  requestMappingTO.IsDefault,
		AnalyteIDs: requestMappingTO.AnalyteIDs,
	}
}

func convertChannelMappingTOToChannelMapping(channelMappingTO channelMappingTO) ChannelMapping {
	return ChannelMapping{
		ID:                channelMappingTO.ID,
		InstrumentChannel: channelMappingTO.InstrumentChannel,
		ChannelID:         channelMappingTO.ChannelID,
	}
}

func convertResultMappingTOToResultMapping(resultMappingTO resultMappingTO) ResultMapping {
	return ResultMapping{
		ID:    resultMappingTO.ID,
		Key:   resultMappingTO.Key,
		Value: resultMappingTO.Value,
		Index: resultMappingTO.Index,
	}
}

func convertSupportedProtocolToSupportedProtocolTO(supportedProtocol SupportedProtocol) supportedProtocolTO {
	to := supportedProtocolTO{
		ID:                supportedProtocol.ID,
		Name:              supportedProtocol.Name,
		Description:       supportedProtocol.Description,
		ProtocolAbilities: convertProtocolAbilitiesToProtocolAbilitiesTOs(supportedProtocol.ProtocolAbilities),
		ProtocolSettings:  convertProtocolSettingsToProtocolSettingsTOs(supportedProtocol.ProtocolSettings),
	}
	return to
}

func convertSupportedProtocolsToSupportedProtocolTOs(supportedProtocols []SupportedProtocol) []supportedProtocolTO {
	tos := make([]supportedProtocolTO, len(supportedProtocols))
	for i := range supportedProtocols {
		tos[i] = convertSupportedProtocolToSupportedProtocolTO(supportedProtocols[i])
	}
	return tos
}

func convertProtocolAbilityToProtocolAbilityTO(protocolAbility ProtocolAbility) protocolAbilityTO {
	to := protocolAbilityTO{
		ConnectionMode:          protocolAbility.ConnectionMode,
		Abilities:               protocolAbility.Abilities,
		RequestMappingAvailable: protocolAbility.RequestMappingAvailable,
	}
	return to
}

func convertProtocolAbilitiesToProtocolAbilitiesTOs(protocolAbilities []ProtocolAbility) []protocolAbilityTO {
	tos := make([]protocolAbilityTO, len(protocolAbilities))
	for i := range protocolAbilities {
		tos[i] = convertProtocolAbilityToProtocolAbilityTO(protocolAbilities[i])
	}
	return tos
}

func convertProtocolSettingToProtocolSettingTO(setting ProtocolSetting) protocolSettingTO {
	return protocolSettingTO{
		ID:          setting.ID,
		Key:         setting.Key,
		Description: setting.Description,
		Type:        setting.Type,
	}
}

func convertProtocolSettingsToProtocolSettingsTOs(settings []ProtocolSetting) []protocolSettingTO {
	tos := make([]protocolSettingTO, len(settings))
	for i := range settings {
		tos[i] = convertProtocolSettingToProtocolSettingTO(settings[i])
	}
	return tos
}

func convertSupportedManufacturerTestToSupportedManufacturerTestTO(supportedManufacturerTest SupportedManufacturerTests) supportedManufacturerTestTO {
	return supportedManufacturerTestTO{
		TestName:          supportedManufacturerTest.TestName,
		Channels:          supportedManufacturerTest.Channels,
		ValidResultValues: supportedManufacturerTest.ValidResultValues,
	}
}

func convertSupportedManufacturerTestsToSupportedManufacturerTestTOs(supportedManufacturerTests []SupportedManufacturerTests) []supportedManufacturerTestTO {
	tos := make([]supportedManufacturerTestTO, len(supportedManufacturerTests))
	for i := range supportedManufacturerTests {
		tos[i] = convertSupportedManufacturerTestToSupportedManufacturerTestTO(supportedManufacturerTests[i])
	}
	return tos
}

func convertTOToSortingRule(to sortingRuleTO, programme string, instrumentID uuid.UUID) SortingRule {
	rule := SortingRule{
		ID:           to.ID,
		InstrumentID: instrumentID,
		Target:       to.Target,
		Programme:    programme,
		Priority:     to.Priority,
	}

	if to.Condition != nil {
		condition := convertTOToCondition(*to.Condition)
		rule.Condition = &condition
	}

	return rule
}

func convertSortingRuleGroupTOsToSortingRules(tos []sortingRuleGroupTO, instrumentID uuid.UUID) []SortingRule {
	sortingRules := make([]SortingRule, 0)
	for i := range tos {
		for _, sortingRule := range tos[i].SortingRules {
			sortingRules = append(sortingRules, convertTOToSortingRule(sortingRule, tos[i].Name, instrumentID))
		}
	}

	return sortingRules
}

func convertTOToCondition(to conditionTO) Condition {
	condition := Condition{
		ID:                  to.ID,
		Name:                to.Name,
		Operator:            to.Operator,
		NegateSubCondition1: to.NegateSubCondition1,
		NegateSubCondition2: to.NegateSubCondition1,
	}
	if to.SubCondition1 != nil {
		subCondition1 := convertTOToCondition(*to.SubCondition1)
		condition.SubCondition1 = &subCondition1
	}
	if to.SubCondition2 != nil {
		subCondition2 := convertTOToCondition(*to.SubCondition2)
		condition.SubCondition2 = &subCondition2
	}
	if to.Operand1 != nil {
		operand1 := convertTOToConditionOperand(*to.Operand1)
		condition.Operand1 = &operand1
	}
	if to.Operand2 != nil {
		operand2 := convertTOToConditionOperand(*to.Operand2)
		condition.Operand2 = &operand2
	}

	return condition
}

func convertTOToConditionOperand(to conditionOperandTO) ConditionOperand {
	return ConditionOperand{
		ID:            to.ID,
		Name:          to.Name,
		Type:          to.Type,
		ConstantValue: to.ConstantValue,
		ExtraValueKey: to.ExtraValueKey,
	}
}

func convertTOsToExpectedControlResults(expectedControlResultTOs []ExpectedControlResultTO) []ExpectedControlResult {
	expectedControlResults := make([]ExpectedControlResult, 0)
	for i := range expectedControlResultTOs {
		expectedControlResults = append(expectedControlResults, convertTOToExpectedControlResult(expectedControlResultTOs[i]))
	}
	return expectedControlResults
}

func convertTOToExpectedControlResult(expectedControlResultTO ExpectedControlResultTO) ExpectedControlResult {
	expectedControlResult := ExpectedControlResult{
		ID:               expectedControlResultTO.ID,
		SampleCode:       expectedControlResultTO.SampleCode,
		AnalyteMappingId: expectedControlResultTO.AnalyteMappingId,
		Operator:         expectedControlResultTO.Operator,
		ExpectedValue:    expectedControlResultTO.ExpectedValue,
		ExpectedValue2:   expectedControlResultTO.ExpectedValue2,
	}

	if expectedControlResultTO.CreatedBy != nil {
		expectedControlResult.CreatedBy = *expectedControlResultTO.CreatedBy
	}

	if expectedControlResultTO.CreatedAt != nil {
		expectedControlResult.CreatedAt = *expectedControlResultTO.CreatedAt
	}

	return expectedControlResult
}
