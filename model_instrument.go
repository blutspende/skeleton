package skeleton

import (
	"github.com/google/uuid"
)

// models

type instrumentTO struct {
	ID                  uuid.UUID             `json:"id"`
	Type                InstrumentType        `json:"instrumentType"`
	Name                string                `json:"name"`
	ProtocolID          uuid.UUID             `json:"protocolId"`
	ProtocolName        Protocol              `json:"type"`
	Enabled             bool                  `json:"enabled"`
	ConnectionMode      ConnectionMode        `json:"connectionMode"`
	ResultMode          ResultMode            `json:"runningMode"`
	CaptureResults      bool                  `json:"captureResults"`
	CaptureDiagnostics  bool                  `json:"captureDiagnostics"`
	ReplyToQuery        bool                  `json:"replyToQuery"`
	Status              string                `json:"status"`
	FileEncoding        string                `json:"fileEncoding"`
	Timezone            string                `json:"timezone"`
	Hostname            string                `json:"hostname"`
	ClientPort          *int                  `json:"clientPort"`
	FtpUsername         *string               `json:"ftpUserName"`
	FtpPassword         *string               `json:"ftpPassword"`
	FtpOrderPath        *string               `json:"ftpOrderPath"`
	FtpOrderFileMask    *string               `json:"ftpOrderFileMask"`
	FtpOrderFileSuffix  *string               `json:"ftpOrderFileSuffix"`
	FtpResultPath       *string               `json:"ftpResultPath"`
	FtpResultFileMask   *string               `json:"ftpResultFileMask"`
	FtpResultFileSuffix *string               `json:"ftpResultFileSuffix"`
	FtpServerType       *string               `json:"ftpServerType"`
	AnalyteMappings     []analyteMappingTO    `json:"analyteMappings"`
	RequestMappings     []requestMappingTO    `json:"requestMappings"`
	Settings            []instrumentSettingTO `json:"instrumentSettings"`
	SortingRuleGroups   []sortingRuleGroupTO  `json:"sortingRuleGroups"`
}

type analyteMappingTO struct {
	ID                uuid.UUID          `json:"id"`
	InstrumentAnalyte string             `json:"instrumentAnalyte"`
	AnalyteID         uuid.UUID          `json:"analyteId"`
	ChannelMappings   []channelMappingTO `json:"channelMappings"`
	ResultMappings    []resultMappingTO  `json:"resultMappings"`
	ResultType        ResultType         `json:"resultType"`
}

type requestMappingTO struct {
	ID         uuid.UUID   `json:"id"`
	Code       string      `json:"code"`
	IsDefault  bool        `json:"isDefault"`
	AnalyteIDs []uuid.UUID `json:"requestMappingAnalyteIds"`
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
	Name              Protocol            `json:"name"`
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
		ReplyToQuery:       instrumentTO.ReplyToQuery,
		FileEncoding:       instrumentTO.FileEncoding,
		Timezone:           instrumentTO.Timezone,
		Hostname:           instrumentTO.Hostname,
		ClientPort:         instrumentTO.ClientPort,
		AnalyteMappings:    make([]AnalyteMapping, len(instrumentTO.AnalyteMappings)),
		RequestMappings:    make([]RequestMapping, len(instrumentTO.RequestMappings)),
		SortingRules:       make([]SortingRule, 0),
		Settings:           convertInstrumentSettingTOsToInstrumentSettings(instrumentTO.Settings),
	}

	if instrumentTO.ConnectionMode == FTP {
		model.FTPConfig = &FTPConfig{
			InstrumentId:     instrumentTO.ID,
			Username:         stringPointerToString(instrumentTO.FtpUsername),
			Password:         stringPointerToString(instrumentTO.FtpPassword),
			OrderPath:        stringPointerToStringWithDefault(instrumentTO.FtpOrderPath, "/"),
			OrderFileMask:    stringPointerToString(instrumentTO.FtpOrderFileMask),
			OrderFileSuffix:  stringPointerToString(instrumentTO.FtpOrderFileSuffix),
			ResultPath:       stringPointerToStringWithDefault(instrumentTO.FtpResultPath, "/"),
			ResultFileMask:   stringPointerToString(instrumentTO.FtpResultFileMask),
			ResultFileSuffix: stringPointerToString(instrumentTO.FtpResultFileSuffix),
			FtpServerType:    stringPointerToString(instrumentTO.FtpServerType),
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
		ID:                analyteMappingTO.ID,
		InstrumentAnalyte: analyteMappingTO.InstrumentAnalyte,
		AnalyteID:         analyteMappingTO.AnalyteID,
		ChannelMappings:   make([]ChannelMapping, len(analyteMappingTO.ChannelMappings)),
		ResultMappings:    make([]ResultMapping, len(analyteMappingTO.ResultMappings)),
		ResultType:        analyteMappingTO.ResultType,
	}

	for i, channelMapping := range analyteMappingTO.ChannelMappings {
		model.ChannelMappings[i] = convertChannelMappingTOToChannelMapping(channelMapping)
	}

	for i, resultMapping := range analyteMappingTO.ResultMappings {
		model.ResultMappings[i] = convertResultMappingTOToResultMapping(resultMapping)
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
