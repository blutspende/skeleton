package skeleton

import (
	"net/http"
	"time"

	"github.com/blutspende/bloodlab-common/encoding"
	"github.com/blutspende/bloodlab-common/instrumentenum"
	"github.com/blutspende/bloodlab-common/timezone"
	"github.com/google/uuid"
)

const (
	Universal_Lis2A2 = "Universal Lis2A2 (Generic)"
)

type GinApi interface {
	Run() error
}

// AnalysisRequest - Analysis Request as processed by our AnalysisService
// Do not use in implementation directly
type AnalysisRequest struct {
	ID                          uuid.UUID
	WorkItemID                  uuid.UUID
	AnalyteID                   uuid.UUID
	SampleCode                  string
	MaterialID                  uuid.UUID
	LaboratoryID                uuid.UUID
	ValidUntilTime              time.Time
	CreatedAt                   time.Time
	ModifiedAt                  *time.Time
	SubjectInfo                 *SubjectInfo
	ExtraValues                 []ExtraValue
	ReexaminationRequestedCount int
}

type CerberusQueueItem struct {
	ID                  uuid.UUID
	JsonMessage         string
	LastHTTPStatus      int
	LastError           string
	LastErrorAt         *time.Time
	TrialCount          int
	RetryNotBefore      time.Time
	RawResponse         string
	ResponseJsonMessage string
	DataType            DataType
}

type DataType string

const (
	AnalysisResultDataType DataType = "AnalysisResult"
	ControlResultDataType  DataType = "ControlResult"
)

// SubjectInfo - Additional Information about the subject for the AnalysisRequest
// Do not use in implentation directly
type SubjectInfo struct {
	Type         SubjectType
	DateOfBirth  *time.Time
	FirstName    *string
	LastName     *string
	DonorID      *string
	DonationID   *string
	DonationType *string
	Pseudonym    *string
}

type ResultStatus string

const (
	// Result status required for Cerberus
	Preliminary ResultStatus = "PRE"
	// Result status required for Cerberus
	Final ResultStatus = "FIN"
)

type Reagent struct {
	ID             uuid.UUID
	Manufacturer   string
	SerialNumber   string
	LotNo          string
	Name           string
	Type           instrumentenum.ReagentType
	CreatedAt      time.Time
	ExpirationDate *time.Time
	ControlResults []ControlResult
}

type ExtraValue struct {
	Key   string
	Value string
}

type ChannelResult struct {
	ID                    uuid.UUID
	ChannelID             uuid.UUID
	QualitativeResult     string
	QualitativeResultEdit bool
	QuantitativeResults   map[string]string
	Images                []Image
}

type Instrument struct {
	ID                 uuid.UUID
	Type               instrumentenum.Type
	Name               string
	ProtocolID         uuid.UUID
	ProtocolName       string
	Enabled            bool
	ConnectionMode     instrumentenum.ConnectionMode
	ResultMode         instrumentenum.ResultMode
	AllowResending     bool
	CaptureResults     bool
	CaptureDiagnostics bool
	ReplyToQuery       bool
	Status             string
	Encoding           encoding.Encoding
	TimeZone           timezone.TimeZone
	Hostname           string
	ClientPort         *int
	FileServerConfig   *FileServerConfig
	AnalyteMappings    []AnalyteMapping
	RequestMappings    []RequestMapping
	SortingRules       []SortingRule
	Settings           []InstrumentSetting
	CreatedAt          time.Time
	ModifiedAt         *time.Time
	DeletedAt          *time.Time
}

type FileServerConfig struct {
	ID               uuid.UUID
	InstrumentId     uuid.UUID
	Username         string
	Password         string
	OrderPath        string
	OrderFileMask    string
	OrderFileSuffix  string
	ResultPath       string
	ResultFileMask   string
	ResultFileSuffix string
	ServerType       instrumentenum.FileServerType
	CreatedAt        time.Time
	DeletedAt        *time.Time
}

type AnalyteMapping struct {
	ID                     uuid.UUID
	InstrumentAnalyte      string
	AnalyteID              uuid.UUID
	ChannelMappings        []ChannelMapping
	ResultMappings         []ResultMapping
	ResultType             instrumentenum.ResultType
	ControlResultRequired  bool
	ExpectedControlResults []ExpectedControlResult
	IsControl              bool
	ValidatedAnalyteIDs    []uuid.UUID //links to InstrumentalAnalyte type
}

type ChannelMapping struct {
	ID                uuid.UUID
	InstrumentChannel string
	ChannelID         uuid.UUID
}

type ExpectedControlResult struct {
	ID               uuid.UUID
	SampleCode       string
	AnalyteMappingId uuid.UUID
	Operator         ConditionOperator
	ExpectedValue    string
	ExpectedValue2   *string
	CreatedAt        time.Time
	DeletedAt        *time.Time
	CreatedBy        uuid.UUID
	DeletedBy        uuid.NullUUID
}

type NotSpecifiedExpectedControlResult struct {
	SampleCode       string
	AnalyteMappingId uuid.UUID
}

// ResultMapping - Maps a ManufacturerTestCode to an AnalyteId (cerberus)
type ResultMapping struct {
	ID    uuid.UUID
	Key   string
	Value string
	Index int
}

// RequestMapping - Maps ManufacturerTestCode (on Instrument) to one or more Analytes (cerberus)
// for transmission to instrument
type RequestMapping struct {
	ID         uuid.UUID
	Code       string
	IsDefault  bool
	AnalyteIDs []uuid.UUID
}

type UploadLogStatus string

const (
	Success UploadLogStatus = "success"
	Failed  UploadLogStatus = "failed"
)

type SubjectType string

const (
	Donor     SubjectType = "DONOR"
	Personal  SubjectType = "PERSONAL"
	Pseudonym SubjectType = "PSEUDONYMIZED"
)

type AnalysisResultSet struct {
	Results        []AnalysisResult
	Reagents       []Reagent
	ControlResults []ControlResult
}

// AnalysisResult - The final result on 'per-workitem' basis to return the result to cerberus.
// Call v1.SubmitAnalysisResult for submission.
type AnalysisResult struct {
	ID                       uuid.UUID
	AnalysisRequest          AnalysisRequest
	AnalyteMapping           AnalyteMapping
	Instrument               Instrument
	SampleCode               string
	MessageInID              uuid.UUID
	Result                   string
	ResultMode               instrumentenum.ResultMode
	Status                   ResultStatus
	ResultYieldDateTime      *time.Time
	ValidUntil               time.Time
	Operator                 string
	TechnicalReleaseDateTime *time.Time
	InstrumentRunID          uuid.UUID
	InstrumentModule         *string
	Edited                   bool
	EditReason               string
	IsInvalid                bool
	WarnFlag                 bool // Todo use it
	Warnings                 []string
	ChannelResults           []ChannelResult
	ExtraValues              []ExtraValue
	Reagents                 []Reagent
	ControlResults           []ControlResult
	Images                   []Image

	deaRawMessageID uuid.NullUUID
}

type ControlResult struct {
	ID                         uuid.UUID
	SampleCode                 string
	MessageInID                uuid.UUID
	AnalyteMapping             AnalyteMapping
	Result                     string
	ExpectedControlResultId    uuid.NullUUID
	IsValid                    bool
	IsComparedToExpectedResult bool
	ExaminedAt                 time.Time
	InstrumentID               uuid.UUID
	InstrumentModule           *string
	Warnings                   []string
	ChannelResults             []ChannelResult
	ExtraValues                []ExtraValue

	deaRawMessageID uuid.NullUUID
}

type StandaloneControlResult struct {
	ControlResult
	Reagents  []Reagent
	ResultIDs []uuid.UUID
}

type MappedStandaloneControlResult struct {
	ControlResult
	Reagents  []Reagent
	ResultIDs map[uuid.UUID]uuid.UUID
}

type AnalysisResultBatchItemReagentInfo struct {
	CerberusID                uuid.UUID
	CerberusControlResultsIDs []uuid.UUID
}

type AnalysisResultBatchItemInfo struct {
	AnalysisResult           *AnalysisResultTO
	CerberusAnalysisResultID *uuid.UUID
	ErrorMessage             string
}

func (i AnalysisResultBatchItemInfo) IsSuccessful() bool {
	return i.CerberusAnalysisResultID != nil && *i.CerberusAnalysisResultID != uuid.Nil && i.ErrorMessage == ""
}

type AnalysisResultBatchResponse struct {
	AnalysisResultBatchItemInfoList []AnalysisResultBatchItemInfo
	ErrorMessage                    string
	HTTPStatusCode                  int
	RawResponse                     string
}

func (r AnalysisResultBatchResponse) HasResult() bool {
	return r.HTTPStatusCode != 0 || r.ErrorMessage != ""
}

func (r AnalysisResultBatchResponse) IsSuccess() bool {
	return r.HTTPStatusCode >= http.StatusOK && r.HTTPStatusCode < http.StatusMultipleChoices
}

type ControlResultBatchResponse struct {
	ErrorMessage   string
	HTTPStatusCode int
	RawResponse    string
}

func (r ControlResultBatchResponse) HasResult() bool {
	return r.HTTPStatusCode != 0 || r.ErrorMessage != ""
}

func (r ControlResultBatchResponse) IsSuccess() bool {
	return r.HTTPStatusCode >= http.StatusOK && r.HTTPStatusCode < http.StatusMultipleChoices
}

type AnalysisRequestSentStatus string

const (
	AnalysisRequestStatusOpen      = "OPEN"
	AnalysisRequestStatusProcessed = "PROCESSED"
)

type AnalysisRequestInfo struct {
	ID                uuid.UUID
	SampleCode        string
	WorkItemID        uuid.UUID
	AnalyteID         uuid.UUID
	RequestCreatedAt  time.Time
	ResultCreatedAt   *time.Time
	ResultID          *uuid.UUID
	AnalyteMappingsID *uuid.UUID
	TestName          *string
	TestResult        *string
	BatchCreatedAt    *time.Time
	Status            string
	SourceIP          string
	InstrumentID      *uuid.UUID
	MappingError      bool
}

// Image Images are Id's as returned by the DEA service where they get uploaded to
type Image struct {
	ID              uuid.UUID
	Name            string
	Description     *string
	ImageBytes      []byte
	DeaImageID      uuid.NullUUID
	UploadedToDeaAt *time.Time
}

// SupportedManufacturerTests Information about the tests that are supported by the manufacturer
// These are the testcodes as they are usually set by the vendor of an instrument. In many cases
// they are hardwired by the driver-software on the respective instrument.
type SupportedManufacturerTests struct {
	// Testname as string as presented by the respective manufacturers protocol
	TestName string
	// List of channels that are known to exist for this test
	Channels []string
	// List of known allowed Results. e.g "pos", "neg" ..... - empty for all
	// these values are to give a hint for the configuration on the ui to allow
	ValidResultValues []string
}

type NumberEncoding string

const (
	NumberEncoding_CommaAndNoThousands      NumberEncoding = "CommaAndNoThousands"      // 15,4
	NumberEncoding_CommaAndThousands        NumberEncoding = "CommaAndThousands"        // 1.500,40
	NumberEncoding_PointAndThousandsAsComma NumberEncoding = "PointAndThousandsAsComma" // 1,200.23
)

type SupportedProtocol struct {
	ID                uuid.UUID
	Name              string
	Description       *string
	ProtocolAbilities []ProtocolAbility
	ProtocolSettings  []ProtocolSetting
}

type ProtocolAbility struct {
	ConnectionMode          instrumentenum.ConnectionMode
	Abilities               []instrumentenum.Ability
	RequestMappingAvailable bool
}

type ProtocolSetting struct {
	ID          uuid.UUID
	Key         string
	Description *string
	Type        instrumentenum.ProtocolSettingType
}

type InstrumentSetting struct {
	ID                uuid.UUID
	ProtocolSettingID uuid.UUID
	Value             string
}

type SortingRule struct {
	ID           uuid.UUID
	InstrumentID uuid.UUID
	Condition    *Condition
	Target       string
	Programme    string
	Priority     int
	CreatedAt    time.Time
	ModifiedAt   *time.Time
	DeletedAt    *time.Time
}

type ConditionError struct {
	ConditionNodeIndex int
	Error              error
}

type Condition struct {
	ID                  uuid.UUID
	Name                *string
	Operator            ConditionOperator
	SubCondition1       *Condition
	SubCondition2       *Condition
	NegateSubCondition1 bool
	NegateSubCondition2 bool
	Operand1            *ConditionOperand
	Operand2            *ConditionOperand
}

type ConditionOperand struct {
	ID            uuid.UUID
	Name          *string
	Type          ConditionOperandType
	ConstantValue *string
	ExtraValueKey *string
}

type ConditionOperandType string

const (
	AnalysisRequestExtraValue ConditionOperandType = "extraValue"
	Analyte                   ConditionOperandType = "analyte"
	Constant                  ConditionOperandType = "constant"
	Laboratory                ConditionOperandType = "laboratory"
	Order                     ConditionOperandType = "order"
	SampleCode                ConditionOperandType = "sampleCode"
	Target                    ConditionOperandType = "target"
	Sample                    ConditionOperandType = "sample"
	DefaultOperand            ConditionOperandType = "default"
)

type ConditionOperator string

// Comparison operators
const (
	And            ConditionOperator = "and"
	Or             ConditionOperator = "or"
	Equals         ConditionOperator = "=="
	NotEquals      ConditionOperator = "!="
	Less           ConditionOperator = "<"
	LessOrEqual    ConditionOperator = "<="
	Greater        ConditionOperator = ">"
	GreaterOrEqual ConditionOperator = ">="
	Contains       ConditionOperator = "contains"
	NotContains    ConditionOperator = "notContains"
	MatchRegex     ConditionOperator = "regex"
	Exists         ConditionOperator = "exists"
	NotExists      ConditionOperator = "notExists"
)

// Lambda operators
const (
	MatchAny               ConditionOperator = "matchAny"
	MatchAll               ConditionOperator = "matchAll"
	TargetApplied          ConditionOperator = "targetApplied"
	TargetNotApplied       ConditionOperator = "targetNotApplied"
	IsNthSample            ConditionOperator = "isNthSample"
	HasNPercentProbability ConditionOperator = "hasNPercentProbability"
	Default                ConditionOperator = "default"
)

// Extra comparison operators for Expected Control Results
const (
	InOpenInterval   ConditionOperator = "inOpenInterval"
	InClosedInterval ConditionOperator = "inClosedInterval"
)

type MessageIn struct {
	ID                 uuid.UUID
	InstrumentID       uuid.UUID
	InstrumentModuleID uuid.NullUUID
	Status             instrumentenum.MessageStatus
	ProtocolID         uuid.UUID
	Type               instrumentenum.MessageType
	Encoding           encoding.Encoding
	Raw                []byte
	Error              *string
	RetryCount         int
	CreatedAt          time.Time
	ModifiedAt         *time.Time

	deaRawMessageID uuid.NullUUID
}

type MessageOut struct {
	ID                  uuid.UUID
	InstrumentID        uuid.UUID
	Status              instrumentenum.MessageStatus
	ProtocolID          uuid.UUID
	Type                instrumentenum.MessageType
	Encoding            encoding.Encoding
	Raw                 []byte
	Error               *string
	RetryCount          int
	TriggerMessageInID  uuid.NullUUID
	ResponseMessageInID uuid.NullUUID
	CreatedAt           time.Time
	ModifiedAt          *time.Time

	deaRawMessageID  uuid.NullUUID
	MessageOutOrders []MessageOutOrder
}

type MessageOutOrder struct {
	ID               uuid.UUID
	MessageOutID     uuid.UUID
	SampleCode       string
	RequestMappingID uuid.UUID

	AnalysisRequestIDs []uuid.UUID
}

type SampleSeenMessage struct {
	InstrumentID uuid.UUID
	ModuleName   string
	SampleCode   string
	SeenAt       time.Time
}

type MessageSampleCode struct {
	MessageSampleCodeId uuid.UUID
	SampleCode          string
	RetryCount          int
}
