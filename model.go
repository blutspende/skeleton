package skeleton

import (
	"net/http"
	"time"

	"github.com/google/uuid"
)

const (
	Universal_Lis2A2 Protocol = "Universal Lis2A2 (Generic)"
)

type GinApi interface {
	Run() error
}

// AnalysisRequestResponseItemV1 - Response of the AnalysisService to indicate the status of the requests
type AnalysisRequestStatus struct {
	WorkItemID uuid.UUID
	Error      error
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
}

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

type ResultMode string

const (
	// Simulated results will not be transmitted to Cerberus and stay within the driver
	Simulation ResultMode = "SIMULATION"
	// Qualification Results are transmitted to cerberus but not returned to any EIA interface
	Qualification ResultMode = "QUALIFICATION"
	// Production allows the results to be returned via EIA
	Production ResultMode = "PRODUCTION"
)

type ResultStatus string

const (
	// Result status required for Cerberus
	Preliminary ResultStatus = "PRE"
	// Result status required for Cerberus
	Final ResultStatus = "FIN"
)

type ResultType string // nolint

const (
	DataType_Int            ResultType = "int"
	DataType_Decimal        ResultType = "decimal"
	DataType_BoundedDecimal ResultType = "boundedDecimal"
	DataType_String         ResultType = "string"
	DataType_Pein           ResultType = "pein"
	DataType_React          ResultType = "react"
	DataType_InValid        ResultType = "invalid"
	DataType_Enum           ResultType = "enum"
)

type ReagentType string

const (
	Standard ReagentType = "reagent"
	Diluent  ReagentType = "diluent"
)

type Reagent struct {
	ID             uuid.UUID
	Manufacturer   string
	SerialNumber   string
	LotNo          string
	Name           string
	Type           ReagentType
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

type ConnectionMode string

const (
	TCPClientMode ConnectionMode = "TCP_CLIENT_ONLY"
	TCPServerMode ConnectionMode = "TCP_SERVER_ONLY"
	FTP           ConnectionMode = "FTP_SFTP"
	HTTP          ConnectionMode = "HTTP"
	TCPMixed      ConnectionMode = "TCP_MIXED"
)

type Instrument struct {
	ID                 uuid.UUID
	Type               InstrumentType
	Name               string
	ProtocolID         uuid.UUID
	ProtocolName       Protocol
	Enabled            bool
	ConnectionMode     ConnectionMode
	ResultMode         ResultMode
	CaptureResults     bool
	CaptureDiagnostics bool
	ReplyToQuery       bool
	Status             string
	FileEncoding       string
	Timezone           string
	Hostname           string
	ClientPort         *int
	FTPConfig          *FTPConfig
	AnalyteMappings    []AnalyteMapping
	RequestMappings    []RequestMapping
	SortingRules       []SortingRule
	Settings           []InstrumentSetting
	CreatedAt          time.Time
	ModifiedAt         *time.Time
	DeletedAt          *time.Time
}

type Protocol string

type FTPConfig struct {
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
	FtpServerType    string
	CreatedAt        time.Time
	DeletedAt        *time.Time
}

type AnalyteMapping struct {
	ID                       uuid.UUID
	InstrumentAnalyte        string
	ControlInstrumentAnalyte *string
	AnalyteID                uuid.UUID
	ChannelMappings          []ChannelMapping
	ResultMappings           []ResultMapping
	ResultType               ResultType
	ControlResultRequired    bool
	ExpectedControlResults   []ExpectedControlResult
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
	ID              uuid.UUID
	AnalysisRequest AnalysisRequest
	AnalyteMapping  AnalyteMapping
	Instrument      Instrument
	SampleCode      string
	// ResultRecordID - reference to raw result record stored in an implementation-created table
	ResultRecordID           uuid.UUID
	BatchID                  uuid.UUID
	Result                   string
	ResultMode               ResultMode
	Status                   ResultStatus
	ResultYieldDateTime      *time.Time
	ValidUntil               time.Time
	Operator                 string
	TechnicalReleaseDateTime *time.Time
	InstrumentRunID          uuid.UUID
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
}

type ControlResult struct {
	ID                         uuid.UUID
	SampleCode                 string
	AnalyteMapping             AnalyteMapping
	Result                     string
	ExpectedControlResultId    uuid.NullUUID
	IsValid                    bool
	IsComparedToExpectedResult bool
	ExaminedAt                 time.Time
	InstrumentID               uuid.UUID
	Warnings                   []string
	ChannelResults             []ChannelResult
	ExtraValues                []ExtraValue
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

type ControlResultBatchItemInfo struct {
	ControlResult      *StandaloneControlResultTO
	CerberusID         uuid.UUID
	CerberusReagentIDs []uuid.UUID
}

type ControlResultBatchResponse struct {
	ControlResultBatchItemInfoList []ControlResultBatchItemInfo
	ErrorMessage                   string
	HTTPStatusCode                 int
	RawResponse                    string
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

type AnalysisResultInfo struct {
	ID               uuid.UUID
	BatchID          *uuid.UUID
	RequestCreatedAt *time.Time
	WorkItemID       *uuid.UUID
	SampleCode       string
	AnalyteID        uuid.UUID
	ResultCreatedAt  time.Time
	TestName         *string
	TestResult       *string
	Status           string
}

type AnalysisBatch struct {
	ID      uuid.UUID
	Results []AnalysisResultInfo
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
	Name              Protocol
	Description       *string
	ProtocolAbilities []ProtocolAbility
	ProtocolSettings  []ProtocolSetting
}

type ProtocolAbility struct {
	ConnectionMode          ConnectionMode
	Abilities               []Ability
	RequestMappingAvailable bool
}

type ProtocolSetting struct {
	ID          uuid.UUID
	Key         string
	Description *string
	Type        ProtocolSettingType
}

type InstrumentSetting struct {
	ID                uuid.UUID
	ProtocolSettingID uuid.UUID
	Value             string
}

type Ability string

const (
	CanAcceptResultsAbility      Ability = "CAN_ACCEPT_RESULTS"
	CanReplyToQueryAbility       Ability = "CAN_REPLY_TO_QUERY"
	CanCaptureDiagnosticsAbility Ability = "CAN_CAPTURE_DIAGNOSTICS"
)

func (a Ability) String() string {
	return string(a)
}

type InstrumentStatus string

const (
	InstrumentOffline InstrumentStatus = "OFFLINE"
	InstrumentReady   InstrumentStatus = "READY"
	InstrumentOnline  InstrumentStatus = "ONLINE"
)

type ProtocolSettingType string

const (
	String   ProtocolSettingType = "string"
	Int      ProtocolSettingType = "int"
	Bool     ProtocolSettingType = "bool"
	Password ProtocolSettingType = "password"
)

type InstrumentType string

const (
	Analyzer InstrumentType = "ANALYZER"
	Sorter   InstrumentType = "SORTER"
)

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
