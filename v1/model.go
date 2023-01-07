package v1

import (
	"time"

	"github.com/google/uuid"
)

const (
	Universal_Lis2A2 ProtocolV1 = "Universal Lis2A2 (Generic)"
)

// AnalysisRequestResponseItemV1 - Response of the AnalysisService to indicate the status of the requests
type AnalysisRequestStatusV1 struct {
	AnalysisRequest *AnalysisRequestV1
	Error           error
}

// AnalysisRequestV1 - Analysis Request as processed by our AnalysisService
// Do not use in implentation directly
type AnalysisRequestV1 struct {
	ID             uuid.UUID
	WorkItemID     uuid.UUID
	AnalyteID      uuid.UUID
	SampleCode     string
	MaterialID     uuid.UUID
	LaboratoryID   uuid.UUID
	ValidUntilTime time.Time
	CreatedAt      time.Time
	SubjectInfo    *SubjectInfoV1
}

// SubjectInfoV1 - Additional Information about the subject for the AnalysisRequestV1
// Do not use in implentation directly
type SubjectInfoV1 struct {
	Type         SubjectTypeV1
	DateOfBirth  *time.Time
	FirstName    *string
	LastName     *string
	DonorID      *string
	DonationID   *string
	DonationType *string
	Pseudonym    *string
}

type ResultModeV1 string

const (
	// Simulated results will not be transmitted to Cerberus and stay within the driver
	Simulation ResultModeV1 = "SIMULATION"
	// Qualification Results are transmitted to cerberus but not returned to any EIA interface
	Qualify ResultModeV1 = "QUALIFY"
	// Production allows the results to be returned via EIA
	Production ResultModeV1 = "PRODUCTION"
)

type ResultStatusV1 string

const (
	// Result status required for Cerberus
	Preliminary ResultStatusV1 = "PRE"
	// Result status required for Cerberus
	Final ResultStatusV1 = "FIN"
)

type ResultTypeV1 string // nolint

const (
	Int            ResultTypeV1 = "int"
	Decimal        ResultTypeV1 = "decimal"
	BoundedDecimal ResultTypeV1 = "boundedDecimal"
	String         ResultTypeV1 = "string"
	Pein           ResultTypeV1 = "pein"
	React          ResultTypeV1 = "react"
	InValid        ResultTypeV1 = "invalid"
	Enum           ResultTypeV1 = "enum"
)

type ReagentType string

const (
	Reagent ReagentType = "Reagent"
	Diluent ReagentType = "Diluent"
)

type ReagentInfoV1 struct {
	SerialNumber            string      `json:"serialNo" db:"serial"`
	Name                    string      `json:"name" db:"name"`
	Code                    string      `json:"code" db:"code"`
	ShelfLife               time.Time   `json:"shelfLife" db:"shelfLife"`
	LotNo                   string      `json:"lotNo"`
	ManufacturerName        string      `json:"manufacturer" db:"manufacturer_name"`
	ReagentManufacturerDate time.Time   `json:"reagentManufacturerDate" db:"reagent_manufacturer_date"`
	ReagentType             ReagentType `json:"reagentType" db:"reagent_type"`
	UseUntil                time.Time   `json:"useUntil"`
	DateCreated             time.Time   `json:"dateCreated" db:"date_created"`
}

type ExtraValueV1 struct {
	Key   string
	Value string
}

type ChannelResultV1 struct {
	ID                    uuid.UUID
	ChannelID             uuid.UUID
	QualitativeResult     string
	QualitativeResultEdit bool
	QuantitativeResults   map[string]string
	Images                []ImageV1
}

type InstrumentV1 struct {
	ID                 uuid.UUID
	Name               string
	ProtocolID         uuid.UUID
	ProtocolName       ProtocolV1
	CaptureResults     bool
	CaptureDiagnostics bool
	ReplyToQuery       bool
	Status             string
	FileEncoding       string
	Timezone           string
	Hostname           string
	ClientPort         int
	ResultMode         ResultModeV1
	AnalyteMappings    []AnalyteMappingV1
	RequestMappings    []RequestMappingV1
}

type ProtocolV1 string

type AnalyteMappingV1 struct {
	ID                uuid.UUID
	InstrumentID      uuid.UUID
	InstrumentAnalyte string
	AnalyteID         uuid.UUID
	ChannelMappings   []ChannelMappingV1
	ResultMappings    []ResultMappingV1
	ResultType        ResultTypeV1
}

type ChannelMappingV1 struct {
	ID                uuid.UUID
	InstrumentChannel string
	ChannelID         uuid.UUID
	AnalyteMappingID  uuid.UUID
}

// ResultMappingV1 - Maps a ManufacturerTestCode to an AnalyteId (cerberus)
type ResultMappingV1 struct {
	ID               uuid.UUID
	AnalyteMappingID uuid.UUID
	Key              string
	Value            string
}

// RequestMappingV1 - Maps ManufacturerTestCode (on Instrument) to one or more Analytes (cerberus)
// for transmission to instrument
type RequestMappingV1 struct {
	ID                       uuid.UUID
	Code                     string
	InstrumentID             uuid.UUID
	SupportedProtocolID      uuid.UUID
	RequestMappingAnalyteIDs []uuid.UUID
}

type UploadLogStatusV1 string

const (
	Success UploadLogStatusV1 = "success"
	Failed  UploadLogStatusV1 = "failed"
)

type SubjectTypeV1 string

const (
	Donor     SubjectTypeV1 = "DONOR"
	Personal  SubjectTypeV1 = "PERSONAL"
	Pseudonym SubjectTypeV1 = "PSEUDONYMIZED"
)

// AnalysisResultV1 - The final result on 'per-workitem' basis to return the result to cerberus.
// Call v1.SubmitAnalysisResult for submission.
type AnalysisResultV1 struct {
	ID              uuid.UUID
	AnalysisRequest AnalysisRequestV1
	AnalyteMapping  AnalyteMappingV1
	Instrument      InstrumentV1
	// ResultRecordID - reference to raw result record stored in an implementation-created table
	ResultRecordID           uuid.UUID
	Result                   string
	Status                   ResultStatusV1
	ResultYieldDateTime      time.Time
	ValidUntil               time.Time
	Operator                 string
	TechnicalReleaseDateTime time.Time
	InstrumentRunID          uuid.UUID
	RunCounter               int
	Edited                   bool
	EditReason               string
	Warnings                 []string
	ChannelResults           []ChannelResultV1
	ExtraValues              []ExtraValueV1
	ReagentInfos             []ReagentInfoV1
	Images                   []ImageV1
	IsSentToCerberus         bool
	ErrorMessage             string
	RetryCount               int
}

type AnalysisResultCreateStatusV1 struct {
	AnalyisResult            *AnalysisResultV1
	Success                  bool
	ErrorMessage             string
	CerberusAnalysisResultID uuid.NullUUID
}

// ImageV1 Images are Id's as returned by the DEA service where they get uploaded to
type ImageV1 struct {
	ID          uuid.UUID
	Name        string
	Description *string
	ChannelName string
}

type SubmitType string

const (
	// Batch with delay, store & send to Cerberus
	SubmitTypeBatchStoreAndSend = ""
	// Store & Send to Cerberus, no batching = single request
	SubmitTypeInstantStoreAndSend = "INSTANT_PROCESS"
	// Store only internally for reference (like the Requests-Search in UI)
	SubmitTypeStoreOnly = "STORE_ONLY"
)

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
