package skeleton

import (
	"time"

	"github.com/google/uuid"
)

const (
	Universal_Lis2A2 Protocol = "Universal Lis2A2 (Generic)"
)

// AnalysisRequestResponseItemV1 - Response of the AnalysisService to indicate the status of the requests
type AnalysisRequestStatus struct {
	AnalysisRequest *AnalysisRequest
	Error           error
}

// AnalysisRequest - Analysis Request as processed by our AnalysisService
// Do not use in implentation directly
type AnalysisRequest struct {
	ID             uuid.UUID
	WorkItemID     uuid.UUID
	AnalyteID      uuid.UUID
	SampleCode     string
	MaterialID     uuid.UUID
	LaboratoryID   uuid.UUID
	ValidUntilTime time.Time
	CreatedAt      time.Time
	SubjectInfo    *SubjectInfo
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
	Qualify ResultMode = "QUALIFY"
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
	Int            ResultType = "int"
	Decimal        ResultType = "decimal"
	BoundedDecimal ResultType = "boundedDecimal"
	String         ResultType = "string"
	Pein           ResultType = "pein"
	React          ResultType = "react"
	InValid        ResultType = "invalid"
	Enum           ResultType = "enum"
)

type ReagentType string

const (
	Reagent ReagentType = "Reagent"
	Diluent ReagentType = "Diluent"
)

type ReagentInfo struct {
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
	AnalyteMappings    []AnalyteMapping
	RequestMappings    []RequestMapping
	CreatedAt          time.Time
	ModifiedAt         *time.Time
	DeletedAt          *time.Time
}

type Protocol string

type AnalyteMapping struct {
	ID                uuid.UUID
	InstrumentAnalyte string
	AnalyteID         uuid.UUID
	ChannelMappings   []ChannelMapping
	ResultMappings    []ResultMapping
	ResultType        ResultType
}

type ChannelMapping struct {
	ID                uuid.UUID
	InstrumentChannel string
	ChannelID         uuid.UUID
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

// AnalysisResult - The final result on 'per-workitem' basis to return the result to cerberus.
// Call v1.SubmitAnalysisResult for submission.
type AnalysisResult struct {
	ID              uuid.UUID
	AnalysisRequest AnalysisRequest
	AnalyteMapping  AnalyteMapping
	Instrument      Instrument
	// ResultRecordID - reference to raw result record stored in an implementation-created table
	ResultRecordID           uuid.UUID
	BatchID                  uuid.UUID
	Result                   string
	Status                   ResultStatus
	ResultYieldDateTime      time.Time
	ValidUntil               time.Time
	Operator                 string
	TechnicalReleaseDateTime time.Time
	InstrumentRunID          uuid.UUID
	RunCounter               int
	Edited                   bool
	EditReason               string
	Warnings                 []string
	ChannelResults           []ChannelResult
	ExtraValues              []ExtraValue
	ReagentInfos             []ReagentInfo
	Images                   []Image
	IsSentToCerberus         bool
	ErrorMessage             string
	RetryCount               int
}

type AnalysisResultCreateStatusV1 struct {
	AnalyisResult            *AnalysisResult
	Success                  bool
	ErrorMessage             string
	CerberusAnalysisResultID uuid.NullUUID
}

// Image Images are Id's as returned by the DEA service where they get uploaded to
type Image struct {
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
