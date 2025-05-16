package skeleton

import (
	"context"
	"time"

	config2 "github.com/blutspende/skeleton/config"
	"github.com/blutspende/skeleton/db"
	"github.com/blutspende/skeleton/migrator"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type SkeletonError error

// SkeletonCallbackHandlerV1 - must implement an EventHandler to react on Events triggered by Skeleton
type SkeletonCallbackHandlerV1 interface {
	// HandleAnalysisRequests is called to inform and check about new analysis request.
	// Returning a not nil will prevent the result being processed further.
	// This method should be used e.g. to update standing caches regarding analysis requests whenever a request is received.
	HandleAnalysisRequests(request []AnalysisRequest) error

	RevokeAnalysisRequests(request []AnalysisRequest)

	ReexamineAnalysisRequests(request []AnalysisRequest)

	// ProcessResultMessage - callback for processing an incoming result message.
	// Returning skeleton.ErrInvalidInstrumentMessage will prevent retries, any other error will trigger the retry logic.
	// Updating messages is handled by skeleton.
	ProcessResultMessage(messageIn MessageIn) (AnalysisResultSet, []StandaloneControlResult, error)
	// ProcessOutgoingInstrumentMessages - callback for processing outgoing messages from driver to instrument when MessageOutHandling
	// is set to MessageOutHandlingDeferred. Return error only for errors that prevent the processing of the entire batch.
	//In that case the entire batch is updated to error status and retried. If only individual messages fail,
	//then return those in messagesToRetry with a nil error, and only those will be retried.
	// messagesToUpdate should contain all messages received as parameter with the proper error, status, retry count set, unless an error is returned.
	//This callback is never called when MessageOutHandling is set to MessageOutHandlingRealtime.
	ProcessOutgoingInstrumentMessages(messages []MessageOut) (messagesToUpdate []MessageOut, messagesToRetry []MessageOut, err error)
	// CreateResponseMessageOutToMessageIn - callback for creating a MessageOut in response to an incoming message, such as
	// a reply to a query, or an ACK message if necessary. If no response message applies, return nil.
	CreateResponseMessageOutToMessageIn(messageIn MessageIn) (*MessageOut, error)
}

// SkeletonAPI is the interface for accessing the skeleton driver capabilities
type SkeletonAPI interface {
	SetCallbackHandler(eventHandler SkeletonCallbackHandlerV1)
	GetCallbackHandler() SkeletonCallbackHandlerV1
	// Log : (info) logging to ui console
	Log(instrumentID uuid.UUID, msg string)
	// Log : (error) logging to ui console
	LogError(instrumentID uuid.UUID, err error)
	// Log : (debug) logging to ui console
	LogDebug(instrumentID uuid.UUID, msg string)

	// GetAnalysisRequestsBySampleCode - Return a list of AnalysisRequests that contain the sampleCode
	// Empty List if nothing is found. Error occurs only on Database-error
	// if allowResending is true, returns all analysis requests for the sample code, without checking if they were already sent to the instrument
	GetAnalysisRequestsBySampleCode(ctx context.Context, sampleCode string, allowResending bool) ([]AnalysisRequest, error)

	// GetAnalysisRequestsBySampleCodes - Return a list of AnalysisRequests that contains the sampleCodes
	// Empty List if nothing is found. Error occurs only on Database-error
	// if allowResending is true, returns all analysis requests for the sample code, without checking if they were already sent to the instrument
	GetAnalysisRequestsBySampleCodes(ctx context.Context, sampleCodes []string, allowResending bool) (map[string][]AnalysisRequest, error)
	// GetAnalysisRequestExtraValues - returns a map of the analysis request extra values. Error occurs on database error.
	GetAnalysisRequestExtraValues(ctx context.Context, analysisRequestID uuid.UUID) (map[string]string, error)
	// SaveAnalysisRequestsInstrumentTransmissions - persists in the database that an outgoing transmission was sent
	// related to the provided analysis request IDs for the instrument with the provided ID
	SaveAnalysisRequestsInstrumentTransmissions(ctx context.Context, analysisRequestIDs []uuid.UUID, instrumentID uuid.UUID) error
	// GetSortingTarget - returns where a sample should be sorted by a sorter instrument, based on sample code and the current
	// programme running on the instrument. Error occurs if no suitable sorting target is found
	GetSortingTarget(ctx context.Context, instrumentIP string, sampleCode string, programme string) (string, error)
	// MarkSortingTargetAsApplied - should be called after sorting instructions were successfully passed from driver to instrument
	MarkSortingTargetAsApplied(ctx context.Context, instrumentIP, sampleCode, programme, target string) error

	// ProcessMessageIn - persists an incoming instrument message (e.g. result) and handles the long term archiving of it
	ProcessMessageIn(ctx context.Context, messageIn MessageIn) (uuid.UUID, *MessageOut, error)
	// SaveMessageOut - persists an outgoing instrument message (e.g. order) and handles the long term archiving of it
	SaveMessageOut(ctx context.Context, messageOut MessageOut) (uuid.UUID, error)

	GetAnalysisResultIdsSinceLastControlByReagent(ctx context.Context, reagent Reagent, examinedAt time.Time, analyteMappingId uuid.UUID, instrumentId uuid.UUID) ([]uuid.UUID, error)
	GetLatestControlResultsByReagent(ctx context.Context, reagent Reagent, resultYieldTime *time.Time, analyteMappingId uuid.UUID, instrumentId uuid.UUID) ([]ControlResult, error)

	// GetInstrument returns all the settings regarding an instrument
	// contains AnalyteMappings[] and RequestMappings[]
	GetInstrument(ctx context.Context, instrumentID uuid.UUID) (Instrument, error)

	// GetInstrumentByIP returns all the settings regarding an instrument
	// contains AnalyteMappings[] and RequestMappings[]
	GetInstrumentByIP(ctx context.Context, ip string) (Instrument, error)

	// GetInstruments - Returns a list of instruments configured for this Driver class
	// contains AnalyteMappings[] and RequestMappings[]
	GetInstruments(ctx context.Context) ([]Instrument, error)

	// FindResultEntities - Convenient: Lookup Instrument, AnalysisRequest and ResultMapping for the analyte at once
	FindResultEntities(ctx context.Context, InstrumentID uuid.UUID, SampleCode string, ManufacturerTestCode string) (Instrument, []AnalysisRequest, AnalyteMapping, error)

	// RegisterProtocol - Registers the supported protocols of a driver class
	RegisterProtocol(ctx context.Context, id uuid.UUID, name string, description string, abilities []ProtocolAbility, settings []ProtocolSetting) error

	// RegisterManufacturerTests - Registers the supported manufacturer tests of the driver
	RegisterManufacturerTests(ctx context.Context, manufacturerTests []SupportedManufacturerTests, sendToCerberus bool) error

	// SetOnlineStatus - Sets the current status of an instrument. Possible values:
	// - ONLINE - instrument is actively connected
	// - READY - instrument is not actively connected, but ready to connect
	// - OFFLINE - instrument is offline
	SetOnlineStatus(ctx context.Context, id uuid.UUID, status InstrumentStatus) error

	// Start - MUST BE CALLED ON STARTUP
	// - migrates skeleton database
	// - launches goroutines for analysis request/result processing
	// - registers driver class in cerberus
	Start() error
}

func New(ctx context.Context, serviceName, displayName string, requestedExtraValueKeys, encodings []string, reagentManufacturers []string, sqlConn *sqlx.DB, dbSchema string, messageOutHandling MessageOutHandling) (SkeletonAPI, error) {
	config, err := config2.ReadConfiguration()
	if err != nil {
		return nil, err
	}
	authManager := NewAuthManager(&config,
		NewRestyClient(context.Background(), &config, true))
	internalApiRestyClient := NewRestyClientWithAuthManager(context.Background(), &config, authManager, config.StandardAPIClientTimeoutSeconds)
	longPollingApiRestyClient := NewRestyClientWithAuthManager(context.Background(), &config, authManager, 0) // do not set timeout for resty client, it is handled by longpollClient (prevents unnecessary context deadline exceeded errors)
	cerberusClient, err := NewCerberusClient(config.CerberusURL, internalApiRestyClient)
	if err != nil {
		return nil, err
	}
	deaClient, err := NewDEAClient(config.DeaURL, internalApiRestyClient)
	if err != nil {
		return nil, err
	}
	dbConn := db.CreateDbConnector(sqlConn)
	manager := NewSkeletonManager(ctx)
	instrumentCache := NewInstrumentCache()
	analysisRepository := NewAnalysisRepository(dbConn, dbSchema)
	instrumentRepository := NewInstrumentRepository(dbConn, dbSchema)
	analysisService := NewAnalysisService(analysisRepository, deaClient, cerberusClient, manager)
	conditionRepository := NewConditionRepository(dbConn, dbSchema)
	conditionService := NewConditionService(conditionRepository)
	sortingRuleRepository := NewSortingRuleRepository(dbConn, dbSchema)
	sortingRuleService := NewSortingRuleService(analysisRepository, conditionService, sortingRuleRepository)
	instrumentService := NewInstrumentService(sortingRuleService, instrumentRepository, manager, instrumentCache, cerberusClient)
	messageInRepository := NewMessageInRepository(dbConn, dbSchema)
	messageOutRepository := NewMessageOutRepository(dbConn, dbSchema)
	messageService := NewMessageService(deaClient, messageInRepository, messageOutRepository, serviceName)

	consoleLogService := NewConsoleLogService(cerberusClient)

	longpollClient := NewLongPollClient(longPollingApiRestyClient, serviceName, config.CerberusURL, config.LongPollingAPIClientTimeoutSeconds, config.LongPollingReattemptWaitSeconds, config.LongPollingLoggingEnabled)

	return NewSkeleton(ctx, serviceName, displayName, requestedExtraValueKeys, encodings, reagentManufacturers, sqlConn, dbSchema, messageOutHandling, migrator.NewSkeletonMigrator(), analysisRepository, analysisService, instrumentService, consoleLogService, messageService, manager, cerberusClient, longpollClient, deaClient, config)
}
