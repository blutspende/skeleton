package skeleton

import (
	"context"
	"time"

	config2 "github.com/blutspende/skeleton/config"
	"github.com/blutspende/skeleton/db"
	"github.com/blutspende/skeleton/migrator"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"golang.org/x/time/rate"
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

	ReprocessMessageIns(messageIns []MessageIn) error
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
	// SubmitAnalysisResultBatch - Submit result batch to Skeleton and Cerberus.
	SubmitAnalysisResultBatch(ctx context.Context, resultBatch AnalysisResultSet) error

	SubmitControlResults(ctx context.Context, controlResults []StandaloneControlResult) error
	// SaveMessageIn - persists an incoming instrument message (e.g. result) and handles the long term archiving of it
	SaveMessageIn(ctx context.Context, messageIn MessageIn) (uuid.UUID, error)
	// UpdateMessageIn - updates an incoming instrument message
	UpdateMessageIn(ctx context.Context, messageIn MessageIn) error
	// GetUnprocessedMessageIns - get all unprocessed incoming instrument messages, up until the start time of skeleton
	GetUnprocessedMessageIns(ctx context.Context) ([]MessageIn, error)
	// GetUnprocessedMessageInsByInstrumentID - get all unprocessed incoming instrument messages, up until the start time of skeleton, for a specific instrument
	GetUnprocessedMessageInsByInstrumentID(ctx context.Context, instrumentID uuid.UUID) ([]MessageIn, error)
	// SaveMessageOut - persist an outgoing instrument message
	SaveMessageOut(ctx context.Context, messageOut MessageOut) (uuid.UUID, error)
	// SaveMessageOutBatch - persists outgoing instrument messages (e.g. order) in a single transaction, and handles the long term archiving of them
	SaveMessageOutBatch(ctx context.Context, messageOuts []MessageOut) ([]uuid.UUID, error)
	// UpdateMessageOut - updates an outgoing instrument message
	UpdateMessageOut(ctx context.Context, messageOut MessageOut) error
	// GetUnprocessedMessageOuts - returns all unprocessed outgoing instrument messages, up until the start time of skeleton
	GetUnprocessedMessageOuts(ctx context.Context) ([]MessageOut, error)
	// GetUnprocessedMessageOutsByInstrumentID - returns all unprocessed outgoing instrument messages, up until the start time of skeleton, for a specific instrument
	GetUnprocessedMessageOutsByInstrumentID(ctx context.Context, instrumentID uuid.UUID) ([]MessageOut, error)
	// AddAnalysisRequestsToMessageOutOrder - adds analysis requests to an existing order
	AddAnalysisRequestsToMessageOutOrder(ctx context.Context, messageOutOrderID uuid.UUID, analysisRequestIDs []uuid.UUID) error
	// DeleteRevokedUnsentOrderMessagesByAnalysisRequestIDs - deletes order messages that were never sent to an instrument, contains the provided
	// analysis request IDs, and all their analysis requests had been revoked. Returns the IDs of the deleted messages.
	DeleteRevokedUnsentOrderMessagesByAnalysisRequestIDs(ctx context.Context, analysisRequestIDs []uuid.UUID) ([]uuid.UUID, error)
	// GetTestCodesToRevokeBySampleCodes - returns a map where the key is the sample code, and the values are the test codes that need
	// to be revoked as per the provided analysis request IDs
	GetTestCodesToRevokeBySampleCodes(ctx context.Context, instrumentID uuid.UUID, analysisRequestIDs []uuid.UUID) (map[string][]string, error)
	// GetMessageOutOrdersBySampleCodesAndRequestMappingIDs - returns all existing orders belonging to a MessageOut for the provided instrument, organized by sample codes and request mapping IDs.
	// If the includePending parameter is set to true, orders are included where the message has not been sent out yet
	GetMessageOutOrdersBySampleCodesAndRequestMappingIDs(ctx context.Context, sampleCodes []string, instrumentID uuid.UUID, includePending bool) (map[string]map[uuid.UUID][]MessageOutOrder, error)
	// RegisterSampleCodesToMessageIn - add sample codes to a message by ID, for lookup purposes
	RegisterSampleCodesToMessageIn(ctx context.Context, messageID uuid.UUID, sampleCodes []string) error
	// RegisterSampleCodesToMessageOut - add sample codes to a message by ID, for lookup purposes
	RegisterSampleCodesToMessageOut(ctx context.Context, messageID uuid.UUID, sampleCodes []string) error

	GetAnalysisResultIdsWithoutControlByReagent(ctx context.Context, controlResult ControlResult, reagent Reagent) ([]uuid.UUID, error)
	GetAnalysisResultIdsWhereLastestControlIsInvalid(ctx context.Context, controlResult ControlResult, reagent Reagent) ([]uuid.UUID, error)
	GetLatestControlResultsByReagent(ctx context.Context, reagent Reagent, resultYieldTime *time.Time, analyteMapping AnalyteMapping, instrumentId uuid.UUID) ([]ControlResult, error)

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

	// RegisterManufacturerTests - Registers the supported manufacturer tests of the driver
	RegisterManufacturerTests(ctx context.Context, manufacturerTests []SupportedManufacturerTests, sendToCerberus bool) error

	// SetOnlineStatus - Sets the current status of an instrument. Possible values:
	// - ONLINE - instrument is actively connected
	// - READY - instrument is not actively connected, but ready to connect
	// - OFFLINE - instrument is offline
	SetOnlineStatus(ctx context.Context, id uuid.UUID, status InstrumentStatus) error

	// Start - MUST BE CALLED ON STARTUP
	// - connects to database
	// - migrates skeleton database
	// - launches goroutines for analysis request/result processing
	// - registers driver class in cerberus
	Start() error

	// GetDbConnection - Provides access to internal database connection
	// - returns database connection
	GetDbConnection() (*sqlx.DB, error)

	// FindAnalyteMapping - Reusable filter method to find analyte mapping by name and type
	FindAnalyteMapping(instrument Instrument, isControl bool, instrumentAnalyte string) (AnalyteMapping, error)
}

func New(ctx context.Context, serviceName, displayName string, requestedExtraValueKeys, encodings []string, reagentManufacturers []string, protocols []SupportedProtocol, dbSchema string) (SkeletonAPI, error) {
	config, err := config2.ReadConfiguration()
	if err != nil {
		return nil, err
	}
	authManager := NewAuthManager(&config,
		NewRestyClient(context.Background(), &config, true))
	rateLimiter := rate.NewLimiter(config.MaxRequestsPerSecond, 1)
	internalApiRestyClient := NewRestyClientWithAuthManager(context.Background(), &config, authManager, rateLimiter, config.StandardAPIClientTimeoutSeconds)
	longPollingApiRestyClient := NewRestyClientWithAuthManager(context.Background(), &config, authManager, rateLimiter, 0) // do not set timeout for resty client, it is handled by longpollClient (prevents unnecessary context deadline exceeded errors)
	cerberusClient, err := NewCerberusClient(config.CerberusURL, internalApiRestyClient)
	if err != nil {
		return nil, err
	}
	deaClient, err := NewDEAClient(config.DeaURL, internalApiRestyClient)
	if err != nil {
		return nil, err
	}
	dbConnector := db.NewPostgres(ctx, &config)
	dbConn := db.NewDbConnection()
	manager := NewSkeletonManager(ctx)
	instrumentCache := NewInstrumentCache()
	analysisRepository := NewAnalysisRepository(dbConn, dbSchema)
	instrumentRepository := NewInstrumentRepository(dbConn, dbSchema)
	analysisService := NewAnalysisService(analysisRepository, instrumentRepository, deaClient, cerberusClient, manager)
	conditionRepository := NewConditionRepository(dbConn, dbSchema)
	conditionService := NewConditionService(conditionRepository)
	sortingRuleRepository := NewSortingRuleRepository(dbConn, dbSchema)
	sortingRuleService := NewSortingRuleService(analysisRepository, conditionService, sortingRuleRepository)
	instrumentService := NewInstrumentService(sortingRuleService, instrumentRepository, manager, instrumentCache, cerberusClient)
	messageInRepository := NewMessageInRepository(dbConn, dbSchema, config.MessageMaxRetries, config.LookBackDays)
	messageOutRepository := NewMessageOutRepository(dbConn, dbSchema, config.MessageMaxRetries, config.LookBackDays)
	messageOutOrderRepository := NewMessageOutOrderRepository(dbConn, dbSchema, config.MessageMaxRetries)
	messageService := NewMessageService(deaClient, messageInRepository, messageOutRepository, messageOutOrderRepository, serviceName)

	consoleLogService := NewConsoleLogService(cerberusClient)

	longpollClient := NewLongPollClient(longPollingApiRestyClient, serviceName, config.CerberusURL, config.LongPollingAPIClientTimeoutSeconds, config.LongPollingReattemptWaitSeconds, config.LongPollingLoggingEnabled)

	return NewSkeleton(ctx, serviceName, displayName, requestedExtraValueKeys, encodings, reagentManufacturers, protocols, dbConnector, dbConn, dbSchema, migrator.NewSkeletonMigrator(), analysisRepository, analysisService, instrumentService, consoleLogService, messageService, manager, cerberusClient, longpollClient, deaClient, config)
}
