package skeleton

import (
	"context"
	"net/http"

	"github.com/DRK-Blutspende-BaWueHe/logcom-api/logcom"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/consolelog/repository"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/consolelog/service"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/server"
	"github.com/gin-gonic/gin"

	config2 "github.com/DRK-Blutspende-BaWueHe/skeleton/config"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/db"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/migrator"
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

	// GetManufacturerTestList is called when the Skeleton requires a list of test names (strings)
	// as known to be valid by the manufacturer of this instrument
	GetManufacturerTestList(instrumentId uuid.UUID, protocolId uuid.UUID) ([]SupportedManufacturerTests, error)

	// GetEncodingList is called when the Skeleton requires a list of supported encodings (strings)
	// as known to be valid by the provided protocol
	GetEncodingList(protocolId uuid.UUID) ([]string, error)

	RevokeAnalysisRequests(request []AnalysisRequest)
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

	// GetAnalysisRequestWithNoResults - return those requests that have no results yet

	//TODO maybe remove
	GetAnalysisRequestWithNoResults(ctx context.Context, currentPage, itemsPerPage int) (requests []AnalysisRequest, maxPages int, err error)
	GetAnalysisRequestsBySampleCode(ctx context.Context, sampleCode string, allowResending bool) ([]AnalysisRequest, error)

	// GetAnalysisRequestsBySampleCodes - Return a list of AnalysisRequests that contains the sampleCodes
	// Empty List if nothing is found. Error occurs only on Database-error
	// if allowResending is true, returns all analysis requests for the sample code, without checking if they were already sent to the instrument
	GetAnalysisRequestsBySampleCodes(ctx context.Context, sampleCodes []string, allowResending bool) (map[string][]AnalysisRequest, error)
	GetRequestMappingsByInstrumentID(ctx context.Context, instrumentID uuid.UUID) ([]RequestMapping, error)

	SaveAnalysisRequestsInstrumentTransmissions(ctx context.Context, analysisRequestIDs []uuid.UUID, instrumentID uuid.UUID) error

	// SubmitAnalysisResult - Submit results to Skeleton and/or Cerberus,
	//
	// SubmitTypes:
	//  * <No Parameter given> = SubmitTypeBatchStoreAndSend (default)
	//  * SubmitTypeBatchStoreAndSend = Batch request and send with a 3 seconds delay
	//  * SubmitTypeInstantStoreAndSend = Instantly send the Result
	//  * SubmitTypeStoreOnly = Store the results and do not send to cerberus
	// By default this function batches the transmissions by collecting them and
	// use the batch-endpoint of cerberus for performance reasons
	SubmitAnalysisResult(ctx context.Context, resultData AnalysisResult, submitTypes ...SubmitType) error

	// GetInstrument returns all the settings regarding an instrument
	// contains AnalyteMappings[] and RequestMappings[]
	GetInstrument(ctx context.Context, instrumentID uuid.UUID) (Instrument, error)

	// GetInstrumentByIP returns all the settings regarding an instrument
	// contains AnalyteMappings[] and RequestMappings[]
	GetInstrumentByIP(ctx context.Context, ip string) (Instrument, error)

	// GetInstruments - Returns a list of instruments configured for this Driver class
	// contains AnalyteMappings[] and RequestMappings[]
	GetInstruments(ctx context.Context) ([]Instrument, error)

	// FindAnalyteByManufacturerTestCode - Search for the analyte that is mapped (check ui for more info)
	// Returns the mapping or model.EmptyAnalyteMapping
	FindAnalyteByManufacturerTestCode(instrument Instrument, testCode string) AnalyteMapping

	// FindResultEntities - Convienient: Lookup Instrument, AnalysisRequest and ResulteMapping for the analyte at once
	FindResultEntities(ctx context.Context, InstrumentID uuid.UUID, SampleCode string, ManufacturerTestCode string) (Instrument, []AnalysisRequest, AnalyteMapping, error)

	// FindResultMapping - Helper function to search for the RESULT mapping
	// ResultMappings can be made via the ui to translate results
	// e.g. "+" -> "pos" to be used in a pein datatype
	FindResultMapping(searchValue string, mapping []ResultMapping) (string, error)

	// RegisterProtocol - Registers
	RegisterProtocol(ctx context.Context, id uuid.UUID, name string, description string, abilities []ProtocolAbility, settings []ProtocolSetting) error

	SetOnlineStatus(ctx context.Context, id uuid.UUID, status InstrumentStatus) error

	// Start - MUST BE CALLED ON STARTUP
	// - migrates skeleton database
	// - launches goroutines for analysis request/result processing
	Start() error
}

func New(sqlConn *sqlx.DB, dbSchema string) (SkeletonAPI, error) {
	config, err := config2.ReadConfiguration()
	if err != nil {
		return nil, err
	}
	authManager := NewAuthManager(&config,
		NewRestyClient(context.Background(), &config, true))
	internalApiRestyClient := NewRestyClientWithAuthManager(context.Background(), &config, authManager)
	cerberusClient, err := NewCerberusClient(config.CerberusURL, internalApiRestyClient)
	if err != nil {
		return nil, err
	}
	deaClient, err := NewDEAClient(config.DeaURL, internalApiRestyClient)
	if err != nil {
		return nil, err
	}
	dbConn := db.CreateDbConnector(sqlConn)
	manager := NewSkeletonManager()
	instrumentCache := NewInstrumentCache()
	analysisRepository := NewAnalysisRepository(dbConn, dbSchema)
	instrumentRepository := NewInstrumentRepository(dbConn, dbSchema)
	consoleLogRepository := repository.NewConsoleLogRepository(500)
	analysisService := NewAnalysisService(analysisRepository, deaClient, cerberusClient, manager)
	instrumentService := NewInstrumentService(&config, instrumentRepository, manager, instrumentCache, cerberusClient)
	consoleLogSSEServer := server.NewConsoleLogSSEServer(service.NewConsoleLogSSEClientListener())
	consoleLogService := service.NewConsoleLogService(consoleLogRepository, consoleLogSSEServer)
	api := NewAPI(&config, authManager, analysisService, instrumentService, consoleLogService, consoleLogSSEServer)

	logcom.Init(logcom.Configuration{
		ServiceName: config.LogComServiceName,
		LogComURL:   config.LogComURL,
		HeaderProvider: func(ctx context.Context) http.Header {
			if ginCtx, ok := ctx.(*gin.Context); ok {
				return ginCtx.Request.Header
			}
			return http.Header{}
		},
	})

	return NewSkeleton(sqlConn, dbSchema, migrator.NewSkeletonMigrator(), api, analysisRepository, analysisService, instrumentService, consoleLogService, manager, cerberusClient, deaClient, config.ResultTransferFlushTimeout, config.ImageRetrySeconds)
}

func NewForTest(config config2.Configuration, sqlConn *sqlx.DB, dbSchema string, cerberusClient Cerberus, deaClient DeaClientV1, authManager AuthManager) (SkeletonAPI, error) {
	dbConn := db.CreateDbConnector(sqlConn)
	manager := NewSkeletonManager()
	instrumentCache := NewInstrumentCache()
	analysisRepository := NewAnalysisRepository(dbConn, dbSchema)
	instrumentRepository := NewInstrumentRepository(dbConn, dbSchema)
	consoleLogRepository := repository.NewConsoleLogRepository(500)
	analysisService := NewAnalysisService(analysisRepository, deaClient, cerberusClient, manager)
	instrumentService := NewInstrumentService(&config, instrumentRepository, manager, instrumentCache, cerberusClient)
	consoleLogSSEServer := server.NewConsoleLogSSEServer(service.NewConsoleLogSSEClientListener())
	consoleLogService := service.NewConsoleLogService(consoleLogRepository, consoleLogSSEServer)
	api := NewAPI(&config, authManager, analysisService, instrumentService, consoleLogService, consoleLogSSEServer)

	return NewSkeleton(sqlConn, dbSchema, migrator.NewSkeletonMigrator(), api, analysisRepository, analysisService, instrumentService, consoleLogService, manager, cerberusClient, deaClient, config.ResultTransferFlushTimeout, config.ImageRetrySeconds)
}
