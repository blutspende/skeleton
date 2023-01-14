package skeleton

import (
	"context"

	config2 "github.com/DRK-Blutspende-BaWueHe/skeleton/config"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/db"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/migrator"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type SkeletonError error

// SkeletonCallbackHandlerV1 - must implement an EventHandler to react on Events triggered by Skeleton
type SkeletonCallbackHandlerV1 interface {
	// HandleAnalysisRequests is called when the Skeleton needs to resolve an updated AnalysisRequest
	// based on data that was probably not processed before. This function is supposed to trigger a
	// SkeletonAPI.SubmitAnalysisResult on the SkeletonAPI if result-data was found.
	HandleAnalysisRequests(request []AnalysisRequest) error

	// GetManufacturerTestList is called when the Skeleton requires a list of test names (strings)
	// as known to be valid by the manufacturer of this instrument
	GetManufacturerTestList(instrumentId uuid.UUID, protocolId uuid.UUID) ([]SupportedManufacturerTests, error)
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
	GetAnalysisRequestWithNoResults(currentPage, itemsPerPage int) (requests []AnalysisRequest, maxPages int, err error)
	GetAnalysisRequestsBySampleCode(sampleCode string) ([]AnalysisRequest, error)

	// GetAnalysisRequestsBySampleCodes - Return a list of AnalysisRequests that contains the sampleCodes
	// Empty List if nothing is found. Error occurs only on Database-error
	GetAnalysisRequestsBySampleCodes(sampleCodes []string) (map[string][]AnalysisRequest, error)
	GetRequestMappingsByInstrumentID(instrumentID uuid.UUID) ([]RequestMapping, error)

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
	GetInstrument(instrumentID uuid.UUID) (Instrument, error)

	// GetInstrumentByIP returns all the settings regarding an instrument
	// contains AnalyteMappings[] and RequestMappings[]
	GetInstrumentByIP(ip string) (Instrument, error)

	// GetInstruments - Returns a list of instruments configured for this Driver class
	// contains AnalyteMappings[] and RequestMappings[]
	GetInstruments() ([]Instrument, error)

	// FindAnalyteByManufacturerTestCode - Search for the analyte that is mapped (check ui for more info)
	// Returns the mapping or model.EmptyAnalyteMapping
	FindAnalyteByManufacturerTestCode(instrument Instrument, testCode string) AnalyteMapping

	// FindResultEntities - Convienient: Lookup Instrument, AnalysisRequest and ResulteMapping for the analyte at once
	FindResultEntities(InstrumentID uuid.UUID, SampleCode string, ManufacturerTestCode string) (Instrument, []AnalysisRequest, AnalyteMapping, error)

	// FindResultMapping - Helper function to search for the RESULT mapping
	// ResultMappings can be made via the ui to translate results
	// e.g. "+" -> "pos" to be used in a pein datatype
	FindResultMapping(searchValue string, mapping []ResultMapping) (string, error)

	// RegisterProtocol - Registers
	RegisterProtocol(ctx context.Context, id uuid.UUID, name string, description string, abilities []ProtocolAbility) error

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
	authManager.StartClientCredentialTask(context.Background())
	internalApiRestyClient := NewRestyClientWithAuthManager(context.Background(), &config, authManager)
	cerberusClient, err := NewCerberusV1Client(config.CerberusURL, internalApiRestyClient)
	if err != nil {
		return nil, err
	}
	dbConn := db.CreateDbConnector(sqlConn)
	manager := NewCallbackManager()
	instrumentCache := NewInstrumentCache()
	analysisRepository := NewAnalysisRepository(dbConn, dbSchema)
	instrumentRepository := NewInstrumentRepository(dbConn, dbSchema)
	analysisService := NewAnalysisService(analysisRepository)
	instrumentService := NewInstrumentService(instrumentRepository, manager, instrumentCache)
	api := NewAPI(&config, authManager, analysisService, instrumentService)
	return NewSkeleton(sqlConn, dbSchema, migrator.NewSkeletonMigrator(), api, analysisRepository, instrumentService, manager, cerberusClient)
}
