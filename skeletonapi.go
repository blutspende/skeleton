package skeleton

import (
	"context"
	bloodlabNet "github.com/DRK-Blutspende-BaWueHe/go-bloodlab-net"
	config2 "github.com/DRK-Blutspende-BaWueHe/skeleton/config"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/db"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/migrator"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type SkeletonError error

// SkeletonCallbackHandlerV1 - must implement an Eventhandler to react on Events triggered by Skeleton
type SkeletonCallbackHandlerV1 interface {
	// HandleIncomingInstrumentData is called when the Instrument has sent a message and this one needs to get processed
	// This method is blocking. The synchroneous processing should be as quick as possible and pass
	// the further process to an asynchroneous job
	HandleIncomingInstrumentData(incomingBytes []byte, session bloodlabNet.Session, instrumentID uuid.UUID, remoteSourceAddress string) error

	// HandleAnalysisRequests is called when the Sekeleton needs to resolve an updated AnalysisRequest
	// based on data that was probably not processed before. This function is supposed to trigger a
	// SkeletionAPI.SubmitAnalysisResult on the SkeletonAPI if result-data was found.
	HandleAnalysisRequests(request []AnalysisRequest) error

	// GetManufacturerTestList is called when the Skeleton requires a list of testnames (strings)
	// as known to be valid by the manfucaturer of this instrument
	GetManufacturerTestList() ([]SupportedManufacturerTests, error)
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

	// GetAnalysisRequestsBySampleCodes - Return a list of Analysisrequests that contains the sampleCodes
	// Empty List if nothing is found. Error occurs only on Database-error
	GetAnalysisRequestsBySampleCodes(sampleCodes []string) ([]AnalysisRequest, error)
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

	// GetInstruments - Returns a list of instruments configured for this Driverclass
	// contains AnalyteMappings[] and RequestMappings[]
	GetInstruments() ([]Instrument, error)

	// FindAnalyteByManufacturerTestCode - Search for the analyte that is mapped (check ui for more info)
	// Returns the mapping or model.EmptyAnalyteMapping
	FindAnalyteByManufacturerTestCode(instrument Instrument, testCode string) AnalyteMapping

	// FindResultMapping - Helper function to search for the RESULT mapping
	// Resultmappings can be made via the ui to translate results
	// e.g. "+" -> "pos" to be used in a pein datatype
	FindResultMapping(searchvalue string, mapping []ResultMapping) (string, error)

	// RegisterProtocol - Registers
	RegisterProtocol(ctx context.Context, id uuid.UUID, name string, description string) error

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
	analysisRepository := NewAnalysisRepository(dbConn, dbSchema)
	instrumentRepository := NewInstrumentRepository(dbConn, dbSchema)
	api := NewAPI(&config, authManager, NewAnalysisService(analysisRepository), NewInstrumentService(instrumentRepository))
	return NewSkeleton(sqlConn, dbSchema, migrator.NewSkeletonMigrator(), api, analysisRepository, instrumentRepository, cerberusClient)
}
