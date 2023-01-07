package v1

import (
	"context"
	"skeleton/model"

	bloodlabNet "github.com/DRK-Blutspende-BaWueHe/go-bloodlab-net"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type SkeletonError error

// SkeletonCallbackHandlerV1 - must implement an Eventhandler to react on Events triggered by Sekelton
type SkeletonCallbackHandlerV1 interface {
	// HandleIncomingInstrumentData is called when the Instrument has send a message and this one needs to get processed
	// This method is blocking. The synchroneous processing should be as quick as possible and pass
	// the further process to an asynchroneous job
	HandleIncomingInstrumentData(incomingBytes []byte, session bloodlabNet.Session, instrumentID uuid.UUID, remoteSourceAddress string) error

	// HandleAnalysisRequests is called when the Sekeleton needs to resolve an updated AnalysisRequest
	// based on data that was probably not processed before. This function is supposed to trigger a
	// SkeletionAPI.SubmitAnalysisResult on the SkeletonAPI if result-data was found.
	HandleAnalysisRequests(request []model.AnalysisRequestV1) error

	// GetManufacturerTestList is called when the Skeleton requires a list of testnames (strings)
	// as known to be valid by the manfucaturer of this instrument
	GetManufacturerTestList() ([]model.SupportedManufacturerTests, error)
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
	GetAnalysisRequestWithNoResults(currentPage, itemsPerPage int) (requests []model.AnalysisRequestV1, maxPages int, err error)
	GetAnalysisRequestsBySampleCode(sampleCode string) ([]model.AnalysisRequestV1, error)

	// GetAnalysisRequestsBySampleCodes - Return a list of Analysisrequests that contains the sampleCodes
	// Empty List if nothing is found. Error occurs only on Database-error
	GetAnalysisRequestsBySampleCodes(sampleCodes []string) ([]model.AnalysisRequestV1, error)
	GetRequestMappingsByInstrumentID(instrumentID uuid.UUID) ([]model.RequestMappingV1, error)

	// SubmitAnalysisResult - Submit results to Skeleton and/or Cerberus,
	//
	// SubmitTypes:
	//  * <No Parameter given> = SubmitTypeBatchStoreAndSend (default)
	//  * SubmitTypeBatchStoreAndSend = Batch request and send with a 3 second delay
	//  * SubmitTypeInstantStoreAndSend = Instantly send the Result
	//  * SubmitTypeStoreOnly = Store the results and do not send to cerberus
	// By default this function batches the transmissions by collecting them and
	// use the batch-endpoint of cerberus for performance reasons
	SubmitAnalysisResult(ctx context.Context, resultData model.AnalysisResultV1, submitTypes ...model.SubmitType) error

	// GetInstrument returns all the settings regarding an instrument
	// contains AnalyteMappings[] and RequestMappings[]
	GetInstrument(instrumentID uuid.UUID) (model.InstrumentV1, error)

	// GetInstruments - Returns a list of instruments configured for this Driverclass
	// contains AnalyteMappings[] and RequestMappings[]
	GetInstruments() ([]model.InstrumentV1, error)

	// FindAnalyteByManufacturerTestCode - Search for the analyte that is mapped (check ui for more info)
	// Returns the mapping or model.EmptyAnalyteMapping
	FindAnalyteByManufacturerTestCode(instrument model.InstrumentV1, testCode string) model.AnalyteMappingV1

	// FindResultMapping - Helper function to search for the RESULT mapping
	// Resultmappings can be made via the ui to translate results
	// e.g. "+" -> "pos" to be used in a pein datatype
	FindResultMapping(searchvalue string, mapping []model.ResultMappingV1) (string, error)

	// Start - MUST BE CALLED ON STARTUP
	// - migrates skeleton database
	// - launches goroutines for analysis request/result processing
	Start(ctx context.Context, db *sqlx.DB, schemaName string) error
}
