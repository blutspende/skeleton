package skeleton

import (
	"context"

	"github.com/google/uuid"
)

// A testrig that can be used for isolated Testing
// Fullfills the SkeletonAPI contract
type SkeletonTestRig struct {
	eventHandler SkeletonCallbackHandlerV1

	StoredAnalysisResults []AnalysisResult
}

func NewTestRig() *SkeletonTestRig {

	return &SkeletonTestRig{
		StoredAnalysisResults: []AnalysisResult{},
	}
}

func (sr *SkeletonTestRig) SetCallbackHandler(eventHandler SkeletonCallbackHandlerV1) {
	sr.eventHandler = eventHandler
}
func (sr *SkeletonTestRig) GetCallbackHandler() SkeletonCallbackHandlerV1 {
	return sr.eventHandler
}

func (sr *SkeletonTestRig) Log(instrumentID uuid.UUID, msg string) {
}

func (sr *SkeletonTestRig) LogError(instrumentID uuid.UUID, err error) {
}

func (sr *SkeletonTestRig) LogDebug(instrumentID uuid.UUID, msg string) {

}

func (sr *SkeletonTestRig) GetAnalysisRequestWithNoResults(ctx context.Context, currentPage, itemsPerPage int) (requests []AnalysisRequest, maxPages int, err error) {
	return []AnalysisRequest{}, 0, nil
}

func (sr *SkeletonTestRig) GetAnalysisRequestsBySampleCode(ctx context.Context, sampleCode string, allowResending bool) ([]AnalysisRequest, error) {
	return []AnalysisRequest{}, nil
}

func (sr *SkeletonTestRig) GetAnalysisRequestsBySampleCodes(ctx context.Context, sampleCodes []string, allowResending bool) (map[string][]AnalysisRequest, error) {
	return map[string][]AnalysisRequest{}, nil
}

func (sr *SkeletonTestRig) GetRequestMappingsByInstrumentID(ctx context.Context, instrumentID uuid.UUID) ([]RequestMapping, error) {
	return []RequestMapping{}, nil
}

func (sr *SkeletonTestRig) SaveAnalysisRequestsInstrumentTransmissions(ctx context.Context, analysisRequestIDs []uuid.UUID, instrumentID uuid.UUID) error {
	return nil
}

func (sr *SkeletonTestRig) SubmitAnalysisResult(ctx context.Context, resultData AnalysisResult, submitTypes ...SubmitType) error {

	sr.StoredAnalysisResults = append(sr.StoredAnalysisResults, resultData)
	return nil
}

func (sr *SkeletonTestRig) SubmitAnalysisResultBatch(ctx context.Context, resultBatch []AnalysisResult, submitTypes ...SubmitType) error {
	return nil
}

func (sr *SkeletonTestRig) GetInstrument(ctx context.Context, instrumentID uuid.UUID) (Instrument, error) {
	return Instrument{}, nil
}

func (sr *SkeletonTestRig) GetInstrumentByIP(ctx context.Context, ip string) (Instrument, error) {
	return Instrument{}, nil
}

func (sr *SkeletonTestRig) GetInstruments(ctx context.Context) ([]Instrument, error) {
	return []Instrument{}, nil
}

func (sr *SkeletonTestRig) FindAnalyteByManufacturerTestCode(instrument Instrument, testCode string) AnalyteMapping {
	return AnalyteMapping{}
}

func (sr *SkeletonTestRig) FindResultEntities(ctx context.Context, InstrumentID uuid.UUID, SampleCode string, ManufacturerTestCode string) (Instrument, []AnalysisRequest, AnalyteMapping, error) {
	return Instrument{}, []AnalysisRequest{}, AnalyteMapping{}, nil
}

func (sr *SkeletonTestRig) FindResultMapping(searchValue string, mapping []ResultMapping) (string, error) {
	return "", nil
}

func (sr *SkeletonTestRig) RegisterProtocol(ctx context.Context, id uuid.UUID, name string, description string, abilities []ProtocolAbility, settings []ProtocolSetting) error {
	return nil
}

func (sr *SkeletonTestRig) SetOnlineStatus(ctx context.Context, id uuid.UUID, status InstrumentStatus) error {
	return nil
}

func (sr *SkeletonTestRig) Start() error {
	return nil
}

// Additonal Testing Functionality
func (sr *SkeletonTestRig) Clear() {
	sr.StoredAnalysisResults = []AnalysisResult{}
}
