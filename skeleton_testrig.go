package skeleton

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// A testrig that can be used for isolated Testing
// Fullfills the SkeletonAPI contract
type SkeletonTestRig struct {
	eventHandler SkeletonCallbackHandlerV1

	StoredAnalysisResults []AnalysisResult
	AnalysisRequests      []*AnalysisRequest
}

func NewTestRig() *SkeletonTestRig {
	return &SkeletonTestRig{
		StoredAnalysisResults: []AnalysisResult{},
		AnalysisRequests:      []*AnalysisRequest{},
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

// This function is not very good as it doesnt fit. TODO: This function implicitly tires to provide a
// pagination without a concept -> REMOVE !!!! If its required provide an iterator implementation that
// then is consequently used in the library itself (currentPage, itemsPerPage gone!)
// Also this Function is a duplicate to GetAnalysisRequestBySampleCode, limiting the choice to
// those with no results is just a parameter (of more to come in the future i assume) TODO: remove this funciton
// and add these as parameters to the Original functions (or a global switch or or...)
func (sr *SkeletonTestRig) GetAnalysisRequestWithNoResults(ctx context.Context, currentPage, itemsPerPage int) (requests []AnalysisRequest, maxPages int, err error) {

	ar := []AnalysisRequest{}
	for _, rqs := range sr.AnalysisRequests {
		isIncluded := false
		for _, having := range sr.StoredAnalysisResults {
			if having.AnalysisRequest.AnalyteID == rqs.ID {
				isIncluded = true
			}
		}
		if !isIncluded {
			ar = append(ar, *rqs)
		}
	}

	return ar, 0, nil
}

// Overridden for Testing: Returns All ARQS for the samplecode
func (sr *SkeletonTestRig) GetAnalysisRequestsBySampleCode(ctx context.Context, sampleCode string, allowResending bool) ([]AnalysisRequest, error) {
	ar := []AnalysisRequest{}
	for _, rqs := range sr.AnalysisRequests {
		if rqs.SampleCode == sampleCode {
			ar = append(ar, *rqs)
		}
	}
	return ar, nil
}

func (sr *SkeletonTestRig) GetAnalysisRequestsBySampleCodes(ctx context.Context, sampleCodes []string, allowResending bool) (map[string][]AnalysisRequest, error) {
	analysisRequestsBySampleCodes := make(map[string][]AnalysisRequest)
	for i := range sampleCodes {
		requests, _ := sr.GetAnalysisRequestsBySampleCode(ctx, sampleCodes[i], false)
		if len(requests) > 0 {
			analysisRequestsBySampleCodes[sampleCodes[i]] = requests
		}
	}
	return analysisRequestsBySampleCodes, nil
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
	sr.StoredAnalysisResults = append(sr.StoredAnalysisResults, resultBatch...)
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

// ---------------------------------------------------------------------------------- Additonal Testing Functionality
// Testfunktion: Clear the stored Analysis Results
func (sr *SkeletonTestRig) ClearStoredAnalysisResults() {
	sr.StoredAnalysisResults = []AnalysisResult{}
}

// Testfunktion: Create an Analysis Request for a SampleCode and analyte id
// The function returns a pointer to the Analysis Request so that the assumed values can be changed in the test
func (sr *SkeletonTestRig) CreateAnalysisRequest(samplecode string, analyteID uuid.UUID) *AnalysisRequest {
	arq := &AnalysisRequest{
		ID:             uuid.New(),
		WorkItemID:     uuid.New(),
		SampleCode:     samplecode,
		AnalyteID:      analyteID,
		MaterialID:     uuid.Nil,
		LaboratoryID:   uuid.Nil,
		ValidUntilTime: time.Now().Add(10 * time.Minute),
		CreatedAt:      time.Now(),
		SubjectInfo:    nil,
	}
	sr.AnalysisRequests = append(sr.AnalysisRequests, arq)
	return arq
}
