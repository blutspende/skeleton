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

	storedInstrumentsMap       map[string]Instrument
	StoredAnalysisResults      []AnalysisResult
	AnalysisRequests           []*AnalysisRequest
	AnalysisRequestExtraValues map[string]string
}

func (sr *SkeletonTestRig) RegisterManufacturerTests(ctx context.Context, manufacturerTests []SupportedManufacturerTests) error {
	return nil
}

func NewTestRig() *SkeletonTestRig {
	return &SkeletonTestRig{
		storedInstrumentsMap:       make(map[string]Instrument),
		StoredAnalysisResults:      []AnalysisResult{},
		AnalysisRequests:           []*AnalysisRequest{},
		AnalysisRequestExtraValues: make(map[string]string),
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

func (sr *SkeletonTestRig) GetAnalysisRequestExtraValues(ctx context.Context, analysisRequestID uuid.UUID) (map[string]string, error) {
	return sr.AnalysisRequestExtraValues, nil
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

func (sr *SkeletonTestRig) SaveAnalysisRequestsInstrumentTransmissions(ctx context.Context, analysisRequestIDs []uuid.UUID, instrumentID uuid.UUID) error {
	return nil
}

func (sr *SkeletonTestRig) SubmitAnalysisResult(ctx context.Context, resultData AnalysisResult) error {
	sr.StoredAnalysisResults = append(sr.StoredAnalysisResults, resultData)
	return nil
}

func (sr *SkeletonTestRig) SubmitAnalysisResultBatch(ctx context.Context, resultBatch []AnalysisResult) error {
	sr.StoredAnalysisResults = append(sr.StoredAnalysisResults, resultBatch...)
	return nil
}

func (sr *SkeletonTestRig) GetInstrument(ctx context.Context, instrumentID uuid.UUID) (Instrument, error) {
	instrument := sr.storedInstrumentsMap[instrumentID.String()]
	return instrument, nil
}

func (sr *SkeletonTestRig) GetInstrumentByIP(ctx context.Context, ip string) (Instrument, error) {
	instrument := sr.storedInstrumentsMap[ip]
	return instrument, nil
}

func (sr *SkeletonTestRig) GetInstruments(ctx context.Context) ([]Instrument, error) {
	instruments := make([]Instrument, 0)
	idMap := make(map[uuid.UUID]any)
	for _, instrument := range sr.storedInstrumentsMap {
		if _, ok := idMap[instrument.ID]; ok {
			continue
		}
		instruments = append(instruments, instrument)
		idMap[instrument.ID] = nil
	}

	return instruments, nil
}

func (sr *SkeletonTestRig) FindResultEntities(ctx context.Context, InstrumentID uuid.UUID, SampleCode string, ManufacturerTestCode string) (Instrument, []AnalysisRequest, AnalyteMapping, error) {
	return Instrument{}, []AnalysisRequest{}, AnalyteMapping{}, nil
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

func (sr *SkeletonTestRig) GetSortingTarget(ctx context.Context, instrumentIP string, sampleCode string, programme string) (string, error) {
	return "", nil
}

func (sr *SkeletonTestRig) AddAnalysisRequestExtraValue(key string, value string) {
	sr.AnalysisRequestExtraValues[key] = value
}

func (sr *SkeletonTestRig) AddInstrument(instrument Instrument) {
	sr.storedInstrumentsMap[instrument.ID.String()] = instrument
	sr.storedInstrumentsMap[instrument.Hostname] = instrument
}

func (sr *SkeletonTestRig) MarkSortingTargetAsApplied(ctx context.Context, instrumentIP, sampleCode, programme, target string) error {
	return nil
}
