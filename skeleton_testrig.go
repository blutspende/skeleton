package skeleton

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"slices"
	"time"

	"github.com/google/uuid"
)

// A testrig that can be used for isolated Testing
// Fullfills the SkeletonAPI contract
type SkeletonTestRig struct {
	eventHandler SkeletonCallbackHandlerV1

	storedInstrumentsMap              map[string]Instrument
	StoredAnalysisResults             []AnalysisResultSet
	StoredMessageIns                  []MessageIn
	UpdatedMessageIns                 []MessageIn
	StoredMessageOuts                 []MessageOut
	UpdatedMessageOuts                []MessageOut
	MessageOutOrders                  []MessageOutOrder
	ControlResults                    map[string][]ControlResult
	StoredStandaloneControlResultSets []StandaloneControlResult
	AnalysisRequests                  []*AnalysisRequest
	AnalysisRequestExtraValues        map[string]string
}

func NewTestRig() *SkeletonTestRig {
	return &SkeletonTestRig{
		storedInstrumentsMap:       make(map[string]Instrument),
		StoredAnalysisResults:      []AnalysisResultSet{},
		ControlResults:             make(map[string][]ControlResult),
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

func (sr *SkeletonTestRig) SubmitAnalysisResult(ctx context.Context, resultData AnalysisResultSet) error {
	sr.StoredAnalysisResults = append(sr.StoredAnalysisResults, resultData)
	return nil
}

func (sr *SkeletonTestRig) SubmitAnalysisResultBatch(ctx context.Context, resultBatch AnalysisResultSet) error {
	sr.StoredAnalysisResults = append(sr.StoredAnalysisResults, resultBatch)
	return nil
}

func (sr *SkeletonTestRig) SubmitControlResults(ctx context.Context, controlResults []StandaloneControlResult) error {
	sr.StoredStandaloneControlResultSets = append(sr.StoredStandaloneControlResultSets, controlResults...)
	return nil
}

func (sr *SkeletonTestRig) GetAnalysisResultIdsWithoutControlByReagent(ctx context.Context, controlResult ControlResult, reagent Reagent) ([]uuid.UUID, error) {
	return make([]uuid.UUID, 0), nil
}

func (sr *SkeletonTestRig) GetAnalysisResultIdsWhereLastestControlIsInvalid(ctx context.Context, controlResult ControlResult, reagent Reagent) ([]uuid.UUID, error) {
	return make([]uuid.UUID, 0), nil
}

func (sr *SkeletonTestRig) GetLatestControlResultsByReagent(ctx context.Context, reagent Reagent, resultYieldTime *time.Time, analyteMappingId uuid.UUID, instrumentId uuid.UUID) ([]ControlResult, error) {
	return sr.ControlResults[fmt.Sprintf("%s%s%s", reagent.Manufacturer, reagent.LotNo, reagent.SerialNumber)], nil
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

func (sr *SkeletonTestRig) SetOnlineStatus(ctx context.Context, id uuid.UUID, status InstrumentStatus) error {
	return nil
}

func (sr *SkeletonTestRig) Start() error {
	return nil
}

// ---------------------------------------------------------------------------------- Additonal Testing Functionality
// Testfunktion: Clear the stored Analysis Results
func (sr *SkeletonTestRig) ClearStoredAnalysisResults() {
	sr.StoredAnalysisResults = []AnalysisResultSet{}
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

func (sr *SkeletonTestRig) RegisterManufacturerTests(ctx context.Context, manufacturerTests []SupportedManufacturerTests, sendToCerberus bool) error {
	return nil
}

func (sr *SkeletonTestRig) SaveMessageIn(ctx context.Context, messageIn MessageIn) (uuid.UUID, error) {
	sr.StoredMessageIns = append(sr.StoredMessageIns, messageIn)
	return messageIn.ID, nil
}

func (sr *SkeletonTestRig) UpdateMessageIn(ctx context.Context, messageIn MessageIn) error {
	sr.UpdatedMessageIns = append(sr.UpdatedMessageIns, messageIn)
	return nil
}

func (sr *SkeletonTestRig) SaveMessageOut(ctx context.Context, messageOut MessageOut) (uuid.UUID, error) {
	sr.StoredMessageOuts = append(sr.StoredMessageOuts, messageOut)
	return messageOut.ID, nil
}

func (sr *SkeletonTestRig) UpdateMessageOut(ctx context.Context, messageOut MessageOut) error {
	sr.UpdatedMessageOuts = append(sr.UpdatedMessageOuts, messageOut)
	return nil
}

func (sr *SkeletonTestRig) GetUnprocessedMessageInsByInstrumentID(ctx context.Context, instrumentID uuid.UUID) ([]MessageIn, error) {
	return nil, nil
}

func (sr *SkeletonTestRig) SaveMessageOutBatch(ctx context.Context, messageOuts []MessageOut) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, len(messageOuts))
	for i := range messageOuts {
		ids[i] = messageOuts[i].ID
		sr.StoredMessageOuts = append(sr.StoredMessageOuts, messageOuts[i])
	}
	return nil, nil
}

func (sr *SkeletonTestRig) GetUnprocessedMessageOutsByInstrumentID(ctx context.Context, instrumentID uuid.UUID) ([]MessageOut, error) {
	return nil, nil
}

func (sr *SkeletonTestRig) AddAnalysisRequestsToMessageOutOrder(ctx context.Context, messageOutOrderID uuid.UUID, analysisRequestIDs []uuid.UUID) error {
	return nil
}

func (sr *SkeletonTestRig) DeleteRevokedUnsentOrderMessagesByAnalysisRequestIDs(ctx context.Context, analysisRequestIDs []uuid.UUID) ([]uuid.UUID, error) {
	return nil, nil
}

func (sr *SkeletonTestRig) GetTestCodesToRevokeBySampleCodes(ctx context.Context, instrumentID uuid.UUID, analysisRequestIDs []uuid.UUID) (map[string][]string, error) {
	return nil, nil
}

func (sr *SkeletonTestRig) GetMessageOutOrdersBySampleCodesAndRequestMappingIDs(ctx context.Context, sampleCodes []string, instrumentID uuid.UUID, includePending bool) (map[string]map[uuid.UUID][]MessageOutOrder, error) {
	messageOutOrdersBySampleCodesAndRequestMappingIDs := make(map[string]map[uuid.UUID][]MessageOutOrder)
	for _, m := range sr.MessageOutOrders {
		if slices.Contains(sampleCodes, m.SampleCode) {
			if _, ok := messageOutOrdersBySampleCodesAndRequestMappingIDs[m.SampleCode]; !ok {
				messageOutOrdersBySampleCodesAndRequestMappingIDs[m.SampleCode] = make(map[uuid.UUID][]MessageOutOrder)
			}
			messageOutOrdersBySampleCodesAndRequestMappingIDs[m.SampleCode][m.RequestMappingID] = append(messageOutOrdersBySampleCodesAndRequestMappingIDs[m.SampleCode][m.RequestMappingID], m)
		}
	}
	return messageOutOrdersBySampleCodesAndRequestMappingIDs, nil
}

func (sr *SkeletonTestRig) GetUnprocessedMessageIns(ctx context.Context) ([]MessageIn, error) {
	return nil, nil
}

func (sr *SkeletonTestRig) GetUnprocessedMessageOuts(ctx context.Context) ([]MessageOut, error) {
	return nil, nil
}

func (sr *SkeletonTestRig) GetDbConnection() (*sqlx.DB, error) {
	return nil, nil
}

func (sr *SkeletonTestRig) RegisterSampleCodesToMessageIn(ctx context.Context, messageID uuid.UUID, sampleCodes []string) error {
	return nil
}

func (sr *SkeletonTestRig) RegisterSampleCodesToMessageOut(ctx context.Context, messageID uuid.UUID, sampleCodes []string) error {
	return nil
}
