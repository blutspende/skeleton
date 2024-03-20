package skeleton

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestContractOfTestRigWithSkeletonAPI(t *testing.T) {

	rig := NewTestRig()

	func(contract SkeletonAPI) {
		// this wont compile when Testrig is not of type SkeletonAPI
	}(rig)
}

// Feature: Can submit to Testrig and access the transmitted Results
func TestSubmitResultCanBeQueried(t *testing.T) {
	rig := NewTestRig()

	ar := AnalysisResult{
		ID:     uuid.New(),
		Result: "pos",
	}
	err := rig.SubmitAnalysisResult(context.Background(), ar, SubmitTypeInstantStoreAndSend)
	assert.Nil(t, err)

	assert.Equal(t, 1, len(rig.StoredAnalysisResults))
	assert.Equal(t, ar.ID, rig.StoredAnalysisResults[0].ID)
	assert.Equal(t, ar.Result, rig.StoredAnalysisResults[0].Result)

	rig.ClearStoredAnalysisResults()
}

func TestCreateAnalysisRequest(t *testing.T) {

	rig := NewTestRig()

	const samplecode = "samplecode123"
	anAnalyteId1 := uuid.New()
	anAnalyteId2 := uuid.New()
	arq1 := rig.CreateAnalysisRequest(samplecode, anAnalyteId1)
	arq2 := rig.CreateAnalysisRequest(samplecode, anAnalyteId2)

	assert.NotNil(t, arq1)
	assert.NotNil(t, arq2)

	// Testing the Simple search
	queriedRequests, err := rig.GetAnalysisRequestsBySampleCode(context.Background(), samplecode, false)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(queriedRequests))

}

func TestAddAnalysisRequestExtraValue(t *testing.T) {
	rig := NewTestRig()
	rig.AddAnalysisRequestExtraValue("DonationType", "M")
	rig.AddAnalysisRequestExtraValue("DonorNumber", "123456")
	extraValuesMap, _ := rig.GetAnalysisRequestExtraValues(context.TODO(), uuid.Nil)
	assert.Equal(t, 2, len(extraValuesMap))

	donationType, ok := extraValuesMap["DonationType"]
	assert.True(t, ok)
	assert.Equal(t, "M", donationType)

	donorNumber, ok := extraValuesMap["DonorNumber"]
	assert.True(t, ok)
	assert.Equal(t, "123456", donorNumber)
}
