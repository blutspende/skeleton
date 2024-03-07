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
}
