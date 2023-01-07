package e2etest

import (
	"astm/skeletonapi"
	"testing"

	"github.com/stretchr/testify/assert"
)

type cerberusBackendMock struct {
	SentAsBatch               bool
	StoredResultsThatWereSent []skeletonapi.AnalysisResultV1
}

func (cbm *cerberusBackendMock) RegisterInstrument(instrument skeletonapi.InstrumentV1) error {
	return nil
}
func (cbm *cerberusBackendMock) PostAnalysisResultBatch(analysisResults []skeletonapi.AnalysisResultV1) ([]skeletonapi.AnalysisResultCreateStatusV1, error) {
	cbm.SentAsBatch = true
	cbm.StoredResultsThatWereSent = append(cbm.StoredResultsThatWereSent, analysisResults...)
	return []skeletonapi.AnalysisResultCreateStatusV1{}, nil
}

func TestResultTransmission(t *testing.T) {

	cerberusBackend := &cerberusBackendMock{
		SentAsBatch:               false,
		StoredResultsThatWereSent: []skeletonapi.AnalysisResultV1{},
	}
	// Mock a Request

	// Create skeleton
	// skeleton := Create(   cerberusBackend  )

	// PostResult : Spec says: "if the posts are less than 3 seconds appart, they get batched automatically"
	// skeleton.PostResult(result1)
	// skeleton.PostResult(result2)
	// skeleton.PostResult(result3)

	// asserts:
	// cerberusMock should have stored the request
	assert.Equal(t, 3, len(cerberusBackend.StoredResultsThatWereSent))
	assert.True(t, cerberusBackend.SentAsBatch)
	// some more 'bout the content.. "not sure" (idiocracy) yet

	// send one more
	cerberusBackend.SentAsBatch = false
	// skeleton.PostResult(result1)
	// time.Sleep(4 * time.Seconds)
	assert.Equal(t, 3+1 /*thats the one result extra*/, len(cerberusBackend.StoredResultsThatWereSent))
	assert.False(t, cerberusBackend.SentAsBatch) // this one was not sent as batch due to >3 seconds
}
