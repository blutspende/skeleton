package skeleton

import (
	"context"
	"github.com/blutspende/skeleton/db"
	"github.com/blutspende/skeleton/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestCreateAnalysisRequests(t *testing.T) {
	mockManager := &mockManager{}
	analysisRequests := []AnalysisRequest{
		{WorkItemID: uuid.MustParse("6cdc3aa0-a024-4c51-8d24-8aa12d489f41")},
		{WorkItemID: uuid.MustParse("92a2ba34-d891-4a1b-89fb-e0c4d717f729")},
	}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	analysisRequestStatuses, err := analysisService.CreateAnalysisRequests(context.TODO(), analysisRequests)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(analysisRequestStatuses))
	assert.Nil(t, analysisRequestStatuses[0].Error)
	assert.Nil(t, analysisRequestStatuses[1].Error)
	assert.Equal(t, 2, len(mockManager.AnalysisRequestsSentForProcessing))
	assert.Equal(t, uuid.MustParse("6cdc3aa0-a024-4c51-8d24-8aa12d489f41"), mockManager.AnalysisRequestsSentForProcessing[0].WorkItemID)
	assert.Equal(t, uuid.MustParse("92a2ba34-d891-4a1b-89fb-e0c4d717f729"), mockManager.AnalysisRequestsSentForProcessing[1].WorkItemID)
}

func TestCreateAnalysisRequestsWithError(t *testing.T) {
	mockManager := &mockManager{}
	analysisRequests := []AnalysisRequest{
		{WorkItemID: uuid.MustParse("6cdc3aa0-a024-4c51-8d24-8aa12d489f41")},
		{WorkItemID: uuid.MustParse("92a2ba34-d891-4a1b-89fb-e0c4d717f729")},
		{WorkItemID: uuid.MustParse("88b87019-ddcc-4d4b-bc04-9e213680e0db")},
		{WorkItemID: uuid.MustParse("c0dbcfb6-6a90-4ab6-bcab-0cfbec4abd06")},
	}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	analysisRequestStatuses, err := analysisService.CreateAnalysisRequests(context.TODO(), analysisRequests)
	assert.NotNil(t, err)
	assert.Equal(t, ErrAnalysisRequestWithMatchingWorkItemIdFound, err)
	assert.Equal(t, 4, len(analysisRequestStatuses))
	assert.Nil(t, analysisRequestStatuses[0].Error)
	assert.Nil(t, analysisRequestStatuses[1].Error)
	assert.Nil(t, analysisRequestStatuses[2].Error)
	assert.NotNil(t, analysisRequestStatuses[3].Error)
	assert.Equal(t, ErrAnalysisRequestWithMatchingWorkItemIdFound, analysisRequestStatuses[3].Error)
	assert.Equal(t, 3, len(mockManager.AnalysisRequestsSentForProcessing))
	assert.Equal(t, uuid.MustParse("6cdc3aa0-a024-4c51-8d24-8aa12d489f41"), mockManager.AnalysisRequestsSentForProcessing[0].WorkItemID)
	assert.Equal(t, uuid.MustParse("92a2ba34-d891-4a1b-89fb-e0c4d717f729"), mockManager.AnalysisRequestsSentForProcessing[1].WorkItemID)
	assert.Equal(t, uuid.MustParse("88b87019-ddcc-4d4b-bc04-9e213680e0db"), mockManager.AnalysisRequestsSentForProcessing[2].WorkItemID)
}

func TestCreateAnalysisResultStatusAndControlResultValid(t *testing.T) {
	analysisResults := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(true, false, nil)

	mockManager := &mockManager{}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	results, err := analysisService.CreateAnalysisResultsBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResults,
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsValid)
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, Final, results[0].Status)
}

func TestCreateAnalysisResultStatusAndControlResultNotAllControlAvailable(t *testing.T) {
	expectedControlResultCreatedAt, _ := utils.FormatTimeStringToBerlinTime("20240925162727", "20060102150405")
	expectedControlResult := ExpectedControlResult{
		ID:             uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
		SampleCode:     "Sample2",
		Operator:       NotEquals,
		ExpectedValue:  "25",
		ExpectedValue2: nil,
		CreatedAt:      expectedControlResultCreatedAt,
		DeletedAt:      nil,
		CreatedBy:      uuid.UUID{},
		DeletedBy:      uuid.NullUUID{},
	}

	analysisResults := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(true, false, &expectedControlResult)

	mockManager := &mockManager{}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	results, err := analysisService.CreateAnalysisResultsBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResults,
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 2, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.Equal(t, 1, len(results[0].Reagents[0].ControlResults))
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsValid)
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	//Expected 2 sample codes, only got control for one
	assert.Equal(t, Preliminary, results[0].Status)
}

func TestCreateAnalysisResultStatusAndControlResultNotValid(t *testing.T) {
	analysisResults := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(true, false, nil)
	analysisResults[0].Reagents[0].ControlResults[0].Result = "37.5"

	mockManager := &mockManager{}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	results, err := analysisService.CreateAnalysisResultsBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResults,
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.Equal(t, false, results[0].Reagents[0].ControlResults[0].IsValid)
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, Final, results[0].Status)
}

func TestCreateAnalysisResultStatusAndControlResultNotMatchingSampleCodes(t *testing.T) {
	analysisResults := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(true, false, nil)
	analysisResults[0].Reagents[0].ControlResults[0].SampleCode = "sample2"
	analysisResults[0].Reagents[0].ControlResults[0].AnalyteMapping = AnalyteMapping{}

	mockManager := &mockManager{}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	results, err := analysisService.CreateAnalysisResultsBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResults,
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.Equal(t, false, results[0].Reagents[0].ControlResults[0].IsValid)
	assert.Equal(t, false, results[0].Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, Preliminary, results[0].Status)
}

func TestCreateAnalysisResultStatusAndControlResultWithoutExpectedControlResult(t *testing.T) {
	analysisResults := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(false, false, nil)

	mockManager := &mockManager{}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	results, err := analysisService.CreateAnalysisResultsBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResults,
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 0, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.Equal(t, false, results[0].Reagents[0].ControlResults[0].IsValid)
	assert.Equal(t, false, results[0].Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, Final, results[0].Status)
}

func TestCreateAnalysisResultStatusAndControlResultWithOperatorNotEqualsValid(t *testing.T) {
	expectedControlResultCreatedAt, _ := utils.FormatTimeStringToBerlinTime("20240925162727", "20060102150405")
	expectedControlResult := ExpectedControlResult{
		ID:             uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
		SampleCode:     "Sample1",
		Operator:       NotEquals,
		ExpectedValue:  "25",
		ExpectedValue2: nil,
		CreatedAt:      expectedControlResultCreatedAt,
		DeletedAt:      nil,
		CreatedBy:      uuid.UUID{},
		DeletedBy:      uuid.NullUUID{},
	}

	analysisResults := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(true, true, &expectedControlResult)

	mockManager := &mockManager{}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	results, err := analysisService.CreateAnalysisResultsBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResults,
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsValid)
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, Final, results[0].Status)
}

func TestCreateAnalysisResultStatusAndControlResultWithOperatorNotEqualsInvalid(t *testing.T) {
	expectedControlResultCreatedAt, _ := utils.FormatTimeStringToBerlinTime("20240925162727", "20060102150405")
	expectedControlResult := ExpectedControlResult{
		ID:             uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
		SampleCode:     "Sample1",
		Operator:       NotEquals,
		ExpectedValue:  "40",
		ExpectedValue2: nil,
		CreatedAt:      expectedControlResultCreatedAt,
		DeletedAt:      nil,
		CreatedBy:      uuid.UUID{},
		DeletedBy:      uuid.NullUUID{},
	}

	analysisResults := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(true, true, &expectedControlResult)

	mockManager := &mockManager{}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	results, err := analysisService.CreateAnalysisResultsBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResults,
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.Equal(t, false, results[0].Reagents[0].ControlResults[0].IsValid)
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, Final, results[0].Status)
}

func TestCreateAnalysisResultStatusAndControlResultWithOperatorGreaterOrEqualValid(t *testing.T) {
	expectedControlResultCreatedAt, _ := utils.FormatTimeStringToBerlinTime("20240925162727", "20060102150405")
	expectedControlResult := ExpectedControlResult{
		ID:             uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
		SampleCode:     "Sample1",
		Operator:       GreaterOrEqual,
		ExpectedValue:  "25",
		ExpectedValue2: nil,
		CreatedAt:      expectedControlResultCreatedAt,
		DeletedAt:      nil,
		CreatedBy:      uuid.UUID{},
		DeletedBy:      uuid.NullUUID{},
	}

	analysisResults := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(true, true, &expectedControlResult)

	mockManager := &mockManager{}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	results, err := analysisService.CreateAnalysisResultsBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResults,
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsValid)
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, Final, results[0].Status)
}

func TestCreateAnalysisResultStatusAndControlResultWithOperatorGreaterOrEqualInvalid(t *testing.T) {
	expectedControlResultCreatedAt, _ := utils.FormatTimeStringToBerlinTime("20240925162727", "20060102150405")
	expectedControlResult := ExpectedControlResult{
		ID:             uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
		SampleCode:     "Sample1",
		Operator:       GreaterOrEqual,
		ExpectedValue:  "45",
		ExpectedValue2: nil,
		CreatedAt:      expectedControlResultCreatedAt,
		DeletedAt:      nil,
		CreatedBy:      uuid.UUID{},
		DeletedBy:      uuid.NullUUID{},
	}

	analysisResults := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(true, true, &expectedControlResult)

	mockManager := &mockManager{}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	results, err := analysisService.CreateAnalysisResultsBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResults,
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.Equal(t, false, results[0].Reagents[0].ControlResults[0].IsValid)
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, Final, results[0].Status)
}

func TestCreateAnalysisResultStatusAndControlResultWithOperatorGreaterValid(t *testing.T) {
	expectedControlResultCreatedAt, _ := utils.FormatTimeStringToBerlinTime("20240925162727", "20060102150405")
	expectedControlResult := ExpectedControlResult{
		ID:             uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
		SampleCode:     "Sample1",
		Operator:       Greater,
		ExpectedValue:  "25",
		ExpectedValue2: nil,
		CreatedAt:      expectedControlResultCreatedAt,
		DeletedAt:      nil,
		CreatedBy:      uuid.UUID{},
		DeletedBy:      uuid.NullUUID{},
	}

	analysisResults := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(true, true, &expectedControlResult)

	mockManager := &mockManager{}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	results, err := analysisService.CreateAnalysisResultsBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResults,
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsValid)
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, Final, results[0].Status)
}

func TestCreateAnalysisResultStatusAndControlResultWithOperatorGreaterInvalid(t *testing.T) {
	expectedControlResultCreatedAt, _ := utils.FormatTimeStringToBerlinTime("20240925162727", "20060102150405")
	expectedControlResult := ExpectedControlResult{
		ID:             uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
		SampleCode:     "Sample1",
		Operator:       Greater,
		ExpectedValue:  "40",
		ExpectedValue2: nil,
		CreatedAt:      expectedControlResultCreatedAt,
		DeletedAt:      nil,
		CreatedBy:      uuid.UUID{},
		DeletedBy:      uuid.NullUUID{},
	}

	analysisResults := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(true, true, &expectedControlResult)

	mockManager := &mockManager{}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	results, err := analysisService.CreateAnalysisResultsBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResults,
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.Equal(t, false, results[0].Reagents[0].ControlResults[0].IsValid)
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, Final, results[0].Status)
}

func TestCreateAnalysisResultStatusAndControlResultWithOperatorLessOrEqualValid(t *testing.T) {
	expectedControlResultCreatedAt, _ := utils.FormatTimeStringToBerlinTime("20240925162727", "20060102150405")
	expectedControlResult := ExpectedControlResult{
		ID:             uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
		SampleCode:     "Sample1",
		Operator:       LessOrEqual,
		ExpectedValue:  "45",
		ExpectedValue2: nil,
		CreatedAt:      expectedControlResultCreatedAt,
		DeletedAt:      nil,
		CreatedBy:      uuid.UUID{},
		DeletedBy:      uuid.NullUUID{},
	}

	analysisResults := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(true, true, &expectedControlResult)

	mockManager := &mockManager{}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	results, err := analysisService.CreateAnalysisResultsBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResults,
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsValid)
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, Final, results[0].Status)
}

func TestCreateAnalysisResultStatusAndControlResultWithOperatorLessOrEqualInvalid(t *testing.T) {
	expectedControlResultCreatedAt, _ := utils.FormatTimeStringToBerlinTime("20240925162727", "20060102150405")
	expectedControlResult := ExpectedControlResult{
		ID:             uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
		SampleCode:     "Sample1",
		Operator:       LessOrEqual,
		ExpectedValue:  "25",
		ExpectedValue2: nil,
		CreatedAt:      expectedControlResultCreatedAt,
		DeletedAt:      nil,
		CreatedBy:      uuid.UUID{},
		DeletedBy:      uuid.NullUUID{},
	}

	analysisResults := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(true, true, &expectedControlResult)

	mockManager := &mockManager{}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	results, err := analysisService.CreateAnalysisResultsBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResults,
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.Equal(t, false, results[0].Reagents[0].ControlResults[0].IsValid)
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, Final, results[0].Status)
}

func TestCreateAnalysisResultStatusAndControlResultWithOperatorLessValid(t *testing.T) {
	expectedControlResultCreatedAt, _ := utils.FormatTimeStringToBerlinTime("20240925162727", "20060102150405")
	expectedControlResult := ExpectedControlResult{
		ID:             uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
		SampleCode:     "Sample1",
		Operator:       Less,
		ExpectedValue:  "45",
		ExpectedValue2: nil,
		CreatedAt:      expectedControlResultCreatedAt,
		DeletedAt:      nil,
		CreatedBy:      uuid.UUID{},
		DeletedBy:      uuid.NullUUID{},
	}

	analysisResults := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(true, true, &expectedControlResult)

	mockManager := &mockManager{}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	results, err := analysisService.CreateAnalysisResultsBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResults,
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsValid)
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, Final, results[0].Status)
}

func TestCreateAnalysisResultStatusAndControlResultWithOperatorLessInvalid(t *testing.T) {
	expectedControlResultCreatedAt, _ := utils.FormatTimeStringToBerlinTime("20240925162727", "20060102150405")
	expectedControlResult := ExpectedControlResult{
		ID:             uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
		SampleCode:     "Sample1",
		Operator:       Less,
		ExpectedValue:  "40",
		ExpectedValue2: nil,
		CreatedAt:      expectedControlResultCreatedAt,
		DeletedAt:      nil,
		CreatedBy:      uuid.UUID{},
		DeletedBy:      uuid.NullUUID{},
	}

	analysisResults := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(true, true, &expectedControlResult)

	mockManager := &mockManager{}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	results, err := analysisService.CreateAnalysisResultsBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResults,
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.Equal(t, false, results[0].Reagents[0].ControlResults[0].IsValid)
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, Final, results[0].Status)
}

func TestCreateAnalysisResultStatusAndControlResultWithOperatorInOpenIntervalValid(t *testing.T) {
	expectedControlResultCreatedAt, _ := utils.FormatTimeStringToBerlinTime("20240925162727", "20060102150405")
	expectedControlResult := ExpectedControlResult{
		ID:             uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
		SampleCode:     "Sample1",
		Operator:       InOpenInterval,
		ExpectedValue:  "40",
		ExpectedValue2: strToPtr("50"),
		CreatedAt:      expectedControlResultCreatedAt,
		DeletedAt:      nil,
		CreatedBy:      uuid.UUID{},
		DeletedBy:      uuid.NullUUID{},
	}

	analysisResults := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(true, true, &expectedControlResult)

	mockManager := &mockManager{}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	results, err := analysisService.CreateAnalysisResultsBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResults,
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsValid)
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, Final, results[0].Status)
}

func TestCreateAnalysisResultStatusAndControlResultWithOperatorInOpenIntervalInvalid(t *testing.T) {
	expectedControlResultCreatedAt, _ := utils.FormatTimeStringToBerlinTime("20240925162727", "20060102150405")
	expectedControlResult := ExpectedControlResult{
		ID:             uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
		SampleCode:     "Sample1",
		Operator:       InOpenInterval,
		ExpectedValue:  "41",
		ExpectedValue2: strToPtr("50"),
		CreatedAt:      expectedControlResultCreatedAt,
		DeletedAt:      nil,
		CreatedBy:      uuid.UUID{},
		DeletedBy:      uuid.NullUUID{},
	}

	analysisResults := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(true, true, &expectedControlResult)

	mockManager := &mockManager{}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	results, err := analysisService.CreateAnalysisResultsBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResults,
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.Equal(t, false, results[0].Reagents[0].ControlResults[0].IsValid)
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, Final, results[0].Status)
}

func TestCreateAnalysisResultStatusAndControlResultWithOperatorInClosedIntervalValid(t *testing.T) {
	expectedControlResultCreatedAt, _ := utils.FormatTimeStringToBerlinTime("20240925162727", "20060102150405")
	expectedControlResult := ExpectedControlResult{
		ID:             uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
		SampleCode:     "Sample1",
		Operator:       InClosedInterval,
		ExpectedValue:  "39",
		ExpectedValue2: strToPtr("50"),
		CreatedAt:      expectedControlResultCreatedAt,
		DeletedAt:      nil,
		CreatedBy:      uuid.UUID{},
		DeletedBy:      uuid.NullUUID{},
	}

	analysisResults := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(true, true, &expectedControlResult)

	mockManager := &mockManager{}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	results, err := analysisService.CreateAnalysisResultsBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResults,
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsValid)
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, Final, results[0].Status)
}

func TestCreateAnalysisResultStatusAndControlResultWithOperatorInClosedIntervalInvalid(t *testing.T) {
	expectedControlResultCreatedAt, _ := utils.FormatTimeStringToBerlinTime("20240925162727", "20060102150405")
	expectedControlResult := ExpectedControlResult{
		ID:             uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
		SampleCode:     "Sample1",
		Operator:       InClosedInterval,
		ExpectedValue:  "40",
		ExpectedValue2: strToPtr("50"),
		CreatedAt:      expectedControlResultCreatedAt,
		DeletedAt:      nil,
		CreatedBy:      uuid.UUID{},
		DeletedBy:      uuid.NullUUID{},
	}

	analysisResults := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(true, true, &expectedControlResult)

	mockManager := &mockManager{}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	results, err := analysisService.CreateAnalysisResultsBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResults,
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.Equal(t, false, results[0].Reagents[0].ControlResults[0].IsValid)
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, Final, results[0].Status)
}

func TestCreateAnalysisResultStatusAndControlResultWithUnsupportedOperator(t *testing.T) {
	expectedControlResultCreatedAt, _ := utils.FormatTimeStringToBerlinTime("20240925162727", "20060102150405")
	expectedControlResult := ExpectedControlResult{
		ID:             uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
		SampleCode:     "Sample1",
		Operator:       NotExists,
		ExpectedValue:  "40",
		ExpectedValue2: nil,
		CreatedAt:      expectedControlResultCreatedAt,
		DeletedAt:      nil,
		CreatedBy:      uuid.UUID{},
		DeletedBy:      uuid.NullUUID{},
	}

	analysisResults := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(true, true, &expectedControlResult)

	mockManager := &mockManager{}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	results, err := analysisService.CreateAnalysisResultsBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResults,
	})
	assert.NotNil(t, err)
	assert.Equal(t, ErrUnsupportedExpectedControlResultFound, err)
	assert.Equal(t, 0, len(results))
}

func TestCreateControlResultBatchWithOnlyPreControlResults(t *testing.T) {
	controlResult, reagent := setupTestDataForStandaloneControlProcessing()
	standaloneControlResults := []StandaloneControlResult{{
		ControlResult: controlResult,
		Reagents:      []Reagent{reagent}},
	}

	mockManager := &mockManager{}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	results, analysisResultIds, err := analysisService.CreateControlResultBatch(context.TODO(), standaloneControlResults)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 0, len(analysisResultIds))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.NotEqual(t, uuid.Nil, results[0].ID)
	assert.Equal(t, true, results[0].IsValid)
	assert.Equal(t, true, results[0].IsComparedToExpectedResult)
}

func TestCreateControlResultBatchWithOnlyPostControlResults(t *testing.T) {
	controlResult, reagent := setupTestDataForStandaloneControlProcessing()

	standaloneControlResults := []StandaloneControlResult{{
		ControlResult: controlResult,
		Reagents:      []Reagent{reagent},
		ResultIDs: []uuid.UUID{uuid.MustParse("1e1a1c8b-16c4-47e4-b923-3bbe0c7360eb"),
			uuid.MustParse("4aea5a88-d01c-4eab-8c5e-9bdde98fedc9"),
			uuid.MustParse("be139961-3179-4706-bcd5-e461498a789e")}},
	}

	mockManager := &mockManager{}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	results, analysisResultIds, err := analysisService.CreateControlResultBatch(context.TODO(), standaloneControlResults)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 3, len(analysisResultIds))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.NotEqual(t, uuid.Nil, results[0].ID)
	assert.Equal(t, true, results[0].IsValid)
	assert.Equal(t, true, results[0].IsComparedToExpectedResult)
}

func TestCreateControlResultBatch(t *testing.T) {
	resultYieldedAt, _ := utils.FormatTimeStringToBerlinTime("20240927162727", "20060102150405")
	expectedControlResultCreatedAt, _ := utils.FormatTimeStringToBerlinTime("20240925162727", "20060102150405")
	controlInstrumentAnalyte := "TESTCONTROLANALYTE"

	expectedControlResult := ExpectedControlResult{
		ID:             uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
		SampleCode:     "Sample1",
		Operator:       Equals,
		ExpectedValue:  "40",
		ExpectedValue2: nil,
		CreatedAt:      expectedControlResultCreatedAt,
		DeletedAt:      nil,
		CreatedBy:      uuid.UUID{},
		DeletedBy:      uuid.NullUUID{},
	}

	analyteMappings := []AnalyteMapping{
		{
			ID:                       uuid.MustParse("c31edad9-586e-4add-bdd7-be37c28c3560"),
			InstrumentAnalyte:        "TESTANALYTE",
			ControlInstrumentAnalyte: &controlInstrumentAnalyte,
			AnalyteID:                uuid.MustParse("fc1948d2-4381-4049-a1d3-8b010b65a0cc"),
			ChannelMappings: []ChannelMapping{
				{
					InstrumentChannel: "TestInstrumentChannel",
					ChannelID:         uuid.MustParse("6ded3ef2-4f98-45bb-b0d5-3ff3bd294d8b"),
				},
			},
			ResultMappings: []ResultMapping{
				{
					Key:   "pos",
					Value: "pos",
					Index: 0,
				},
				{
					Key:   "neg",
					Value: "neg",
					Index: 1,
				},
			},
			ResultType:             "pein",
			ControlResultRequired:  true,
			ExpectedControlResults: []ExpectedControlResult{expectedControlResult},
		},
	}

	controlResult1 := ControlResult{
		SampleCode:     "Sample1",
		AnalyteMapping: analyteMappings[0],
		Result:         "35",
		ExpectedControlResultId: uuid.NullUUID{
			UUID:  uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
			Valid: true,
		},
		IsValid:                    false,
		IsComparedToExpectedResult: false,
		ExaminedAt:                 resultYieldedAt,
		InstrumentID:               uuid.UUID{},
		Warnings:                   nil,
		ChannelResults:             nil,
		ExtraValues:                nil,
	}

	reagent1 := Reagent{
		Manufacturer:   "Roche",
		SerialNumber:   "000000002",
		LotNo:          "000000003",
		Name:           "",
		Type:           Standard,
		CreatedAt:      time.Time{},
		ControlResults: []ControlResult{controlResult1},
	}

	controlResult, reagent := setupTestDataForStandaloneControlProcessing()

	standaloneControlResults := []StandaloneControlResult{{
		ControlResult: controlResult1,
		Reagents:      []Reagent{reagent1}}, {
		ControlResult: controlResult,
		Reagents:      []Reagent{reagent},
		ResultIDs: []uuid.UUID{uuid.MustParse("1e1a1c8b-16c4-47e4-b923-3bbe0c7360eb"),
			uuid.MustParse("4aea5a88-d01c-4eab-8c5e-9bdde98fedc9"),
			uuid.MustParse("be139961-3179-4706-bcd5-e461498a789e")}},
	}

	mockManager := &mockManager{}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	results, analysisResultIds, err := analysisService.CreateControlResultBatch(context.TODO(), standaloneControlResults)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(results))
	assert.Equal(t, 3, len(analysisResultIds))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.NotEqual(t, uuid.Nil, results[0].ID)
	assert.Equal(t, false, results[0].IsValid)
	assert.Equal(t, true, results[0].IsComparedToExpectedResult)
	assert.Equal(t, 1, len(results[1].AnalyteMapping.ExpectedControlResults))
	assert.NotEqual(t, uuid.Nil, results[1].ID)
	assert.Equal(t, true, results[1].IsValid)
	assert.Equal(t, true, results[1].IsComparedToExpectedResult)
}

func setupTestDataForAnalysisResultStatusAndControlResultValidCheck(addExpectedControlResult bool, useOnlyTheIncludedExpectedResult bool, result *ExpectedControlResult) []AnalysisResult {
	resultYieldedAt, _ := utils.FormatTimeStringToBerlinTime("20240927162727", "20060102150405")
	expectedControlResultCreatedAt, _ := utils.FormatTimeStringToBerlinTime("20240925162727", "20060102150405")
	validUntil, _ := utils.FormatTimeStringToBerlinTime("20240930162727", "20060102150405")
	controlInstrumentAnalyte := "TESTCONTROLANALYTE"

	expectedControlResults := make([]ExpectedControlResult, 0)

	if !useOnlyTheIncludedExpectedResult {
		expectedControlResult := ExpectedControlResult{
			ID:             uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
			SampleCode:     "Sample1",
			Operator:       Equals,
			ExpectedValue:  "40",
			ExpectedValue2: nil,
			CreatedAt:      expectedControlResultCreatedAt,
			DeletedAt:      nil,
			CreatedBy:      uuid.UUID{},
			DeletedBy:      uuid.NullUUID{},
		}
		expectedControlResults = append(expectedControlResults, expectedControlResult)
	}

	if result != nil {
		expectedControlResults = append(expectedControlResults, *result)
	}

	analyteMappings := []AnalyteMapping{
		{
			ID:                       uuid.MustParse("c31edad9-586e-4add-bdd7-be37c28c3560"),
			InstrumentAnalyte:        "TESTANALYTE",
			ControlInstrumentAnalyte: &controlInstrumentAnalyte,
			AnalyteID:                uuid.MustParse("fc1948d2-4381-4049-a1d3-8b010b65a0cc"),
			ChannelMappings: []ChannelMapping{
				{
					InstrumentChannel: "TestInstrumentChannel",
					ChannelID:         uuid.MustParse("6ded3ef2-4f98-45bb-b0d5-3ff3bd294d8b"),
				},
			},
			ResultMappings: []ResultMapping{
				{
					Key:   "pos",
					Value: "pos",
					Index: 0,
				},
				{
					Key:   "neg",
					Value: "neg",
					Index: 1,
				},
			},
			ResultType:            "pein",
			ControlResultRequired: true,
		},
	}
	if addExpectedControlResult {
		analyteMappings[0].ExpectedControlResults = expectedControlResults
	}

	instrument := Instrument{
		ID:              uuid.MustParse("abb539a3-286f-4c15-a7b7-2e9adf6eab74"),
		Name:            "TestInstrument",
		Type:            Analyzer,
		ProtocolID:      uuid.MustParse("abb539a3-286f-4c15-a7b7-2e9adf6eab91"),
		ProtocolName:    "Test Protocol",
		Enabled:         true,
		ConnectionMode:  TCPMixed,
		ResultMode:      Qualification,
		Status:          "ONLINE",
		FileEncoding:    "UTF8",
		Timezone:        "Europe/Budapest",
		Hostname:        "192.168.1.20",
		AnalyteMappings: analyteMappings,
	}

	controlResult := ControlResult{
		SampleCode:     "Sample1",
		AnalyteMapping: analyteMappings[0],
		Result:         "40",
		ExpectedControlResultId: uuid.NullUUID{
			UUID:  uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
			Valid: true,
		},
		IsValid:                    false,
		IsComparedToExpectedResult: false,
		ExaminedAt:                 resultYieldedAt,
		InstrumentID:               instrument.ID,
		Warnings:                   nil,
		ChannelResults:             nil,
		ExtraValues:                nil,
	}

	regent := Reagent{
		Manufacturer:   "Roche",
		SerialNumber:   "000000001",
		LotNo:          "000000002",
		Type:           Standard,
		Name:           "",
		CreatedAt:      time.Time{},
		ControlResults: []ControlResult{controlResult},
	}

	analysisResults := []AnalysisResult{
		{
			AnalysisRequest:          AnalysisRequest{},
			AnalyteMapping:           analyteMappings[0],
			Instrument:               instrument,
			SampleCode:               "",
			ResultRecordID:           uuid.MustParse("92a2ba34-d891-4a1b-89fb-e0c4d717f729"),
			BatchID:                  uuid.MustParse("88b87019-ddcc-4d4b-bc04-9e213680e0db"),
			Result:                   "",
			ResultMode:               Qualification,
			Status:                   Final,
			ResultYieldDateTime:      &resultYieldedAt,
			ValidUntil:               validUntil,
			Operator:                 "",
			TechnicalReleaseDateTime: nil,
			InstrumentRunID:          uuid.MustParse("c0dbcfb6-6a90-4ab6-bcab-0cfbec4abd06"),
			Edited:                   false,
			EditReason:               "",
			IsInvalid:                false,
			WarnFlag:                 false,
			Warnings:                 nil,
			ChannelResults:           nil,
			ExtraValues:              nil,
			Reagents:                 []Reagent{regent},
			ControlResults:           nil,
			Images:                   nil,
		},
	}

	return analysisResults
}

func setupTestDataForStandaloneControlProcessing() (ControlResult, Reagent) {
	resultYieldedAt, _ := utils.FormatTimeStringToBerlinTime("20240927162727", "20060102150405")
	expectedControlResultCreatedAt, _ := utils.FormatTimeStringToBerlinTime("20240925162727", "20060102150405")
	controlInstrumentAnalyte := "TESTCONTROLANALYTE"

	expectedControlResult := ExpectedControlResult{
		ID:             uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
		SampleCode:     "Sample1",
		Operator:       Equals,
		ExpectedValue:  "40",
		ExpectedValue2: nil,
		CreatedAt:      expectedControlResultCreatedAt,
		DeletedAt:      nil,
		CreatedBy:      uuid.UUID{},
		DeletedBy:      uuid.NullUUID{},
	}

	expectedControlResults := []ExpectedControlResult{expectedControlResult}

	analyteMappings := []AnalyteMapping{
		{
			ID:                       uuid.MustParse("c31edad9-586e-4add-bdd7-be37c28c3560"),
			InstrumentAnalyte:        "TESTANALYTE",
			ControlInstrumentAnalyte: &controlInstrumentAnalyte,
			AnalyteID:                uuid.MustParse("fc1948d2-4381-4049-a1d3-8b010b65a0cc"),
			ChannelMappings: []ChannelMapping{
				{
					InstrumentChannel: "TestInstrumentChannel",
					ChannelID:         uuid.MustParse("6ded3ef2-4f98-45bb-b0d5-3ff3bd294d8b"),
				},
			},
			ResultMappings: []ResultMapping{
				{
					Key:   "pos",
					Value: "pos",
					Index: 0,
				},
				{
					Key:   "neg",
					Value: "neg",
					Index: 1,
				},
			},
			ResultType:             "pein",
			ControlResultRequired:  true,
			ExpectedControlResults: expectedControlResults,
		},
	}

	controlResult := ControlResult{
		SampleCode:     "Sample1",
		AnalyteMapping: analyteMappings[0],
		Result:         "40",
		ExpectedControlResultId: uuid.NullUUID{
			UUID:  uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
			Valid: true,
		},
		IsValid:                    false,
		IsComparedToExpectedResult: false,
		ExaminedAt:                 resultYieldedAt,
		InstrumentID:               uuid.UUID{},
		Warnings:                   nil,
		ChannelResults:             nil,
		ExtraValues:                nil,
	}

	reagent := Reagent{
		Manufacturer:   "Roche",
		SerialNumber:   "000000001",
		LotNo:          "000000002",
		Type:           Standard,
		Name:           "",
		CreatedAt:      time.Time{},
		ControlResults: []ControlResult{controlResult},
	}

	return controlResult, reagent
}

type mockManager struct {
	ControlResultForProcessing        []MappedStandaloneControlResult
	AnalysisRequestsForProcessing     []AnalysisResult
	AnalysisRequestsSentForProcessing []AnalysisRequest
}

func (m *mockManager) SendControlResultForProcessing(controlResult MappedStandaloneControlResult) {
	m.ControlResultForProcessing = append(m.ControlResultForProcessing, controlResult)
}

func (m *mockManager) GetControlResultChan() chan MappedStandaloneControlResult {
	return nil
}

func (m *mockManager) EnqueueInstrument(id uuid.UUID, event instrumentEventType) {

}
func (m *mockManager) RegisterInstrumentQueueListener(listener InstrumentQueueListener, events ...instrumentEventType) {

}
func (m *mockManager) SetCallbackHandler(eventHandler SkeletonCallbackHandlerV1) {

}
func (m *mockManager) GetCallbackHandler() SkeletonCallbackHandlerV1 {
	return nil
}
func (m *mockManager) SendAnalysisRequestsForProcessing(analysisRequests []AnalysisRequest) {
	m.AnalysisRequestsSentForProcessing = append(m.AnalysisRequestsSentForProcessing, analysisRequests...)
}
func (m *mockManager) GetProcessableAnalysisRequestQueue() *utils.ConcurrentQueue[[]AnalysisRequest] {
	return nil
}
func (m *mockManager) SendResultForProcessing(analysisResult AnalysisResult) {
	m.AnalysisRequestsForProcessing = append(m.AnalysisRequestsForProcessing, analysisResult)
}
func (m *mockManager) GetResultChan() chan AnalysisResult {
	return nil
}

type extendedMockAnalysisRepo struct {
	analysisRepositoryMock
}

func (r *extendedMockAnalysisRepo) GetUnprocessedControlResultIDs(ctx context.Context) ([]uuid.UUID, error) {
	return nil, nil
}

func (r *extendedMockAnalysisRepo) GetUnprocessedAnalysisResultIDsByControlResultIDs(ctx context.Context, controlResultIDs []uuid.UUID) (map[uuid.UUID]map[uuid.UUID]uuid.UUID, error) {
	return nil, nil
}

func (r *extendedMockAnalysisRepo) GetUnprocessedReagentIDsByControlResultIDs(ctx context.Context, controlResultIDs []uuid.UUID) (map[uuid.UUID][]uuid.UUID, error) {
	return nil, nil
}

func (r *extendedMockAnalysisRepo) GetReagentsByIDs(ctx context.Context, reagentIDs []uuid.UUID) (map[uuid.UUID]Reagent, error) {
	return nil, nil
}

func (r *extendedMockAnalysisRepo) GetControlResultsByIDs(ctx context.Context, controlResultIDs []uuid.UUID) (map[uuid.UUID]ControlResult, error) {
	return nil, nil
}

func (r *extendedMockAnalysisRepo) UpdateCerberusQueueItemStatus(ctx context.Context, queueItem CerberusQueueItem) error {
	return nil
}

func (r *extendedMockAnalysisRepo) GetControlResultQueueItems(ctx context.Context) ([]CerberusQueueItem, error) {
	return []CerberusQueueItem{}, nil
}

func (r *extendedMockAnalysisRepo) CreateControlResultQueueItem(ctx context.Context, controlResults []StandaloneControlResult) (uuid.UUID, error) {
	return uuid.New(), nil
}

func (r *extendedMockAnalysisRepo) CreateReagents(ctx context.Context, reagents []Reagent) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, len(reagents))
	for i := range reagents {
		ids[i] = uuid.New()
	}
	return ids, nil
}

func (r *extendedMockAnalysisRepo) CreateControlResultBatch(ctx context.Context, controlResults []ControlResult) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, len(controlResults))
	for i := range controlResults {
		ids[i] = uuid.New()
	}
	return ids, nil
}

func (r *extendedMockAnalysisRepo) CreateReagentControlResultRelations(ctx context.Context, relationDAOs []reagentControlResultRelationDAO) error {
	return nil
}

func (r *extendedMockAnalysisRepo) CreateAnalysisResultControlResultRelations(ctx context.Context, relationDAOs []analysisResultControlResultRelationDAO) error {
	return nil
}

func (r *extendedMockAnalysisRepo) GetCerberusIDForAnalysisResults(ctx context.Context, analysisResultIDs []uuid.UUID) (map[uuid.UUID]uuid.UUID, error) {
	return map[uuid.UUID]uuid.UUID{}, nil
}

func (r *extendedMockAnalysisRepo) SaveCerberusIDForAnalysisResult(ctx context.Context, analysisResultID uuid.UUID, cerberusID uuid.UUID) error {
	return nil
}

func (r *extendedMockAnalysisRepo) SaveCerberusIDForControlResult(ctx context.Context, controlResultID uuid.UUID, cerberusID uuid.UUID) error {
	return nil
}

func (r *extendedMockAnalysisRepo) SaveCerberusIDForReagent(ctx context.Context, reagentID uuid.UUID, cerberusID uuid.UUID) error {
	return nil
}

func (r *extendedMockAnalysisRepo) GetAnalysisResultIdsSinceLastControlByReagent(ctx context.Context, reagent Reagent, examinedAt time.Time) ([]uuid.UUID, error) {
	return nil, nil
}

func (r *extendedMockAnalysisRepo) GetLatestControlResultIdByReagent(ctx context.Context, reagent Reagent, resultYieldTime *time.Time) (ControlResult, error) {
	return ControlResult{}, nil
}

func (r *extendedMockAnalysisRepo) MarkReagentControlResultRelationsAsProcessed(ctx context.Context, controlResultID uuid.UUID, reagentIDs []uuid.UUID) error {
	return nil
}

func (r *extendedMockAnalysisRepo) MarkAnalysisResultControlResultRelationsAsProcessed(ctx context.Context, controlResultID uuid.UUID, analysisResultIDs []uuid.UUID) error {
	return nil
}

func (r *extendedMockAnalysisRepo) CreateAnalysisRequestsBatch(ctx context.Context, analysisRequests []AnalysisRequest) ([]uuid.UUID, []uuid.UUID, error) {
	workItemIDs := make([]uuid.UUID, 0)
	for i := range analysisRequests {
		workItemIDs = append(workItemIDs, analysisRequests[i].WorkItemID)
	}
	if len(workItemIDs) < 3 {
		return nil, workItemIDs, nil
	}
	return nil, workItemIDs[:len(workItemIDs)-1], nil
}

func (r *extendedMockAnalysisRepo) WithTransaction(tx db.DbConnector) AnalysisRepository {
	return r
}
