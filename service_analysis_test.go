package skeleton

import (
	"context"
	"github.com/blutspende/skeleton/db"
	"github.com/blutspende/skeleton/utils"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func formatTimeStringToBerlinTime(timeString, format string) (time.Time, error) {
	location, err := time.LoadLocation(string("Europe/Berlin"))
	if err != nil {
		log.Error().Err(err).Msg("Can not load Location")
		return time.Time{}, err
	}
	return time.ParseInLocation(format, timeString, location)
}

func TestCreateAnalysisRequests(t *testing.T) {
	mockManager := &mockManager{}
	analysisRequests := []AnalysisRequest{
		{WorkItemID: uuid.MustParse("6cdc3aa0-a024-4c51-8d24-8aa12d489f41")},
		{WorkItemID: uuid.MustParse("92a2ba34-d891-4a1b-89fb-e0c4d717f729")},
	}
	analysisService := NewAnalysisService(&analysisRepositoryMock{}, nil, nil, mockManager)
	err := analysisService.CreateAnalysisRequests(context.TODO(), analysisRequests)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(mockManager.AnalysisRequestsSentForProcessing))
	assert.Equal(t, uuid.MustParse("6cdc3aa0-a024-4c51-8d24-8aa12d489f41"), mockManager.AnalysisRequestsSentForProcessing[0].WorkItemID)
	assert.Equal(t, uuid.MustParse("92a2ba34-d891-4a1b-89fb-e0c4d717f729"), mockManager.AnalysisRequestsSentForProcessing[1].WorkItemID)
}

func TestCreateAnalysisRequestDuplicates(t *testing.T) {
	mockManager := &mockManager{}
	analysisRequests := []AnalysisRequest{
		{ID: uuid.MustParse("5e095a05-ede9-4fa4-aa6d-05d8514aa3b6"), WorkItemID: uuid.MustParse("6cdc3aa0-a024-4c51-8d24-8aa12d489f41")},
		{ID: uuid.MustParse("07831faf-69f9-4c32-b994-603bd774a516"), WorkItemID: uuid.MustParse("6cdc3aa0-a024-4c51-8d24-8aa12d489f41")},
		{ID: uuid.MustParse("66c96c9b-cf3a-4916-adbe-3ae0db005391"), WorkItemID: uuid.MustParse("88b87019-ddcc-4d4b-bc04-9e213680e0db")},
		{ID: uuid.MustParse("83a80ca2-54e9-4833-8cd1-e8bb5202b33e"), WorkItemID: uuid.MustParse("c0dbcfb6-6a90-4ab6-bcab-0cfbec4abd06")},
	}
	analysisService := NewAnalysisService(&analysisRepositoryMock{}, nil, nil, mockManager)
	err := analysisService.CreateAnalysisRequests(context.TODO(), analysisRequests)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(mockManager.AnalysisRequestsSentForProcessing))
	assert.Equal(t, uuid.MustParse("6cdc3aa0-a024-4c51-8d24-8aa12d489f41"), mockManager.AnalysisRequestsSentForProcessing[0].WorkItemID)
	assert.Equal(t, uuid.MustParse("5e095a05-ede9-4fa4-aa6d-05d8514aa3b6"), mockManager.AnalysisRequestsSentForProcessing[0].ID)

	assert.Equal(t, uuid.MustParse("88b87019-ddcc-4d4b-bc04-9e213680e0db"), mockManager.AnalysisRequestsSentForProcessing[1].WorkItemID)
	assert.Equal(t, uuid.MustParse("66c96c9b-cf3a-4916-adbe-3ae0db005391"), mockManager.AnalysisRequestsSentForProcessing[1].ID)

	assert.Equal(t, uuid.MustParse("c0dbcfb6-6a90-4ab6-bcab-0cfbec4abd06"), mockManager.AnalysisRequestsSentForProcessing[2].WorkItemID)
	assert.Equal(t, uuid.MustParse("83a80ca2-54e9-4833-8cd1-e8bb5202b33e"), mockManager.AnalysisRequestsSentForProcessing[2].ID)
}

func TestCreateAnalysisResultStatusAndControlResultValid(t *testing.T) {
	analysisResult := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(true, nil)

	result, err := setAnalysisResultStatusBasedOnControlResults(analysisResult, nil, true)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(result.AnalyteMapping.ExpectedControlResults))
	assert.True(t, result.Reagents[0].ControlResults[0].IsValid)
	assert.True(t, result.Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, Final, result.Status)
}

func TestCreateAnalysisResultStatusAndControlResultNotAllControlAvailable(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
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

	analysisResult := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(true, &expectedControlResult)

	result, err := setAnalysisResultStatusBasedOnControlResults(analysisResult, nil, true)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(result.AnalyteMapping.ExpectedControlResults))
	assert.Equal(t, 1, len(result.Reagents[0].ControlResults))
	assert.True(t, result.Reagents[0].ControlResults[0].IsValid)
	assert.True(t, result.Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	//Expected 2 sample codes, only got control for one
	assert.Equal(t, Preliminary, result.Status)
}

func TestCreateAnalysisResultStatusAndControlResultIncludingCommonControlResults(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
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

	analysisResult := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(true, &expectedControlResult)
	commonControlResult := setupControlResultForValidation(analysisResult.AnalyteMapping, analysisResult.Instrument.ID)
	commonControlResult.SampleCode = "Sample2"

	result, err := setAnalysisResultStatusBasedOnControlResults(analysisResult, []ControlResult{commonControlResult}, true)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(result.AnalyteMapping.ExpectedControlResults))
	//Expected 2 sample codes, the second one is passed in as a common
	assert.Equal(t, Final, result.Status)
}

func TestCreateAnalysisResultStatusAndControlResultNotValid(t *testing.T) {
	analysisResult := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(true, nil)
	analysisResult.Reagents[0].ControlResults[0].Result = "37.5"

	result, err := setAnalysisResultStatusBasedOnControlResults(analysisResult, nil, true)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(result.AnalyteMapping.ExpectedControlResults))
	assert.False(t, result.Reagents[0].ControlResults[0].IsValid)
	assert.True(t, result.Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, Final, result.Status)
}

func TestCreateAnalysisResultStatusAndControlResultNotMatchingSampleCodes(t *testing.T) {
	analysisResult := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(true, nil)
	analysisResult.Reagents[0].ControlResults[0].SampleCode = "sample2"
	analysisResult.Reagents[0].ControlResults[0].AnalyteMapping = AnalyteMapping{}

	result, err := setAnalysisResultStatusBasedOnControlResults(analysisResult, nil, true)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(result.AnalyteMapping.ExpectedControlResults))
	assert.False(t, result.Reagents[0].ControlResults[0].IsValid)
	assert.False(t, result.Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, Preliminary, result.Status)
}

func TestCreateAnalysisResultStatusAndControlResultNotRequiringExpectedControlResult(t *testing.T) {
	analysisResult := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(false, nil)
	analysisResult.AnalyteMapping.ControlResultRequired = false

	result, err := setAnalysisResultStatusBasedOnControlResults(analysisResult, nil, true)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(result.AnalyteMapping.ExpectedControlResults))
	assert.False(t, result.Reagents[0].ControlResults[0].IsValid)
	assert.False(t, result.Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, Final, result.Status)
}

func TestCreateAnalysisResultStatusAndControlResultWithoutExpectedControlResult(t *testing.T) {
	analysisResult := setupTestDataForAnalysisResultStatusAndControlResultValidCheck(false, nil)

	result, err := setAnalysisResultStatusBasedOnControlResults(analysisResult, nil, true)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(result.AnalyteMapping.ExpectedControlResults))
	assert.False(t, result.Reagents[0].ControlResults[0].IsValid)
	assert.False(t, result.Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, Preliminary, result.Status)
}

func TestCalculateControlResultIsValidAndExpectedControlResultIdWhereControlIsValid(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
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

	analyteMapping := setupAnalyteMappingForControlValidation()
	analyteMapping.ExpectedControlResults = []ExpectedControlResult{expectedControlResult}

	controlResult := setupControlResultForValidation(analyteMapping, uuid.New())

	result, err := setControlResultIsValidAndExpectedControlResultId(controlResult)
	assert.Nil(t, err)
	assert.True(t, result.IsValid)
	assert.True(t, result.IsComparedToExpectedResult)
	assert.Equal(t, expectedControlResult.ID, result.ExpectedControlResultId.UUID)
}

func TestCalculateControlResultIsValidAndExpectedControlResultIdWhereControlIsNotValid(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
	expectedControlResult := ExpectedControlResult{
		ID:             uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
		SampleCode:     "Sample1",
		Operator:       Equals,
		ExpectedValue:  "25",
		ExpectedValue2: nil,
		CreatedAt:      expectedControlResultCreatedAt,
		DeletedAt:      nil,
		CreatedBy:      uuid.UUID{},
		DeletedBy:      uuid.NullUUID{},
	}

	analyteMapping := setupAnalyteMappingForControlValidation()
	analyteMapping.ExpectedControlResults = []ExpectedControlResult{expectedControlResult}

	controlResult := setupControlResultForValidation(analyteMapping, uuid.New())

	result, err := setControlResultIsValidAndExpectedControlResultId(controlResult)
	assert.Nil(t, err)
	assert.False(t, result.IsValid)
	assert.True(t, result.IsComparedToExpectedResult)
	assert.Equal(t, expectedControlResult.ID, result.ExpectedControlResultId.UUID)
}

func TestCalculateControlResultIsValidAndExpectedControlResultIdWhereErrorOnValidatingResult(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
	expectedControlResult := ExpectedControlResult{
		ID:             uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
		SampleCode:     "Sample1",
		Operator:       NotExists,
		ExpectedValue:  "25",
		ExpectedValue2: nil,
		CreatedAt:      expectedControlResultCreatedAt,
		DeletedAt:      nil,
		CreatedBy:      uuid.UUID{},
		DeletedBy:      uuid.NullUUID{},
	}

	analyteMapping := setupAnalyteMappingForControlValidation()
	analyteMapping.ExpectedControlResults = []ExpectedControlResult{expectedControlResult}

	controlResult := setupControlResultForValidation(analyteMapping, uuid.New())

	result, err := setControlResultIsValidAndExpectedControlResultId(controlResult)
	assert.NotNil(t, err)
	assert.Equal(t, ErrUnsupportedExpectedControlResultFound, err)
	assert.False(t, result.IsValid)
	assert.False(t, result.IsComparedToExpectedResult)
	assert.False(t, result.ExpectedControlResultId.Valid)
}

func TestCalculateControlResultIsValidAndExpectedControlResultIdWhereResultNotValidated(t *testing.T) {
	analyteMapping := setupAnalyteMappingForControlValidation()

	controlResult := setupControlResultForValidation(analyteMapping, uuid.New())

	result, err := setControlResultIsValidAndExpectedControlResultId(controlResult)
	assert.Nil(t, err)
	assert.False(t, result.IsValid)
	assert.False(t, result.IsComparedToExpectedResult)
	assert.False(t, result.ExpectedControlResultId.Valid)
}

func TestCalculateControlResultIsValidWithOperatorEqualsValid(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
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

	controlResult := setupControlResultForValidation(AnalyteMapping{}, uuid.New())

	result, err := calculateControlResultIsValid(controlResult.Result, expectedControlResult)
	assert.Nil(t, err)
	assert.True(t, result)
}

func TestCalculateControlResultIsValidWithOperatorEqualsInvalid(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
	expectedControlResult := ExpectedControlResult{
		ID:             uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
		SampleCode:     "Sample1",
		Operator:       Equals,
		ExpectedValue:  "25",
		ExpectedValue2: nil,
		CreatedAt:      expectedControlResultCreatedAt,
		DeletedAt:      nil,
		CreatedBy:      uuid.UUID{},
		DeletedBy:      uuid.NullUUID{},
	}

	controlResult := setupControlResultForValidation(AnalyteMapping{}, uuid.New())

	result, err := calculateControlResultIsValid(controlResult.Result, expectedControlResult)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestCalculateControlResultIsValidWithOperatorNotEqualsValid(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
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

	controlResult := setupControlResultForValidation(AnalyteMapping{}, uuid.New())

	result, err := calculateControlResultIsValid(controlResult.Result, expectedControlResult)
	assert.Nil(t, err)
	assert.True(t, result)
}

func TestCalculateControlResultIsValidWithOperatorNotEqualsInvalid(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
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

	controlResult := setupControlResultForValidation(AnalyteMapping{}, uuid.New())

	result, err := calculateControlResultIsValid(controlResult.Result, expectedControlResult)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestCalculateControlResultIsValidWithOperatorGreaterOrEqualValid(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
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

	controlResult := setupControlResultForValidation(AnalyteMapping{}, uuid.New())

	result, err := calculateControlResultIsValid(controlResult.Result, expectedControlResult)
	assert.Nil(t, err)
	assert.True(t, result)
}

func TestCalculateControlResultIsValidWithOperatorGreaterOrEqualInvalid(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
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

	controlResult := setupControlResultForValidation(AnalyteMapping{}, uuid.New())

	result, err := calculateControlResultIsValid(controlResult.Result, expectedControlResult)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestCalculateControlResultIsValidWithOperatorGreaterValid(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
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

	controlResult := setupControlResultForValidation(AnalyteMapping{}, uuid.New())

	result, err := calculateControlResultIsValid(controlResult.Result, expectedControlResult)
	assert.Nil(t, err)
	assert.True(t, result)
}

func TestCalculateControlResultIsValidWithOperatorGreaterInvalid(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
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

	controlResult := setupControlResultForValidation(AnalyteMapping{}, uuid.New())

	result, err := calculateControlResultIsValid(controlResult.Result, expectedControlResult)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestCalculateControlResultIsValidWithOperatorLessOrEqualValid(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
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

	controlResult := setupControlResultForValidation(AnalyteMapping{}, uuid.New())

	result, err := calculateControlResultIsValid(controlResult.Result, expectedControlResult)
	assert.Nil(t, err)
	assert.True(t, result)
}

func TestCalculateControlResultIsValidWithOperatorLessOrEqualInvalid(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
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

	controlResult := setupControlResultForValidation(AnalyteMapping{}, uuid.New())

	result, err := calculateControlResultIsValid(controlResult.Result, expectedControlResult)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestCalculateControlResultIsValidWithOperatorLessValid(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
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

	controlResult := setupControlResultForValidation(AnalyteMapping{}, uuid.New())

	result, err := calculateControlResultIsValid(controlResult.Result, expectedControlResult)
	assert.Nil(t, err)
	assert.True(t, result)
}

func TestCalculateControlResultIsValidWithOperatorLessInvalid(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
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

	controlResult := setupControlResultForValidation(AnalyteMapping{}, uuid.New())

	result, err := calculateControlResultIsValid(controlResult.Result, expectedControlResult)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestCalculateControlResultIsValidWithOperatorInOpenIntervalValid(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
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

	controlResult := setupControlResultForValidation(AnalyteMapping{}, uuid.New())

	result, err := calculateControlResultIsValid(controlResult.Result, expectedControlResult)
	assert.Nil(t, err)
	assert.True(t, result)
}

func TestCalculateControlResultIsValidWithOperatorInOpenIntervalInvalid(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
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

	controlResult := setupControlResultForValidation(AnalyteMapping{}, uuid.New())

	result, err := calculateControlResultIsValid(controlResult.Result, expectedControlResult)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestCalculateControlResultIsValidWithOperatorInClosedIntervalValid(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
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

	controlResult := setupControlResultForValidation(AnalyteMapping{}, uuid.New())

	result, err := calculateControlResultIsValid(controlResult.Result, expectedControlResult)
	assert.Nil(t, err)
	assert.True(t, result)
}

func TestCalculateControlResultIsValidWithOperatorInClosedIntervalInvalid(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
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

	controlResult := setupControlResultForValidation(AnalyteMapping{}, uuid.New())

	result, err := calculateControlResultIsValid(controlResult.Result, expectedControlResult)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestCalculateControlResultIsValidWithUnsupportedOperator(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
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

	controlResult := setupControlResultForValidation(AnalyteMapping{}, uuid.New())

	result, err := calculateControlResultIsValid(controlResult.Result, expectedControlResult)
	assert.NotNil(t, err)
	assert.Equal(t, ErrUnsupportedExpectedControlResultFound, err)
	assert.False(t, result)
}

func TestCreateAnalysisResultControlRelationsWithControlAttachedToAnalysisResult(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
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

	analysisResults := setupTestDataForAnalysisResultReagentAndControlRelationCheck(true, true, &expectedControlResult)
	analysisResults[0].Reagents[0].ID = uuid.MustParse("e45a3bb9-a968-403b-9cb3-5895b5c89cde")
	analysisResults[0].Reagents[1].ID = uuid.MustParse("e45a3bb9-a968-403b-9cb3-5895b5c89cdf")
	analysisResults[0].ControlResults[0].ID = uuid.MustParse("cbb539a3-286f-4c15-a7b7-2e9adf6eab74")

	mockManager := &mockManager{}
	extendedMockAnalysisRepo := &extendedMockAnalysisRepo{}
	analysisService := NewAnalysisService(extendedMockAnalysisRepo, nil, nil, mockManager)
	results, err := analysisService.CreateAnalysisResultsBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResults,
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.Equal(t, 2, len(results[0].Reagents))
	assert.Equal(t, 1, len(results[0].Reagents[0].ControlResults))
	assert.Equal(t, 1, len(results[0].Reagents[1].ControlResults))
	assert.Equal(t, false, results[0].Reagents[0].ControlResults[0].IsValid)
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, false, results[0].Reagents[1].ControlResults[0].IsValid)
	assert.Equal(t, true, results[0].Reagents[1].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, 0, len(results[0].ControlResults))

	assert.Equal(t, 2, len(extendedMockAnalysisRepo.analysisResultReagentRelationDAOs))
	assert.Equal(t, 1, len(extendedMockAnalysisRepo.analysisResultControlResultRelationDAOs))
	assert.Equal(t, 2, len(extendedMockAnalysisRepo.reagentControlResultRelationDAOs))
	assert.Equal(t, Final, results[0].Status)
}

func TestCreateMultipleAnalysisResultControlRelationsWithControlAttachedToAnalysisResultSet(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
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

	analysisResults := setupTestDataForAnalysisResultReagentAndControlRelationCheck(true, true, &expectedControlResult)
	analysisResults[0].SampleCode = "Sample1"
	controlResults := analysisResults[0].ControlResults
	analysisResults[0].ControlResults = nil
	analysisResults = append(analysisResults, setupTestDataForAnalysisResultReagentAndControlRelationCheck(true, true, &expectedControlResult)...)
	analysisResults[1].ControlResults = nil

	mockManager := &mockManager{}
	extendedMockAnalysisRepo := &extendedMockAnalysisRepo{}
	analysisService := NewAnalysisService(extendedMockAnalysisRepo, nil, nil, mockManager)
	results, err := analysisService.CreateAnalysisResultsBatch(context.TODO(), AnalysisResultSet{
		Results:        analysisResults,
		ControlResults: controlResults,
	})
	assert.Nil(t, err)
	assert.Equal(t, 2, len(results))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.Equal(t, 2, len(results[0].Reagents))
	assert.Equal(t, 1, len(results[0].Reagents[0].ControlResults))
	assert.Equal(t, 1, len(results[0].Reagents[1].ControlResults))
	assert.Equal(t, false, results[0].Reagents[0].ControlResults[0].IsValid)
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, false, results[0].Reagents[1].ControlResults[0].IsValid)
	assert.Equal(t, true, results[0].Reagents[1].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, 0, len(results[0].ControlResults))
	assert.Equal(t, 1, len(results[1].AnalyteMapping.ExpectedControlResults))
	assert.Equal(t, 2, len(results[1].Reagents))
	assert.Equal(t, 1, len(results[1].Reagents[0].ControlResults))
	assert.Equal(t, 1, len(results[1].Reagents[1].ControlResults))
	assert.Equal(t, false, results[1].Reagents[0].ControlResults[0].IsValid)
	assert.Equal(t, true, results[1].Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, false, results[1].Reagents[1].ControlResults[0].IsValid)
	assert.Equal(t, true, results[1].Reagents[1].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, 0, len(results[1].ControlResults))

	assert.Equal(t, 4, len(extendedMockAnalysisRepo.analysisResultReagentRelationDAOs))
	assert.Equal(t, 2, len(extendedMockAnalysisRepo.analysisResultControlResultRelationDAOs))
	assert.Equal(t, 2, len(extendedMockAnalysisRepo.reagentControlResultRelationDAOs))
	assert.Equal(t, Final, results[0].Status)
	assert.Equal(t, Final, results[1].Status)
}

func TestCreateAnalysisResultReagentControlRelations(t *testing.T) {
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
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

	analysisResults := setupTestDataForAnalysisResultReagentAndControlRelationCheck(true, true, &expectedControlResult)

	mockManager := &mockManager{}
	extendedMockAnalysisRepo := &extendedMockAnalysisRepo{}
	analysisService := NewAnalysisService(extendedMockAnalysisRepo, nil, nil, mockManager)
	results, err := analysisService.CreateAnalysisResultsBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResults,
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.Equal(t, 2, len(results[0].Reagents))
	assert.Equal(t, 1, len(results[0].Reagents[0].ControlResults))
	assert.Equal(t, 1, len(results[0].Reagents[1].ControlResults))
	assert.Equal(t, false, results[0].Reagents[0].ControlResults[0].IsValid)
	assert.Equal(t, true, results[0].Reagents[0].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, false, results[0].Reagents[1].ControlResults[0].IsValid)
	assert.Equal(t, true, results[0].Reagents[1].ControlResults[0].IsComparedToExpectedResult)
	assert.Equal(t, 0, len(results[0].ControlResults))

	assert.Equal(t, 2, len(extendedMockAnalysisRepo.analysisResultReagentRelationDAOs))
	assert.Equal(t, 1, len(extendedMockAnalysisRepo.analysisResultControlResultRelationDAOs))
	assert.Equal(t, 2, len(extendedMockAnalysisRepo.reagentControlResultRelationDAOs))
	assert.Equal(t, Final, results[0].Status)
}

func TestCreateControlResultBatchWithOnlyPreControlResults(t *testing.T) {
	controlResult, reagent := setupTestDataForStandaloneControlProcessing()
	standaloneControlResults := []StandaloneControlResult{{
		ControlResult: controlResult,
		Reagents:      []Reagent{reagent}},
	}

	mockManager := &mockManager{}
	extendedMockAnalysisRepo := &extendedMockAnalysisRepo{}
	analysisService := NewAnalysisService(extendedMockAnalysisRepo, nil, nil, mockManager)
	results, analysisResultIds, err := analysisService.CreateControlResultBatch(context.TODO(), standaloneControlResults)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 0, len(analysisResultIds))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.NotEqual(t, uuid.Nil, results[0].ID)
	assert.Equal(t, true, results[0].IsValid)
	assert.Equal(t, true, results[0].IsComparedToExpectedResult)

	assert.Equal(t, 0, len(extendedMockAnalysisRepo.analysisResultReagentRelationDAOs))
	assert.Equal(t, 0, len(extendedMockAnalysisRepo.analysisResultControlResultRelationDAOs))
	assert.Equal(t, 1, len(extendedMockAnalysisRepo.reagentControlResultRelationDAOs))
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
	extendedMockAnalysisRepo := &extendedMockAnalysisRepo{}
	analysisService := NewAnalysisService(extendedMockAnalysisRepo, nil, nil, mockManager)
	results, analysisResultIds, err := analysisService.CreateControlResultBatch(context.TODO(), standaloneControlResults)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 3, len(analysisResultIds))
	assert.Equal(t, 1, len(results[0].AnalyteMapping.ExpectedControlResults))
	assert.NotEqual(t, uuid.Nil, results[0].ID)
	assert.Equal(t, true, results[0].IsValid)
	assert.Equal(t, true, results[0].IsComparedToExpectedResult)

	assert.Equal(t, 0, len(extendedMockAnalysisRepo.analysisResultReagentRelationDAOs))
	assert.Equal(t, 3, len(extendedMockAnalysisRepo.analysisResultControlResultRelationDAOs))
	assert.Equal(t, 1, len(extendedMockAnalysisRepo.reagentControlResultRelationDAOs))
}

func TestCreateControlResultBatch(t *testing.T) {
	resultYieldedAt, _ := formatTimeStringToBerlinTime("20240927162727", "20060102150405")
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
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
	extendedMockAnalysisRepo := &extendedMockAnalysisRepo{}
	analysisService := NewAnalysisService(extendedMockAnalysisRepo, nil, nil, mockManager)
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

	assert.Equal(t, 0, len(extendedMockAnalysisRepo.analysisResultReagentRelationDAOs))
	assert.Equal(t, 3, len(extendedMockAnalysisRepo.analysisResultControlResultRelationDAOs))
	assert.Equal(t, 2, len(extendedMockAnalysisRepo.reagentControlResultRelationDAOs))
}

func setupTestDataForAnalysisResultStatusAndControlResultValidCheck(addExpectedControlResult bool, result *ExpectedControlResult) AnalysisResult {
	resultYieldedAt, _ := formatTimeStringToBerlinTime("20240927162727", "20060102150405")
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
	validUntil, _ := formatTimeStringToBerlinTime("20240930162727", "20060102150405")

	expectedControlResults := make([]ExpectedControlResult, 0)

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

	if result != nil {
		expectedControlResults = append(expectedControlResults, *result)
	}

	analyteMappings := []AnalyteMapping{setupAnalyteMappingForControlValidation()}
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

	controlResult := setupControlResultForValidation(analyteMappings[0], instrument.ID)
	controlResult.ExpectedControlResultId = uuid.NullUUID{
		UUID:  uuid.MustParse(expectedControlResults[0].ID.String()),
		Valid: true,
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

	analysisResult := AnalysisResult{
		AnalysisRequest:          AnalysisRequest{},
		AnalyteMapping:           analyteMappings[0],
		Instrument:               instrument,
		SampleCode:               "",
		DEARawMessageID:          uuid.MustParse("92a2ba34-d891-4a1b-89fb-e0c4d717f729"),
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
	}

	return analysisResult
}

func setupAnalyteMappingForControlValidation() AnalyteMapping {
	controlInstrumentAnalyte := "TESTCONTROLANALYTE"
	return AnalyteMapping{
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
	}
}

func setupControlResultForValidation(analyteMapping AnalyteMapping, instrumentId uuid.UUID) ControlResult {
	return ControlResult{
		SampleCode:                 "Sample1",
		AnalyteMapping:             analyteMapping,
		Result:                     "40",
		IsValid:                    false,
		IsComparedToExpectedResult: false,
		ExaminedAt:                 time.Now(),
		InstrumentID:               instrumentId,
		Warnings:                   nil,
		ChannelResults:             nil,
		ExtraValues:                nil,
	}
}

func setupTestDataForAnalysisResultReagentAndControlRelationCheck(addExpectedControlResult bool, useOnlyTheIncludedExpectedResult bool, result *ExpectedControlResult) []AnalysisResult {
	resultYieldedAt, _ := formatTimeStringToBerlinTime("20240927162727", "20060102150405")
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
	validUntil, _ := formatTimeStringToBerlinTime("20240930162727", "20060102150405")
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
		ControlResults: nil,
	}

	regent2 := Reagent{
		Manufacturer:   "Roche",
		SerialNumber:   "000000002",
		LotNo:          "000000003",
		Type:           Standard,
		Name:           "",
		CreatedAt:      time.Time{},
		ControlResults: nil,
	}

	analysisResults := []AnalysisResult{
		{
			AnalysisRequest:          AnalysisRequest{},
			AnalyteMapping:           analyteMappings[0],
			Instrument:               instrument,
			SampleCode:               "",
			DEARawMessageID:          uuid.MustParse("92a2ba34-d891-4a1b-89fb-e0c4d717f729"),
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
			Reagents:                 []Reagent{regent, regent2},
			ControlResults:           []ControlResult{controlResult},
			Images:                   nil,
		},
	}

	return analysisResults
}

func setupTestDataForStandaloneControlProcessing() (ControlResult, Reagent) {
	resultYieldedAt, _ := formatTimeStringToBerlinTime("20240927162727", "20060102150405")
	expectedControlResultCreatedAt, _ := formatTimeStringToBerlinTime("20240925162727", "20060102150405")
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
	AnalysisResultsForProcessing      []AnalysisResult
	AnalysisRequestsSentForProcessing []AnalysisRequest
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
	m.AnalysisResultsForProcessing = append(m.AnalysisResultsForProcessing, analysisResult)
}
func (m *mockManager) GetResultChan() chan AnalysisResult {
	return nil
}

func (m *mockManager) SendAnalyteMappingsToValidateControlResults(analyteMappingIds []uuid.UUID) {}
func (m *mockManager) GetControlValidationChan() chan []uuid.UUID {
	return nil
}

func (m *mockManager) SendControlResultIdsToAnalysisResultStatusRecalculation(controlResultIds []uuid.UUID) {
}
func (m *mockManager) GetAnalysisResultStatusRecalculationChan() chan []uuid.UUID {
	return nil
}

type extendedMockAnalysisRepo struct {
	analysisRepositoryMock
	analysisResultControlResultRelationDAOs []analysisResultControlResultRelationDAO
	analysisResultReagentRelationDAOs       []analysisResultReagentRelationDAO
	reagentControlResultRelationDAOs        []reagentControlResultRelationDAO
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

func (r *extendedMockAnalysisRepo) CreateReagents(ctx context.Context, reagents []Reagent) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, len(reagents))
	for i := range reagents {
		ids[i] = uuid.New()
	}
	return ids, nil
}

func (r *extendedMockAnalysisRepo) CreateControlResultBatch(ctx context.Context, controlResults []ControlResult) ([]ControlResult, error) {
	for i := range controlResults {
		controlResults[i].ID = uuid.New()
	}
	return controlResults, nil
}

func (r *extendedMockAnalysisRepo) UpdateControlResultBatch(ctx context.Context, controlResults []ControlResult) error {
	return nil
}

func (r *extendedMockAnalysisRepo) CreateReagentControlResultRelations(ctx context.Context, relationDAOs []reagentControlResultRelationDAO) error {
	r.reagentControlResultRelationDAOs = relationDAOs
	return nil
}

func (r *extendedMockAnalysisRepo) CreateAnalysisResultControlResultRelations(ctx context.Context, relationDAOs []analysisResultControlResultRelationDAO) error {
	r.analysisResultControlResultRelationDAOs = relationDAOs
	return nil
}

func (r *extendedMockAnalysisRepo) CreateAnalysisResultReagentRelations(ctx context.Context, relationDAOs []analysisResultReagentRelationDAO) error {
	r.analysisResultReagentRelationDAOs = relationDAOs
	return nil
}

func (r *extendedMockAnalysisRepo) SaveCerberusIDForAnalysisResult(ctx context.Context, analysisResultID uuid.UUID, cerberusID uuid.UUID) error {
	return nil
}

func (r *extendedMockAnalysisRepo) GetAnalysisResultIdsSinceLastControlByReagent(ctx context.Context, reagent Reagent, examinedAt time.Time, analyteMappingId uuid.UUID, instrumentId uuid.UUID) ([]uuid.UUID, error) {
	return nil, nil
}

func (r *extendedMockAnalysisRepo) GetLatestControlResultsByReagent(ctx context.Context, reagent Reagent, resultYieldTime *time.Time, analyteMappingId uuid.UUID, instrumentId uuid.UUID) ([]ControlResult, error) {
	return nil, nil
}

func (r *extendedMockAnalysisRepo) GetControlResultsToValidate(ctx context.Context, analyteMappingIds []uuid.UUID) ([]ControlResult, error) {
	return nil, nil
}

func (r *extendedMockAnalysisRepo) MarkReagentControlResultRelationsAsProcessed(ctx context.Context, controlResultID uuid.UUID, reagentIDs []uuid.UUID) error {
	return nil
}

func (r *extendedMockAnalysisRepo) MarkAnalysisResultControlResultRelationsAsProcessed(ctx context.Context, controlResultID uuid.UUID, analysisResultIDs []uuid.UUID) error {
	return nil
}

func (r *extendedMockAnalysisRepo) WithTransaction(tx db.DbConnection) AnalysisRepository {
	return r
}
