package skeleton

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/DRK-Blutspende-BaWueHe/skeleton/config"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/consolelog/repository"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/consolelog/service"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/db"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/migrator"
	"github.com/MicahParks/keyfunc"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

func TestSkeletonStart(t *testing.T) {
	sqlConn, err := sqlx.Connect("pgx", "host=localhost port=5551 user=postgres password=postgres dbname=postgres sslmode=disable")
	assert.Nil(t, err)

	skeletonApi, err := New(sqlConn, "skeleton")
	if err != nil {
		return
	}

	go func() {
		time.Sleep(5 * time.Second)
		runtime.Goexit()
	}()

	err = skeletonApi.Start()
	if err != nil {
		t.Fail()
	}
}

func TestSubmitAnalysisResultWithoutRequests(t *testing.T) {
	sqlConn, _ := sqlx.Connect("pgx", "host=localhost port=5551 user=postgres password=postgres dbname=postgres sslmode=disable")

	schemaName := "testSubmitAnalysisResultsWithoutRequests"
	_, _ = sqlConn.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp" schema public;`)
	_, _ = sqlConn.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE;`, schemaName))
	_, _ = sqlConn.Exec(fmt.Sprintf(`CREATE SCHEMA %s;`, schemaName))

	config := config.Configuration{
		APIPort:                          5000,
		Authorization:                    false,
		PermittedOrigin:                  "*",
		ApplicationName:                  "Instrument API Test",
		TCPListenerPort:                  5401,
		InstrumentTransferRetryDelayInMs: 100,
	}
	dbConn := db.CreateDbConnector(sqlConn)

	analysisRepository := NewAnalysisRepository(dbConn, schemaName)
	instrumentRepository := NewInstrumentRepository(dbConn, schemaName)
	consoleLogRepository := repository.NewConsoleLogRepository(500)

	authManager := authManagerMock{
		getJWKSFunc: func() (*keyfunc.JWKS, error) {
			return &keyfunc.JWKS{}, nil
		},
		getClientCredentialFunc: func() (string, error) {
			return "authtoken", nil
		},
	}
	skeletonManager := NewSkeletonManager()
	skeletonManager.SetCallbackHandler(&skeletonCallbackHandlerV1Mock{
		handleAnalysisRequestsFunc: func(request []AnalysisRequest) error {
			return nil
		},
	})
	cerberusClientMock := &cerberusClientMock{
		registerInstrumentFunc: func(instrument Instrument) error {
			return nil
		},
		sendAnalysisResultBatchFunc: func(analysisResults []AnalysisResultTO) (AnalysisResultBatchResponse, error) {
			result1ID := uuid.MustParse("dcb320af-e842-46af-a4ee-878db18c95a3")
			result2ID := uuid.MustParse("147bff1c-55e0-4318-8ce5-23e0e12d073e")
			return AnalysisResultBatchResponse{
				AnalysisResultBatchItemInfoList: []AnalysisResultBatchItemInfo{
					{
						AnalysisResult:           &analysisResultsWithoutAnalysisRequestsTest_analysisResultTOs[0],
						CerberusAnalysisResultID: &result1ID,
						ErrorMessage:             "",
					},
					{
						AnalysisResult:           &analysisResultsWithoutAnalysisRequestsTest_analysisResultTOs[1],
						CerberusAnalysisResultID: &result2ID,
						ErrorMessage:             "",
					},
				},
				ErrorMessage:   "",
				HTTPStatusCode: 202,
				RawResponse:    "",
			}, nil
		},
	}
	deaClientMock := &deaClientMock{}

	analysisService := NewAnalysisService(analysisRepository, skeletonManager)
	instrumentService := NewInstrumentService(&config, instrumentRepository, skeletonManager, NewInstrumentCache(), cerberusClientMock)
	consoleLogService := service.NewConsoleLogService(consoleLogRepository, nil)

	responseRecorder := &httptest.ResponseRecorder{}
	ginContext, ginEngine := gin.CreateTestContext(responseRecorder)

	api := newAPI(ginEngine, &config, &authManager, analysisService, instrumentService, consoleLogService, nil)

	skeletonInstance, _ := NewSkeleton(sqlConn, schemaName, migrator.NewSkeletonMigrator(), api, analysisRepository, analysisService, instrumentService, consoleLogService, skeletonManager, cerberusClientMock, deaClientMock)

	_, _ = sqlConn.Exec(fmt.Sprintf(`INSERT INTO %s.sk_supported_protocols (id, "name", description)
		VALUES ('abb539a3-286f-4c15-a7b7-2e9adf6eab91', 'IH-1000 v5.2', 'IHCOM');`, schemaName))
	_, _ = sqlConn.Exec(fmt.Sprintf(`INSERT INTO %s.sk_instruments(id, protocol_id, "name", hostname, client_port, enabled, connection_mode, running_mode, captureresults, capturediagnostics, replytoquery, status, sent_to_cerberus, timezone, file_encoding) 
		VALUES ('93f36696-5ff0-45a9-87eb-ca5c064c5890', 'abb539a3-286f-4c15-a7b7-2e9adf6eab91', 'TestInstrument', '192.168.1.13', NULL, TRUE, 'TCP_SERVER_ONLY', 'TEST', TRUE, TRUE, TRUE, 'ONLINE', TRUE, 'Europe/Budapest', 'UTF8');`, schemaName))
	_, _ = sqlConn.Exec(fmt.Sprintf(`INSERT INTO %s.sk_analyte_mappings(id, instrument_id, instrument_analyte, analyte_id, result_type)
		VALUES ('8facbaeb-368f-482a-9169-4b128632f9e0', '93f36696-5ff0-45a9-87eb-ca5c064c5890', 'TESTANALYTE', '51bfea41-1b7e-48f7-8b35-46d930216de7', 'pein');`, schemaName))
	_, _ = sqlConn.Exec(fmt.Sprintf(`INSERT INTO %s.sk_channel_mappings(id, instrument_channel, channel_id, analyte_mapping_id) 
		VALUES ('eb780147-a519-4e88-9a5f-961ce531f219', 'TestInstrumentChannel', '9fe9b1f9-e1fd-4669-873b-c2446d5d6b6f', '8facbaeb-368f-482a-9169-4b128632f9e0');`, schemaName))
	_, _ = sqlConn.Exec(fmt.Sprintf(`INSERT INTO %s.sk_result_mappings(id, analyte_mapping_id, "key", "value", "index") 
		VALUES ('0e49a9a7-8ef0-4ef3-a1e1-6277398fcc08', :analyte_mapping_id, 'pos', 'pos', 0),
		       ('329080eb-2dca-4a05-9730-24444cc3b487', :analyte_mapping_id, 'neg', 'neg', 1);`, schemaName))
	_, _ = sqlConn.Exec(fmt.Sprintf(`INSERT INTO %s.sk_request_mappings(id, code, instrument_id) 
		VALUES ('9e83ad17-40bc-44b2-b6c9-50a9f559387b', 'Test1', '93f36696-5ff0-45a9-87eb-ca5c064c5890');`, schemaName))

	go func() {
		_ = skeletonInstance.Start()
	}()

	for _, analysisResult := range analysisResultsWithoutAnalysisRequestsTest_analysisResults {
		err := skeletonInstance.SubmitAnalysisResult(context.TODO(), analysisResult)
		assert.Nil(t, err)
	}

	time.Sleep(5 * time.Second)

	var resultCount int
	err := sqlConn.QueryRowx(fmt.Sprintf(`SELECT COUNT(*) FROM %s.sk_analysis_results;`, schemaName)).Scan(&resultCount)
	assert.Nil(t, err)
	assert.Equal(t, 2, resultCount)

	ginContext.Request, _ = http.NewRequest(http.MethodPost, "/v1/analysis-requests/batch", bytes.NewBuffer([]byte(analysisResultsWithoutAnalysisRequestsTest_AnalysisRequests)))

	ginEngine.ServeHTTP(responseRecorder, ginContext.Request)

	if responseRecorder.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, responseRecorder.Code)
	}

	time.Sleep(70 * time.Second)
	assert.Equal(t, 2, len(cerberusClientMock.AnalysisResults))
	assert.Equal(t, uuid.MustParse("660d1095-c8f6-4899-946e-935bfddfaa69"), cerberusClientMock.AnalysisResults[0].WorkingItemID)
	assert.Equal(t, analysisResultsWithoutAnalysisRequestsTest_instrument.ID, cerberusClientMock.AnalysisResults[0].InstrumentID)
	assert.Equal(t, analysisResultsWithoutAnalysisRequestsTest_analysisResults[0].Result, cerberusClientMock.AnalysisResults[0].Result)
	assert.Equal(t, uuid.MustParse("55abb455-5c35-464a-aa9b-26ea5690c6ca"), cerberusClientMock.AnalysisResults[1].WorkingItemID)
	assert.Equal(t, analysisResultsWithoutAnalysisRequestsTest_instrument.ID, cerberusClientMock.AnalysisResults[1].InstrumentID)
	assert.Equal(t, analysisResultsWithoutAnalysisRequestsTest_analysisResults[1].Result, cerberusClientMock.AnalysisResults[1].Result)
	assert.NotEqual(t, AnalysisResultBatchItemInfo{}, cerberusClientMock.BatchResponse)
}

// Todo - Complete the test
func TestSubmitAnalysisResultWithRequests(t *testing.T) {
	sqlConn, _ := sqlx.Connect("pgx", "host=localhost port=5551 user=postgres password=postgres dbname=postgres sslmode=disable")

	schemaName := "testSubmitAnalysisResultsWithRequests"
	_, _ = sqlConn.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp" schema public;`)
	_, _ = sqlConn.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE;`, schemaName))
	_, _ = sqlConn.Exec(fmt.Sprintf(`CREATE SCHEMA %s;`, schemaName))

	config := config.Configuration{
		APIPort:                          5000,
		Authorization:                    false,
		PermittedOrigin:                  "*",
		ApplicationName:                  "Instrument API Test",
		TCPListenerPort:                  5401,
		InstrumentTransferRetryDelayInMs: 100,
	}
	dbConn := db.CreateDbConnector(sqlConn)

	analysisRepository := NewAnalysisRepository(dbConn, schemaName)
	instrumentRepository := NewInstrumentRepository(dbConn, schemaName)
	consoleLogRepository := repository.NewConsoleLogRepository(500)

	authManager := authManagerMock{
		getJWKSFunc: func() (*keyfunc.JWKS, error) {
			return &keyfunc.JWKS{}, nil
		},
		getClientCredentialFunc: func() (string, error) {
			return "authtoken", nil
		},
	}
	skeletonManager := NewSkeletonManager()
	skeletonManager.SetCallbackHandler(&skeletonCallbackHandlerV1Mock{
		handleAnalysisRequestsFunc: func(request []AnalysisRequest) error {
			return nil
		},
	})
	cerberusClientMock := &cerberusClientMock{
		registerInstrumentFunc: func(instrument Instrument) error {
			return nil
		},
	}

	deaClientMock := &deaClientMock{}

	analysisService := NewAnalysisService(analysisRepository, skeletonManager)
	instrumentService := NewInstrumentService(&config, instrumentRepository, skeletonManager, NewInstrumentCache(), cerberusClientMock)
	consoleLogService := service.NewConsoleLogService(consoleLogRepository, nil)

	api := NewAPI(&config, &authManager, analysisService, instrumentService, consoleLogService, nil)

	skeletonInstance, _ := NewSkeleton(sqlConn, schemaName, migrator.NewSkeletonMigrator(), api, analysisRepository, analysisService, instrumentService, consoleLogService, skeletonManager, cerberusClientMock, deaClientMock)

	_, _ = sqlConn.Exec(fmt.Sprintf(`INSERT INTO %s.sk_supported_protocols (id, "name", description) VALUES ('9bec3063-435d-490f-bec0-88a6633ef4c2', 'IH-1000 v5.2', 'IHCOM');`, schemaName))

	go func() {
		_ = skeletonInstance.Start()
	}()

	analysisRequests := []AnalysisRequest{
		{
			ID:             uuid.New(),
			WorkItemID:     uuid.New(),
			AnalyteID:      uuid.New(),
			SampleCode:     "",
			MaterialID:     uuid.New(),
			LaboratoryID:   uuid.New(),
			ValidUntilTime: time.Now().UTC().Add(14 * 24 * time.Hour),
			CreatedAt:      time.Now().UTC(),
			SubjectInfo:    nil,
		},
	}
	_, err := analysisService.CreateAnalysisRequests(context.TODO(), analysisRequests)
	assert.Nil(t, err)

	analyteMappings := []AnalyteMapping{
		{
			ID:                uuid.MustParse("8facbaeb-368f-482a-9169-4b128632f9e0"),
			InstrumentAnalyte: "TESTANALYTE",
			AnalyteID:         uuid.MustParse("51bfea41-1b7e-48f7-8b35-46d930216de7"),
			ChannelMappings: []ChannelMapping{
				{
					ID:                uuid.MustParse("eb780147-a519-4e88-9a5f-961ce531f219"),
					InstrumentChannel: "TestInstrumentChannel",
					ChannelID:         uuid.MustParse("9fe9b1f9-e1fd-4669-873b-c2446d5d6b6f"),
				},
			},
			ResultMappings: []ResultMapping{
				{
					ID:    uuid.MustParse("0e49a9a7-8ef0-4ef3-a1e1-6277398fcc08"),
					Key:   "pos",
					Value: "pos",
					Index: 0,
				},
				{
					ID:    uuid.MustParse("329080eb-2dca-4a05-9730-24444cc3b487"),
					Key:   "neg",
					Value: "neg",
					Index: 1,
				},
			},
			ResultType: "pein",
		},
	}

	instrumentID := uuid.MustParse("93f36696-5ff0-45a9-87eb-ca5c064c5890")
	instrument := Instrument{
		ID:                 instrumentID,
		Name:               "TestInstrument",
		ProtocolID:         uuid.MustParse("9bec3063-435d-490f-bec0-88a6633ef4c2"),
		ProtocolName:       "IH-1000 v5.2",
		Enabled:            true,
		ConnectionMode:     "TCP_SERVER_ONLY",
		ResultMode:         "TEST",
		CaptureResults:     true,
		CaptureDiagnostics: true,
		ReplyToQuery:       true,
		Status:             "ONLINE",
		FileEncoding:       "UTF8",
		Timezone:           "Europe/Budapest",
		Hostname:           "192.168.1.35",
		ClientPort:         nil,
		AnalyteMappings:    analyteMappings,
		RequestMappings: []RequestMapping{
			{
				ID:   uuid.MustParse("9e83ad17-40bc-44b2-b6c9-50a9f559387b"),
				Code: "Test1",
				AnalyteIDs: []uuid.UUID{
					uuid.MustParse("7444d776-3ff7-40b6-9b3c-2e0c58337528"),
					uuid.MustParse("c9cc51ec-2b63-45df-b0a8-98f325027f8f"),
				},
			},
		},
		CreatedAt:  time.Now().UTC(),
		ModifiedAt: nil,
		DeletedAt:  nil,
	}

	batchID := uuid.MustParse("ddd34c4d-62f9-4621-bb16-efad459a9bfe")
	analysisResults := []AnalysisResult{
		{
			AnalysisRequest:          AnalysisRequest{},
			AnalyteMapping:           analyteMappings[0],
			Instrument:               instrument,
			SampleCode:               "testSampleCode",
			ResultRecordID:           uuid.MustParse("2f369489-77d3-464e-87e2-edbeffa62ae7"),
			BatchID:                  batchID,
			Result:                   "pos",
			ResultMode:               "TEST",
			Status:                   "PRE",
			ResultYieldDateTime:      nil,
			ValidUntil:               time.Now().Add(1 * time.Minute),
			Operator:                 "TestOperator",
			TechnicalReleaseDateTime: nil,
			InstrumentRunID:          uuid.Nil,
			Edited:                   false,
			EditReason:               "",
			WarnFlag:                 false,
			Warnings:                 []string{"test warning"},
			ChannelResults:           []ChannelResult{},
			ExtraValues:              []ExtraValue{},
			ReagentInfos:             []ReagentInfo{},
			Images:                   []Image{},
		},
		{
			AnalysisRequest:          AnalysisRequest{},
			AnalyteMapping:           analyteMappings[0],
			Instrument:               instrument,
			SampleCode:               "testSampleCode2",
			ResultRecordID:           uuid.MustParse("43a7b261-3e1d-4065-935a-ac15841f13e4"),
			BatchID:                  batchID,
			Result:                   "pos",
			ResultMode:               "TEST",
			Status:                   "PRE",
			ResultYieldDateTime:      nil,
			ValidUntil:               time.Now().Add(2 * time.Minute),
			Operator:                 "TestOperator",
			TechnicalReleaseDateTime: nil,
			InstrumentRunID:          uuid.Nil,
			Edited:                   false,
			EditReason:               "",
			WarnFlag:                 false,
			Warnings:                 []string{},
			ChannelResults:           []ChannelResult{},
			ExtraValues:              []ExtraValue{},
			ReagentInfos:             []ReagentInfo{},
			Images:                   []Image{},
		},
	}

	for _, analysisResult := range analysisResults {
		err := skeletonInstance.SubmitAnalysisResult(context.TODO(), analysisResult)
		assert.Nil(t, err)
	}

	time.Sleep(4 * time.Second)
	assert.Equal(t, 0, len(cerberusClientMock.AnalysisResults))
}

func TestRegisterProtocol(t *testing.T) {
	//TODO
	// register protocol
	// register instrument with mentioned protocol (call endpoint ???) -> expect to pass
	// try register instrument with random protocolID -> expect to fail
}

type skeletonCallbackHandlerV1Mock struct {
	handleAnalysisRequestsFunc func(request []AnalysisRequest) error
	getManufacturerTestFunc    func(instrumentId uuid.UUID, protocolId uuid.UUID) ([]SupportedManufacturerTests, error)
	getEncodingList            func(protocolId uuid.UUID) ([]string, error)
	revokeAnalysisRequests     func(request []AnalysisRequest)
}

func (m *skeletonCallbackHandlerV1Mock) HandleAnalysisRequests(request []AnalysisRequest) error {
	if m.handleAnalysisRequestsFunc == nil {
		return errors.New("not implemented")
	}
	return m.handleAnalysisRequestsFunc(request)
}

func (m *skeletonCallbackHandlerV1Mock) GetManufacturerTestList(instrumentId uuid.UUID, protocolId uuid.UUID) ([]SupportedManufacturerTests, error) {
	if m.getManufacturerTestFunc == nil {
		return nil, errors.New("not implemented")
	}
	return m.getManufacturerTestFunc(instrumentId, protocolId)
}

func (m *skeletonCallbackHandlerV1Mock) GetEncodingList(protocolId uuid.UUID) ([]string, error) {
	if m.getEncodingList == nil {
		return nil, errors.New("not implemented")
	}
	return m.getEncodingList(protocolId)
}

func (m *skeletonCallbackHandlerV1Mock) RevokeAnalysisRequests(request []AnalysisRequest) {
	if m.revokeAnalysisRequests == nil {
		return
	}
	m.revokeAnalysisRequests(request)
}

type authManagerMock struct {
	getJWKSFunc                    func() (*keyfunc.JWKS, error)
	getClientCredentialFunc        func() (string, error)
	invalidateClientCredentialFunc func()
}

func (m *authManagerMock) GetJWKS() (*keyfunc.JWKS, error) {
	if m.getJWKSFunc == nil {
		return nil, errors.New("not implemented")
	}
	return m.getJWKSFunc()
}

func (m *authManagerMock) GetClientCredential() (string, error) {
	if m.getClientCredentialFunc == nil {
		return "", errors.New("not implemented")
	}
	return m.getClientCredentialFunc()
}

func (m *authManagerMock) InvalidateClientCredential() {
	if m.invalidateClientCredentialFunc == nil {
		return
	}
	m.invalidateClientCredentialFunc()
}

type deaClientMock struct {
}

func (m *deaClientMock) UploadImage(fileData []byte, name string) (uuid.UUID, error) {
	return uuid.New(), nil
}

type cerberusClientMock struct {
	registerInstrumentFunc      func(instrument Instrument) error
	sendAnalysisResultBatchFunc func(analysisResults []AnalysisResultTO) (AnalysisResultBatchResponse, error)

	AnalysisResults []AnalysisResultTO
	BatchResponse   AnalysisResultBatchResponse
}

func (m *cerberusClientMock) RegisterInstrument(instrument Instrument) error {
	if m.registerInstrumentFunc == nil {
		return errors.New("not implemented")
	}
	return m.registerInstrumentFunc(instrument)
}

func (m *cerberusClientMock) SendAnalysisResultBatch(analysisResults []AnalysisResultTO) (AnalysisResultBatchResponse, error) {
	m.AnalysisResults = append(m.AnalysisResults, analysisResults...)
	if m.sendAnalysisResultBatchFunc == nil {
		return AnalysisResultBatchResponse{}, errors.New("not implemented")
	}
	response, err := m.sendAnalysisResultBatchFunc(analysisResults)
	m.BatchResponse = response
	return response, err
}

var analysisResultsWithoutAnalysisRequestsTest_analysisResults = []AnalysisResult{
	{
		AnalysisRequest:          AnalysisRequest{},
		AnalyteMapping:           analysisResultsWithoutAnalysisRequestsTest_analyteMappings[0],
		Instrument:               analysisResultsWithoutAnalysisRequestsTest_instrument,
		SampleCode:               "TestSampleCode1",
		ResultRecordID:           uuid.MustParse("2f369489-77d3-464e-87e2-edbeffa62ae7"),
		BatchID:                  uuid.MustParse("ddd34c4d-62f9-4621-bb16-efad459a9bfe"),
		Result:                   "pos",
		ResultMode:               "TEST",
		Status:                   "PRE",
		ResultYieldDateTime:      nil,
		ValidUntil:               time.Now().Add(1 * time.Minute),
		Operator:                 "TestOperator",
		TechnicalReleaseDateTime: nil,
		InstrumentRunID:          uuid.Nil,
		Edited:                   false,
		EditReason:               "",
		WarnFlag:                 false,
		Warnings:                 []string{"test warning"},
		ChannelResults:           []ChannelResult{},
		ExtraValues:              []ExtraValue{},
		ReagentInfos:             []ReagentInfo{},
		Images:                   []Image{},
	},
	{
		AnalysisRequest:          AnalysisRequest{},
		AnalyteMapping:           analysisResultsWithoutAnalysisRequestsTest_analyteMappings[0],
		Instrument:               analysisResultsWithoutAnalysisRequestsTest_instrument,
		SampleCode:               "TestSampleCode2",
		ResultRecordID:           uuid.MustParse("43a7b261-3e1d-4065-935a-ac15841f13e4"),
		BatchID:                  uuid.MustParse("ddd34c4d-62f9-4621-bb16-efad459a9bfe"),
		Result:                   "pos",
		ResultMode:               "TEST",
		Status:                   "PRE",
		ResultYieldDateTime:      nil,
		ValidUntil:               time.Now().Add(2 * time.Minute),
		Operator:                 "TestOperator",
		TechnicalReleaseDateTime: nil,
		InstrumentRunID:          uuid.Nil,
		Edited:                   false,
		EditReason:               "",
		WarnFlag:                 false,
		Warnings:                 []string{},
		ChannelResults:           []ChannelResult{},
		ExtraValues:              []ExtraValue{},
		ReagentInfos:             []ReagentInfo{},
		Images:                   []Image{},
	},
}

var analysisResultsWithoutAnalysisRequestsTest_analysisResultTOs = []AnalysisResultTO{
	{
		InstrumentID:             analysisResultsWithoutAnalysisRequestsTest_instrument.ID,
		Result:                   "pos",
		Mode:                     "TEST",
		Status:                   "PRE",
		ResultYieldDateTime:      nil,
		ValidUntil:               time.Now().Add(1 * time.Minute),
		Operator:                 "TestOperator",
		TechnicalReleaseDateTime: nil,
		InstrumentRunID:          uuid.Nil,
		Edited:                   false,
		EditReason:               "",
		WarnFlag:                 false,
		Warnings:                 []string{"test warning"},
		ChannelResults:           []ChannelResultTO{},
		ExtraValues:              []ExtraValueTO{},
		ReagentInfos:             []ReagentInfoTO{},
		Images:                   []ImageTO{},
	},
	{
		InstrumentID:             analysisResultsWithoutAnalysisRequestsTest_instrument.ID,
		Result:                   "pos",
		Mode:                     "TEST",
		Status:                   "PRE",
		ResultYieldDateTime:      nil,
		ValidUntil:               time.Now().Add(2 * time.Minute),
		Operator:                 "TestOperator",
		TechnicalReleaseDateTime: nil,
		InstrumentRunID:          uuid.Nil,
		Edited:                   false,
		EditReason:               "",
		WarnFlag:                 false,
		Warnings:                 []string{},
		ChannelResults:           []ChannelResultTO{},
		ExtraValues:              []ExtraValueTO{},
		ReagentInfos:             []ReagentInfoTO{},
		Images:                   []ImageTO{},
	},
}

var analysisResultsWithoutAnalysisRequestsTest_instrument = Instrument{
	ID:                 uuid.MustParse("93f36696-5ff0-45a9-87eb-ca5c064c5890"),
	Name:               "TestInstrument",
	ProtocolID:         uuid.MustParse("abb539a3-286f-4c15-a7b7-2e9adf6eab91"),
	ProtocolName:       "IH-1000 v5.2",
	Enabled:            true,
	ConnectionMode:     "TCP_SERVER_ONLY",
	ResultMode:         "SIMULATION",
	CaptureResults:     true,
	CaptureDiagnostics: true,
	ReplyToQuery:       true,
	Status:             "ONLINE",
	FileEncoding:       "UTF8",
	Timezone:           "Europe/Budapest",
	Hostname:           "192.168.1.13",
	ClientPort:         nil,
	AnalyteMappings:    analysisResultsWithoutAnalysisRequestsTest_analyteMappings,
	RequestMappings: []RequestMapping{
		{
			ID:   uuid.MustParse("9e83ad17-40bc-44b2-b6c9-50a9f559387b"),
			Code: "Test1",
			AnalyteIDs: []uuid.UUID{
				uuid.MustParse("7444d776-3ff7-40b6-9b3c-2e0c58337528"),
				uuid.MustParse("c9cc51ec-2b63-45df-b0a8-98f325027f8f"),
			},
		},
	},
	CreatedAt:  time.Now().UTC(),
	ModifiedAt: nil,
	DeletedAt:  nil,
}

var analysisResultsWithoutAnalysisRequestsTest_analyteMappings = []AnalyteMapping{
	{
		ID:                uuid.MustParse("8facbaeb-368f-482a-9169-4b128632f9e0"),
		InstrumentAnalyte: "TESTANALYTE",
		AnalyteID:         uuid.MustParse("51bfea41-1b7e-48f7-8b35-46d930216de7"),
		ChannelMappings: []ChannelMapping{
			{
				ID:                uuid.MustParse("eb780147-a519-4e88-9a5f-961ce531f219"),
				InstrumentChannel: "TestInstrumentChannel",
				ChannelID:         uuid.MustParse("9fe9b1f9-e1fd-4669-873b-c2446d5d6b6f"),
			},
		},
		ResultMappings: []ResultMapping{
			{
				ID:    uuid.MustParse("0e49a9a7-8ef0-4ef3-a1e1-6277398fcc08"),
				Key:   "pos",
				Value: "pos",
				Index: 0,
			},
			{
				ID:    uuid.MustParse("329080eb-2dca-4a05-9730-24444cc3b487"),
				Key:   "neg",
				Value: "neg",
				Index: 1,
			},
		},
		ResultType: "pein",
	},
}

var analysisResultsWithoutAnalysisRequestsTest_AnalysisRequests = `[
  {
    "workItemId": "660d1095-c8f6-4899-946e-935bfddfaa69",
    "analyteId": "51bfea41-1b7e-48f7-8b35-46d930216de7",
    "sampleCode": "TestSampleCode1",
    "materialId": "50820d7e-3f5b-4452-aa4b-bebfc3c15002",
    "laboratoryId": "3072973a-25d7-43d0-840f-d5a3de8e3aa5",
    "validUntilTime": "2099-01-10T11:30:59.000Z",
    "subject": null
  },
  {
    "workItemId": "55abb455-5c35-464a-aa9b-26ea5690c6ca",
    "analyteId": "51bfea41-1b7e-48f7-8b35-46d930216de7",
    "sampleCode": "TestSampleCode2",
    "materialId": "50820d7e-3f5b-4452-aa4b-bebfc3c15002",
    "laboratoryId": "3072973a-25d7-43d0-840f-d5a3de8e3aa5",
    "validUntilTime": "2099-01-10T11:30:59.000Z",
    "subject": null
  }
]`
