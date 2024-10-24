package skeleton

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/MicahParks/keyfunc"
	"github.com/blutspende/skeleton/config"
	"github.com/blutspende/skeleton/consolelog/repository"
	"github.com/blutspende/skeleton/consolelog/service"
	"github.com/blutspende/skeleton/db"
	"github.com/blutspende/skeleton/migrator"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"

	timeout "github.com/vearne/gin-timeout"
)

const serviceName = "testInstrumentDriver"

func TestSkeletonStart(t *testing.T) {
	sqlConn, err := sqlx.Connect("pgx", "host=localhost port=5551 user=postgres password=postgres dbname=postgres sslmode=disable")
	assert.Nil(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	skeletonApi, err := New(ctx, serviceName, []string{}, sqlConn, "skeleton")
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

func TestSubmitAnalysisRequestsParallel(t *testing.T) {
	sqlConn, _ := sqlx.Connect("pgx", "host=localhost port=5551 user=postgres password=postgres dbname=postgres sslmode=disable")

	schemaName := "testSubmitAnalysisRequestsParallel"
	_, _ = sqlConn.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp" schema public;`)
	_, _ = sqlConn.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE;`, schemaName))
	_, _ = sqlConn.Exec(fmt.Sprintf(`CREATE SCHEMA %s;`, schemaName))

	configuration := config.Configuration{
		APIPort:                          5678,
		Authorization:                    false,
		PermittedOrigin:                  "*",
		ApplicationName:                  "Submit Analysis Request Parallel Test",
		TCPListenerPort:                  5401,
		InstrumentTransferRetryDelayInMs: 100,
		ResultTransferFlushTimeout:       5,
		ImageRetrySeconds:                60,
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

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	skeletonManager := NewSkeletonManager(ctx)
	skeletonManager.SetCallbackHandler(&skeletonCallbackHandlerV1Mock{
		handleAnalysisRequestsFunc: func(request []AnalysisRequest) error {
			time.Sleep(2 * time.Second)
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

	analysisService := NewAnalysisService(analysisRepository, deaClientMock, cerberusClientMock, skeletonManager)
	conditionRepository := NewConditionRepository(dbConn, schemaName)
	conditionService := NewConditionService(conditionRepository)
	sortingRuleRepository := NewSortingRuleRepository(dbConn, schemaName)
	sortingRuleService := NewSortingRuleService(analysisRepository, conditionService, sortingRuleRepository)
	instrumentService := NewInstrumentService(&configuration, sortingRuleService, instrumentRepository, skeletonManager, NewInstrumentCache(), cerberusClientMock)
	consoleLogService := service.NewConsoleLogService(consoleLogRepository, nil)

	ginEngine := gin.New()

	ginEngine.Use(timeout.Timeout(timeout.WithTimeout(5*time.Second), timeout.WithErrorHttpCode(http.StatusRequestTimeout)))

	api := newAPI(ginEngine, &configuration, &authManager, analysisService, instrumentService, consoleLogService, nil)

	skeletonInstance, _ := NewSkeleton(ctx, serviceName, []string{}, sqlConn, schemaName, migrator.NewSkeletonMigrator(), api, analysisRepository, analysisService, instrumentService, consoleLogService, sortingRuleService, skeletonManager, cerberusClientMock, deaClientMock, configuration)

	go func() {
		_ = skeletonInstance.Start()
	}()

	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			responseRecorder := &httptest.ResponseRecorder{}
			ginContext := gin.CreateTestContextOnly(responseRecorder, ginEngine)

			ginContext.Request, _ = http.NewRequest(http.MethodPost, "/v1/analysis-requests/batch", bytes.NewBuffer([]byte(generateAnalysisRequestsJson(500))))
			ginEngine.ServeHTTP(responseRecorder, ginContext.Request)

			if responseRecorder.Code != http.StatusOK {
				t.Errorf("Expected status %d, got %d", http.StatusOK, responseRecorder.Code)
			}
		}()
	}

	wg.Wait()
}

func TestSubmitAnalysisResultWithoutRequests(t *testing.T) {
	sqlConn, _ := sqlx.Connect("pgx", "host=localhost port=5551 user=postgres password=postgres dbname=postgres sslmode=disable")

	schemaName := "testSubmitAnalysisResultsWithoutRequests"
	_, _ = sqlConn.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp" schema public;`)
	_, _ = sqlConn.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE;`, schemaName))
	_, _ = sqlConn.Exec(fmt.Sprintf(`CREATE SCHEMA %s;`, schemaName))

	configuration := config.Configuration{
		APIPort:                          5000,
		Authorization:                    false,
		PermittedOrigin:                  "*",
		ApplicationName:                  "Instrument API Test",
		TCPListenerPort:                  5401,
		InstrumentTransferRetryDelayInMs: 100,
		ResultTransferFlushTimeout:       5,
		ImageRetrySeconds:                60,
		AnalysisRequestWorkerPoolSize:    1,
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

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	skeletonManager := NewSkeletonManager(ctx)
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

	analysisService := NewAnalysisService(analysisRepository, deaClientMock, cerberusClientMock, skeletonManager)
	conditionRepository := NewConditionRepository(dbConn, schemaName)
	conditionService := NewConditionService(conditionRepository)
	sortingRuleRepository := NewSortingRuleRepository(dbConn, schemaName)
	sortingRuleService := NewSortingRuleService(analysisRepository, conditionService, sortingRuleRepository)
	instrumentService := NewInstrumentService(&configuration, sortingRuleService, instrumentRepository, skeletonManager, NewInstrumentCache(), cerberusClientMock)
	consoleLogService := service.NewConsoleLogService(consoleLogRepository, nil)

	responseRecorder := &httptest.ResponseRecorder{}
	ginContext, ginEngine := gin.CreateTestContext(responseRecorder)

	api := newAPI(ginEngine, &configuration, &authManager, analysisService, instrumentService, consoleLogService, nil)

	skeletonInstance, _ := NewSkeleton(ctx, serviceName, []string{}, sqlConn, schemaName, migrator.NewSkeletonMigrator(), api, analysisRepository, analysisService, instrumentService, consoleLogService, sortingRuleService, skeletonManager, cerberusClientMock, deaClientMock, configuration)

	_, _ = sqlConn.Exec(fmt.Sprintf(`INSERT INTO %s.sk_supported_protocols (id, "name", description)
		VALUES ('abb539a3-286f-4c15-a7b7-2e9adf6eab91', 'IH-1000 v5.2', 'IHCOM');`, schemaName))
	_, _ = sqlConn.Exec(fmt.Sprintf(`INSERT INTO %s.sk_instruments(id, protocol_id, "name", hostname, client_port, enabled, connection_mode, running_mode, captureresults, capturediagnostics, replytoquery, status, sent_to_cerberus, timezone, file_encoding) 
		VALUES ('93f36696-5ff0-45a9-87eb-ca5c064c5890', 'abb539a3-286f-4c15-a7b7-2e9adf6eab91', 'TestInstrument', '192.168.1.13', NULL, TRUE, 'TCP_SERVER_ONLY', 'PRODUCTION', TRUE, TRUE, TRUE, 'ONLINE', TRUE, 'Europe/Budapest', 'UTF8');`, schemaName))
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

	time.Sleep(1 * time.Second)

	err := skeletonInstance.SubmitAnalysisResultBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResultsWithoutAnalysisRequestsTest_analysisResults,
	})
	assert.Nil(t, err)

	time.Sleep(5 * time.Second)

	var resultCount int
	err = sqlConn.QueryRowx(fmt.Sprintf(`SELECT COUNT(*) FROM %s.sk_analysis_results;`, schemaName)).Scan(&resultCount)
	assert.Nil(t, err)
	assert.Equal(t, 2, resultCount)

	ginContext.Request, _ = http.NewRequest(http.MethodPost, "/v1/analysis-requests/batch", bytes.NewBuffer([]byte(analysisResultsWithoutAnalysisRequestsTest_AnalysisRequests)))

	ginEngine.ServeHTTP(responseRecorder, ginContext.Request)

	if responseRecorder.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, responseRecorder.Code)
	}

	time.Sleep(10 * time.Second)
	assert.Equal(t, 2, len(cerberusClientMock.AnalysisResults))
	assert.Equal(t, uuid.MustParse("660d1095-c8f6-4899-946e-935bfddfaa69"), cerberusClientMock.AnalysisResults[0].WorkingItemID)
	assert.Equal(t, analysisResultsWithoutAnalysisRequestsTest_instrument.ID, cerberusClientMock.AnalysisResults[0].InstrumentID)
	assert.Equal(t, analysisResultsWithoutAnalysisRequestsTest_analysisResults[0].Result, cerberusClientMock.AnalysisResults[0].Result)
	assert.Equal(t, true, cerberusClientMock.AnalysisResults[0].IsInvalid)
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

	configuration := config.Configuration{
		APIPort:                          5000,
		Authorization:                    false,
		PermittedOrigin:                  "*",
		ApplicationName:                  "Instrument API Test",
		TCPListenerPort:                  5401,
		InstrumentTransferRetryDelayInMs: 100,
		ResultTransferFlushTimeout:       5,
		ImageRetrySeconds:                60,
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

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	skeletonManager := NewSkeletonManager(ctx)
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

	analysisService := NewAnalysisService(analysisRepository, deaClientMock, cerberusClientMock, skeletonManager)
	conditionRepository := NewConditionRepository(dbConn, schemaName)
	conditionService := NewConditionService(conditionRepository)
	sortingRuleRepository := NewSortingRuleRepository(dbConn, schemaName)
	sortingRuleService := NewSortingRuleService(analysisRepository, conditionService, sortingRuleRepository)
	instrumentService := NewInstrumentService(&configuration, sortingRuleService, instrumentRepository, skeletonManager, NewInstrumentCache(), cerberusClientMock)
	consoleLogService := service.NewConsoleLogService(consoleLogRepository, nil)

	api := NewAPI(&configuration, &authManager, analysisService, instrumentService, consoleLogService, nil)

	skeletonInstance, _ := NewSkeleton(ctx, serviceName, []string{}, sqlConn, schemaName, migrator.NewSkeletonMigrator(), api, analysisRepository, analysisService, instrumentService, consoleLogService, sortingRuleService, skeletonManager, cerberusClientMock, deaClientMock, configuration)

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
		ResultMode:         "PRODUCTION",
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
			ResultMode:               "PRODUCTION",
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
			Reagents:                 []Reagent{},
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
			ResultMode:               "PRODUCTION",
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
			Reagents:                 []Reagent{},
			Images:                   []Image{},
		},
	}

	err = skeletonInstance.SubmitAnalysisResultBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResults,
	})
	assert.Nil(t, err)

	time.Sleep(4 * time.Second)
	assert.Equal(t, 0, len(cerberusClientMock.AnalysisResults))
}

func TestRegisterProtocol(t *testing.T) {
	//TODO
	// register protocol
	// register instrument with mentioned protocol (call endpoint ???) -> expect to pass
	// try register instrument with random protocolID -> expect to fail
}

func TestAnalysisResultsReprocessing(t *testing.T) {
	sqlConn, _ := sqlx.Connect("pgx", "host=localhost port=5551 user=postgres password=postgres dbname=postgres sslmode=disable")

	schemaName := "testAnalysisResultsReprocessing"
	_, _ = sqlConn.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp" schema public;`)
	_, _ = sqlConn.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE;`, schemaName))
	_, _ = sqlConn.Exec(fmt.Sprintf(`CREATE SCHEMA %s;`, schemaName))

	configuration := config.Configuration{
		APIPort:                          5678,
		Authorization:                    false,
		PermittedOrigin:                  "*",
		ApplicationName:                  "Submit Analysis Request Parallel Test",
		TCPListenerPort:                  5401,
		InstrumentTransferRetryDelayInMs: 100,
		ResultTransferFlushTimeout:       5,
		ImageRetrySeconds:                60,
	}
	dbConn := db.CreateDbConnector(sqlConn)

	analysisRepositoryMock := &analysisRepositoryMock{}
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

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	skeletonManager := NewSkeletonManager(ctx)
	skeletonManager.SetCallbackHandler(&skeletonCallbackHandlerV1Mock{
		handleAnalysisRequestsFunc: func(request []AnalysisRequest) error {
			time.Sleep(2 * time.Second)
			return nil
		},
	})
	cerberusClientMock := &cerberusClientMock{
		registerInstrumentFunc: func(instrument Instrument) error {
			return nil
		},
		sendAnalysisResultBatchFunc: func(analysisResults []AnalysisResultTO) (AnalysisResultBatchResponse, error) {
			return AnalysisResultBatchResponse{}, nil
		},
	}
	deaClientMock := &deaClientMock{}

	analysisServiceMock := &analysisServiceMock{}
	conditionRepository := NewConditionRepository(dbConn, schemaName)
	conditionService := NewConditionService(conditionRepository)
	sortingRuleRepository := NewSortingRuleRepository(dbConn, schemaName)
	analysisRepository := NewAnalysisRepository(dbConn, schemaName)
	sortingRuleService := NewSortingRuleService(analysisRepository, conditionService, sortingRuleRepository)
	instrumentService := NewInstrumentService(&configuration, sortingRuleService, instrumentRepository, skeletonManager, NewInstrumentCache(), cerberusClientMock)
	consoleLogService := service.NewConsoleLogService(consoleLogRepository, nil)
	ginEngine := gin.New()

	ginEngine.Use(timeout.Timeout(timeout.WithTimeout(5*time.Second), timeout.WithErrorHttpCode(http.StatusRequestTimeout)))

	api := newAPI(ginEngine, &configuration, &authManager, analysisServiceMock, instrumentService, consoleLogService, nil)

	skeletonInstance, _ := NewSkeleton(ctx, serviceName, []string{}, sqlConn, schemaName, migrator.NewSkeletonMigrator(), api, analysisRepositoryMock, analysisServiceMock, instrumentService, consoleLogService, sortingRuleService, skeletonManager, cerberusClientMock, deaClientMock, configuration)

	go func() {
		_ = skeletonInstance.Start()
	}()

	time.Sleep(1 * time.Second)
}

func TestSubmitControlResultsProcessing(t *testing.T) {
	sqlConn, _ := sqlx.Connect("pgx", "host=localhost port=5551 user=postgres password=postgres dbname=postgres sslmode=disable")

	schemaName := "testSubmitControlResultsProcessing"
	_, _ = sqlConn.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp" schema public;`)
	_, _ = sqlConn.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE;`, schemaName))
	_, _ = sqlConn.Exec(fmt.Sprintf(`CREATE SCHEMA %s;`, schemaName))

	configuration := config.Configuration{
		APIPort:                          5679,
		Authorization:                    false,
		PermittedOrigin:                  "*",
		ApplicationName:                  "Submit Control Results Processing Test",
		TCPListenerPort:                  5401,
		InstrumentTransferRetryDelayInMs: 100,
		ResultTransferFlushTimeout:       5,
		ImageRetrySeconds:                60,
	}
	dbConn := db.CreateDbConnector(sqlConn)

	analysisRepositoryMock := &analysisRepositoryMock{}
	conditionRepository := NewConditionRepository(dbConn, schemaName)
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

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	skeletonManagerMock := &mockManager{}
	cerberusClientMock := &cerberusClientMock{
		registerInstrumentFunc: func(instrument Instrument) error {
			return nil
		},
		sendAnalysisResultBatchFunc: func(analysisResults []AnalysisResultTO) (AnalysisResultBatchResponse, error) {
			return AnalysisResultBatchResponse{}, nil
		},
	}
	deaClientMock := &deaClientMock{}

	analysisService := NewAnalysisService(analysisRepositoryMock, deaClientMock, cerberusClientMock, skeletonManagerMock)
	conditionService := NewConditionService(conditionRepository)
	sortingRuleRepository := NewSortingRuleRepository(dbConn, schemaName)
	sortingRuleService := NewSortingRuleService(analysisRepositoryMock, conditionService, sortingRuleRepository)

	instrumentService := NewInstrumentService(&configuration, sortingRuleService, instrumentRepository, skeletonManagerMock, NewInstrumentCache(), cerberusClientMock)
	consoleLogService := service.NewConsoleLogService(consoleLogRepository, nil)

	ginEngine := gin.New()

	ginEngine.Use(timeout.Timeout(timeout.WithTimeout(5*time.Second), timeout.WithErrorHttpCode(http.StatusRequestTimeout)))

	api := newAPI(ginEngine, &configuration, &authManager, analysisService, instrumentService, consoleLogService, nil)

	skeletonInstance, _ := NewSkeleton(ctx, serviceName, []string{}, sqlConn, schemaName, migrator.NewSkeletonMigrator(), api, analysisRepositoryMock, analysisService, instrumentService, consoleLogService, sortingRuleService, skeletonManagerMock, cerberusClientMock, deaClientMock, configuration)

	go func() {
		_ = skeletonInstance.Start()
	}()

	expectedControlResult := ExpectedControlResult{
		ID:             uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
		SampleCode:     "Sample1",
		Operator:       Equals,
		ExpectedValue:  "40",
		ExpectedValue2: nil,
		CreatedAt:      time.Time{},
		DeletedAt:      nil,
		CreatedBy:      uuid.UUID{},
		DeletedBy:      uuid.NullUUID{},
	}

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
			ResultType:             "pein",
			ExpectedControlResults: []ExpectedControlResult{expectedControlResult},
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
		ResultMode:         "PRODUCTION",
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

	reagent := Reagent{
		ID:             uuid.MustParse("69f853b0-fe43-4bd1-a8d4-64c0ff704558"),
		Manufacturer:   "Test Manufacturer",
		SerialNumber:   "0001",
		LotNo:          "0002",
		Type:           Standard,
		CreatedAt:      time.Time{},
		ExpirationDate: nil,
		ControlResults: nil,
	}

	batchID := uuid.MustParse("ddd34c4d-62f9-4621-bb16-efad459a9bfe")
	analysisResultId1 := uuid.MustParse("88888acb-5449-4012-96c2-2dd761b62b19")
	analysisResultId2 := uuid.MustParse("69f853b0-fe43-4bd1-a8d4-64c0ff704558")
	analysisResults := []AnalysisResult{
		{
			ID:                       analysisResultId1,
			AnalysisRequest:          AnalysisRequest{},
			AnalyteMapping:           analyteMappings[0],
			Instrument:               instrument,
			SampleCode:               "testSampleCode",
			ResultRecordID:           uuid.MustParse("2f369489-77d3-464e-87e2-edbeffa62ae7"),
			BatchID:                  batchID,
			Result:                   "pos",
			ResultMode:               "PRODUCTION",
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
			Reagents:                 []Reagent{reagent},
			Images:                   []Image{},
		},
		{
			ID:                       analysisResultId2,
			AnalysisRequest:          AnalysisRequest{},
			AnalyteMapping:           analyteMappings[0],
			Instrument:               instrument,
			SampleCode:               "testSampleCode2",
			ResultRecordID:           uuid.MustParse("43a7b261-3e1d-4065-935a-ac15841f13e4"),
			BatchID:                  batchID,
			Result:                   "pos",
			ResultMode:               "PRODUCTION",
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
			Reagents:                 []Reagent{reagent},
			Images:                   []Image{},
		},
	}

	controlResult := ControlResult{
		ID:             uuid.UUID{},
		SampleCode:     "",
		AnalyteMapping: analyteMappings[0],
		Result:         "40",
		ExpectedControlResultId: uuid.NullUUID{
			UUID:  uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
			Valid: true,
		},
		IsValid:                    false,
		IsComparedToExpectedResult: false,
		ExaminedAt:                 time.Time{},
		InstrumentID:               instrumentID,
		Warnings:                   nil,
		ChannelResults:             nil,
		ExtraValues:                nil,
	}

	err := skeletonInstance.SubmitAnalysisResultBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResults,
	})
	assert.Nil(t, err)

	time.Sleep(4 * time.Second)

	skeletonManagerMock.AnalysisRequestsForProcessing = make([]AnalysisResult, 0)
	skeletonManagerMock.ControlResultForProcessing = make([]MappedStandaloneControlResult, 0)

	err = skeletonInstance.SubmitControlResults(context.TODO(), []StandaloneControlResult{
		{
			ControlResult: controlResult,
			Reagents:      []Reagent{reagent},
			ResultIDs:     []uuid.UUID{analysisResultId1, analysisResultId2},
		},
	})
	assert.Nil(t, err)

	time.Sleep(4 * time.Second)
	assert.Equal(t, 2, len(skeletonManagerMock.AnalysisRequestsForProcessing))
	assert.Equal(t, 0, len(skeletonManagerMock.ControlResultForProcessing))

	skeletonManagerMock.AnalysisRequestsForProcessing = make([]AnalysisResult, 0)
	skeletonManagerMock.ControlResultForProcessing = make([]MappedStandaloneControlResult, 0)

	err = skeletonInstance.SubmitControlResults(context.TODO(), []StandaloneControlResult{
		{
			ControlResult: controlResult,
			Reagents:      []Reagent{reagent},
			ResultIDs:     []uuid.UUID{},
		},
	})
	assert.Nil(t, err)

	time.Sleep(4 * time.Second)
	assert.Equal(t, 0, len(skeletonManagerMock.AnalysisRequestsForProcessing))
	assert.Equal(t, 1, len(skeletonManagerMock.ControlResultForProcessing))

	skeletonManagerMock.AnalysisRequestsForProcessing = make([]AnalysisResult, 0)
	skeletonManagerMock.ControlResultForProcessing = make([]MappedStandaloneControlResult, 0)
	controlResultWithoutAnalysisResult := ControlResult{
		ID:             uuid.UUID{},
		SampleCode:     "",
		AnalyteMapping: analyteMappings[0],
		Result:         "40",
		ExpectedControlResultId: uuid.NullUUID{
			UUID:  uuid.MustParse("5d175eb3-e70f-405e-ab33-c15a854f17a0"),
			Valid: true,
		},
		IsValid:                    false,
		IsComparedToExpectedResult: false,
		ExaminedAt:                 time.Time{},
		InstrumentID:               instrumentID,
		Warnings:                   nil,
		ChannelResults:             nil,
		ExtraValues:                nil,
	}

	err = skeletonInstance.SubmitControlResults(context.TODO(), []StandaloneControlResult{
		{
			ControlResult: controlResultWithoutAnalysisResult,
			Reagents:      []Reagent{reagent},
			ResultIDs:     []uuid.UUID{},
		},
		{
			ControlResult: controlResult,
			Reagents:      []Reagent{reagent},
			ResultIDs:     []uuid.UUID{analysisResultId1, analysisResultId2},
		},
	})
	assert.Nil(t, err)

	time.Sleep(4 * time.Second)
	assert.Equal(t, 2, len(skeletonManagerMock.AnalysisRequestsForProcessing))
	assert.Equal(t, 1, len(skeletonManagerMock.ControlResultForProcessing))
}

type skeletonCallbackHandlerV1Mock struct {
	handleAnalysisRequestsFunc func(request []AnalysisRequest) error
	getManufacturerTestFunc    func(instrumentId uuid.UUID, protocolId uuid.UUID) ([]SupportedManufacturerTests, error)
	getEncodingList            func(protocolId uuid.UUID) ([]string, error)
	revokeAnalysisRequests     func(request []AnalysisRequest) error
	reprocessInstrumentData    func(batchIDs []uuid.UUID) error
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

func (m *skeletonCallbackHandlerV1Mock) ReexamineAnalysisRequests(request []AnalysisRequest) {
	if m.revokeAnalysisRequests == nil {
		return
	}
	m.revokeAnalysisRequests(request)
}

func (m *skeletonCallbackHandlerV1Mock) RevokeAnalysisRequests(request []AnalysisRequest) {
	if m.revokeAnalysisRequests == nil {
		return
	}
	m.revokeAnalysisRequests(request)
}

func (m *skeletonCallbackHandlerV1Mock) ReprocessInstrumentData(batchIDs []uuid.UUID) error {
	if m.reprocessInstrumentData == nil {
		return nil
	}
	return m.reprocessInstrumentData(batchIDs)
}

func (m *skeletonCallbackHandlerV1Mock) ReprocessInstrumentDataBySampleCode(sampleCode string) error {
	if m.reprocessInstrumentData == nil {
		return nil
	}
	return m.ReprocessInstrumentDataBySampleCode(sampleCode)
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
	registerInstrumentFunc           func(instrument Instrument) error
	registerInstrumentDriverFunc     func(serviceName string, apiVersion string, apiPort uint16, tlsEnabled bool, extraValueKeys []string) error
	sendAnalysisResultBatchFunc      func(analysisResults []AnalysisResultTO) (AnalysisResultBatchResponse, error)
	sendControlResultBatchFunc       func(controlResults []StandaloneControlResultTO) (ControlResultBatchResponse, error)
	sendAnalysisResultImageBatchFunc func(images []WorkItemResultImageTO) error

	AnalysisResults      []AnalysisResultTO
	ControlResults       []StandaloneControlResultTO
	BatchResponse        AnalysisResultBatchResponse
	ControlBatchResponse ControlResultBatchResponse
}

func (m *cerberusClientMock) SendControlResultBatch(controlResults []StandaloneControlResultTO) (ControlResultBatchResponse, error) {
	m.ControlResults = append(m.ControlResults, controlResults...)
	if m.sendControlResultBatchFunc == nil {
		return ControlResultBatchResponse{}, errors.New("not implemented")
	}
	response, err := m.sendControlResultBatchFunc(controlResults)
	m.ControlBatchResponse = response
	return response, err
}

func (m *cerberusClientMock) RegisterInstrument(instrument Instrument) error {
	if m.registerInstrumentFunc == nil {
		return errors.New("not implemented")
	}
	return m.registerInstrumentFunc(instrument)
}

func (m *cerberusClientMock) RegisterInstrumentDriver(serviceName string, apiVersion string, apiPort uint16, tlsEnabled bool, extraValueKeys []string) error {
	if m.registerInstrumentDriverFunc == nil {
		return nil
	}
	return m.registerInstrumentDriverFunc(serviceName, apiVersion, apiPort, tlsEnabled, extraValueKeys)
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

func (m *cerberusClientMock) SendAnalysisResultImageBatch(images []WorkItemResultImageTO) error {
	if m.sendAnalysisResultImageBatchFunc == nil {
		return errors.New("not implemented")
	}

	return m.sendAnalysisResultImageBatchFunc(images)
}

func generateAnalysisRequestsJson(count int) string {
	json := `[`

	for i := 0; i < count; i++ {
		json += fmt.Sprintf(`{
					"workItemId": "%s",
					"analyteId": "51bfea41-1b7e-48f7-8b35-46d930216de7",
					"sampleCode": "TestSampleCode%d",
					"materialId": "50820d7e-3f5b-4452-aa4b-bebfc3c15002",
					"laboratoryId": "3072973a-25d7-43d0-840f-d5a3de8e3aa5",
					"validUntilTime": "2099-01-10T11:30:59.000Z",
					"subject": null
				}`, uuid.New(), i)

		if i != count-1 {
			json += `,`
		}
	}

	json += `]`

	return json
}

type analysisServiceMock struct {
}

func (m *analysisServiceMock) QueueControlResults(ctx context.Context, results []MappedStandaloneControlResult) error {
	return nil
}

func (m *analysisServiceMock) SaveCerberusIDsForControlResultBatchItems(ctx context.Context, controlResults []ControlResultBatchItemInfo) {
}

func (m *analysisServiceMock) SaveCerberusIDsForAnalysisResultBatchItems(ctx context.Context, analysisResults []AnalysisResultBatchItemInfo) {
}

func (m *analysisServiceMock) GetUnprocessedMappedStandaloneControlResultsByIDs(ctx context.Context, controlResultIDs []uuid.UUID) ([]MappedStandaloneControlResult, error) {
	return []MappedStandaloneControlResult{}, nil
}

func (m *analysisServiceMock) CreateAnalysisRequests(ctx context.Context, analysisRequests []AnalysisRequest) ([]AnalysisRequestStatus, error) {
	return nil, nil
}
func (m *analysisServiceMock) ProcessAnalysisRequests(ctx context.Context, analysisRequests []AnalysisRequest) error {
	return nil
}
func (m *analysisServiceMock) RevokeAnalysisRequests(ctx context.Context, workItemIDs []uuid.UUID) error {
	return nil
}
func (m *analysisServiceMock) ReexamineAnalysisRequestsBatch(ctx context.Context, workItemIDs []uuid.UUID) error {
	return nil
}
func (m *analysisServiceMock) GetAnalysisRequestsInfo(ctx context.Context, instrumentID uuid.UUID, filter Filter) ([]AnalysisRequestInfo, int, error) {
	return nil, 0, nil
}
func (m *analysisServiceMock) GetAnalysisResultsInfo(ctx context.Context, instrumentID uuid.UUID, filter Filter) ([]AnalysisResultInfo, int, error) {
	return nil, 0, nil
}
func (m *analysisServiceMock) GetAnalysisBatches(ctx context.Context, instrumentID uuid.UUID, filter Filter) ([]AnalysisBatch, int, error) {
	return nil, 0, nil
}
func (m *analysisServiceMock) CreateAnalysisResultsBatch(ctx context.Context, analysisResults AnalysisResultSet) ([]AnalysisResult, error) {
	for i := range analysisResults.Results {
		analysisResults.Results[i].ID = uuid.New()
	}
	return analysisResults.Results, nil
}
func (m *analysisServiceMock) CreateControlResultBatch(ctx context.Context, controlResults []StandaloneControlResult) ([]StandaloneControlResult, []uuid.UUID, error) {
	return nil, nil, nil
}
func (m *analysisServiceMock) GetAnalysisResultsByIDsWithRecalculatedStatus(ctx context.Context, analysisResultIDs []uuid.UUID) ([]AnalysisResult, error) {
	return nil, nil
}
func (m *analysisServiceMock) QueueAnalysisResults(ctx context.Context, results []AnalysisResult) error {
	return nil
}

func (m *analysisServiceMock) RetransmitResult(ctx context.Context, resultID uuid.UUID) error {
	return nil
}
func (m *analysisServiceMock) RetransmitResultBatches(ctx context.Context, batchIDs []uuid.UUID) error {
	return nil
}
func (m *analysisServiceMock) ReprocessInstrumentData(ctx context.Context, batchIDs []uuid.UUID) {
}
func (m *analysisServiceMock) ProcessStuckImagesToDEA(ctx context.Context) {
}
func (m *analysisServiceMock) ProcessStuckImagesToCerberus(ctx context.Context) {
}

type analysisRepositoryMock struct {
	callCount int
}

func (m *analysisRepositoryMock) DeleteOldCerberusQueueItems(ctx context.Context, cleanupDays, limit int) (int64, error) {
	return 0, nil
}

func (m *analysisRepositoryMock) DeleteOldAnalysisRequestsWithTx(ctx context.Context, cleanupDays, limit int, tx db.DbConnector) (int64, error) {
	return 0, nil
}

func (m *analysisRepositoryMock) DeleteOldAnalysisResultsWithTx(ctx context.Context, cleanupDays, limit int, tx db.DbConnector) (int64, error) {
	return 0, nil
}

func (m *analysisRepositoryMock) UpdateCerberusQueueItemStatus(ctx context.Context, queueItem CerberusQueueItem) error {
	return nil
}

func (m *analysisRepositoryMock) GetControlResultQueueItems(ctx context.Context) ([]CerberusQueueItem, error) {
	return []CerberusQueueItem{}, nil
}

func (m *analysisRepositoryMock) CreateControlResultQueueItem(ctx context.Context, controlResults []StandaloneControlResult) (uuid.UUID, error) {
	return uuid.Nil, nil
}

func (m *analysisRepositoryMock) GetUnprocessedControlResultIDs(ctx context.Context) ([]uuid.UUID, error) {
	return []uuid.UUID{}, nil
}

func (m *analysisRepositoryMock) GetUnprocessedAnalysisResultIDsByControlResultIDs(ctx context.Context, controlResultIDs []uuid.UUID) (map[uuid.UUID]map[uuid.UUID]uuid.UUID, error) {
	return make(map[uuid.UUID]map[uuid.UUID]uuid.UUID), nil
}

func (m *analysisRepositoryMock) GetUnprocessedReagentIDsByControlResultIDs(ctx context.Context, controlResultIDs []uuid.UUID) (map[uuid.UUID][]uuid.UUID, error) {
	return make(map[uuid.UUID][]uuid.UUID), nil
}

func (m *analysisRepositoryMock) CreateReagents(ctx context.Context, reagents []Reagent) ([]uuid.UUID, error) {
	return []uuid.UUID{}, nil
}

func (m *analysisRepositoryMock) GetReagentsByIDs(ctx context.Context, reagentIDs []uuid.UUID) (map[uuid.UUID]Reagent, error) {
	return make(map[uuid.UUID]Reagent), nil
}

func (m *analysisRepositoryMock) CreateControlResultBatch(ctx context.Context, controlResults []ControlResult) ([]uuid.UUID, error) {
	return []uuid.UUID{}, nil
}

func (m *analysisRepositoryMock) GetControlResultsByIDs(ctx context.Context, controlResultIDs []uuid.UUID) (map[uuid.UUID]ControlResult, error) {
	return make(map[uuid.UUID]ControlResult), nil
}

func (m *analysisRepositoryMock) CreateReagentControlResultRelations(ctx context.Context, relationDAOs []reagentControlResultRelationDAO) error {
	return nil
}

func (m *analysisRepositoryMock) CreateAnalysisResultControlResultRelations(ctx context.Context, relationDAOs []analysisResultControlResultRelationDAO) error {
	return nil
}

func (m *analysisRepositoryMock) GetCerberusIDForAnalysisResults(ctx context.Context, analysisResultIDs []uuid.UUID) (map[uuid.UUID]uuid.UUID, error) {
	return make(map[uuid.UUID]uuid.UUID), nil
}

func (m *analysisRepositoryMock) SaveCerberusIDForAnalysisResult(ctx context.Context, analysisResultID uuid.UUID, cerberusID uuid.UUID) error {
	return nil
}

func (m *analysisRepositoryMock) SaveCerberusIDForControlResult(ctx context.Context, controlResultID uuid.UUID, cerberusID uuid.UUID) error {
	return nil
}

func (m *analysisRepositoryMock) SaveCerberusIDForReagent(ctx context.Context, reagentID uuid.UUID, cerberusID uuid.UUID) error {
	return nil
}

func (m *analysisRepositoryMock) MarkReagentControlResultRelationsAsProcessed(ctx context.Context, controlResultID uuid.UUID, reagentIDs []uuid.UUID) error {
	return nil
}

func (m *analysisRepositoryMock) MarkAnalysisResultControlResultRelationsAsProcessed(ctx context.Context, controlResultID uuid.UUID, analysisResultIDs []uuid.UUID) error {
	return nil
}

func (m *analysisRepositoryMock) CreateAnalysisRequestExtraValues(ctx context.Context, extraValuesByAnalysisRequestIDs map[uuid.UUID][]ExtraValue) error {
	return nil
}

func (m *analysisRepositoryMock) GetSampleCodesByOrderID(ctx context.Context, orderID uuid.UUID) ([]string, error) {
	return nil, nil
}

func (m *analysisRepositoryMock) GetUnprocessedAnalysisResultIDs(ctx context.Context) ([]uuid.UUID, error) {
	return make([]uuid.UUID, 660), nil
}

func (m *analysisRepositoryMock) GetAnalysisResultsByIDs(ctx context.Context, ids []uuid.UUID) ([]AnalysisResult, error) {
	return make([]AnalysisResult, len(ids)), nil
}

func (m *analysisRepositoryMock) CreateAnalysisRequestsBatch(ctx context.Context, analysisRequests []AnalysisRequest) ([]uuid.UUID, []uuid.UUID, error) {
	return nil, nil, nil
}
func (m *analysisRepositoryMock) GetAnalysisRequestsBySampleCodeAndAnalyteID(ctx context.Context, sampleCodes string, analyteID uuid.UUID) ([]AnalysisRequest, error) {
	return nil, nil
}
func (m *analysisRepositoryMock) GetAnalysisRequestsBySampleCodes(ctx context.Context, sampleCodes []string, allowResending bool) (map[string][]AnalysisRequest, error) {
	return nil, nil
}
func (m *analysisRepositoryMock) GetAnalysisRequestsInfo(ctx context.Context, instrumentID uuid.UUID, filter Filter) ([]AnalysisRequestInfo, int, error) {
	return nil, 0, nil
}
func (m *analysisRepositoryMock) GetAnalysisResultsInfo(ctx context.Context, instrumentID uuid.UUID, filter Filter) ([]AnalysisResultInfo, int, error) {
	return nil, 0, nil
}
func (m *analysisRepositoryMock) GetAnalysisBatches(ctx context.Context, instrumentID uuid.UUID, filter Filter) ([]AnalysisBatch, int, error) {
	return nil, 0, nil
}
func (m *analysisRepositoryMock) CreateSubjectsBatch(ctx context.Context, subjectInfosByAnalysisRequestID map[uuid.UUID]SubjectInfo) (map[uuid.UUID]uuid.UUID, error) {
	return nil, nil
}
func (m *analysisRepositoryMock) GetSubjectsByAnalysisRequestIDs(ctx context.Context, analysisRequestIDs []uuid.UUID) (map[uuid.UUID]SubjectInfo, error) {
	return nil, nil
}
func (m *analysisRepositoryMock) GetAnalysisRequestsByWorkItemIDs(ctx context.Context, workItemIds []uuid.UUID) ([]AnalysisRequest, error) {
	return nil, nil
}
func (m *analysisRepositoryMock) RevokeAnalysisRequests(ctx context.Context, workItemIds []uuid.UUID) error {
	return nil
}
func (m *analysisRepositoryMock) DeleteAnalysisRequestExtraValues(ctx context.Context, workItemIds []uuid.UUID) error {
	return nil
}
func (m *analysisRepositoryMock) IncreaseReexaminationRequestedCount(ctx context.Context, workItemIDs []uuid.UUID) error {
	return nil
}
func (m *analysisRepositoryMock) IncreaseSentToInstrumentCounter(ctx context.Context, analysisRequestIDs []uuid.UUID) error {
	return nil
}
func (m *analysisRepositoryMock) SaveAnalysisRequestsInstrumentTransmissions(ctx context.Context, analysisRequestIDs []uuid.UUID, instrumentID uuid.UUID) error {
	return nil
}
func (m *analysisRepositoryMock) CreateAnalysisResultsBatch(ctx context.Context, analysisResults []AnalysisResult) ([]AnalysisResult, error) {
	return analysisResults, nil
}
func (m *analysisRepositoryMock) UpdateStatusAnalysisResultsBatch(ctx context.Context, analysisResultsToUpdate []AnalysisResult) error {
	return nil
}
func (m *analysisRepositoryMock) CreateAnalysisResultReagentRelations(ctx context.Context, relationDAOs []analysisResultReagentRelationDAO) error {
	return nil
}
func (m *analysisRepositoryMock) GetAnalysisResultsBySampleCodeAndAnalyteID(ctx context.Context, sampleCode string, analyteID uuid.UUID) ([]AnalysisResult, error) {
	return nil, nil
}
func (m *analysisRepositoryMock) GetAnalysisResultByID(ctx context.Context, id uuid.UUID, allowDeletedAnalyteMapping bool) (AnalysisResult, error) {
	return AnalysisResult{}, nil
}
func (m *analysisRepositoryMock) GetAnalysisResultsByBatchIDs(ctx context.Context, batchIDs []uuid.UUID) ([]AnalysisResult, error) {
	return nil, nil
}
func (m *analysisRepositoryMock) CreateAnalysisResultExtraValues(ctx context.Context, extraValuesByAnalysisRequestIDs map[uuid.UUID][]ExtraValue) error {
	return nil
}
func (m *analysisRepositoryMock) CreateChannelResults(ctx context.Context, channelResults []ChannelResult, analysisResultID uuid.UUID) ([]uuid.UUID, error) {
	return nil, nil
}
func (m *analysisRepositoryMock) CreateChannelResultQuantitativeValues(ctx context.Context, quantitativeValuesByChannelResultIDs map[uuid.UUID]map[string]string) error {
	return nil
}
func (m *analysisRepositoryMock) CreateReagentsByAnalysisResultID(ctx context.Context, reagentsByAnalysisResultID map[uuid.UUID][]Reagent) (map[uuid.UUID]map[uuid.UUID][]ControlResult, map[uuid.UUID][]Reagent, error) {
	reagentsMapWithIds := make(map[uuid.UUID][]Reagent)
	controlResultsMap := make(map[uuid.UUID]map[uuid.UUID][]ControlResult)
	for analysisResultId, reagents := range reagentsByAnalysisResultID {
		reagentArray := make([]Reagent, 0)
		for i, reagent := range reagents {
			reagentsByAnalysisResultID[analysisResultId][i].ID = uuid.New()
			reagentArray = append(reagentArray, reagentsByAnalysisResultID[analysisResultId][i])
			controlResults := make([]ControlResult, 0)
			for j := range reagent.ControlResults {
				reagentsByAnalysisResultID[analysisResultId][i].ControlResults[j].ID = uuid.New()
				controlResults = append(controlResults, reagentsByAnalysisResultID[analysisResultId][i].ControlResults[j])
			}
			controlResultsMap[analysisResultId] = make(map[uuid.UUID][]ControlResult)
			controlResultsMap[analysisResultId][reagentsByAnalysisResultID[analysisResultId][i].ID] = controlResults
		}
		reagentsMapWithIds[analysisResultId] = reagentArray
	}
	return controlResultsMap, reagentsMapWithIds, nil
}
func (m *analysisRepositoryMock) CreateReagentBatch(ctx context.Context, reagents []Reagent) ([]Reagent, error) {
	return nil, nil
}
func (m *analysisRepositoryMock) CreateControlResults(ctx context.Context, controlResultsMap map[uuid.UUID]map[uuid.UUID][]ControlResult) (map[uuid.UUID]map[uuid.UUID][]uuid.UUID, error) {
	controlResultIdsMap := make(map[uuid.UUID]map[uuid.UUID][]uuid.UUID)
	for analysisResultId, reagentMap := range controlResultsMap {
		for reagentId, controlResults := range reagentMap {
			controlResultIdsMap[analysisResultId] = make(map[uuid.UUID][]uuid.UUID)
			controlResultIds := make([]uuid.UUID, 0)
			for i := 0; i < len(controlResults); i++ {
				controlResultIds = append(controlResultIds, uuid.New())
			}
			controlResultIdsMap[analysisResultId][reagentId] = controlResultIds
		}
	}
	return controlResultIdsMap, nil
}
func (m *analysisRepositoryMock) CreateWarnings(ctx context.Context, warningsByAnalysisResultID map[uuid.UUID][]string) error {
	return nil
}
func (m *analysisRepositoryMock) GetAnalysisResultsByBatchIDsMapped(ctx context.Context, batchIDs []uuid.UUID) (map[uuid.UUID][]AnalysisResultInfo, error) {
	return nil, nil
}
func (r *analysisRepositoryMock) GetAnalysisRequestExtraValuesByAnalysisRequestID(ctx context.Context, analysisRequestID uuid.UUID) (map[string]string, error) {
	return nil, nil
}
func (m *analysisRepositoryMock) GetAnalysisResultQueueItems(ctx context.Context) ([]CerberusQueueItem, error) {
	return nil, nil
}
func (m *analysisRepositoryMock) UpdateAnalysisResultQueueItemStatus(ctx context.Context, queueItem CerberusQueueItem) error {
	return nil
}
func (m *analysisRepositoryMock) CreateAnalysisResultQueueItem(ctx context.Context, analysisResults []AnalysisResult) (uuid.UUID, error) {
	return uuid.New(), nil
}
func (m *analysisRepositoryMock) SaveImages(ctx context.Context, images []imageDAO) ([]uuid.UUID, error) {
	return nil, nil
}
func (m *analysisRepositoryMock) SaveControlResultImages(ctx context.Context, images []controlResultImageDAO) ([]uuid.UUID, error) {
	return nil, nil
}
func (m *analysisRepositoryMock) GetStuckImageIDsForDEA(ctx context.Context) ([]uuid.UUID, error) {
	return nil, nil
}
func (m *analysisRepositoryMock) GetStuckImageIDsForCerberus(ctx context.Context) ([]uuid.UUID, error) {
	return nil, nil
}
func (m *analysisRepositoryMock) GetImagesForDEAUploadByIDs(ctx context.Context, ids []uuid.UUID) ([]imageDAO, error) {
	return nil, nil
}
func (m *analysisRepositoryMock) GetImagesForCerberusSyncByIDs(ctx context.Context, ids []uuid.UUID) ([]cerberusImageDAO, error) {
	return nil, nil
}
func (m *analysisRepositoryMock) SaveDEAImageID(ctx context.Context, imageID, deaImageID uuid.UUID) error {
	return nil
}
func (m *analysisRepositoryMock) IncreaseImageUploadRetryCount(ctx context.Context, imageID uuid.UUID, error string) error {
	return nil
}
func (m *analysisRepositoryMock) MarkImagesAsSyncedToCerberus(ctx context.Context, ids []uuid.UUID) error {
	return nil
}
func (m *analysisRepositoryMock) GetUnprocessedAnalysisRequests(ctx context.Context) ([]AnalysisRequest, error) {
	return nil, nil
}
func (m *analysisRepositoryMock) MarkAnalysisRequestsAsProcessed(ctx context.Context, analysisRequestIDs []uuid.UUID) error {
	return nil
}
func (m *analysisRepositoryMock) MarkAnalysisResultsAsProcessed(ctx context.Context, analysisRequestIDs []uuid.UUID) error {
	return nil
}
func (m *analysisRepositoryMock) CreateTransaction() (db.DbConnector, error) {
	return db.CreateDbConnector(&sqlx.DB{}), nil
}
func (m *analysisRepositoryMock) WithTransaction(tx db.DbConnector) AnalysisRepository {
	return m
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
		ResultMode:               "PRODUCTION",
		Status:                   "PRE",
		ResultYieldDateTime:      nil,
		ValidUntil:               time.Now().Add(1 * time.Minute),
		Operator:                 "TestOperator",
		TechnicalReleaseDateTime: nil,
		InstrumentRunID:          uuid.Nil,
		Edited:                   false,
		IsInvalid:                true,
		EditReason:               "",
		WarnFlag:                 false,
		Warnings:                 []string{"test warning"},
		ChannelResults:           []ChannelResult{},
		ExtraValues:              []ExtraValue{},
		Reagents: []Reagent{
			{
				ID:             uuid.New(),
				Manufacturer:   "manufacturer",
				SerialNumber:   "serialNumber",
				LotNo:          "lotNo",
				Type:           Standard,
				ExpirationDate: nil,
				ControlResults: nil,
			},
		},
		Images: []Image{},
	},
	{
		AnalysisRequest:          AnalysisRequest{},
		AnalyteMapping:           analysisResultsWithoutAnalysisRequestsTest_analyteMappings[0],
		Instrument:               analysisResultsWithoutAnalysisRequestsTest_instrument,
		SampleCode:               "TestSampleCode2",
		ResultRecordID:           uuid.MustParse("43a7b261-3e1d-4065-935a-ac15841f13e4"),
		BatchID:                  uuid.MustParse("ddd34c4d-62f9-4621-bb16-efad459a9bfe"),
		Result:                   "pos",
		ResultMode:               "PRODUCTION",
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
		Reagents:                 []Reagent{},
		Images:                   []Image{},
	},
}

var analysisResultsWithoutAnalysisRequestsTest_analysisResultTOs = []AnalysisResultTO{
	{
		InstrumentID:             analysisResultsWithoutAnalysisRequestsTest_instrument.ID,
		Result:                   "pos",
		Mode:                     "PRODUCTION",
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
		Reagents:                 []ReagentTO{},
		Images:                   []ImageTO{},
	},
	{
		InstrumentID:             analysisResultsWithoutAnalysisRequestsTest_instrument.ID,
		Result:                   "pos",
		Mode:                     "PRODUCTION",
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
		Reagents:                 []ReagentTO{},
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
	ResultMode:         "PRODUCTION",
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
