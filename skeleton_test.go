package skeleton

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/MicahParks/keyfunc"
	"github.com/blutspende/skeleton/config"
	"github.com/blutspende/skeleton/db"
	"github.com/blutspende/skeleton/migrator"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/assert"
)

const serviceName = "testInstrumentDriver"
const displayName = "testDisplayName"

func TestSubmitAnalysisResultWithoutRequests(t *testing.T) {
	dbConn, schemaName, pg, sqlConn := setupDbConnector("testSubmitAnalysisRequestsParallel")

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

	analysisRepository := NewAnalysisRepository(dbConn, schemaName)
	instrumentRepository := NewInstrumentRepository(dbConn, schemaName)
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
	instrumentService := NewInstrumentService(sortingRuleService, instrumentRepository, NewSkeletonManager(ctx), NewInstrumentCache(), cerberusClientMock)
	consoleLogService := NewConsoleLogService(cerberusClientMock)
	messageInRepository := NewMessageInRepository(dbConn, schemaName, 0, 0)
	messageOutRepository := NewMessageOutRepository(dbConn, schemaName, 0, 0)
	messageOutOrderRepository := NewMessageOutOrderRepository(dbConn, schemaName, 0)
	messageService := NewMessageService(deaClientMock, messageInRepository, messageOutRepository, messageOutOrderRepository, schemaName)

	skeletonInstance, _ := NewSkeleton(ctx, serviceName, displayName, []string{}, []string{}, []string{}, pg, dbConn, schemaName, migrator.NewSkeletonMigrator(), analysisRepository, analysisService, instrumentService, consoleLogService, messageService, skeletonManager, cerberusClientMock, &longPollClientMock{AnalysisRequests: analysisResultsWithoutAnalysisRequestsTest_AnalysisRequests}, deaClientMock, configuration)

	_, _ = sqlConn.Exec(fmt.Sprintf(`INSERT INTO %s.sk_supported_protocols (id, "name", description)
		VALUES ('abb539a3-286f-4c15-a7b7-2e9adf6eab91', 'IH-1000 v5.2', 'IHCOM');`, schemaName))
	_, _ = sqlConn.Exec(fmt.Sprintf(`INSERT INTO %s.sk_instruments(id, protocol_id, "name", hostname, client_port, enabled, connection_mode, running_mode, captureresults, capturediagnostics, replytoquery, status, timezone, file_encoding) 
		VALUES ('93f36696-5ff0-45a9-87eb-ca5c064c5890', 'abb539a3-286f-4c15-a7b7-2e9adf6eab91', 'TestInstrument', '192.168.1.13', NULL, TRUE, 'TCP_SERVER_ONLY', 'PRODUCTION', TRUE, TRUE, TRUE, 'ONLINE', 'Europe/Budapest', 'UTF8');`, schemaName))
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
	time.Sleep(5 * time.Second)

	var resultCount int
	err = sqlConn.QueryRowx(fmt.Sprintf(`SELECT COUNT(*) FROM %s.sk_analysis_results;`, schemaName)).Scan(&resultCount)
	assert.Nil(t, err)
	assert.Equal(t, 2, resultCount)

	time.Sleep(10 * time.Second)
	assert.Equal(t, 2, len(cerberusClientMock.AnalysisResults))
	sort.Slice(cerberusClientMock.AnalysisResults, func(i, j int) bool {
		return cerberusClientMock.AnalysisResults[i].WorkingItemID.String() < cerberusClientMock.AnalysisResults[j].WorkingItemID.String()
	})
	assert.Equal(t, uuid.MustParse("55abb455-5c35-464a-aa9b-26ea5690c6ca"), cerberusClientMock.AnalysisResults[0].WorkingItemID)
	assert.Equal(t, analysisResultsWithoutAnalysisRequestsTest_instrument.ID, cerberusClientMock.AnalysisResults[0].InstrumentID)
	assert.Equal(t, analysisResultsWithoutAnalysisRequestsTest_analysisResults[1].Result, cerberusClientMock.AnalysisResults[0].Result)
	assert.NotEqual(t, AnalysisResultBatchItemInfo{}, cerberusClientMock.BatchResponse)
	assert.Equal(t, uuid.MustParse("660d1095-c8f6-4899-946e-935bfddfaa69"), cerberusClientMock.AnalysisResults[1].WorkingItemID)
	assert.Equal(t, analysisResultsWithoutAnalysisRequestsTest_instrument.ID, cerberusClientMock.AnalysisResults[1].InstrumentID)
	assert.Equal(t, analysisResultsWithoutAnalysisRequestsTest_analysisResults[0].Result, cerberusClientMock.AnalysisResults[1].Result)
	assert.Equal(t, true, cerberusClientMock.AnalysisResults[1].IsInvalid)
}

// Todo - Complete the test
func TestSubmitAnalysisResultWithRequests(t *testing.T) {
	dbConn, schemaName, pg, sqlConn := setupDbConnector("testSubmitAnalysisResultsWithRequests")

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

	analysisRepository := NewAnalysisRepository(dbConn, schemaName)
	instrumentRepository := NewInstrumentRepository(dbConn, schemaName)
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

	longPollClientMock := &longPollClientMock{}
	deaClientMock := &deaClientMock{}

	analysisService := NewAnalysisService(analysisRepository, deaClientMock, cerberusClientMock, skeletonManager)
	conditionRepository := NewConditionRepository(dbConn, schemaName)
	conditionService := NewConditionService(conditionRepository)
	sortingRuleRepository := NewSortingRuleRepository(dbConn, schemaName)
	sortingRuleService := NewSortingRuleService(analysisRepository, conditionService, sortingRuleRepository)
	instrumentService := NewInstrumentService(sortingRuleService, instrumentRepository, NewSkeletonManager(ctx), NewInstrumentCache(), cerberusClientMock)
	consoleLogService := NewConsoleLogService(cerberusClientMock)
	messageInRepository := NewMessageInRepository(dbConn, schemaName, 0, 0)
	messageOutRepository := NewMessageOutRepository(dbConn, schemaName, 0, 0)
	messageOutOrderRepository := NewMessageOutOrderRepository(dbConn, schemaName, 0)
	messageService := NewMessageService(deaClientMock, messageInRepository, messageOutRepository, messageOutOrderRepository, schemaName)
	skeletonInstance, _ := NewSkeleton(ctx, serviceName, displayName, []string{}, []string{}, []string{}, pg, dbConn, schemaName, migrator.NewSkeletonMigrator(), analysisRepository, analysisService, instrumentService, consoleLogService, messageService, skeletonManager, cerberusClientMock, longPollClientMock, deaClientMock, configuration)
	_, _ = sqlConn.Exec(fmt.Sprintf(`INSERT INTO %s.sk_supported_protocols (id, "name", description) VALUES ('9bec3063-435d-490f-bec0-88a6633ef4c2', 'IH-1000 v5.2', 'IHCOM');`, schemaName))

	go func() {
		_ = skeletonInstance.Start()
	}()

	time.Sleep(1 * time.Second)

	err := skeletonInstance.SubmitAnalysisResultBatch(context.TODO(), AnalysisResultSet{
		Results: analysisResultsWithoutAnalysisRequestsTest_analysisResults,
	})
	assert.Nil(t, err)

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
	err = analysisService.CreateAnalysisRequests(context.TODO(), analysisRequests)
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
		Encoding:           "UTF8",
		TimeZone:           "Europe/Budapest",
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
			AnalysisRequest: AnalysisRequest{},
			AnalyteMapping:  analyteMappings[0],
			Instrument:      instrument,
			SampleCode:      "testSampleCode",
			DEARawMessageID: uuid.NullUUID{
				UUID:  uuid.MustParse("2f369489-77d3-464e-87e2-edbeffa62ae7"),
				Valid: true,
			},
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
			AnalysisRequest: AnalysisRequest{},
			AnalyteMapping:  analyteMappings[0],
			Instrument:      instrument,
			SampleCode:      "testSampleCode2",
			DEARawMessageID: uuid.NullUUID{
				UUID:  uuid.MustParse("43a7b261-3e1d-4065-935a-ac15841f13e4"),
				Valid: true,
			},
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
	dbConn, schemaName, pg, _ := setupDbConnector("testAnalysisResultsReprocessing")

	configuration := config.Configuration{
		APIPort:                          5679,
		Authorization:                    false,
		PermittedOrigin:                  "*",
		ApplicationName:                  "Submit Analysis Request Parallel Test",
		TCPListenerPort:                  5401,
		InstrumentTransferRetryDelayInMs: 100,
		ResultTransferFlushTimeout:       5,
		ImageRetrySeconds:                60,
	}

	analysisRepositoryMock := &analysisRepositoryMock{}
	instrumentRepository := NewInstrumentRepository(dbConn, schemaName)
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
	longPollClient := &longPollClientMock{}
	deaClientMock := &deaClientMock{}

	analysisServiceMock := &analysisServiceMock{}
	conditionRepository := NewConditionRepository(dbConn, schemaName)
	conditionService := NewConditionService(conditionRepository)
	sortingRuleRepository := NewSortingRuleRepository(dbConn, schemaName)
	analysisRepository := NewAnalysisRepository(dbConn, schemaName)
	sortingRuleService := NewSortingRuleService(analysisRepository, conditionService, sortingRuleRepository)
	instrumentService := NewInstrumentService(sortingRuleService, instrumentRepository, NewSkeletonManager(ctx), NewInstrumentCache(), cerberusClientMock)
	consoleLogService := NewConsoleLogService(cerberusClientMock)
	messageInRepository := NewMessageInRepository(dbConn, schemaName, 0, 0)
	messageOutRepository := NewMessageOutRepository(dbConn, schemaName, 0, 0)
	messageOutOrderRepository := NewMessageOutOrderRepository(dbConn, schemaName, 0)
	messageService := NewMessageService(deaClientMock, messageInRepository, messageOutRepository, messageOutOrderRepository, schemaName)
	skeletonInstance, _ := NewSkeleton(ctx, serviceName, displayName, []string{}, []string{}, []string{}, pg, dbConn, schemaName, migrator.NewSkeletonMigrator(), analysisRepositoryMock, analysisServiceMock, instrumentService, consoleLogService, messageService, skeletonManager, cerberusClientMock, longPollClient, deaClientMock, configuration)
	go func() {
		_ = skeletonInstance.Start()
	}()

	time.Sleep(1 * time.Second)
}

func TestGenerateRawMessageFilename(t *testing.T) {
	ts, _ := time.Parse("2006-01-02-15:04:05.000000", "2025-02-18-20:11:24.123456")
	filename := generateRawMessageFileName("infinity", ts)
	assert.Equal(t, "infinity_2025-02-18-20-11-24_123456", filename)
	filename = generateRawMessageFileName("Sarstedt Host : The Sorter", ts)
	assert.Equal(t, "Sarstedt_Host_The_Sorter_2025-02-18-20-11-24_123456", filename)
}

func TestSubmitControlResultsProcessing(t *testing.T) {
	dbConn, schemaName, pg, _ := setupDbConnector("testSubmitControlResultsProcessing")

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

	analysisRepositoryMock := &analysisRepositoryMock{}
	conditionRepository := NewConditionRepository(dbConn, schemaName)
	instrumentRepository := NewInstrumentRepository(dbConn, schemaName)

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

	instrumentService := NewInstrumentService(sortingRuleService, instrumentRepository, skeletonManagerMock, NewInstrumentCache(), cerberusClientMock)
	consoleLogService := NewConsoleLogService(cerberusClientMock)
	messageInRepository := NewMessageInRepository(dbConn, schemaName, 0, 0)
	messageOutRepository := NewMessageOutRepository(dbConn, schemaName, 0, 0)
	messageOutOrderRepository := NewMessageOutOrderRepository(dbConn, schemaName, 0)
	messageService := NewMessageService(deaClientMock, messageInRepository, messageOutRepository, messageOutOrderRepository, schemaName)

	skeletonInstance, _ := NewSkeleton(ctx, serviceName, displayName, []string{}, []string{}, []string{}, pg, dbConn, schemaName, migrator.NewSkeletonMigrator(), analysisRepositoryMock, analysisService, instrumentService, consoleLogService, messageService, skeletonManagerMock, cerberusClientMock, &longPollClientMock{AnalysisRequests: analysisResultsWithoutAnalysisRequestsTest_AnalysisRequests}, deaClientMock, configuration)
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
		Encoding:           "UTF8",
		TimeZone:           "Europe/Budapest",
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
		Name:           "Reagent name",
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
			ID:              analysisResultId1,
			AnalysisRequest: AnalysisRequest{},
			AnalyteMapping:  analyteMappings[0],
			Instrument:      instrument,
			SampleCode:      "testSampleCode",
			DEARawMessageID: uuid.NullUUID{
				UUID:  uuid.MustParse("2f369489-77d3-464e-87e2-edbeffa62ae7"),
				Valid: true,
			},
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
			ID:              analysisResultId2,
			AnalysisRequest: AnalysisRequest{},
			AnalyteMapping:  analyteMappings[0],
			Instrument:      instrument,
			SampleCode:      "testSampleCode2",
			DEARawMessageID: uuid.NullUUID{
				UUID:  uuid.MustParse("43a7b261-3e1d-4065-935a-ac15841f13e4"),
				Valid: true,
			},
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

	analysisRequests := []AnalysisRequest{
		{
			ID:                          uuid.MustParse("4a7a8b97-59f5-46bc-9fab-0674504e4cc6"),
			WorkItemID:                  uuid.UUID{},
			AnalyteID:                   analyteMappings[0].AnalyteID,
			SampleCode:                  "testSampleCode",
			MaterialID:                  uuid.UUID{},
			LaboratoryID:                uuid.UUID{},
			ValidUntilTime:              time.Time{},
			CreatedAt:                   time.Time{},
			ModifiedAt:                  nil,
			SubjectInfo:                 nil,
			ExtraValues:                 nil,
			ReexaminationRequestedCount: 0,
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

	time.Sleep(6 * time.Second)

	skeletonManagerMock.AnalysisResultsForProcessing = make([]AnalysisResult, 0)

	analysisRepositoryMock.analysisResultsById = analysisResults
	analysisRepositoryMock.analysisRequests = analysisRequests
	err = skeletonInstance.SubmitControlResults(context.TODO(), []StandaloneControlResult{
		{
			ControlResult: controlResult,
			Reagents:      []Reagent{reagent},
			ResultIDs:     []uuid.UUID{analysisResultId1, analysisResultId2},
		},
	})
	assert.Nil(t, err)

	time.Sleep(6 * time.Second)
	assert.Equal(t, 2, len(skeletonManagerMock.AnalysisResultsForProcessing))

	analysisRepositoryMock.analysisResultsById = make([]AnalysisResult, 0)
	analysisRepositoryMock.analysisRequests = make([]AnalysisRequest, 0)
	skeletonManagerMock.AnalysisResultsForProcessing = make([]AnalysisResult, 0)

	err = skeletonInstance.SubmitControlResults(context.TODO(), []StandaloneControlResult{
		{
			ControlResult: controlResult,
			Reagents:      []Reagent{reagent},
			ResultIDs:     []uuid.UUID{},
		},
	})
	assert.Nil(t, err)

	time.Sleep(6 * time.Second)
	assert.Equal(t, 0, len(skeletonManagerMock.AnalysisResultsForProcessing))

	analysisRepositoryMock.analysisResultsById = analysisResults
	analysisRepositoryMock.analysisRequests = analysisRequests
	skeletonManagerMock.AnalysisResultsForProcessing = make([]AnalysisResult, 0)
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

	time.Sleep(6 * time.Second)
	assert.Equal(t, 2, len(skeletonManagerMock.AnalysisResultsForProcessing))
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

type longPollClientMock struct {
	InstrumentMessage         InstrumentMessageTO
	ReprocessMessage          ReprocessMessageTO
	AnalysisRequests          []AnalysisRequest
	ReexaminedWorkItemIDs     []uuid.UUID
	RevokedWorkItemIDs        []uuid.UUID
	ExpectedControlResults    []ExpectedControlResultMessageTO
	instrumentConfigsChan     chan InstrumentMessageTO
	reprocessEventChan        chan ReprocessMessageTO
	analysisRequestChan       chan []AnalysisRequest
	reexaminedWorkItemIDsChan chan []uuid.UUID
	revokedWorkItemIDsChan    chan []uuid.UUID
	expectedControlResultChan chan ExpectedControlResultMessageTO
}

func (m *longPollClientMock) GetExpectedControlResultsChan() chan ExpectedControlResultMessageTO {
	if m.expectedControlResultChan == nil {
		m.expectedControlResultChan = make(chan ExpectedControlResultMessageTO)
	}
	return m.expectedControlResultChan
}

func (m *longPollClientMock) StartExpectedControlResultsLongPolling(ctx context.Context) {
	if len(m.ExpectedControlResults) > 0 {
		for _, expectedControlResult := range m.ExpectedControlResults {
			m.GetExpectedControlResultsChan() <- expectedControlResult
		}
	}
}

func (m *longPollClientMock) GetInstrumentConfigsChan() chan InstrumentMessageTO {
	if m.instrumentConfigsChan == nil {
		m.instrumentConfigsChan = make(chan InstrumentMessageTO)
	}
	return m.instrumentConfigsChan
}

func (m *longPollClientMock) GetReprocessEventsChan() chan ReprocessMessageTO {
	if m.reprocessEventChan == nil {
		m.reprocessEventChan = make(chan ReprocessMessageTO)
	}
	return m.reprocessEventChan
}

func (m *longPollClientMock) GetAnalysisRequestsChan() chan []AnalysisRequest {
	if m.analysisRequestChan == nil {
		m.analysisRequestChan = make(chan []AnalysisRequest, 10)
	}
	return m.analysisRequestChan
}
func (m *longPollClientMock) GetRevokedWorkItemIDsChan() chan []uuid.UUID {
	if m.revokedWorkItemIDsChan == nil {
		m.revokedWorkItemIDsChan = make(chan []uuid.UUID, 10)
	}
	return m.revokedWorkItemIDsChan
}
func (m *longPollClientMock) GetReexaminedWorkItemIDsChan() chan []uuid.UUID {
	if m.reexaminedWorkItemIDsChan == nil {
		m.reexaminedWorkItemIDsChan = make(chan []uuid.UUID, 10)
	}
	return m.reexaminedWorkItemIDsChan
}

func (m *longPollClientMock) StartInstrumentConfigsLongPolling(ctx context.Context) {
	m.GetInstrumentConfigsChan() <- m.InstrumentMessage
}

func (m *longPollClientMock) StartReprocessEventsLongPolling(ctx context.Context) {
	m.GetReprocessEventsChan() <- m.ReprocessMessage
}

func (m *longPollClientMock) StartAnalysisRequestLongPolling(ctx context.Context) {
	if len(m.AnalysisRequests) > 0 {
		m.GetAnalysisRequestsChan() <- m.AnalysisRequests
	}
}
func (m *longPollClientMock) StartRevokedReexaminedWorkItemIDsLongPolling(ctx context.Context) {
	if len(m.RevokedWorkItemIDs) > 0 {
		m.GetRevokedWorkItemIDsChan() <- m.RevokedWorkItemIDs
	}
	if len(m.ReexaminedWorkItemIDs) > 0 {
		m.GetReexaminedWorkItemIDsChan() <- m.ReexaminedWorkItemIDs
	}
}

type deaClientMock struct {
}

func (m *deaClientMock) UploadFile(fileData []byte, name string) (uuid.UUID, error) {
	return uuid.Nil, nil
}

func (m *deaClientMock) UploadImage(fileData []byte, name string) (uuid.UUID, error) {
	return uuid.New(), nil
}

type cerberusClientMock struct {
	registerInstrumentFunc              func(instrument Instrument) error
	registerInstrumentDriverFunc        func(name, displayName string, extraValueKeys []string, protocols []supportedProtocolTO, tests []supportedManufacturerTestTO, encodings []string, reagentManufacturers []string) error
	sendAnalysisResultBatchFunc         func(analysisResults []AnalysisResultTO) (AnalysisResultBatchResponse, error)
	sendControlResultBatchFunc          func(controlResults []StandaloneControlResultTO) (ControlResultBatchResponse, error)
	sendAnalysisResultImageBatchFunc    func(images []WorkItemResultImageTO) error
	verifyInstrumentHashFunc            func(hash string) error
	verifyExpectedControlResultHashFunc func(hash string) error

	AnalysisResults                     []AnalysisResultTO
	ControlResults                      []StandaloneControlResultTO
	BatchResponse                       AnalysisResultBatchResponse
	ControlBatchResponse                ControlResultBatchResponse
	VerifiedInstrumentHashes            []string
	VerifiedExpectedControlResultHashes []string
}

func (m *cerberusClientMock) VerifyExpectedControlResultsHash(hash string) error {
	if m.verifyExpectedControlResultHashFunc == nil {
		return nil
	}
	err := m.verifyExpectedControlResultHashFunc(hash)
	if err == nil {
		m.VerifiedExpectedControlResultHashes = append(m.VerifiedExpectedControlResultHashes, hash)
	}
	return err
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

func (m *cerberusClientMock) RegisterInstrumentDriver(name, displayName string, extraValueKeys []string, protocols []supportedProtocolTO, tests []supportedManufacturerTestTO, encodings []string, reagentManufacturers []string) error {
	if m.registerInstrumentDriverFunc == nil {
		return nil
	}
	return m.registerInstrumentDriverFunc(name, displayName, extraValueKeys, protocols, tests, encodings, reagentManufacturers)
}

func (m *cerberusClientMock) VerifyInstrumentHash(hash string) error {
	if m.verifyInstrumentHashFunc == nil {
		return nil
	}
	err := m.verifyInstrumentHashFunc(hash)
	if err == nil {
		m.VerifiedInstrumentHashes = append(m.VerifiedInstrumentHashes, hash)
	}
	return err
}

func (m *cerberusClientMock) SyncAnalysisRequests(workItemIDs []uuid.UUID, syncType string) error {
	return nil
}

func (m *cerberusClientMock) SendConsoleLog(instrumentId uuid.UUID, logLevel LogLevel, message string, messageType string) error {
	return nil
}

func (m *cerberusClientMock) RegisterManufacturerTests(driverName string, tests []supportedManufacturerTestTO) error {
	return nil
}

func (m *cerberusClientMock) SetInstrumentOnlineStatus(instrumentId uuid.UUID, status InstrumentStatus) error {
	return nil
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

func (m *analysisServiceMock) SaveCerberusIDsForAnalysisResultBatchItems(ctx context.Context, analysisResults []AnalysisResultBatchItemInfo) {
}

func (m *analysisServiceMock) CreateAnalysisRequests(ctx context.Context, analysisRequests []AnalysisRequest) error {
	return nil
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
func (m *analysisServiceMock) GetAnalysisResultsByIDsWithRecalculatedStatus(ctx context.Context, analysisResultIDs []uuid.UUID, reValidateControlResult bool) ([]AnalysisResult, error) {
	return nil, nil
}
func (m *analysisServiceMock) ValidateAndUpdatingExistingControlResults(ctx context.Context, analyteMappingIds []uuid.UUID) error {
	return nil
}
func (m *analysisServiceMock) AnalysisResultStatusRecalculationAndSendForProcessingIfFinal(ctx context.Context, controlResultIds []uuid.UUID) error {
	return nil
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
	callCount           int
	analysisResultsById []AnalysisResult
	analysisRequests    []AnalysisRequest
	savedWorkItemIDs    map[uuid.UUID]any
}

func (m *analysisRepositoryMock) UpdateAnalysisResultDEARawMessageID(ctx context.Context, analysisResultID uuid.UUID, deaRawMessageID uuid.NullUUID) error {
	return nil
}

func (m *analysisRepositoryMock) DeleteOldCerberusQueueItems(ctx context.Context, cleanupDays, limit int) (int64, error) {
	return 0, nil
}

func (m *analysisRepositoryMock) DeleteOldAnalysisRequestsWithTx(ctx context.Context, cleanupDays, limit int, tx db.DbConnection) (int64, error) {
	return 0, nil
}

func (m *analysisRepositoryMock) DeleteOldAnalysisResultsWithTx(ctx context.Context, cleanupDays, limit int, tx db.DbConnection) (int64, error) {
	return 0, nil
}

func (m *analysisRepositoryMock) UpdateCerberusQueueItemStatus(ctx context.Context, queueItem CerberusQueueItem) error {
	return nil
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

func (m *analysisRepositoryMock) CreateControlResultBatch(ctx context.Context, controlResults []ControlResult) ([]ControlResult, error) {
	return []ControlResult{}, nil
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

func (m *analysisRepositoryMock) GetAnalysisResultIdsWithoutControlByReagent(ctx context.Context, controlResult ControlResult, reagent Reagent) ([]uuid.UUID, error) {
	return nil, nil
}

func (m *analysisRepositoryMock) GetAnalysisResultIdsWhereLastestControlIsInvalid(ctx context.Context, controlResult ControlResult, reagent Reagent) ([]uuid.UUID, error) {
	return nil, nil
}

func (m *analysisRepositoryMock) GetLatestControlResultIdByReagent(ctx context.Context, reagent Reagent, resultYieldTime *time.Time) (ControlResult, error) {
	return ControlResult{}, nil
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
	if len(m.analysisResultsById) > 0 {
		return m.analysisResultsById, nil
	}
	return make([]AnalysisResult, len(ids)), nil
}

func (m *analysisRepositoryMock) CreateAnalysisRequestsBatch(ctx context.Context, analysisRequests []AnalysisRequest) ([]uuid.UUID, error) {
	if m.savedWorkItemIDs == nil {
		m.savedWorkItemIDs = make(map[uuid.UUID]any)
	}
	analysisRequestIDs := make([]uuid.UUID, 0)
	for i := range analysisRequests {
		if _, ok := m.savedWorkItemIDs[analysisRequests[i].WorkItemID]; !ok {
			m.savedWorkItemIDs[analysisRequests[i].WorkItemID] = nil
			analysisRequestIDs = append(analysisRequestIDs, analysisRequests[i].ID)
		}
	}
	return analysisRequestIDs, nil
}
func (m *analysisRepositoryMock) GetAnalysisRequestsBySampleCodeAndAnalyteID(ctx context.Context, sampleCodes string, analyteID uuid.UUID) ([]AnalysisRequest, error) {
	if len(m.analysisRequests) > 0 {
		return m.analysisRequests, nil
	}
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
	for i := range analysisResults {
		analysisResults[i].ID = uuid.New()
	}
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
func (m *analysisRepositoryMock) GetAnalysisResultByCerberusID(ctx context.Context, id uuid.UUID, allowDeletedAnalyteMapping bool) (AnalysisResult, error) {
	return AnalysisResult{}, nil
}
func (m *analysisRepositoryMock) GetAnalysisResultsByBatchIDs(ctx context.Context, batchIDs []uuid.UUID) ([]AnalysisResult, error) {
	return nil, nil
}
func (m *analysisRepositoryMock) GetAnalysisResultIdsForStatusRecalculationByControlIds(ctx context.Context, controlResultIds []uuid.UUID) ([]uuid.UUID, error) {
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
	uniqueReagentToIncomingIndexMap := make(map[string]uuid.UUID)
	for analysisResultId, reagents := range reagentsByAnalysisResultID {
		reagentArray := make([]Reagent, 0)
		for i, reagent := range reagents {
			if _, ok := uniqueReagentToIncomingIndexMap[getUniqueReagentString(convertReagentToDAO(reagent))]; !ok {
				newReagentId := uuid.New()
				reagentsByAnalysisResultID[analysisResultId][i].ID = newReagentId
				uniqueReagentToIncomingIndexMap[getUniqueReagentString(convertReagentToDAO(reagent))] = newReagentId
			} else {
				reagentsByAnalysisResultID[analysisResultId][i].ID = uniqueReagentToIncomingIndexMap[getUniqueReagentString(convertReagentToDAO(reagent))]
			}
			reagentArray = append(reagentArray, reagentsByAnalysisResultID[analysisResultId][i])
			controlResults := make([]ControlResult, 0)
			for j := range reagent.ControlResults {
				reagentsByAnalysisResultID[analysisResultId][i].ControlResults[j].ID = uuid.New()
				controlResults = append(controlResults, reagentsByAnalysisResultID[analysisResultId][i].ControlResults[j])
			}
			if _, ok := controlResultsMap[analysisResultId]; !ok {
				controlResultsMap[analysisResultId] = make(map[uuid.UUID][]ControlResult)
			}
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
		controlResultIdsMap[analysisResultId] = make(map[uuid.UUID][]uuid.UUID)
		for reagentId, controlResults := range reagentMap {
			controlResultIds := make([]uuid.UUID, 0)
			for i := 0; i < len(controlResults); i++ {
				controlResultIds = append(controlResultIds, uuid.New())
			}
			controlResultIdsMap[analysisResultId][reagentId] = controlResultIds
		}
	}
	return controlResultIdsMap, nil
}
func (m *analysisRepositoryMock) UpdateControlResultBatch(ctx context.Context, controlResults []ControlResult) error {
	return nil
}
func (m *analysisRepositoryMock) CreateWarnings(ctx context.Context, warningsByAnalysisResultID map[uuid.UUID][]string) error {
	return nil
}
func (m *analysisRepositoryMock) GetAnalysisResultsByBatchIDsMapped(ctx context.Context, batchIDs []uuid.UUID) (map[uuid.UUID][]AnalysisResultInfo, error) {
	return nil, nil
}
func (m *analysisRepositoryMock) GetAnalysisRequestExtraValuesByAnalysisRequestID(ctx context.Context, analysisRequestID uuid.UUID) (map[string]string, error) {
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

func (m *analysisRepositoryMock) GetLatestControlResultsByReagent(ctx context.Context, reagent Reagent, resultYieldTime *time.Time, analyteMappingId uuid.UUID, instrumentId uuid.UUID) ([]ControlResult, error) {
	return nil, nil
}
func (m *analysisRepositoryMock) GetControlResultsToValidate(ctx context.Context, analyteMappingIds []uuid.UUID) ([]ControlResult, error) {
	return nil, nil
}
func (m *analysisRepositoryMock) MarkAnalysisRequestsAsProcessed(ctx context.Context, analysisRequestIDs []uuid.UUID) error {
	return nil
}
func (m *analysisRepositoryMock) MarkAnalysisResultsAsProcessed(ctx context.Context, analysisRequestIDs []uuid.UUID) error {
	return nil
}
func (m *analysisRepositoryMock) CreateTransaction() (db.DbConnection, error) {
	return db.NewDbConnection(), nil
}
func (m *analysisRepositoryMock) WithTransaction(tx db.DbConnection) AnalysisRepository {
	return m
}

var analysisResultsWithoutAnalysisRequestsTest_analysisResults = []AnalysisResult{
	{
		AnalysisRequest: AnalysisRequest{},
		AnalyteMapping:  analysisResultsWithoutAnalysisRequestsTest_analyteMappings[0],
		Instrument:      analysisResultsWithoutAnalysisRequestsTest_instrument,
		SampleCode:      "TestSampleCode1",
		DEARawMessageID: uuid.NullUUID{
			UUID:  uuid.MustParse("2f369489-77d3-464e-87e2-edbeffa62ae7"),
			Valid: true,
		},
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
				Name:           "name",
				Type:           Standard,
				ExpirationDate: nil,
				ControlResults: nil,
			},
		},
		Images: []Image{},
	},
	{
		AnalysisRequest: AnalysisRequest{},
		AnalyteMapping:  analysisResultsWithoutAnalysisRequestsTest_analyteMappings[0],
		Instrument:      analysisResultsWithoutAnalysisRequestsTest_instrument,
		SampleCode:      "TestSampleCode2",
		DEARawMessageID: uuid.NullUUID{
			UUID:  uuid.MustParse("43a7b261-3e1d-4065-935a-ac15841f13e4"),
			Valid: true,
		},
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
	Encoding:           "UTF8",
	TimeZone:           "Europe/Budapest",
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

var analysisResultsWithoutAnalysisRequestsTest_AnalysisRequests = []AnalysisRequest{
	{
		WorkItemID:     uuid.MustParse("660d1095-c8f6-4899-946e-935bfddfaa69"),
		AnalyteID:      uuid.MustParse("51bfea41-1b7e-48f7-8b35-46d930216de7"),
		SampleCode:     "TestSampleCode1",
		MaterialID:     uuid.MustParse("50820d7e-3f5b-4452-aa4b-bebfc3c15002"),
		LaboratoryID:   uuid.MustParse("3072973a-25d7-43d0-840f-d5a3de8e3aa5"),
		ValidUntilTime: time.Now().AddDate(1, 0, 0),
	},
	{
		WorkItemID:     uuid.MustParse("55abb455-5c35-464a-aa9b-26ea5690c6ca"),
		AnalyteID:      uuid.MustParse("51bfea41-1b7e-48f7-8b35-46d930216de7"),
		SampleCode:     "TestSampleCode2",
		MaterialID:     uuid.MustParse("50820d7e-3f5b-4452-aa4b-bebfc3c15002"),
		LaboratoryID:   uuid.MustParse("3072973a-25d7-43d0-840f-d5a3de8e3aa5"),
		ValidUntilTime: time.Now().AddDate(1, 0, 0),
	},
}
