package skeleton

import (
	"context"
	"errors"
	"github.com/blutspende/skeleton/db"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestCreateUpdateDeleteFtpConfig(t *testing.T) {
	dbConn, schemaName := setupDbConnectorAndRunMigration("instrument_test")

	cerberusClientMock := &cerberusClientMock{}
	instrumentRepository := NewInstrumentRepository(dbConn, schemaName)

	ctx, cancelSkeleton := context.WithCancel(context.Background())
	defer cancelSkeleton()

	conditionRepository := NewConditionRepository(dbConn, schemaName)
	conditionService := NewConditionService(conditionRepository)
	sortingRuleRepository := NewSortingRuleRepository(dbConn, schemaName)
	analysisRepository := NewAnalysisRepository(dbConn, schemaName)
	sortingRuleService := NewSortingRuleService(analysisRepository, conditionService, sortingRuleRepository)
	instrumentService := NewInstrumentService(sortingRuleService, instrumentRepository, NewSkeletonManager(ctx), NewInstrumentCache(), cerberusClientMock)

	clientPort := 22
	instrumentWithFtp := Instrument{
		//_ = Instrument{
		Name:           "TestInstrument",
		Type:           Analyzer,
		ProtocolID:     uuid.MustParse("abb539a3-286f-4c15-a7b7-2e9adf6eab91"),
		ProtocolName:   "Test Protocol",
		Enabled:        true,
		ConnectionMode: FTP,
		ResultMode:     Production,
		Status:         "ONLINE",
		Encoding:       "UTF8",
		TimeZone:       "Europe/Budapest",
		Hostname:       "192.168.1.20",
		ClientPort:     &clientPort,
		FTPConfig: &FTPConfig{
			Username:         "test",
			Password:         "test",
			OrderPath:        "/remote",
			OrderFileMask:    "*.EXP",
			OrderFileSuffix:  ".*",
			ResultPath:       "/result",
			ResultFileMask:   "*",
			ResultFileSuffix: ".TPL",
			FtpServerType:    "ftp",
		},
	}

	ctx = context.Background()
	ctx = context.WithValue(ctx, "Authorization", "BearerToken")
	instrumentId, err := instrumentService.CreateInstrument(ctx, instrumentWithFtp)
	assert.Nil(t, err)

	//instrumentId, _ := uuid.NewUUID()
	// test that the FTP config created properly
	instrument, err := instrumentService.GetInstrumentByID(ctx, nil, instrumentId, false)
	assert.Nil(t, err)
	assert.Equal(t, "TestInstrument", instrument.Name)
	assert.Equal(t, FTP, instrument.ConnectionMode)
	assert.NotNil(t, instrument.FTPConfig)
	assert.Equal(t, instrumentId, instrument.FTPConfig.InstrumentId)
	assert.Equal(t, "test", instrument.FTPConfig.Username)
	assert.Equal(t, "test", instrument.FTPConfig.Password)
	assert.Equal(t, "/remote", instrument.FTPConfig.OrderPath)
	assert.Equal(t, "*.EXP", instrument.FTPConfig.OrderFileMask)
	assert.Equal(t, ".*", instrument.FTPConfig.OrderFileSuffix)
	assert.Equal(t, "/result", instrument.FTPConfig.ResultPath)
	assert.Equal(t, "*", instrument.FTPConfig.ResultFileMask)
	assert.Equal(t, ".TPL", instrument.FTPConfig.ResultFileSuffix)
	assert.Equal(t, "ftp", instrument.FTPConfig.FtpServerType)

	err = instrumentService.UpdateInstrument(ctx, Instrument{
		ID:             instrumentId,
		Name:           "TestInstrument",
		Type:           Analyzer,
		ProtocolID:     uuid.MustParse("abb539a3-286f-4c15-a7b7-2e9adf6eab91"),
		ProtocolName:   "Test Protocol",
		Enabled:        true,
		ConnectionMode: FTP,
		ResultMode:     Production,
		Status:         "ONLINE",
		Encoding:       "UTF8",
		TimeZone:       "Europe/Budapest",
		Hostname:       "192.168.1.20",
		ClientPort:     &clientPort,
		FTPConfig: &FTPConfig{
			InstrumentId:     instrumentId,
			Username:         "updatedUsername",
			Password:         "updatesPass",
			OrderPath:        "/remote/updated",
			OrderFileMask:    "*.updated.EXP",
			OrderFileSuffix:  "updated.*",
			ResultPath:       "/result/updated",
			ResultFileMask:   "*updated*",
			ResultFileSuffix: ".updated.TPL",
			FtpServerType:    "sftp",
		},
	}, uuid.MustParse("9d5fb5e9-65a1-4479-8f82-25b04145bfe1"))
	assert.Nil(t, err)

	// test that the FTP config updated properly
	instrument, err = instrumentService.GetInstrumentByID(ctx, nil, instrumentId, false)
	assert.Nil(t, err)
	assert.NotNil(t, instrument.FTPConfig)
	assert.Equal(t, instrumentId, instrument.FTPConfig.InstrumentId)
	assert.Equal(t, "updatedUsername", instrument.FTPConfig.Username)
	assert.Equal(t, "updatesPass", instrument.FTPConfig.Password)
	assert.Equal(t, "/remote/updated", instrument.FTPConfig.OrderPath)
	assert.Equal(t, "*.updated.EXP", instrument.FTPConfig.OrderFileMask)
	assert.Equal(t, "updated.*", instrument.FTPConfig.OrderFileSuffix)
	assert.Equal(t, "/result/updated", instrument.FTPConfig.ResultPath)
	assert.Equal(t, "*updated*", instrument.FTPConfig.ResultFileMask)
	assert.Equal(t, ".updated.TPL", instrument.FTPConfig.ResultFileSuffix)
	assert.Equal(t, "sftp", instrument.FTPConfig.FtpServerType)

	err = instrumentService.DeleteInstrument(ctx, instrumentId)
	assert.Nil(t, err)

	// test that the FTP config deleted when instrument deleted
	_, err = instrumentRepository.GetFtpConfigByInstrumentId(ctx, instrumentId)
	assert.Equal(t, err, ErrFtpConfigNotFound)
}

func TestFtpConfigConnectionModeChange(t *testing.T) {
	dbConn, schemaName := setupDbConnectorAndRunMigration("instrument_test")

	cerberusClientMock := &cerberusClientMock{}
	instrumentRepository := NewInstrumentRepository(dbConn, schemaName)

	ctx, cancelSkeleton := context.WithCancel(context.Background())
	defer cancelSkeleton()

	conditionRepository := NewConditionRepository(dbConn, schemaName)
	conditionService := NewConditionService(conditionRepository)
	sortingRuleRepository := NewSortingRuleRepository(dbConn, schemaName)
	analysisRepository := NewAnalysisRepository(dbConn, schemaName)
	sortingRuleService := NewSortingRuleService(analysisRepository, conditionService, sortingRuleRepository)
	instrumentService := NewInstrumentService(sortingRuleService, instrumentRepository, NewSkeletonManager(ctx), NewInstrumentCache(), cerberusClientMock)

	clientPort := 22
	ctx = context.Background()
	ctx = context.WithValue(ctx, "Authorization", "BearerToken")

	instrumentId, err := instrumentService.CreateInstrument(ctx, Instrument{
		Name:           "TestInstrument",
		Type:           Analyzer,
		ProtocolID:     uuid.MustParse("abb539a3-286f-4c15-a7b7-2e9adf6eab91"),
		ProtocolName:   "Test Protocol",
		Enabled:        true,
		ConnectionMode: TCPClientMode,
		ResultMode:     Production,
		Status:         "ONLINE",
		Encoding:       "UTF8",
		TimeZone:       "Europe/Budapest",
		Hostname:       "192.168.1.20",
		ClientPort:     &clientPort,
	})
	assert.Nil(t, err)

	// instrument created, but FTP config not
	instrument, err := instrumentService.GetInstrumentByID(ctx, nil, instrumentId, false)
	assert.Nil(t, err)
	assert.Equal(t, "TestInstrument", instrument.Name)
	assert.Equal(t, TCPClientMode, instrument.ConnectionMode)
	assert.Nil(t, instrument.FTPConfig)

	err = instrumentService.UpdateInstrument(ctx, Instrument{
		ID:             instrumentId,
		Name:           "TestInstrument",
		Type:           Analyzer,
		ProtocolID:     uuid.MustParse("abb539a3-286f-4c15-a7b7-2e9adf6eab91"),
		ProtocolName:   "Test Protocol",
		Enabled:        true,
		ConnectionMode: FTP,
		ResultMode:     Production,
		Status:         "ONLINE",
		Encoding:       "UTF8",
		TimeZone:       "Europe/Budapest",
		Hostname:       "192.168.1.20",
		ClientPort:     &clientPort,
		FTPConfig: &FTPConfig{
			InstrumentId:     instrumentId,
			Username:         "test",
			Password:         "test",
			OrderPath:        "/remote",
			OrderFileMask:    "*.EXP",
			OrderFileSuffix:  "",
			ResultPath:       "/result",
			ResultFileMask:   "",
			ResultFileSuffix: ".TPL",
			FtpServerType:    "ftp",
		},
	}, uuid.MustParse("9d5fb5e9-65a1-4479-8f82-25b04145bfe1"))
	assert.Nil(t, err)

	// test that FTP config created after connection mode updated to FTP and config passed
	instrument, err = instrumentService.GetInstrumentByID(ctx, nil, instrumentId, false)
	assert.Nil(t, err)
	assert.Equal(t, FTP, instrument.ConnectionMode)
	assert.NotNil(t, instrument.FTPConfig)
	assert.Equal(t, instrumentId, instrument.FTPConfig.InstrumentId)
	assert.Equal(t, "test", instrument.FTPConfig.Username)
	assert.Equal(t, "test", instrument.FTPConfig.Password)
	assert.Equal(t, "/remote", instrument.FTPConfig.OrderPath)
	assert.Equal(t, "*.EXP", instrument.FTPConfig.OrderFileMask)
	assert.Equal(t, "", instrument.FTPConfig.OrderFileSuffix)
	assert.Equal(t, "/result", instrument.FTPConfig.ResultPath)
	assert.Equal(t, "", instrument.FTPConfig.ResultFileMask)
	assert.Equal(t, ".TPL", instrument.FTPConfig.ResultFileSuffix)
	assert.Equal(t, "ftp", instrument.FTPConfig.FtpServerType)

	err = instrumentService.UpdateInstrument(ctx, Instrument{
		ID:             instrumentId,
		Name:           "TestInstrument",
		Type:           Analyzer,
		ProtocolID:     uuid.MustParse("abb539a3-286f-4c15-a7b7-2e9adf6eab91"),
		ProtocolName:   "Test Protocol",
		Enabled:        true,
		ConnectionMode: TCPMixed,
		ResultMode:     Production,
		Status:         "ONLINE",
		Encoding:       "UTF8",
		TimeZone:       "Europe/Budapest",
		Hostname:       "192.168.1.20",
		ClientPort:     &clientPort,
	}, uuid.MustParse("9d5fb5e9-65a1-4479-8f82-25b04145bfe1"))
	assert.Nil(t, err)

	// test that FTP config deleted after the instrument update
	_, err = instrumentRepository.GetFtpConfigByInstrumentId(ctx, instrumentId)
	assert.Equal(t, err, ErrFtpConfigNotFound)
}

func TestUpdateInstrument(t *testing.T) {
	dbConn, schemaName := setupDbConnectorAndRunMigration("instrument_test")

	cerberusClientMock := &cerberusClientMock{
		verifyInstrumentHashFunc: func(hash string) error {
			return nil
		},
	}
	instrumentRepository := NewInstrumentRepository(dbConn, schemaName)

	ctx, cancelSkeleton := context.WithCancel(context.Background())
	defer cancelSkeleton()

	conditionRepository := NewConditionRepository(dbConn, schemaName)
	conditionService := NewConditionService(conditionRepository)
	sortingRuleRepository := NewSortingRuleRepository(dbConn, schemaName)
	analysisRepository := NewAnalysisRepository(dbConn, schemaName)
	sortingRuleService := NewSortingRuleService(analysisRepository, conditionService, sortingRuleRepository)
	instrumentService := NewInstrumentService(sortingRuleService, instrumentRepository, NewSkeletonManager(ctx), NewInstrumentCache(), cerberusClientMock)

	var protocolID uuid.UUID
	err := dbConn.QueryRowx(`INSERT INTO instrument_test.sk_supported_protocols (name, description) VALUES('TestProtocol', 'Test Protocol Description') RETURNING id;`).Scan(&protocolID)
	assert.Nil(t, err)

	clientPort := 1234

	ctx = context.Background()
	ctx = context.WithValue(ctx, "Authorization", "BearerToken")

	instr := Instrument{
		ID:                 uuid.New(),
		Name:               "TestInstrument",
		Type:               Analyzer,
		ProtocolID:         protocolID,
		ProtocolName:       "TestProtocol",
		Enabled:            true,
		ConnectionMode:     TCPMixed,
		ResultMode:         Simulation,
		CaptureResults:     true,
		CaptureDiagnostics: false,
		ReplyToQuery:       false,
		Status:             "TestStatus",
		Encoding:           "UTF8",
		TimeZone:           "Europe/Berlin",
		Hostname:           "TestHost",
		ClientPort:         &clientPort,
	}
	instrumentID, err := instrumentService.CreateInstrument(ctx, instr)
	assert.Nil(t, err)

	instrument, err := instrumentService.GetInstrumentByID(ctx, nil, instrumentID, false)
	assert.Nil(t, err)
	assert.Equal(t, "TestInstrument", instrument.Name)
	assert.Equal(t, protocolID, instrument.ProtocolID)
	assert.Contains(t, cerberusClientMock.VerifiedInstrumentHashes, HashInstrument(instr))

	analyteID1 := uuid.New()
	controlAnalyteID1 := uuid.New()
	analyteMappingID1 := uuid.New()
	controlAnalyteMappingID1 := uuid.New()
	channelID1 := uuid.New()
	controlMappingID1 := uuid.New()

	instr = Instrument{
		ID:                 instrumentID,
		Name:               "TestInstrumentUpdated",
		Type:               Analyzer,
		ProtocolID:         protocolID,
		ProtocolName:       "TestProtocol",
		Enabled:            true,
		ConnectionMode:     TCPMixed,
		ResultMode:         Simulation,
		CaptureResults:     true,
		CaptureDiagnostics: false,
		ReplyToQuery:       false,
		Status:             "TestStatus",
		Encoding:           "UTF8",
		TimeZone:           "Europe/Berlin",
		Hostname:           "TestHost",
		ClientPort:         &clientPort,
		AnalyteMappings: []AnalyteMapping{
			{
				ID:                analyteMappingID1,
				InstrumentAnalyte: "TESTANALYTE",
				AnalyteID:         analyteID1,
				ChannelMappings: []ChannelMapping{
					{
						InstrumentChannel: "TestInstrumentChannel",
						ChannelID:         channelID1,
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
				ResultType:  "pein",
				AnalyteType: Result,
			},
			{
				ID:                controlAnalyteMappingID1,
				InstrumentAnalyte: "TESTCONTROLANALYTE",
				AnalyteID:         controlAnalyteID1,
				ChannelMappings: []ChannelMapping{
					{
						InstrumentChannel: "TestInstrumentChannel",
						ChannelID:         channelID1,
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
				ResultType:  "pein",
				AnalyteType: Control,
			},
		},
		RequestMappings: []RequestMapping{
			{
				Code:      "ReqMap",
				IsDefault: true,
				AnalyteIDs: []uuid.UUID{
					analyteID1,
				},
			},
		},
		ControlMappings: []ControlMapping{
			{
				ID:        controlMappingID1,
				AnalyteID: analyteID1,
				ControlAnalyteIDs: []uuid.UUID{
					controlAnalyteID1,
				},
			},
		},
	}
	err = instrumentService.UpdateInstrument(ctx, instr, uuid.MustParse("9d5fb5e9-65a1-4479-8f82-25b04145bfe1"))
	assert.Nil(t, err)
	assert.Contains(t, cerberusClientMock.VerifiedInstrumentHashes, HashInstrument(instr))

	instrument, err = instrumentService.GetInstrumentByID(ctx, nil, instrumentID, false)
	assert.Equal(t, "TestInstrumentUpdated", instrument.Name)
	assert.Len(t, instrument.AnalyteMappings, 2)
	assert.Contains(t, []uuid.UUID{analyteID1, controlAnalyteID1}, instrument.AnalyteMappings[0].AnalyteID)
	assert.Contains(t, []uuid.UUID{analyteID1, controlAnalyteID1}, instrument.AnalyteMappings[1].AnalyteID)
	assert.Equal(t, "pein", string(instrument.AnalyteMappings[0].ResultType))
	assert.Len(t, instrument.AnalyteMappings[0].ChannelMappings, 1)
	assert.Equal(t, "TestInstrumentChannel", instrument.AnalyteMappings[0].ChannelMappings[0].InstrumentChannel)
	assert.Equal(t, channelID1, instrument.AnalyteMappings[0].ChannelMappings[0].ChannelID)
	assert.Len(t, instrument.AnalyteMappings[0].ResultMappings, 2)

	for _, resultMapping := range instrument.AnalyteMappings[0].ResultMappings {
		if resultMapping.Index == 0 {
			assert.Equal(t, "pos", resultMapping.Key)
			assert.Equal(t, "pos", resultMapping.Value)
		} else if resultMapping.Index == 1 {
			assert.Equal(t, "neg", resultMapping.Key)
			assert.Equal(t, "neg", resultMapping.Value)
		} else {
			assert.Fail(t, "result mapping should not exists")
		}
	}

	assert.Equal(t, "pein", string(instrument.AnalyteMappings[1].ResultType))
	assert.Len(t, instrument.AnalyteMappings[1].ChannelMappings, 1)
	assert.Equal(t, "TestInstrumentChannel", instrument.AnalyteMappings[1].ChannelMappings[0].InstrumentChannel)
	assert.Equal(t, channelID1, instrument.AnalyteMappings[1].ChannelMappings[0].ChannelID)
	assert.Len(t, instrument.AnalyteMappings[1].ResultMappings, 2)

	assert.Len(t, instrument.RequestMappings, 1)
	assert.Equal(t, "ReqMap", instrument.RequestMappings[0].Code)
	assert.Equal(t, true, instrument.RequestMappings[0].IsDefault)
	assert.Len(t, instrument.RequestMappings[0].AnalyteIDs, 1)
	assert.Equal(t, analyteID1, instrument.RequestMappings[0].AnalyteIDs[0])
	assert.Len(t, instrument.ControlMappings, 1)
	assert.Equal(t, analyteID1, instrument.ControlMappings[0].AnalyteID)
	assert.Len(t, instrument.ControlMappings[0].ControlAnalyteIDs, 1)
	assert.Equal(t, controlAnalyteID1, instrument.ControlMappings[0].ControlAnalyteIDs[0])

	analyteID2 := uuid.New()
	analyteID3 := uuid.New()
	channelID2 := uuid.New()
	controlMappingID2 := uuid.New()
	controlMappingID3 := uuid.New()

	instr = Instrument{
		ID:                 instrumentID,
		Name:               "TestInstrumentUpdated2",
		Type:               Analyzer,
		ProtocolID:         protocolID,
		ProtocolName:       "TestProtocol",
		Enabled:            true,
		ConnectionMode:     TCPMixed,
		ResultMode:         Simulation,
		CaptureResults:     true,
		CaptureDiagnostics: false,
		ReplyToQuery:       false,
		Status:             "TestStatus",
		Encoding:           "UTF8",
		TimeZone:           "Europe/Berlin",
		Hostname:           "TestHost",
		ClientPort:         &clientPort,
		AnalyteMappings: []AnalyteMapping{
			{
				ID:                analyteMappingID1,
				InstrumentAnalyte: "TESTANALYTE",
				AnalyteID:         analyteID1,
				ChannelMappings: []ChannelMapping{
					{
						InstrumentChannel: "TestInstrumentChannel",
						ChannelID:         channelID1,
					},
					{
						InstrumentChannel: "TestInstrumentChannel2",
						ChannelID:         channelID2,
					},
				},
				ResultMappings: []ResultMapping{
					{
						Key:   "pos",
						Value: "pos",
						Index: 0,
					},
					{
						Key:   "alt",
						Value: "alt",
						Index: 2,
					},
				},
				ResultType:  "pein",
				AnalyteType: Result,
			},
			{
				ID:                uuid.New(),
				InstrumentAnalyte: "TESTANALYTE2",
				AnalyteID:         analyteID2,
				ChannelMappings: []ChannelMapping{
					{
						InstrumentChannel: "TestInstrumentChannel2",
						ChannelID:         channelID2,
					},
				},
				ResultMappings: []ResultMapping{
					{
						Key:   "alt",
						Value: "alt",
						Index: 2,
					},
				},
				ResultType:  "pein",
				AnalyteType: Result,
			},
		},
		RequestMappings: []RequestMapping{
			{
				Code:      "ReqMap2",
				IsDefault: false,
				AnalyteIDs: []uuid.UUID{
					analyteID2,
					analyteID3,
				},
			},
		},
		ControlMappings: []ControlMapping{
			{
				ID:        controlMappingID2,
				AnalyteID: analyteID1,
				ControlAnalyteIDs: []uuid.UUID{
					controlAnalyteID1,
				},
			},
			{
				ID:        controlMappingID3,
				AnalyteID: analyteID2,
				ControlAnalyteIDs: []uuid.UUID{
					controlAnalyteID1,
				},
			},
		},
	}
	err = instrumentService.UpdateInstrument(ctx, instr, uuid.MustParse("9d5fb5e9-65a1-4479-8f82-25b04145bfe1"))
	assert.Nil(t, err)
	assert.Contains(t, cerberusClientMock.VerifiedInstrumentHashes, HashInstrument(instr))

	instrument, err = instrumentService.GetInstrumentByID(ctx, nil, instrumentID, false)
	assert.Equal(t, "TestInstrumentUpdated2", instrument.Name)
	assert.Len(t, instrument.AnalyteMappings, 2)

	for _, analyteMapping := range instrument.AnalyteMappings {
		if analyteMapping.InstrumentAnalyte == "TESTANALYTE" {
			assert.Equal(t, analyteID1, analyteMapping.AnalyteID)
			assert.Equal(t, "pein", string(analyteMapping.ResultType))
			assert.Len(t, analyteMapping.ChannelMappings, 2)

			for _, channelMapping := range analyteMapping.ChannelMappings {
				if channelMapping.InstrumentChannel == "TestInstrumentChannel" {
					assert.Equal(t, channelID1, channelMapping.ChannelID)
				} else if channelMapping.InstrumentChannel == "TestInstrumentChannel2" {
					assert.Equal(t, channelID2, channelMapping.ChannelID)
				} else {
					assert.Fail(t, "channel mapping should not exists")
				}
			}

			assert.Len(t, analyteMapping.ResultMappings, 2)

			for _, resultMapping := range analyteMapping.ResultMappings {
				if resultMapping.Index == 0 {
					assert.Equal(t, "pos", resultMapping.Key)
					assert.Equal(t, "pos", resultMapping.Value)
				} else if resultMapping.Index == 2 {
					assert.Equal(t, "alt", resultMapping.Key)
					assert.Equal(t, "alt", resultMapping.Value)
				} else {
					assert.Fail(t, "result mapping should not exists")
				}
			}
		} else if analyteMapping.InstrumentAnalyte == "TESTANALYTE2" {
			assert.Equal(t, analyteID2, analyteMapping.AnalyteID)
			assert.Equal(t, "pein", string(analyteMapping.ResultType))
			assert.Len(t, analyteMapping.ChannelMappings, 1)
			assert.Equal(t, "TestInstrumentChannel2", analyteMapping.ChannelMappings[0].InstrumentChannel)
			assert.Equal(t, channelID2, analyteMapping.ChannelMappings[0].ChannelID)
			assert.Len(t, analyteMapping.ResultMappings, 1)
			assert.Equal(t, "alt", analyteMapping.ResultMappings[0].Key)
			assert.Equal(t, "alt", analyteMapping.ResultMappings[0].Value)
			assert.Equal(t, 2, analyteMapping.ResultMappings[0].Index)
		} else {
			assert.Fail(t, "analyte mapping should not exists")
		}
	}

	assert.Len(t, instrument.RequestMappings, 1)
	assert.Equal(t, "ReqMap2", instrument.RequestMappings[0].Code)
	assert.Equal(t, false, instrument.RequestMappings[0].IsDefault)
	assert.Len(t, instrument.RequestMappings[0].AnalyteIDs, 2)
	assert.Contains(t, instrument.RequestMappings[0].AnalyteIDs, analyteID2)
	assert.Contains(t, instrument.RequestMappings[0].AnalyteIDs, analyteID3)

	assert.Len(t, instrument.ControlMappings, 2)
	assert.Contains(t, []uuid.UUID{analyteID1, analyteID2}, instrument.ControlMappings[0].AnalyteID)
	assert.Contains(t, []uuid.UUID{analyteID1, analyteID2}, instrument.ControlMappings[1].AnalyteID)
	assert.Len(t, instrument.ControlMappings[0].ControlAnalyteIDs, 1)
	assert.Equal(t, controlAnalyteID1, instrument.ControlMappings[0].ControlAnalyteIDs[0])
	assert.Len(t, instrument.ControlMappings[1].ControlAnalyteIDs, 1)
	assert.Equal(t, controlAnalyteID1, instrument.ControlMappings[1].ControlAnalyteIDs[0])
}

func TestNotVerifiedInstrument(t *testing.T) {
	dbConn, schemaName := setupDbConnectorAndRunMigration("instrument_test")

	cerberusClientMock := &cerberusClientMock{
		verifyInstrumentHashFunc: func(hash string) error {
			return errors.New("instrument hash verification failed")
		},
	}
	instrumentRepository := NewInstrumentRepository(dbConn, schemaName)

	ctx, cancelSkeleton := context.WithCancel(context.Background())
	defer cancelSkeleton()

	conditionRepository := NewConditionRepository(dbConn, schemaName)
	conditionService := NewConditionService(conditionRepository)
	sortingRuleRepository := NewSortingRuleRepository(dbConn, schemaName)
	analysisRepository := NewAnalysisRepository(dbConn, schemaName)
	sortingRuleService := NewSortingRuleService(analysisRepository, conditionService, sortingRuleRepository)
	instrumentService := NewInstrumentService(sortingRuleService, instrumentRepository, NewSkeletonManager(ctx), NewInstrumentCache(), cerberusClientMock)

	var protocolID uuid.UUID
	err := dbConn.QueryRowx(`INSERT INTO instrument_test.sk_supported_protocols (name, description) VALUES('TestProtocol', 'Test Protocol Description') RETURNING id;`).Scan(&protocolID)
	assert.Nil(t, err)

	clientPort := 1234

	ctx = context.Background()
	ctx = context.WithValue(ctx, "Authorization", "BearerToken")

	instrId := uuid.New()
	instr := Instrument{
		ID:                 instrId,
		Name:               "TestInstrument",
		Type:               Analyzer,
		ProtocolID:         protocolID,
		ProtocolName:       "TestProtocol",
		Enabled:            true,
		ConnectionMode:     TCPMixed,
		ResultMode:         Simulation,
		CaptureResults:     true,
		CaptureDiagnostics: false,
		ReplyToQuery:       false,
		Status:             "TestStatus",
		Encoding:           "UTF8",
		TimeZone:           "Europe/Berlin",
		Hostname:           "TestHost",
		ClientPort:         &clientPort,
	}
	_, err = instrumentService.CreateInstrument(ctx, instr)
	assert.NotNil(t, err)

	_, err = instrumentService.GetInstrumentByID(ctx, nil, instrId, true)
	assert.Equal(t, ErrInstrumentNotFound, err)
}

func TestUpdateExpectedControlResult(t *testing.T) {
	dbConn, schemaName := setupDbConnectorAndRunMigration("expectedcontrolresult_test")

	cerberusClientMock := &cerberusClientMock{
		verifyExpectedControlResultHashFunc: func(hash string) error {
			return nil
		},
	}
	instrumentRepositoryMock := &instrumentRepositoryMock{
		db: dbConn,
	}

	ctxWithCancel, cancel := context.WithCancel(context.Background())

	defer cancel()
	conditionRepository := NewConditionRepository(dbConn, schemaName)
	conditionService := NewConditionService(conditionRepository)
	sortingRuleRepository := NewSortingRuleRepository(dbConn, schemaName)
	analysisRepository := NewAnalysisRepository(dbConn, schemaName)
	sortingRuleService := NewSortingRuleService(analysisRepository, conditionService, sortingRuleRepository)
	instrumentService := NewInstrumentService(sortingRuleService, instrumentRepositoryMock, NewSkeletonManager(ctxWithCancel), NewInstrumentCache(), cerberusClientMock)

	analyteMappingId := uuid.MustParse("e97b3b33-6b2e-4da2-9883-11a3fe4a93bc")
	expectedControlResultId2 := uuid.MustParse("0edf9c18-42e4-4ce0-ba4a-9c03ec1e775a")
	expectedControlResultId3 := uuid.MustParse("3454ed41-b940-4a47-b9e7-88c8892c5d38")
	expectedControlResultId4 := uuid.MustParse("982de611-8f2a-47c8-9c20-f9bc0a09610d")
	instrumentId := uuid.MustParse("8dbf35f8-f029-4c28-bf60-42a9c6c24261")

	analyteMapping := AnalyteMapping{
		InstrumentAnalyte: "TESTANALYTE",
		ID:                analyteMappingId,
		AnalyteID:         uuid.MustParse("5018bcf2-21c9-4e97-b595-4ae993497d8c"),
		ChannelMappings: []ChannelMapping{
			{
				InstrumentChannel: "TestInstrumentChannel",
				ChannelID:         uuid.MustParse("e5d9907f-3f48-4860-afb9-18ac12fcc87e"),
			},
		},
		ResultType: "int",
	}

	resultMappings := make([]ResultMapping, 0)
	resultMappings = append(resultMappings, ResultMapping{
		Key:   "1",
		Value: "1",
		Index: 0,
	})
	resultMappings = append(resultMappings, ResultMapping{
		Key:   "2",
		Value: "2",
		Index: 1,
	})
	resultMappings = append(resultMappings, ResultMapping{
		Key:   "3",
		Value: "3",
		Index: 2,
	})
	resultMappings = append(resultMappings, ResultMapping{
		Key:   "4",
		Value: "4",
		Index: 3,
	})
	instrumentRepositoryMock.analyteMappings = make(map[uuid.UUID][]AnalyteMapping)
	instrumentRepositoryMock.analyteMappings[instrumentId] = []AnalyteMapping{analyteMapping}
	instrumentRepositoryMock.resultMappings = make(map[uuid.UUID][]ResultMapping)
	instrumentRepositoryMock.resultMappings[analyteMappingId] = resultMappings

	instrumentRepositoryMock.existingExpectedControlResults = make([]ExpectedControlResult, 0)
	instrumentRepositoryMock.existingExpectedControlResults = append(instrumentRepositoryMock.existingExpectedControlResults, ExpectedControlResult{
		ID:               expectedControlResultId2,
		AnalyteMappingId: analyteMappingId,
		SampleCode:       "sampleCode2",
		Operator:         ">",
		ExpectedValue:    "1",
		ExpectedValue2:   nil,
	})
	instrumentRepositoryMock.existingExpectedControlResults = append(instrumentRepositoryMock.existingExpectedControlResults, ExpectedControlResult{
		ID:               expectedControlResultId3,
		AnalyteMappingId: analyteMappingId,
		SampleCode:       "sampleCode3",
		Operator:         "==",
		ExpectedValue:    "2",
		ExpectedValue2:   nil,
	})
	instrumentRepositoryMock.existingExpectedControlResults = append(instrumentRepositoryMock.existingExpectedControlResults, ExpectedControlResult{
		ID:               expectedControlResultId4,
		AnalyteMappingId: analyteMappingId,
		SampleCode:       "sampleCode4",
		Operator:         "<",
		ExpectedValue:    "3",
		ExpectedValue2:   nil,
	})

	expectedControlResults := make([]ExpectedControlResult, 0)
	expectedControlResults = append(expectedControlResults, ExpectedControlResult{
		ID:               uuid.Nil,
		AnalyteMappingId: analyteMappingId,
		SampleCode:       "sampleCode0",
		Operator:         ">",
		ExpectedValue:    "2",
		ExpectedValue2:   nil,
	})
	expectedControlResults = append(expectedControlResults, ExpectedControlResult{
		ID:               uuid.Nil,
		AnalyteMappingId: analyteMappingId,
		SampleCode:       "sampleCode1",
		Operator:         ">",
		ExpectedValue:    "2",
		ExpectedValue2:   nil,
	})
	expectedControlResults = append(expectedControlResults, ExpectedControlResult{
		ID:               expectedControlResultId2,
		AnalyteMappingId: analyteMappingId,
		SampleCode:       "sampleCode2",
		Operator:         "==",
		ExpectedValue:    "3",
		ExpectedValue2:   nil,
	})
	expectedControlResults = append(expectedControlResults, ExpectedControlResult{
		ID:               expectedControlResultId3,
		AnalyteMappingId: analyteMappingId,
		SampleCode:       "sampleCode3",
		Operator:         "<",
		ExpectedValue:    "4",
		ExpectedValue2:   nil,
	})

	err := instrumentService.UpdateExpectedControlResults(ctxWithCancel, instrumentId, expectedControlResults, uuid.New())
	assert.Nil(t, err)
	assert.Contains(t, cerberusClientMock.VerifiedExpectedControlResultHashes, HashExpectedControlResults(expectedControlResults))

	assert.Equal(t, 2, len(instrumentRepositoryMock.createdExpectedControlResults))
	assert.Equal(t, "sampleCode0", instrumentRepositoryMock.createdExpectedControlResults[0].SampleCode)
	assert.Equal(t, "sampleCode1", instrumentRepositoryMock.createdExpectedControlResults[1].SampleCode)

	assert.Equal(t, 2, len(instrumentRepositoryMock.updatedExpectedControlResults))
	updatedExpectedControlsMap := make(map[uuid.UUID]ExpectedControlResult)
	updatedExpectedControlsMap[instrumentRepositoryMock.updatedExpectedControlResults[0].ID] = instrumentRepositoryMock.updatedExpectedControlResults[0]
	updatedExpectedControlsMap[instrumentRepositoryMock.updatedExpectedControlResults[1].ID] = instrumentRepositoryMock.updatedExpectedControlResults[1]
	assert.Equal(t, expectedControlResultId2, updatedExpectedControlsMap[expectedControlResultId2].ID)
	assert.Equal(t, Equals, updatedExpectedControlsMap[expectedControlResultId2].Operator)
	assert.Equal(t, "3", updatedExpectedControlsMap[expectedControlResultId2].ExpectedValue)
	assert.Equal(t, expectedControlResultId3, updatedExpectedControlsMap[expectedControlResultId3].ID)
	assert.Equal(t, Less, updatedExpectedControlsMap[expectedControlResultId3].Operator)
	assert.Equal(t, "4", updatedExpectedControlsMap[expectedControlResultId3].ExpectedValue)

	assert.Equal(t, 1, len(instrumentRepositoryMock.deletedExpectedControlResultIds))
	assert.Equal(t, expectedControlResultId4, instrumentRepositoryMock.deletedExpectedControlResultIds[0])
}

func TestUpdateExpectedControlResult2(t *testing.T) {
	dbConn, schemaName := setupDbConnectorAndRunMigration("expectedcontrolresult_test")

	cerberusClientMock := &cerberusClientMock{
		verifyExpectedControlResultHashFunc: func(hash string) error {
			return nil
		},
	}
	instrumentRepositoryMock := &instrumentRepositoryMock{
		db: dbConn,
	}

	ctxWithCancel, cancel := context.WithCancel(context.Background())

	defer cancel()
	conditionRepository := NewConditionRepository(dbConn, schemaName)
	conditionService := NewConditionService(conditionRepository)
	sortingRuleRepository := NewSortingRuleRepository(dbConn, schemaName)
	analysisRepository := NewAnalysisRepository(dbConn, schemaName)
	sortingRuleService := NewSortingRuleService(analysisRepository, conditionService, sortingRuleRepository)
	instrumentService := NewInstrumentService(sortingRuleService, instrumentRepositoryMock, NewSkeletonManager(ctxWithCancel), NewInstrumentCache(), cerberusClientMock)

	analyteMappingId := uuid.MustParse("e97b3b33-6b2e-4da2-9883-11a3fe4a93bc")
	analyteMappingId2 := uuid.MustParse("f17cb1d4-b281-464f-a2d8-2e08dcfa76f8")
	analyteMappingId3 := uuid.MustParse("475876c7-c676-4a30-9961-cc97751c77e2")
	analyteMappingId4 := uuid.MustParse("1672764b-45b2-4f61-88a3-65d666b0b060")
	analyteMappingId5 := uuid.MustParse("ec05da16-67ae-428c-a94f-da62657f619c")
	expectedControlResultId2 := uuid.MustParse("0edf9c18-42e4-4ce0-ba4a-9c03ec1e775a")
	expectedControlResultId3 := uuid.MustParse("3454ed41-b940-4a47-b9e7-88c8892c5d38")
	expectedControlResultId4 := uuid.MustParse("982de611-8f2a-47c8-9c20-f9bc0a09610d")
	instrumentId := uuid.MustParse("8dbf35f8-f029-4c28-bf60-42a9c6c24261")

	analyteMappings := make([]AnalyteMapping, 0)
	analyteMappings = append(analyteMappings, AnalyteMapping{
		InstrumentAnalyte: "TESTANALYTE",
		ID:                analyteMappingId,
		AnalyteID:         uuid.MustParse("5018bcf2-21c9-4e97-b595-4ae993497d8c"),
		ChannelMappings: []ChannelMapping{
			{
				InstrumentChannel: "TestInstrumentChannel",
				ChannelID:         uuid.MustParse("e5d9907f-3f48-4860-afb9-18ac12fcc87e"),
			},
		},
		ResultType: "int",
	})
	analyteMappings = append(analyteMappings, AnalyteMapping{
		InstrumentAnalyte: "TESTANALYTE2",
		ID:                analyteMappingId2,
		AnalyteID:         uuid.MustParse("5018bcf2-21c9-4e97-b595-4ae993497d8c"),
		ChannelMappings: []ChannelMapping{
			{
				InstrumentChannel: "TestInstrumentChannel",
				ChannelID:         uuid.MustParse("e5d9907f-3f48-4860-afb9-18ac12fcc87e"),
			},
		},
		ResultType: "int",
	})
	analyteMappings = append(analyteMappings, AnalyteMapping{
		InstrumentAnalyte: "TESTANALYTE3",
		ID:                analyteMappingId3,
		AnalyteID:         uuid.MustParse("5018bcf2-21c9-4e97-b595-4ae993497d8c"),
		ChannelMappings: []ChannelMapping{
			{
				InstrumentChannel: "TestInstrumentChannel",
				ChannelID:         uuid.MustParse("e5d9907f-3f48-4860-afb9-18ac12fcc87e"),
			},
		},
		ResultType: "int",
	})
	analyteMappings = append(analyteMappings, AnalyteMapping{
		InstrumentAnalyte: "TESTANALYTE4",
		ID:                analyteMappingId4,
		AnalyteID:         uuid.MustParse("5018bcf2-21c9-4e97-b595-4ae993497d8c"),
		ChannelMappings: []ChannelMapping{
			{
				InstrumentChannel: "TestInstrumentChannel",
				ChannelID:         uuid.MustParse("e5d9907f-3f48-4860-afb9-18ac12fcc87e"),
			},
		},
		ResultType: "int",
	})
	analyteMappings = append(analyteMappings, AnalyteMapping{
		InstrumentAnalyte: "TESTANALYTE5",
		ID:                analyteMappingId5,
		AnalyteID:         uuid.MustParse("5018bcf2-21c9-4e97-b595-4ae993497d8c"),
		ChannelMappings: []ChannelMapping{
			{
				InstrumentChannel: "TestInstrumentChannel",
				ChannelID:         uuid.MustParse("e5d9907f-3f48-4860-afb9-18ac12fcc87e"),
			},
		},
		ResultType: "int",
	})

	resultMappings := make([]ResultMapping, 0)
	resultMappings = append(resultMappings, ResultMapping{
		Key:   "1",
		Value: "1",
		Index: 0,
	})
	resultMappings = append(resultMappings, ResultMapping{
		Key:   "2",
		Value: "2",
		Index: 1,
	})
	instrumentRepositoryMock.analyteMappings = make(map[uuid.UUID][]AnalyteMapping)
	instrumentRepositoryMock.analyteMappings[instrumentId] = analyteMappings
	instrumentRepositoryMock.resultMappings = make(map[uuid.UUID][]ResultMapping)
	instrumentRepositoryMock.resultMappings[analyteMappingId] = resultMappings
	instrumentRepositoryMock.resultMappings[analyteMappingId2] = resultMappings
	instrumentRepositoryMock.resultMappings[analyteMappingId3] = resultMappings
	instrumentRepositoryMock.resultMappings[analyteMappingId4] = resultMappings
	instrumentRepositoryMock.resultMappings[analyteMappingId5] = resultMappings

	instrumentRepositoryMock.existingExpectedControlResults = make([]ExpectedControlResult, 0)
	instrumentRepositoryMock.existingExpectedControlResults = append(instrumentRepositoryMock.existingExpectedControlResults, ExpectedControlResult{
		ID:               expectedControlResultId2,
		AnalyteMappingId: analyteMappingId3,
		SampleCode:       "sampleCode",
		Operator:         ">",
		ExpectedValue:    "1",
		ExpectedValue2:   nil,
	})
	instrumentRepositoryMock.existingExpectedControlResults = append(instrumentRepositoryMock.existingExpectedControlResults, ExpectedControlResult{
		ID:               expectedControlResultId3,
		AnalyteMappingId: analyteMappingId4,
		SampleCode:       "sampleCode",
		Operator:         "==",
		ExpectedValue:    "2",
		ExpectedValue2:   nil,
	})
	instrumentRepositoryMock.existingExpectedControlResults = append(instrumentRepositoryMock.existingExpectedControlResults, ExpectedControlResult{
		ID:               expectedControlResultId4,
		AnalyteMappingId: analyteMappingId5,
		SampleCode:       "sampleCode",
		Operator:         "<",
		ExpectedValue:    "3",
		ExpectedValue2:   nil,
	})

	expectedControlResults := make([]ExpectedControlResult, 0)
	expectedControlResults = append(expectedControlResults, ExpectedControlResult{
		ID:               uuid.Nil,
		AnalyteMappingId: analyteMappingId,
		SampleCode:       "sampleCode",
		Operator:         ">",
		ExpectedValue:    "2",
		ExpectedValue2:   nil,
	})
	expectedControlResults = append(expectedControlResults, ExpectedControlResult{
		ID:               uuid.Nil,
		AnalyteMappingId: analyteMappingId2,
		SampleCode:       "sampleCode",
		Operator:         ">",
		ExpectedValue:    "2",
		ExpectedValue2:   nil,
	})
	expectedControlResults = append(expectedControlResults, ExpectedControlResult{
		ID:               expectedControlResultId2,
		AnalyteMappingId: analyteMappingId3,
		SampleCode:       "sampleCode",
		Operator:         "==",
		ExpectedValue:    "3",
		ExpectedValue2:   nil,
	})
	expectedControlResults = append(expectedControlResults, ExpectedControlResult{
		ID:               expectedControlResultId3,
		AnalyteMappingId: analyteMappingId4,
		SampleCode:       "sampleCode",
		Operator:         "<",
		ExpectedValue:    "4",
		ExpectedValue2:   nil,
	})

	err := instrumentService.UpdateExpectedControlResults(ctxWithCancel, instrumentId, expectedControlResults, uuid.New())
	assert.Nil(t, err)
	assert.Contains(t, cerberusClientMock.VerifiedExpectedControlResultHashes, HashExpectedControlResults(expectedControlResults))

	assert.Equal(t, 2, len(instrumentRepositoryMock.createdExpectedControlResults))
	assert.Equal(t, analyteMappingId, instrumentRepositoryMock.createdExpectedControlResults[0].AnalyteMappingId)
	assert.Equal(t, analyteMappingId2, instrumentRepositoryMock.createdExpectedControlResults[1].AnalyteMappingId)

	assert.Equal(t, 2, len(instrumentRepositoryMock.updatedExpectedControlResults))
	updatedExpectedControlsMap := make(map[uuid.UUID]ExpectedControlResult)
	updatedExpectedControlsMap[instrumentRepositoryMock.updatedExpectedControlResults[0].ID] = instrumentRepositoryMock.updatedExpectedControlResults[0]
	updatedExpectedControlsMap[instrumentRepositoryMock.updatedExpectedControlResults[1].ID] = instrumentRepositoryMock.updatedExpectedControlResults[1]
	assert.Equal(t, expectedControlResultId2, updatedExpectedControlsMap[expectedControlResultId2].ID)
	assert.Equal(t, Equals, updatedExpectedControlsMap[expectedControlResultId2].Operator)
	assert.Equal(t, "3", updatedExpectedControlsMap[expectedControlResultId2].ExpectedValue)
	assert.Equal(t, expectedControlResultId3, updatedExpectedControlsMap[expectedControlResultId3].ID)
	assert.Equal(t, Less, updatedExpectedControlsMap[expectedControlResultId3].Operator)
	assert.Equal(t, "4", updatedExpectedControlsMap[expectedControlResultId3].ExpectedValue)

	assert.Equal(t, 1, len(instrumentRepositoryMock.deletedExpectedControlResultIds))
	assert.Equal(t, expectedControlResultId4, instrumentRepositoryMock.deletedExpectedControlResultIds[0])
}

func TestNotVerifiedExpectedControlResults(t *testing.T) {
	dbConn, schemaName := setupDbConnectorAndRunMigration("expectedcontrolresult_test")

	cerberusClientMock := &cerberusClientMock{
		verifyExpectedControlResultHashFunc: func(hash string) error {
			return nil
		},
	}
	instrumentRepositoryMock := &instrumentRepositoryMock{
		db: dbConn,
	}

	ctxWithCancel, cancel := context.WithCancel(context.Background())

	defer cancel()
	conditionRepository := NewConditionRepository(dbConn, schemaName)
	conditionService := NewConditionService(conditionRepository)
	sortingRuleRepository := NewSortingRuleRepository(dbConn, schemaName)
	analysisRepository := NewAnalysisRepository(dbConn, schemaName)
	sortingRuleService := NewSortingRuleService(analysisRepository, conditionService, sortingRuleRepository)
	instrumentService := NewInstrumentService(sortingRuleService, instrumentRepositoryMock, NewSkeletonManager(ctxWithCancel), NewInstrumentCache(), cerberusClientMock)

	analyteMappingId := uuid.MustParse("e97b3b33-6b2e-4da2-9883-11a3fe4a93bc")
	expectedControlResultId := uuid.MustParse("0edf9c18-42e4-4ce0-ba4a-9c03ec1e775a")
	instrumentId := uuid.MustParse("8dbf35f8-f029-4c28-bf60-42a9c6c24261")

	analyteMappings := make([]AnalyteMapping, 0)
	analyteMappings = append(analyteMappings, AnalyteMapping{
		InstrumentAnalyte: "TESTANALYTE",
		ID:                analyteMappingId,
		AnalyteID:         uuid.MustParse("5018bcf2-21c9-4e97-b595-4ae993497d8c"),
		ChannelMappings: []ChannelMapping{
			{
				InstrumentChannel: "TestInstrumentChannel",
				ChannelID:         uuid.MustParse("e5d9907f-3f48-4860-afb9-18ac12fcc87e"),
			},
		},
		ResultType: "int",
	})

	resultMappings := make([]ResultMapping, 0)
	resultMappings = append(resultMappings, ResultMapping{
		Key:   "1",
		Value: "1",
		Index: 0,
	})
	resultMappings = append(resultMappings, ResultMapping{
		Key:   "2",
		Value: "2",
		Index: 1,
	})
	instrumentRepositoryMock.analyteMappings = make(map[uuid.UUID][]AnalyteMapping)
	instrumentRepositoryMock.analyteMappings[instrumentId] = analyteMappings
	instrumentRepositoryMock.resultMappings = make(map[uuid.UUID][]ResultMapping)
	instrumentRepositoryMock.resultMappings[analyteMappingId] = resultMappings

	instrumentRepositoryMock.existingExpectedControlResults = make([]ExpectedControlResult, 0)

	expectedControlResults := make([]ExpectedControlResult, 0)
	expectedControlResults = append(expectedControlResults, ExpectedControlResult{
		ID:               expectedControlResultId,
		AnalyteMappingId: analyteMappingId,
		SampleCode:       "sampleCode",
		Operator:         ">",
		ExpectedValue:    "2",
		ExpectedValue2:   nil,
	})

	err := instrumentService.CreateExpectedControlResults(ctxWithCancel, expectedControlResults, uuid.New())
	assert.Nil(t, err)

	ecrs, _ := instrumentService.GetExpectedControlResultsByInstrumentId(ctxWithCancel, instrumentId)
	assert.Empty(t, ecrs)
}

type instrumentRepositoryMock struct {
	existingExpectedControlResults  []ExpectedControlResult
	createdExpectedControlResults   []ExpectedControlResult
	updatedExpectedControlResults   []ExpectedControlResult
	deletedExpectedControlResultIds []uuid.UUID
	analyteMappings                 map[uuid.UUID][]AnalyteMapping
	resultMappings                  map[uuid.UUID][]ResultMapping
	db                              db.DbConnection
	ExpectedControlResults          []ExpectedControlResult
}

func (r *instrumentRepositoryMock) UpsertRequestMappings(ctx context.Context, requestMappings []RequestMapping, instrumentID uuid.UUID) error {
	return nil
}

func (r *instrumentRepositoryMock) UpsertManufacturerTests(ctx context.Context, manufacturerTests []SupportedManufacturerTests) error {
	return nil
}

func (r *instrumentRepositoryMock) GetManufacturerTests(ctx context.Context) ([]SupportedManufacturerTests, error) {
	return make([]SupportedManufacturerTests, 1), nil
}

func (r *instrumentRepositoryMock) CreateInstrument(ctx context.Context, instrument Instrument) (uuid.UUID, error) {
	return uuid.Nil, nil
}
func (r *instrumentRepositoryMock) GetInstruments(ctx context.Context) ([]Instrument, error) {
	return make([]Instrument, 0), nil
}
func (r *instrumentRepositoryMock) GetInstrumentChanges(ctx context.Context, timeFrom time.Time) ([]Instrument, error) {
	return make([]Instrument, 0), nil
}
func (r *instrumentRepositoryMock) GetInstrumentByID(ctx context.Context, id uuid.UUID) (Instrument, error) {
	return Instrument{
		ID:             uuid.MustParse("68f34e1d-1faa-4101-9e79-a743b420ab4e"),
		Name:           "test",
		ProtocolID:     uuid.MustParse("abb539a3-286f-4c15-a7b7-2e9adf6eab91"),
		ProtocolName:   "Test Protocol",
		Enabled:        true,
		ConnectionMode: "TCP_MIXED",
		ResultMode:     "SIMULATION",
		Status:         "ONLINE",
		Encoding:       "UTF8",
		TimeZone:       "Europe/Budapest",
		Hostname:       "192.168.1.20",
		Settings: []InstrumentSetting{
			{
				ID:                uuid.Nil,
				ProtocolSettingID: uuid.MustParse("1f663361-3f2d-4c43-8cf6-65cec3fc88ab"),
				Value:             "SomeSetting",
			},
			{
				ID:                uuid.Nil,
				ProtocolSettingID: uuid.MustParse("c81c77cf-f17a-402d-a44b-a0194eb00a29"),
				Value:             "ThisIsMyPassword",
			},
		},
	}, nil
}
func (r *instrumentRepositoryMock) GetInstrumentByIP(ctx context.Context, ip string) (Instrument, error) {
	return Instrument{
		ID:             uuid.MustParse("68f34e1d-1faa-4101-9e79-a743b420ab4e"),
		Name:           "test",
		ProtocolID:     uuid.MustParse("abb539a3-286f-4c15-a7b7-2e9adf6eab91"),
		ProtocolName:   "Test Protocol",
		Enabled:        true,
		ConnectionMode: "TCP_MIXED",
		ResultMode:     "SIMULATION",
		Status:         "ONLINE",
		Encoding:       "UTF8",
		TimeZone:       "Europe/Budapest",
		Hostname:       "192.168.1.20",
		Settings: []InstrumentSetting{
			{
				ID:                uuid.Nil,
				ProtocolSettingID: uuid.MustParse("1f663361-3f2d-4c43-8cf6-65cec3fc88ab"),
				Value:             "SomeSetting",
			},
			{
				ID:                uuid.Nil,
				ProtocolSettingID: uuid.MustParse("c81c77cf-f17a-402d-a44b-a0194eb00a29"),
				Value:             "ThisIsMyPassword",
			},
		},
	}, nil
}
func (r *instrumentRepositoryMock) UpdateInstrument(ctx context.Context, instrument Instrument) error {
	return nil
}
func (r *instrumentRepositoryMock) DeleteInstrument(ctx context.Context, id uuid.UUID) error {
	return nil
}

func (r *instrumentRepositoryMock) CreateFtpConfig(ctx context.Context, ftpConfig FTPConfig) error {
	return nil
}

func (r *instrumentRepositoryMock) GetFtpConfigByInstrumentId(ctx context.Context, instrumentId uuid.UUID) (FTPConfig, error) {
	return FTPConfig{}, nil
}

func (r *instrumentRepositoryMock) DeleteFtpConfig(ctx context.Context, instrumentId uuid.UUID) error {
	return nil
}

func (r *instrumentRepositoryMock) GetProtocolByID(ctx context.Context, id uuid.UUID) (SupportedProtocol, error) {
	return SupportedProtocol{}, nil
}
func (r *instrumentRepositoryMock) GetSupportedProtocols(ctx context.Context) ([]SupportedProtocol, error) {
	return make([]SupportedProtocol, 0), nil
}
func (r *instrumentRepositoryMock) UpsertSupportedProtocol(ctx context.Context, id uuid.UUID, name string, description string) error {
	return nil
}
func (r *instrumentRepositoryMock) GetProtocolAbilities(ctx context.Context, protocolID uuid.UUID) ([]ProtocolAbility, error) {
	return make([]ProtocolAbility, 0), nil
}
func (r *instrumentRepositoryMock) UpsertProtocolAbilities(ctx context.Context, protocolID uuid.UUID, protocolAbilities []ProtocolAbility) error {
	return nil
}
func (r *instrumentRepositoryMock) GetProtocolSettings(ctx context.Context, protocolID uuid.UUID) ([]ProtocolSetting, error) {
	return []ProtocolSetting{
		{
			ID:   uuid.MustParse("1f663361-3f2d-4c43-8cf6-65cec3fc88ab"),
			Key:  "Some setting",
			Type: String,
		},
		{
			ID:   uuid.MustParse("c81c77cf-f17a-402d-a44b-a0194eb00a29"),
			Key:  "Password",
			Type: Password,
		},
	}, nil
}
func (r *instrumentRepositoryMock) UpsertProtocolSetting(ctx context.Context, protocolID uuid.UUID, protocolSetting ProtocolSetting) error {
	return nil
}
func (r *instrumentRepositoryMock) DeleteProtocolSettings(ctx context.Context, protocolSettingIDs []uuid.UUID) error {
	return nil
}
func (r *instrumentRepositoryMock) UpdateInstrumentStatus(ctx context.Context, id uuid.UUID, status InstrumentStatus) error {
	return nil
}
func (r *instrumentRepositoryMock) UpsertAnalyteMappings(ctx context.Context, analyteMappings []AnalyteMapping, instrumentID uuid.UUID) ([]uuid.UUID, error) {
	return make([]uuid.UUID, 0), nil
}
func (r *instrumentRepositoryMock) GetAnalyteMappings(ctx context.Context, instrumentIDs []uuid.UUID) (map[uuid.UUID][]AnalyteMapping, error) {
	if len(r.analyteMappings) > 0 {
		return r.analyteMappings, nil
	}
	return make(map[uuid.UUID][]AnalyteMapping), nil
}
func (r *instrumentRepositoryMock) GetExpectedControlResultsForControlValidation(ctx context.Context, instrumentID uuid.UUID, analyteID uuid.UUID) ([]ExpectedControlResult, error) {
	return r.ExpectedControlResults, nil
}
func (r *instrumentRepositoryMock) DeleteAnalyteMappings(ctx context.Context, ids []uuid.UUID) error {
	return nil
}
func (r *instrumentRepositoryMock) UpsertChannelMappings(ctx context.Context, channelMappings []ChannelMapping, analyteMappingID uuid.UUID) ([]uuid.UUID, error) {
	return make([]uuid.UUID, 0), nil
}
func (r *instrumentRepositoryMock) GetChannelMappings(ctx context.Context, analyteMappingIDs []uuid.UUID) (map[uuid.UUID][]ChannelMapping, error) {
	return make(map[uuid.UUID][]ChannelMapping), nil
}
func (r *instrumentRepositoryMock) DeleteChannelMappings(ctx context.Context, ids []uuid.UUID) error {
	return nil
}
func (r *instrumentRepositoryMock) UpsertResultMappings(ctx context.Context, resultMappings []ResultMapping, analyteMappingID uuid.UUID) ([]uuid.UUID, error) {
	return make([]uuid.UUID, 0), nil
}
func (r *instrumentRepositoryMock) GetResultMappings(ctx context.Context, analyteMappingIDs []uuid.UUID) (map[uuid.UUID][]ResultMapping, error) {
	if len(r.resultMappings) > 0 {
		return r.resultMappings, nil
	}
	return make(map[uuid.UUID][]ResultMapping), nil
}
func (r *instrumentRepositoryMock) DeleteResultMappings(ctx context.Context, ids []uuid.UUID) error {
	return nil
}
func (r *instrumentRepositoryMock) CreateExpectedControlResults(ctx context.Context, expectedControlResults []ExpectedControlResult) ([]uuid.UUID, error) {
	r.createdExpectedControlResults = expectedControlResults
	return nil, nil
}
func (r *instrumentRepositoryMock) UpdateExpectedControlResults(ctx context.Context, expectedControlResults []ExpectedControlResult) error {
	r.updatedExpectedControlResults = expectedControlResults
	return nil
}
func (r *instrumentRepositoryMock) DeleteExpectedControlResults(ctx context.Context, ids []uuid.UUID, deletedByUserId uuid.UUID) error {
	r.deletedExpectedControlResultIds = ids
	return nil
}
func (r *instrumentRepositoryMock) DeleteExpectedControlResultsByAnalyteMappingIDs(ctx context.Context, ids []uuid.UUID, deletedByUserId uuid.UUID) error {
	r.deletedExpectedControlResultIds = ids
	return nil
}
func (r *instrumentRepositoryMock) GetExpectedControlResultsByInstrumentId(ctx context.Context, instrumentId uuid.UUID) ([]ExpectedControlResult, error) {
	return nil, nil
}
func (r *instrumentRepositoryMock) GetNotSpecifiedExpectedControlResultsByInstrumentId(ctx context.Context, instrumentId uuid.UUID) ([]NotSpecifiedExpectedControlResult, error) {
	return nil, nil
}
func (r *instrumentRepositoryMock) GetExpectedControlResultsByInstrumentIdAndSampleCodes(ctx context.Context, instrumentId uuid.UUID, sampleCodes []string) (map[uuid.UUID]ExpectedControlResult, error) {
	existingExpectedControlResults := make(map[uuid.UUID]ExpectedControlResult)
	for _, existingExpectedControlResult := range r.existingExpectedControlResults {
		existingExpectedControlResults[existingExpectedControlResult.ID] = existingExpectedControlResult
	}
	return existingExpectedControlResults, nil
}
func (r *instrumentRepositoryMock) GetExpectedControlResultsByAnalyteMappingIds(ctx context.Context, analyteMappingIds []uuid.UUID) (map[uuid.UUID][]ExpectedControlResult, error) {
	return nil, nil
}
func (r *instrumentRepositoryMock) UpsertRequestMappingAnalytes(ctx context.Context, analyteIDsByRequestMappingID map[uuid.UUID][]uuid.UUID) error {
	return nil
}
func (r *instrumentRepositoryMock) GetRequestMappings(ctx context.Context, instrumentIDs []uuid.UUID) (map[uuid.UUID][]RequestMapping, error) {
	return make(map[uuid.UUID][]RequestMapping), nil
}
func (r *instrumentRepositoryMock) GetRequestMappingAnalytes(ctx context.Context, requestMappingIDs []uuid.UUID) (map[uuid.UUID][]uuid.UUID, error) {
	return make(map[uuid.UUID][]uuid.UUID), nil
}
func (r *instrumentRepositoryMock) DeleteRequestMappings(ctx context.Context, requestMappingIDs []uuid.UUID) error {
	return nil
}
func (r *instrumentRepositoryMock) DeleteRequestMappingAnalytes(ctx context.Context, requestMappingID uuid.UUID, analyteIDs []uuid.UUID) error {
	return nil
}

func (r *instrumentRepositoryMock) GetControlMappings(ctx context.Context, instrumentIDs []uuid.UUID) (map[uuid.UUID][]ControlMapping, error) {
	return nil, nil
}
func (r *instrumentRepositoryMock) CreateControlMappings(ctx context.Context, controlMappings []ControlMapping, instrumentID uuid.UUID) ([]uuid.UUID, error) {
	return nil, nil
}
func (r *instrumentRepositoryMock) UpdateControlMapping(ctx context.Context, controlMapping ControlMapping, instrumentID uuid.UUID) error {
	return nil
}
func (r *instrumentRepositoryMock) DeleteControlMappings(ctx context.Context, controlMappingIDs []uuid.UUID) error {
	return nil
}
func (r *instrumentRepositoryMock) DeleteControlMappingsByInstrumentId(ctx context.Context, instrumentId uuid.UUID) error {
	return nil
}
func (r *instrumentRepositoryMock) GetControlMappingAnalytes(ctx context.Context, controlMappingIDs []uuid.UUID) (map[uuid.UUID][]uuid.UUID, error) {
	return nil, nil
}
func (r *instrumentRepositoryMock) CreateControlMappingAnalytes(ctx context.Context, controlAnalyteIDsByControlAnalyteMappingID map[uuid.UUID][]uuid.UUID) error {
	return nil
}
func (r *instrumentRepositoryMock) DeleteControlMappingAnalytesByControlMappingIDs(ctx context.Context, controlMappingIDs []uuid.UUID) error {
	return nil
}
func (r *instrumentRepositoryMock) DeleteControlMappingAnalytesByInstrumentID(ctx context.Context, instrumentID uuid.UUID) error {
	return nil
}
func (r *instrumentRepositoryMock) DeleteControlMappingAnalytesByControlMappingIDAndControlAnalyteIDs(ctx context.Context, controlAnalyteMapping uuid.UUID, controlAnalyteIDs []uuid.UUID) error {
	return nil
}

func (r *instrumentRepositoryMock) GetEncodings(ctx context.Context) ([]string, error) {
	return make([]string, 0), nil
}
func (r *instrumentRepositoryMock) GetInstrumentsSettings(ctx context.Context, instrumentIDs []uuid.UUID) (map[uuid.UUID][]InstrumentSetting, error) {
	return make(map[uuid.UUID][]InstrumentSetting), nil
}
func (r *instrumentRepositoryMock) UpsertInstrumentSetting(ctx context.Context, instrumentID uuid.UUID, setting InstrumentSetting) error {
	return nil
}
func (r *instrumentRepositoryMock) DeleteInstrumentSettings(ctx context.Context, ids []uuid.UUID) error {
	return nil
}
func (r *instrumentRepositoryMock) CheckAnalytesUsage(ctx context.Context, analyteIDs []uuid.UUID) (map[uuid.UUID][]Instrument, error) {
	return make(map[uuid.UUID][]Instrument), nil
}
func (r *instrumentRepositoryMock) CreateTransaction() (db.DbConnection, error) {
	return r.db, nil
}
func (r *instrumentRepositoryMock) WithTransaction(tx db.DbConnection) InstrumentRepository {
	return r
}
