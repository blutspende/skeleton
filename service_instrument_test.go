package skeleton

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/DRK-Blutspende-BaWueHe/skeleton/config"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/db"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/migrator"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

func TestRegisterCreatedInstrument(t *testing.T) {
	sqlConn, _ := sqlx.Connect("postgres", "host=localhost port=5551 user=postgres password=postgres dbname=postgres sslmode=disable")
	schemaName := "instrument_test"
	_, _ = sqlConn.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp" schema public;`)
	_, _ = sqlConn.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE;`, schemaName))
	_, _ = sqlConn.Exec(fmt.Sprintf(`CREATE SCHEMA %s;`, schemaName))
	migrator := migrator.NewSkeletonMigrator()
	_ = migrator.Run(context.Background(), sqlConn, schemaName)
	_, _ = sqlConn.Exec(fmt.Sprintf(`INSERT INTO %s.sk_supported_protocols (id, "name", description) VALUES ('abb539a3-286f-4c15-a7b7-2e9adf6eab91', 'IH-1000 v5.2', 'IHCOM');`, schemaName))

	config := config.Configuration{
		APIPort:                          5000,
		Authorization:                    false,
		PermittedOrigin:                  "*",
		ApplicationName:                  "Register instrument retry test",
		TCPListenerPort:                  5401,
		InstrumentTransferRetryDelayInMs: 50,
	}
	testDoneCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	registerInstrumentAfterTrialCount := 3
	dbConn := db.CreateDbConnector(sqlConn)
	cerberusClientMock := &cerberusClientMock{
		registerInstrumentFunc: func(instrument Instrument) error {
			if registerInstrumentAfterTrialCount > 0 {
				registerInstrumentAfterTrialCount--
				return errors.New("failed to send to cerberus")
			}
			time.AfterFunc(100*time.Millisecond, cancel)
			return nil
		},
	}
	instrumentRepository := NewInstrumentRepository(dbConn, schemaName)
	instrumentService := NewInstrumentService(&config, instrumentRepository, NewSkeletonManager(), NewInstrumentCache(), cerberusClientMock)

	_, _ = instrumentService.CreateInstrument(context.Background(), Instrument{
		ID:             uuid.MustParse("68f34e1d-1faa-4101-9e79-a743b420ab4e"),
		Name:           "test",
		ProtocolID:     uuid.MustParse("abb539a3-286f-4c15-a7b7-2e9adf6eab91"),
		ProtocolName:   "Test Protocol",
		Enabled:        true,
		ConnectionMode: "TCP_MIXED",
		ResultMode:     "SIMULATION",
		Status:         "ONLINE",
		FileEncoding:   "UTF8",
		Timezone:       "Europe/Budapest",
		Hostname:       "192.168.1.20",
	})

	<-testDoneCtx.Done()
	if err := testDoneCtx.Err(); err == context.DeadlineExceeded {
		t.Fail()
	}
}

func TestUpdateInstrument(t *testing.T) {
	sqlConn, _ := sqlx.Connect("postgres", "host=localhost port=5551 user=postgres password=postgres dbname=postgres sslmode=disable")
	schemaName := "instrument_test"
	_, _ = sqlConn.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp" schema public;`)
	_, _ = sqlConn.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE;`, schemaName))
	_, _ = sqlConn.Exec(fmt.Sprintf(`CREATE SCHEMA %s;`, schemaName))
	skeletonMigrator := migrator.NewSkeletonMigrator()
	_ = skeletonMigrator.Run(context.Background(), sqlConn, schemaName)
	_, _ = sqlConn.Exec(fmt.Sprintf(`INSERT INTO %s.sk_supported_protocols (id, "name", description) VALUES ('abb539a3-286f-4c15-a7b7-2e9adf6eab91', 'IH-1000 v5.2', 'IHCOM');`, schemaName))

	configuration := config.Configuration{
		APIPort:                          5000,
		Authorization:                    false,
		PermittedOrigin:                  "*",
		ApplicationName:                  "Register instrument retry test",
		TCPListenerPort:                  5401,
		InstrumentTransferRetryDelayInMs: 50,
	}
	dbConn := db.CreateDbConnector(sqlConn)
	cerberusClientMock := &cerberusClientMock{
		registerInstrumentFunc: func(instrument Instrument) error {
			return nil
		},
	}
	instrumentRepository := NewInstrumentRepository(dbConn, schemaName)
	instrumentService := NewInstrumentService(&configuration, instrumentRepository, NewSkeletonManager(), NewInstrumentCache(), cerberusClientMock)

	var protocolID uuid.UUID
	err := dbConn.QueryRowx(`INSERT INTO instrument_test.sk_supported_protocols (name, description) VALUES('Test Protocol', 'Test Protocol Description') RETURNING id;`).Scan(&protocolID)
	assert.Nil(t, err)

	clientPort := 1234

	ctx := context.Background()
	ctx = context.WithValue(ctx, "Authorization", "BearerToken")

	instrumentID, err := instrumentService.CreateInstrument(ctx, Instrument{
		Name:               "TestInstrument",
		ProtocolID:         protocolID,
		ProtocolName:       "TestProtocol",
		Enabled:            true,
		ConnectionMode:     TCPMixed,
		ResultMode:         Simulation,
		CaptureResults:     true,
		CaptureDiagnostics: false,
		ReplyToQuery:       false,
		Status:             "TestStatus",
		FileEncoding:       "UTF8",
		Timezone:           "Europe/Berlin",
		Hostname:           "TestHost",
		ClientPort:         &clientPort,
	})
	assert.Nil(t, err)

	instrument, err := instrumentService.GetInstrumentByID(ctx, nil, instrumentID, false)
	assert.Nil(t, err)
	assert.Equal(t, "TestInstrument", instrument.Name)
	assert.Equal(t, protocolID, instrument.ProtocolID)

	analyteID1 := uuid.New()
	channelID1 := uuid.New()

	err = instrumentService.UpdateInstrument(ctx, Instrument{
		ID:                 instrumentID,
		Name:               "TestInstrumentUpdated",
		ProtocolID:         protocolID,
		ProtocolName:       "TestProtocol",
		Enabled:            true,
		ConnectionMode:     TCPMixed,
		ResultMode:         Simulation,
		CaptureResults:     true,
		CaptureDiagnostics: false,
		ReplyToQuery:       false,
		Status:             "TestStatus",
		FileEncoding:       "UTF8",
		Timezone:           "Europe/Berlin",
		Hostname:           "TestHost",
		ClientPort:         &clientPort,
		AnalyteMappings: []AnalyteMapping{
			{
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
				ResultType: "pein",
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
	})

	assert.Nil(t, err)
	instrument, err = instrumentService.GetInstrumentByID(ctx, nil, instrumentID, false)
	assert.Equal(t, "TestInstrumentUpdated", instrument.Name)
	assert.Len(t, instrument.AnalyteMappings, 1)
	assert.Equal(t, analyteID1, instrument.AnalyteMappings[0].AnalyteID)
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

	assert.Len(t, instrument.RequestMappings, 1)
	assert.Equal(t, "ReqMap", instrument.RequestMappings[0].Code)
	assert.Equal(t, true, instrument.RequestMappings[0].IsDefault)
	assert.Len(t, instrument.RequestMappings[0].AnalyteIDs, 1)
	assert.Equal(t, analyteID1, instrument.RequestMappings[0].AnalyteIDs[0])

	analyteID2 := uuid.New()
	analyteID3 := uuid.New()
	channelID2 := uuid.New()

	err = instrumentService.UpdateInstrument(ctx, Instrument{
		ID:                 instrumentID,
		Name:               "TestInstrumentUpdated2",
		ProtocolID:         protocolID,
		ProtocolName:       "TestProtocol",
		Enabled:            true,
		ConnectionMode:     TCPMixed,
		ResultMode:         Simulation,
		CaptureResults:     true,
		CaptureDiagnostics: false,
		ReplyToQuery:       false,
		Status:             "TestStatus",
		FileEncoding:       "UTF8",
		Timezone:           "Europe/Berlin",
		Hostname:           "TestHost",
		ClientPort:         &clientPort,
		AnalyteMappings: []AnalyteMapping{
			{
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
				ResultType: "pein",
			},
			{
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
				ResultType: "pein",
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
	})

	assert.Nil(t, err)
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
}

func TestHidePassword(t *testing.T) {
	configuration := config.Configuration{
		APIPort:                          5000,
		Authorization:                    false,
		PermittedOrigin:                  "*",
		ApplicationName:                  "Register instrument retry test",
		TCPListenerPort:                  5401,
		InstrumentTransferRetryDelayInMs: 50,
	}
	cerberusClientMock := &cerberusClientMock{
		registerInstrumentFunc: func(instrument Instrument) error {
			return nil
		},
	}
	mockInstrumentRepo := &instrumentRepositoryMock{db.CreateDbConnector(&sqlx.DB{})}
	instrumentService := NewInstrumentService(&configuration, mockInstrumentRepo, NewSkeletonManager(), NewInstrumentCache(), cerberusClientMock)
	instrument, err := instrumentService.GetInstrumentByID(context.TODO(), mockInstrumentRepo.db, uuid.MustParse("68f34e1d-1faa-4101-9e79-a743b420ab4e"), false)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(instrument.Settings))
	assert.Equal(t, "ThisIsMyPassword", instrument.Settings[1].Value)
	assert.Equal(t, uuid.MustParse("c81c77cf-f17a-402d-a44b-a0194eb00a29"), instrument.Settings[1].ProtocolSettingID)
	instrumentService.HidePassword(context.TODO(), &instrument)
	assert.Equal(t, "SomeSetting", instrument.Settings[0].Value)
	assert.Equal(t, uuid.MustParse("1f663361-3f2d-4c43-8cf6-65cec3fc88ab"), instrument.Settings[0].ProtocolSettingID)
	assert.Equal(t, "", instrument.Settings[1].Value)
}

type instrumentRepositoryMock struct {
	db db.DbConnector
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
		FileEncoding:   "UTF8",
		Timezone:       "Europe/Budapest",
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
		FileEncoding:   "UTF8",
		Timezone:       "Europe/Budapest",
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
func (r *instrumentRepositoryMock) MarkAsSentToCerberus(ctx context.Context, id uuid.UUID) error {
	return nil
}
func (r *instrumentRepositoryMock) GetUnsentToCerberus(ctx context.Context) ([]uuid.UUID, error) {
	return make([]uuid.UUID, 0), nil
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
func (r *instrumentRepositoryMock) CreateAnalyteMappings(ctx context.Context, analyteMappings []AnalyteMapping, instrumentID uuid.UUID) ([]uuid.UUID, error) {
	return make([]uuid.UUID, 0), nil
}
func (r *instrumentRepositoryMock) GetAnalyteMappings(ctx context.Context, instrumentIDs []uuid.UUID) (map[uuid.UUID][]AnalyteMapping, error) {
	return make(map[uuid.UUID][]AnalyteMapping), nil
}
func (r *instrumentRepositoryMock) UpdateAnalyteMapping(ctx context.Context, analyteMapping AnalyteMapping) error {
	return nil
}
func (r *instrumentRepositoryMock) DeleteAnalyteMappings(ctx context.Context, ids []uuid.UUID) error {
	return nil
}
func (r *instrumentRepositoryMock) CreateChannelMappings(ctx context.Context, channelMappings []ChannelMapping, analyteMappingID uuid.UUID) ([]uuid.UUID, error) {
	return make([]uuid.UUID, 0), nil
}
func (r *instrumentRepositoryMock) GetChannelMappings(ctx context.Context, analyteMappingIDs []uuid.UUID) (map[uuid.UUID][]ChannelMapping, error) {
	return make(map[uuid.UUID][]ChannelMapping), nil
}
func (r *instrumentRepositoryMock) UpdateChannelMapping(ctx context.Context, channelMapping ChannelMapping) error {
	return nil
}
func (r *instrumentRepositoryMock) DeleteChannelMappings(ctx context.Context, ids []uuid.UUID) error {
	return nil
}
func (r *instrumentRepositoryMock) CreateResultMappings(ctx context.Context, resultMappings []ResultMapping, analyteMappingID uuid.UUID) ([]uuid.UUID, error) {
	return make([]uuid.UUID, 0), nil
}
func (r *instrumentRepositoryMock) GetResultMappings(ctx context.Context, analyteMappingIDs []uuid.UUID) (map[uuid.UUID][]ResultMapping, error) {
	return make(map[uuid.UUID][]ResultMapping), nil
}
func (r *instrumentRepositoryMock) UpdateResultMapping(ctx context.Context, resultMapping ResultMapping) error {
	return nil
}
func (r *instrumentRepositoryMock) DeleteResultMappings(ctx context.Context, ids []uuid.UUID) error {
	return nil
}
func (r *instrumentRepositoryMock) CreateRequestMappings(ctx context.Context, requestMappings []RequestMapping, instrumentID uuid.UUID) ([]uuid.UUID, error) {
	return make([]uuid.UUID, 0), nil
}
func (r *instrumentRepositoryMock) UpsertRequestMappingAnalytes(ctx context.Context, analyteIDsByRequestMappingID map[uuid.UUID][]uuid.UUID) error {
	return nil
}
func (r *instrumentRepositoryMock) UpdateRequestMapping(ctx context.Context, requestMapping RequestMapping) error {
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
func (r *instrumentRepositoryMock) CreateTransaction() (db.DbConnector, error) {
	return r.db, nil
}
func (r *instrumentRepositoryMock) WithTransaction(tx db.DbConnector) InstrumentRepository {
	return r
}
