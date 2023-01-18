package skeleton

import (
	"context"
	"errors"
	"fmt"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/config"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/db"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/migrator"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"testing"
	"time"
)

func TestRegisterCreatedInstrument(t *testing.T) {
	postgres := embeddedpostgres.NewDatabase()
	_ = postgres.Start()
	defer postgres.Stop()
	sqlConn, _ := sqlx.Connect("postgres", "host=localhost port=5432 user=postgres password=postgres dbname=postgres sslmode=disable")
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
	instrumentService := NewInstrumentService(&config, instrumentRepository, NewCallbackManager(), NewInstrumentCache(), cerberusClientMock)

	_, _ = instrumentService.CreateInstrument(context.Background(), Instrument{
		ID:             uuid.MustParse("68f34e1d-1faa-4101-9e79-a743b420ab4e"),
		Name:           "test",
		ProtocolID:     uuid.MustParse("abb539a3-286f-4c15-a7b7-2e9adf6eab91"),
		ProtocolName:   "Test Protocol",
		Enabled:        true,
		ConnectionMode: "TCP_MIXED",
		ResultMode:     "TEST",
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

type cerberusClientMock struct {
	registerInstrumentFunc func(instrument Instrument) error
}

func (m *cerberusClientMock) RegisterInstrument(instrument Instrument) error {
	if m.registerInstrumentFunc == nil {
		return nil
	}
	return m.registerInstrumentFunc(instrument)
}

func (m *cerberusClientMock) SendAnalysisResultBatch(analysisResults []AnalysisResult) (AnalysisResultBatchResponse, error) {
	return AnalysisResultBatchResponse{}, nil
}
