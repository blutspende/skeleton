package services_test

import (
	"context"
	"fmt"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"skeleton/config"
	"skeleton/db"
	"skeleton/migrator"
	"skeleton/repositories"
	"skeleton/services"
	"skeleton/v1"
	"testing"
	"time"
)

func TestSubmitAnalysisResult(t *testing.T) {
	postgres := embeddedpostgres.NewDatabase()
	postgres.Start()
	defer postgres.Stop()
	sqlConn, err := sqlx.Connect("postgres", "host=localhost port=5432 user=postgres password=postgres dbname=postgres sslmode=disable")

	schemaName := "testSubmitAnalysisResults"
	assert.Nil(t, err)

	_, err = sqlConn.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp" schema public;`)
	assert.Nil(t, err)

	_, err = sqlConn.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE;`, schemaName))
	assert.Nil(t, err)

	_, err = sqlConn.Exec(fmt.Sprintf(`CREATE SCHEMA %s;`, schemaName))
	assert.Nil(t, err)

	testConfig := config.Configuration{}
	testConfig.PostgresDB.Host = "localhost"
	testConfig.PostgresDB.Port = 5432
	testConfig.PostgresDB.User = "postgres"
	testConfig.PostgresDB.Pass = "postgres"
	testConfig.PostgresDB.Database = "postgres"
	testConfig.PostgresDB.SSLMode = "disable"

	dbConn := db.CreateDbConnector(sqlConn)
	analysisRepository := repositories.NewAnalysisRepository(dbConn, schemaName)

	analysisService := services.NewAnalysisService()

	skeleton := NewV1(migrator.NewSkeletonMigrator(), analysisService, analysisRepository, &cerberusClientMock{})
	analysisRequests := []v1.AnalysisRequestV1{
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
	analysisService.CreateAnalysisRequests(context.TODO(), analysisRequests)
	skeleton.SubmitAnalysisResult(context.TODO(), v1.AnalysisResultV1{
		ID:              uuid.UUID{},
		AnalysisRequest: analysisRequests[0],
		AnalyteMapping: v1.AnalyteMappingV1{
			AnalyteID:    uuid.New(),
			InstrumentID: uuid.New(),
		},
		Instrument:               v1.InstrumentV1{},
		ResultRecordID:           uuid.UUID{},
		Result:                   "",
		Status:                   "",
		ResultYieldDateTime:      time.Time{},
		ValidUntil:               time.Time{},
		Operator:                 "",
		TechnicalReleaseDateTime: time.Time{},
		InstrumentRunID:          uuid.UUID{},
		RunCounter:               0,
		Edited:                   false,
		EditReason:               "",
		Warnings:                 nil,
		ChannelResults:           nil,
		ExtraValues:              nil,
		ReagentInfos:             nil,
		Images:                   nil,
		IsSentToCerberus:         false,
		ErrorMessage:             "",
		RetryCount:               0,
	})
}

type cerberusClientMock struct {
	analysisResults []v1.AnalysisResultV1
}

func (m *cerberusClientMock) RegisterInstrument(instrument v1.InstrumentV1) error {
	return nil
}

func (m *cerberusClientMock) PostAnalysisResultBatch(analysisResults []v1.AnalysisResultV1) ([]v1.AnalysisResultCreateStatusV1, error) {
	analysisResults = append(analysisResults, analysisResults...)
	resp := make([]v1.AnalysisResultCreateStatusV1, len(analysisResults))
	for i := range resp {
		resp[i].Success = true
	}
	return resp, nil
}
