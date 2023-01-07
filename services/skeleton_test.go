package services_test

import (
	"context"
	"fmt"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/config"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/db"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/migrator"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/model"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/repositories"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/services"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
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
	cerberusClientMock := cerberusClientMock{}

	skeleton := services.New(migrator.NewSkeletonMigrator(), analysisService, analysisRepository,&cerberusClientMock)
	analysisRequests := []model.AnalysisRequestV1{
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
	_, err = analysisService.CreateAnalysisRequests(context.TODO(), analysisRequests)
	assert.Nil(t, err)

	err = skeleton.SubmitAnalysisResult(context.TODO(), model.AnalysisResultV1{
		ID:              uuid.UUID{},
		AnalysisRequest: analysisRequests[0],
		AnalyteMapping: model.AnalyteMappingV1{
			AnalyteID:    uuid.New(),
			InstrumentID: uuid.New(),
		},
		Instrument:               model.InstrumentV1{},
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
	assert.Nil(t, err)
	time.Sleep(4 * time.Second)
	assert.Equal(t, 1, len(cerberusClientMock.AnalysisResults))
}

type cerberusClientMock struct {
	AnalysisResults []model.AnalysisResultV1
	ResponseStatuses []model.AnalysisResultCreateStatusV1
}

func (m *cerberusClientMock) RegisterInstrument(instrument model.InstrumentV1) error {
	return nil
}

func (m *cerberusClientMock) PostAnalysisResultBatch(analysisResults []model.AnalysisResultV1) ([]model.AnalysisResultCreateStatusV1, error) {
	m.AnalysisResults = append(m.AnalysisResults, analysisResults...)
	resp := make([]model.AnalysisResultCreateStatusV1, len(analysisResults))
	for i := range resp {
		resp[i].Success = true
	}
	m.ResponseStatuses = resp
	return resp, nil
}
