package skeleton_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/DRK-Blutspende-BaWueHe/skeleton"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/db"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/migrator"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

func TestSkeletonStart(t *testing.T) {
	postgres := embeddedpostgres.NewDatabase()
	postgres.Start()
	defer postgres.Stop()
	sqlConn, err := sqlx.Connect("postgres", "host=localhost port=5432 user=postgres password=postgres dbname=postgres sslmode=disable")
	assert.Nil(t, err)

	skeletonApi, err := skeleton.New(sqlConn, "skeleton")
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

func TestSubmitAnalysisResult(t *testing.T) {
	postgres := embeddedpostgres.NewDatabase()
	postgres.Start()
	defer postgres.Stop()
	sqlConn, err := sqlx.Connect("postgres", "host=localhost port=5432 user=postgres password=postgres dbname=postgres sslmode=disable")
	assert.Nil(t, err)

	schemaName := "testSubmitAnalysisResults"

	_, err = sqlConn.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp" schema public;`)
	assert.Nil(t, err)

	_, err = sqlConn.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE;`, schemaName))
	assert.Nil(t, err)

	_, err = sqlConn.Exec(fmt.Sprintf(`CREATE SCHEMA %s;`, schemaName))
	assert.Nil(t, err)

	dbConn := db.CreateDbConnector(sqlConn)
	analysisRepository := skeleton.NewAnalysisRepository(dbConn, schemaName)
	instrumentRepository := skeleton.NewInstrumentRepository(dbConn, schemaName)

	analysisService := skeleton.NewAnalysisService(analysisRepository)
	instrumentService := skeleton.NewInstrumentService(instrumentRepository, nil, nil, nil)
	cerberusClientMock := cerberusClientMock{}

	skeletonInstance, _ := skeleton.NewSkeleton(sqlConn, schemaName, migrator.NewSkeletonMigrator(), nil, analysisRepository, instrumentService, nil, &cerberusClientMock)
	func() {
		err = skeletonInstance.Start()
		assert.Nil(t, err)
	}()
	analysisRequests := []skeleton.AnalysisRequest{
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

	//TODO
	//create instrument + analytemapping

	instrumentID := uuid.New()

	analysisResult := skeleton.AnalysisResult{
		ID:              uuid.UUID{},
		AnalysisRequest: analysisRequests[0],
		AnalyteMapping: skeleton.AnalyteMapping{
			AnalyteID: analysisRequests[0].ID,
		},
		Instrument: skeleton.Instrument{
			ID: instrumentID,
		},
		ResultRecordID:           uuid.UUID{},
		Result:                   "pos",
		Status:                   "",
		ResultYieldDateTime:      time.Time{},
		ValidUntil:               time.Time{},
		Operator:                 "",
		TechnicalReleaseDateTime: time.Time{},
		InstrumentRunID:          uuid.UUID{},
		RunCounter:               0,
		Edited:                   false,
		EditReason:               "",
		Warnings:                 []string{"test warning"},
		ChannelResults:           nil,
		ExtraValues:              nil,
		ReagentInfos:             nil,
		Images:                   nil,
		IsSentToCerberus:         false,
		ErrorMessage:             "",
		RetryCount:               0,
	}

	err = skeletonInstance.SubmitAnalysisResult(context.TODO(), analysisResult)
	assert.Nil(t, err)
	time.Sleep(4 * time.Second)
	assert.Equal(t, 1, len(cerberusClientMock.AnalysisResults))
	assert.Equal(t, analysisRequests[0].WorkItemID, cerberusClientMock.AnalysisResults[0].AnalysisRequest.WorkItemID)
	assert.Equal(t, analysisRequests[0].SampleCode, cerberusClientMock.AnalysisResults[0].AnalysisRequest.SampleCode)
	assert.Equal(t, analysisRequests[0].AnalyteID, cerberusClientMock.AnalysisResults[0].AnalysisRequest.AnalyteID)
	assert.Equal(t, instrumentID, cerberusClientMock.AnalysisResults[0].Instrument.ID)
	assert.Equal(t, analysisResult.Result, cerberusClientMock.AnalysisResults[0].Result)
}

//func TestCreateInstrument(t *testing.T) {
//	postgres := embeddedpostgres.NewDatabase()
//	postgres.Start()
//	defer postgres.Stop()
//	sqlConn, err := sqlx.Connect("postgres", "host=localhost port=5432 user=postgres password=postgres dbname=postgres sslmode=disable")
//	assert.Nil(t, err)
//
//	schemaName := "testSubmitAnalysisResults"
//
//	_, err = sqlConn.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp" schema public;`)
//	assert.Nil(t, err)
//
//	_, err = sqlConn.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE;`, schemaName))
//	assert.Nil(t, err)
//
//	_, err = sqlConn.Exec(fmt.Sprintf(`CREATE SCHEMA %s;`, schemaName))
//	assert.Nil(t, err)
//
//	dbConn := db.CreateDbConnector(sqlConn)
//	analysisRepository := skeleton.NewAnalysisRepository(dbConn, schemaName)
//	instrumentRepository := skeleton.NewInstrumentRepository(dbConn, schemaName)
//
//	cerberusClientMock := cerberusClientMock{}
//
//	//todo
//	skeletonInstance := skeleton.NewSkeleton(sqlConn, schemaName, migrator.NewSkeletonMigrator(), nil, analysisRepository, instrumentRepository, &cerberusClientMock)
//	func() {
//		err = skeletonInstance.Start()
//		assert.Nil(t, err)
//	}()
//
//}

func TestRegisterProtocol(t *testing.T) {
	//TODO
	// register protocol
	// register instrument with mentioned protocol (call endpoint ???) -> expect to pass
	// try register instrument with random protocolID -> expect to fail
}

type cerberusClientMock struct {
	AnalysisResults  []skeleton.AnalysisResult
	ResponseStatuses []skeleton.AnalysisResultCreateStatusV1
}

func (m *cerberusClientMock) RegisterInstrument(instrument skeleton.Instrument) error {
	return nil
}

func (m *cerberusClientMock) PostAnalysisResultBatch(analysisResults []skeleton.AnalysisResult) ([]skeleton.AnalysisResultCreateStatusV1, error) {
	m.AnalysisResults = append(m.AnalysisResults, analysisResults...)
	resp := make([]skeleton.AnalysisResultCreateStatusV1, len(analysisResults))
	for i := range resp {
		resp[i].Success = true
	}
	m.ResponseStatuses = resp
	return resp, nil
}
