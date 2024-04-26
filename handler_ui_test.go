package skeleton

import (
	"bytes"
	"context"
	"fmt"
	"github.com/blutspende/skeleton/config"
	"github.com/blutspende/skeleton/db"
	"github.com/blutspende/skeleton/migrator"
	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCreateInstrument(t *testing.T) {
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
		ApplicationName:                  "Instrument API Test",
		TCPListenerPort:                  5401,
		InstrumentTransferRetryDelayInMs: 100,
	}
	dbConn := db.CreateDbConnector(sqlConn)
	cerberusClientMock := &cerberusClientMock{}
	instrumentRepository := NewInstrumentRepository(dbConn, schemaName)

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	instrumentService := NewInstrumentService(&config, instrumentRepository, NewSkeletonManager(ctx), NewInstrumentCache(), cerberusClientMock)

	responseRecorder := &httptest.ResponseRecorder{}
	c, engine := gin.CreateTestContext(responseRecorder)
	c.Request, _ = http.NewRequest(http.MethodPost, "/v1/instruments", bytes.NewBuffer([]byte(`{
  "id": "85fd0a74-e45a-4f9f-a862-89469050023d",
  "protocolId": "abb539a3-286f-4c15-a7b7-2e9adf6eab91",
  "type": "ANALYZER",
  "name": "Asdmen",
  "hostname": "192.168.1.10",
  "clientPort": null,
  "enabled": true,
  "captureResults": true,
  "captureDiagnostics": true,
  "replyToQuery": true,
  "status": "OFFLINE",
  "connectionMode": "TCP_SERVER_ONLY",
  "runningMode": "SIMULATION",
  "analyteMappings": [
    {
      "id": "6b40e8ad-a66c-405a-87e1-c9278c54d6a8",
      "instrumentAnalyte": "asdd",
      "analyteId": "1d56018c-2fac-444d-b049-d5fc9975d04b",
      "channelMappings": [],
      "resultMappings": [
        {
          "key": "pos",
          "value": "pos"
        }
      ],
      "resultType": "pein"
    }
  ],
  "protocolAbilities": null,
  "requestMappings": [
    {
      "id": "0f3d041b-a729-46a2-90c8-c612930b19dd",
      "code": "ZSO",
      "requestMappingAnalyteIds": [
        "e647f844-0caa-4927-be38-effac08b7264",
        "37eeed36-9471-419e-8e70-576a9189fba2"
      ]
    }
  ],
  "fileEncoding": "UTF8",
  "timezone": "Europe/Budapest",
  "ftpServerType": "",
  "ftpServerHostname": "",
  "ftpServerHostkey": "",
  "ftpServerBasepath": "",
  "ftpServerFilemaskDownload": "",
  "ftpServerFilemaskUpload": "",
  "ftpServerUsername": "",
  "ftpServerPassword": "",
  "ftpServerPublicKey": "",
  "ftpServerPort": ""
}`)))
	api := api{
		config:            &config,
		engine:            engine,
		instrumentService: instrumentService,
	}
	engine.POST("/v1/instruments", api.CreateInstrument)

	engine.ServeHTTP(responseRecorder, c.Request)

	if responseRecorder.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, responseRecorder.Code)
	}
}

func TestCreateInstrumentWithoutRequestMapping(t *testing.T) {
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
		ApplicationName:                  "Instrument API Test",
		TCPListenerPort:                  5401,
		InstrumentTransferRetryDelayInMs: 100,
	}
	dbConn := db.CreateDbConnector(sqlConn)
	cerberusClientMock := &cerberusClientMock{}
	instrumentRepository := NewInstrumentRepository(dbConn, schemaName)

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	instrumentService := NewInstrumentService(&config, instrumentRepository, NewSkeletonManager(ctx), NewInstrumentCache(), cerberusClientMock)

	responseRecorder := &httptest.ResponseRecorder{}
	c, engine := gin.CreateTestContext(responseRecorder)
	c.Request, _ = http.NewRequest(http.MethodPost, "/v1/instruments", bytes.NewBuffer([]byte(`{
  "id": "85fd0a74-e45a-4f9f-a862-89469050023d",
  "protocolId": "abb539a3-286f-4c15-a7b7-2e9adf6eab91",
  "type": "IH-1000 v5.2",
  "name": "Asdmen",
  "hostname": "192.168.1.10",
  "clientPort": null,
  "enabled": true,
  "captureResults": true,
  "captureDiagnostics": true,
  "replyToQuery": true,
  "status": "OFFLINE",
  "connectionMode": "TCP_SERVER_ONLY",
  "runningMode": "SIMULATION",
  "analyteMappings": [
    {
      "id": "6b40e8ad-a66c-405a-87e1-c9278c54d6a8",
      "instrumentAnalyte": "asdd",
      "analyteId": "1d56018c-2fac-444d-b049-d5fc9975d04b",
      "channelMappings": [],
      "resultMappings": [
        {
          "key": "pos",
          "value": "pos"
        }
      ],
      "resultType": "pein"
    }
  ],
  "protocolAbilities": null,
  "requestMappings": [],
  "fileEncoding": "UTF8",
  "timezone": "Europe/Budapest",
  "ftpServerType": "",
  "ftpServerHostname": "",
  "ftpServerHostkey": "",
  "ftpServerBasepath": "",
  "ftpServerFilemaskDownload": "",
  "ftpServerFilemaskUpload": "",
  "ftpServerUsername": "",
  "ftpServerPassword": "",
  "ftpServerPublicKey": "",
  "ftpServerPort": ""
}`)))
	api := api{
		config:            &config,
		engine:            engine,
		instrumentService: instrumentService,
	}
	engine.POST("/v1/instruments", api.CreateInstrument)

	engine.ServeHTTP(responseRecorder, c.Request)

	if responseRecorder.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, responseRecorder.Code)
	}
}
