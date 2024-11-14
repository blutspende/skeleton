package skeleton

import (
	"fmt"
	"github.com/blutspende/skeleton/config"
	"github.com/blutspende/skeleton/consolelog/service"
	"github.com/blutspende/skeleton/middleware"
	"github.com/blutspende/skeleton/server"
	"net/http/pprof"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
)

type api struct {
	config                     *config.Configuration
	engine                     *gin.Engine
	analysisService            AnalysisService
	instrumentService          InstrumentService
	consoleLogService          service.ConsoleLogService
	createAnalysisRequestMutex sync.Mutex
	consoleLogSSEServer        *server.ConsoleLogSSEServer
}

func (api *api) Run() error {
	if api.config.EnableTLS {
		return api.engine.RunTLS(fmt.Sprintf("bloodlab:%d", api.config.APIPort), api.config.BloodlabCertPath, api.config.BloodlabKeyPath)
	}

	return api.engine.Run(fmt.Sprintf(":%d", api.config.APIPort))
}

func NewAPI(config *config.Configuration,
	authManager AuthManager,
	analysisService AnalysisService,
	instrumentService InstrumentService,
	consoleLogService service.ConsoleLogService,
	consoleLogSSEServer *server.ConsoleLogSSEServer) GinApi {
	return newAPI(gin.New(), config, authManager, analysisService, instrumentService, consoleLogService, consoleLogSSEServer)
}

func NewAPIForTesting(engine *gin.Engine,
	config *config.Configuration,
	authManager AuthManager,
	analysisService AnalysisService,
	instrumentService InstrumentService,
	consoleLogService service.ConsoleLogService,
	consoleLogSSEServer *server.ConsoleLogSSEServer) GinApi {
	return newAPI(engine, config, authManager, analysisService, instrumentService, consoleLogService, consoleLogSSEServer)
}

func newAPI(engine *gin.Engine, config *config.Configuration, authManager AuthManager,
	analysisService AnalysisService, instrumentService InstrumentService, consoleLogService service.ConsoleLogService,
	consoleLogSSEServer *server.ConsoleLogSSEServer) GinApi {

	if config.LogLevel <= zerolog.DebugLevel {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	engine.Use(gin.Recovery())
	engine.Use(gin.Logger())

	api := api{
		config:              config,
		engine:              engine,
		analysisService:     analysisService,
		instrumentService:   instrumentService,
		consoleLogService:   consoleLogService,
		consoleLogSSEServer: consoleLogSSEServer,
	}

	corsMiddleWare := middleware.CreateCorsMiddleware(config)
	engine.Use(corsMiddleWare)

	root := engine.Group("")
	root.GET("/health", api.GetHealth)

	v1Group := root.Group("v1")

	if api.config.Authorization {
		authMiddleWare := middleware.CheckAuth(authManager)
		v1Group.Use(authMiddleWare)
	}

	instrumentsGroup := v1Group.Group("/instruments")
	{
		instrumentsGroup.GET("", api.GetInstruments)
		instrumentsGroup.POST("", middleware.RoleProtection([]middleware.UserRole{middleware.Admin}, true, api.config.Authorization), api.CreateInstrument)
		instrumentsGroup.GET("/:instrumentId", api.GetInstrumentByID)
		instrumentsGroup.GET("/:instrumentId/requests", api.GetAnalysisRequestsInfo)
		instrumentsGroup.GET("/:instrumentId/results", api.GetAnalysisResultsInfo)
		instrumentsGroup.GET("/:instrumentId/list/transmissions", api.GetAnalysisBatches)
		instrumentsGroup.PUT("/:instrumentId", middleware.RoleProtection([]middleware.UserRole{middleware.Admin}, true, api.config.Authorization), api.UpdateInstrument)
		instrumentsGroup.DELETE("/:instrumentId", middleware.RoleProtection([]middleware.UserRole{middleware.Admin}, true, api.config.Authorization), api.DeleteInstrument)
		instrumentsGroup.POST("/result/:resultID/retransmit", api.RetransmitResult)
		instrumentsGroup.POST("/result/retransmit/batches", api.RetransmitResultBatches)
		instrumentsGroup.POST("/reprocess", api.ReprocessInstrumentData)
		instrumentsGroup.POST("/reprocess/sample-code", api.ReprocessInstrumentDataBySampleCode)
		instrumentsGroup.GET("/protocol/:protocolId/encodings", api.GetEncodings)
		instrumentsGroup.GET("/:instrumentId/console/handshake", middleware.SSEHeadersMiddleware(), api.consoleLogSSEServer.ServeHTTP())

		messagesGroup := instrumentsGroup.Group("/:instrumentId/messages")
		{
			messagesGroup.GET("", api.GetMessages)
		}
	}

	expectedControlResultsGroup := v1Group.Group("/expected-control-results")
	{
		expectedControlResultsGroup.GET("/:instrumentId", api.GetExpectedControlResultsByInstrumentId)
		expectedControlResultsGroup.PUT("/:instrumentId", api.UpdateExpectedControlResults)
	}

	protocolVersions := v1Group.Group("/protocol-versions")
	{
		protocolVersions.GET("", api.GetSupportedProtocols)
		protocolVersions.GET("/:protocolVersionId/abilities", api.GetProtocolAbilities)
		protocolVersions.GET("/:protocolVersionId/manufacturer-tests", api.GetManufacturerTests)
	}

	analysisRequests := v1Group.Group("analysis-requests")
	{
		analysisRequests.POST("/batch", api.CreateAnalysisRequestBatch)
		analysisRequests.DELETE("/batch", api.RevokeAnalysisRequestBatch)
		analysisRequests.POST("/batch/reexamine", api.ReexamineAnalysisRequestBatch)
	}

	v1Group.POST("/analytes/usage", api.CheckAnalytesUsage)

	// Development-option enables debugger, this can have side-effects
	if api.config.Development {
		debug := root.Group("/debug/pprof")
		{
			debug.GET("/", gin.WrapF(pprof.Index))
			debug.GET("/cmdline", gin.WrapF(pprof.Cmdline))
			debug.GET("/profile", gin.WrapF(pprof.Profile))
			debug.GET("/symbol", gin.WrapF(pprof.Symbol))
			debug.GET("/trace", gin.WrapF(pprof.Trace))
			debug.GET("/allocs", gin.WrapH(pprof.Handler("allocs")))
			debug.GET("/block", gin.WrapH(pprof.Handler("block")))
			debug.GET("/goroutine", gin.WrapH(pprof.Handler("goroutine")))
			debug.GET("/heap", gin.WrapH(pprof.Handler("heap")))
			debug.GET("/mutex", gin.WrapH(pprof.Handler("mutex")))
			debug.GET("/threadcreate", gin.WrapH(pprof.Handler("threadcreate")))
			debug.POST("/symbol", gin.WrapF(pprof.Symbol))
		}
	}

	return &api
}
