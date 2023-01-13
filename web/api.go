package web

import (
	"fmt"
	"github.com/DRK-Blutspende-BaWueHe/skeleton"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/web/middleware"
	"net/http/pprof"
	"sync"

	"github.com/rs/zerolog"

	"github.com/gin-gonic/gin"
)

type Api interface {
	Run() error
}

type api struct {
	config            *skeleton.Configuration
	engine            *gin.Engine
	analysisService   skeleton.AnalysisService
	instrumentService skeleton.InstrumentService
	/*healthHandler               handlers.HealthHandler
	instrumentsHandler          handlers.Instruments
	analyteMappingHandler       handlers.AnalyteMapping
	requestVisualizationHandler handlers.RequestVisualization
	resultMappingHandler        handlers.ResultMapping
	analysisRequestHandler      handlers.AnalysisRequest
	requestMapping              handlers.RequestMapping
	channelMapping              handlers.ChannelMapping*/
	createAnalysisRequestMutex sync.Mutex
}

func (api *api) Run() error {
	if api.config.EnableTLS {
		return api.engine.RunTLS(fmt.Sprintf("bloodlab:%d", api.config.APIPort), api.config.BloodlabCertPath, api.config.BloodlabKeyPath)
	}

	return api.engine.Run(fmt.Sprintf(":%d", api.config.APIPort))
}

func NewAPI(config *skeleton.Configuration, authManager skeleton.AuthManager,
	analysisService skeleton.AnalysisService, instrumentService skeleton.InstrumentService) Api {

	if config.LogLevel <= zerolog.DebugLevel {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	engine := gin.New()
	engine.Use(gin.Recovery())

	api := api{
		config:            config,
		engine:            engine,
		analysisService:   analysisService,
		instrumentService: instrumentService,
		/*healthHandler:               healthHandler,
		instrumentsHandler:          instrumentsHandler,
		analyteMappingHandler:       analyteMappingHandler,
		requestVisualizationHandler: requestVisualizationHandler,
		resultMappingHandler:        resultMappingHandler,
		analysisRequestHandler:      analysisRequestHandler,
		requestMapping:              requestMapping,
		channelMapping:              channelMapping,*/
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
		instrumentsGroup.POST("", api.CreateInstrument)
		instrumentsGroup.GET("/:instrumentId", api.GetInstrumentByID)
		//instrumentsGroup.GET("/:instrumentID/requests", api.GetAnalysisRequests)
		//instrumentsGroup.GET("/:instrumentID/results", api.GetAnalysisResults)
		//instrumentsGroup.GET("/channel-results/:requestID", api.GetChannelResultsForRequest)
		//instrumentsGroup.GET("/:instrumentID/list/transmissions", api.GetListOfTransmission)
		instrumentsGroup.PUT("/:instrumentId", api.UpdateInstrument)
		instrumentsGroup.DELETE("/:instrumentId", api.DeleteInstrument)
		//instrumentsGroup.POST("/request/:requestID/add-to-queue", api.AddRequestToTransferQueue)
		//instrumentsGroup.POST("/request/add-message-batch-to-queue", api.AddTransmissionsBatchToTransferQueue)
	}
	/*
		mappingGroup := v1Group.Group("/:analyteMappingID")
		{
			mappingGroup.PUT("/result-mappings", api.resultMappingHandler.UpdateResultMapping)
			mappingGroup.PUT("/channel-mappings", api.channelMapping.UpdateChannelMapping)
		}
	*/
	/*
		requestMappingGroup := instrumentsGroup.Group("/:instrumentID/request-mapping")
		{
			requestMappingGroup.POST("", api.requestMapping.CreateMapping)
			requestMappingGroup.PUT("", api.requestMapping.UpdateMapping)
			requestMappingGroup.DELETE("/:mappingID", api.requestMapping.DeleteMapping)
		}
	*/
	/*
		messagesGroup := instrumentsGroup.Group("/:instrumentID/messages")
		{
			messagesGroup.GET("", api.instrumentsHandler.GetMessages)
		}
	*/

	protocolVersions := v1Group.Group("/protocol-versions")
	{
		protocolVersions.GET("", api.GetSupportedProtocols)
		protocolVersions.GET("/:protocolVersionID/abilities", api.GetProtocolAbilities)
		//protocolVersions.GET("/:protocolVersionID/manufacturer-tests", api.instrumentsHandler.GetSupportedTestnames)
	}

	/*
		analysisRequests := v1Group.Group("analysis-requests")
		analysisRequests.POST("", api.analysisRequestHandler.CreateAnalysisRequest)
		analysisRequests.POST("/batch", api.analysisRequestHandler.CreateAnalysisRequestBatch)
	*/
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
