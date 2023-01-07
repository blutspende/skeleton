package api

import (
	"astm/skeleton/api/middleware"
	"astm/skeleton/auth"
	"astm/skeleton/config"
	"astm/skeleton/services"
	"fmt"
	"net/http/pprof"
	"sync"

	"github.com/rs/zerolog"

	"github.com/gin-gonic/gin"
)

type Api interface {
	Run() error
}

type api struct {
	config          *config.Configuration
	engine          *gin.Engine
	analysisService services.AnalysisService
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
	} else {
		return api.engine.Run(fmt.Sprintf(":%d", api.config.APIPort))
	}
}

func NewAPI(config *config.Configuration, authManager auth.AuthManager,
	analysisService services.AnalysisService) Api {

	if config.LogLevel <= zerolog.DebugLevel {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	engine := gin.New()
	engine.Use(gin.Recovery())

	api := api{
		config:          config,
		engine:          engine,
		analysisService: analysisService,
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

	v1Group.GET("/")
	/*
		instrumentsGroup := v1Group.Group("/instruments")
		{
			instrumentsGroup.GET("", api.instrumentsHandler.GetInstruments)
			instrumentsGroup.POST("", api.instrumentsHandler.PostInstrument)
			instrumentsGroup.GET("/:instrumentID", api.instrumentsHandler.GetInstrumentByID)
			instrumentsGroup.GET("/:instrumentID/requests", api.requestVisualizationHandler.GetAnalysisRequests)
			instrumentsGroup.GET("/:instrumentID/results", api.requestVisualizationHandler.GetAnalysisResults)
			instrumentsGroup.GET("/channel-results/:requestID", api.analysisRequestHandler.GetChannelResultsForRequest)
			instrumentsGroup.GET("/:instrumentID/list/transmissions", api.instrumentsHandler.GetListOfTransmission)
			instrumentsGroup.PUT("/:instrumentID", api.instrumentsHandler.PutInstrument)
			instrumentsGroup.DELETE("/:instrumentID", api.instrumentsHandler.DeleteInstrument)
			instrumentsGroup.POST("/request/:requestID/add-to-queue", api.analysisRequestHandler.AddRequestToTransferQueue)
			instrumentsGroup.POST("/request/add-message-batch-to-queue", api.analysisRequestHandler.AddTransmissionsBatchToTransferQueue)
		}
	*/
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
	/*
		protocolVersions := v1Group.Group("/protocol-versions")
		{
			protocolVersions.GET("", api.instrumentsHandler.GetSupportedInstrumentTypes)
			protocolVersions.GET("/:protocolVersionID/abilities", api.instrumentsHandler.GetAbilities)
			protocolVersions.GET("/:protocolVersionID/manufacturer-tests", api.instrumentsHandler.GetSupportedTestnames)
		}
	*/
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
