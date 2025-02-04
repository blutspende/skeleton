package skeleton

import (
	"fmt"
	"github.com/blutspende/skeleton/config"
	"github.com/blutspende/skeleton/consolelog/service"
	"github.com/blutspende/skeleton/middleware"
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
	consoleLogService service.ConsoleLogService) GinApi {
	return newAPI(gin.New(), config, authManager, analysisService, instrumentService, consoleLogService)
}

func NewAPIForTesting(engine *gin.Engine,
	config *config.Configuration,
	authManager AuthManager,
	analysisService AnalysisService,
	instrumentService InstrumentService,
	consoleLogService service.ConsoleLogService) GinApi {
	return newAPI(engine, config, authManager, analysisService, instrumentService, consoleLogService)
}

func newAPI(engine *gin.Engine, config *config.Configuration, authManager AuthManager,
	analysisService AnalysisService, instrumentService InstrumentService, consoleLogService service.ConsoleLogService) GinApi {

	if config.LogLevel <= zerolog.DebugLevel {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	engine.Use(gin.Recovery())
	engine.Use(gin.Logger())

	api := api{
		config:            config,
		engine:            engine,
		analysisService:   analysisService,
		instrumentService: instrumentService,
		consoleLogService: consoleLogService,
	}

	corsMiddleWare := middleware.CreateCorsMiddleware(config)
	engine.Use(corsMiddleWare)

	root := engine.Group("")

	v1Group := root.Group("v1")

	if api.config.Authorization {
		authMiddleWare := middleware.CheckAuth(authManager)
		v1Group.Use(authMiddleWare)
	}

	expectedControlResultsGroup := v1Group.Group("/expected-control-results")
	{
		expectedControlResultsGroup.GET("/:instrumentId", middleware.RoleProtection([]middleware.UserRole{
			middleware.Admin,
			middleware.MedLabSuper,
			middleware.MedLabHead,
			middleware.MedLabDoc,
			middleware.MedLabAssist,
			middleware.ITSupport}, false, api.config.Authorization), api.GetExpectedControlResultsByInstrumentId)
		expectedControlResultsGroup.GET("/not-specified/:instrumentId", middleware.RoleProtection([]middleware.UserRole{
			middleware.Admin,
			middleware.MedLabSuper,
			middleware.MedLabHead,
			middleware.MedLabDoc,
			middleware.MedLabAssist}, false, api.config.Authorization), api.GetNotSpecifiedExpectedControlResultsByInstrumentId)
		expectedControlResultsGroup.POST("/:instrumentId", middleware.RoleProtection([]middleware.UserRole{
			middleware.Admin,
			middleware.MedLabSuper,
			middleware.MedLabHead,
			middleware.MedLabDoc,
			middleware.MedLabAssist}, false, api.config.Authorization), api.CreateExpectedControlResults)
		expectedControlResultsGroup.PUT("/:instrumentId", middleware.RoleProtection([]middleware.UserRole{
			middleware.Admin,
			middleware.MedLabSuper,
			middleware.MedLabHead,
			middleware.MedLabDoc,
			middleware.MedLabAssist}, false, api.config.Authorization), api.UpdateExpectedControlResults)
		expectedControlResultsGroup.DELETE("/:expectedControlResultId", middleware.RoleProtection([]middleware.UserRole{
			middleware.Admin,
			middleware.MedLabSuper,
			middleware.MedLabHead,
			middleware.MedLabDoc,
			middleware.MedLabAssist}, false, api.config.Authorization), api.DeleteExpectedControlResult)
	}

	return &api
}
