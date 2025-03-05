package skeleton

import (
	"errors"
	"github.com/blutspende/skeleton/middleware"
	"net/http"
	"time"

	"github.com/blutspende/skeleton/utils"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type instrumentTO struct {
	ID                  uuid.UUID             `json:"id"`
	Type                InstrumentType        `json:"instrumentType"`
	Name                string                `json:"name"`
	ProtocolID          uuid.UUID             `json:"protocolId"`
	ProtocolName        Protocol              `json:"type"`
	Enabled             bool                  `json:"enabled"`
	ConnectionMode      ConnectionMode        `json:"connectionMode"`
	ResultMode          ResultMode            `json:"runningMode"`
	CaptureResults      bool                  `json:"captureResults"`
	CaptureDiagnostics  bool                  `json:"captureDiagnostics"`
	ReplyToQuery        bool                  `json:"replyToQuery"`
	Status              string                `json:"status"`
	FileEncoding        string                `json:"fileEncoding"`
	Timezone            string                `json:"timezone"`
	Hostname            string                `json:"hostname"`
	ClientPort          *int                  `json:"clientPort"`
	FtpUsername         *string               `json:"ftpUserName"`
	FtpPassword         *string               `json:"ftpPassword"`
	FtpOrderPath        *string               `json:"ftpOrderPath"`
	FtpOrderFileMask    *string               `json:"ftpOrderFileMask"`
	FtpOrderFileSuffix  *string               `json:"ftpOrderFileSuffix"`
	FtpResultPath       *string               `json:"ftpResultPath"`
	FtpResultFileMask   *string               `json:"ftpResultFileMask"`
	FtpResultFileSuffix *string               `json:"ftpResultFileSuffix"`
	FtpServerType       *string               `json:"ftpServerType"`
	AnalyteMappings     []analyteMappingTO    `json:"analyteMappings"`
	RequestMappings     []requestMappingTO    `json:"requestMappings"`
	Settings            []instrumentSettingTO `json:"instrumentSettings"`
	SortingRuleGroups   []sortingRuleGroupTO  `json:"sortingRuleGroups"`
}

type listInstrumentTO struct {
	ID           uuid.UUID  `json:"id"`
	Name         string     `json:"name"`
	ProtocolName Protocol   `json:"type"`
	Status       string     `json:"status"`
	ResultMode   ResultMode `json:"runningMode"`
}

type analyteMappingTO struct {
	ID                       uuid.UUID          `json:"id"`
	InstrumentAnalyte        string             `json:"instrumentAnalyte"`
	AnalyteID                uuid.UUID          `json:"analyteId"`
	ChannelMappings          []channelMappingTO `json:"channelMappings"`
	ResultMappings           []resultMappingTO  `json:"resultMappings"`
	ResultType               ResultType         `json:"resultType"`
	ControlRequired          bool               `json:"controlRequired"`
	InstrumentControlAnalyte *string            `json:"instrumentControlAnalyte"`
}

type requestMappingTO struct {
	ID         uuid.UUID   `json:"id"`
	Code       string      `json:"code"`
	IsDefault  bool        `json:"isDefault"`
	AnalyteIDs []uuid.UUID `json:"requestMappingAnalyteIds"`
}

type channelMappingTO struct {
	ID                uuid.UUID `json:"id"`
	InstrumentChannel string    `json:"instrumentChannel"`
	ChannelID         uuid.UUID `json:"channelId"`
}

type resultMappingTO struct {
	ID    uuid.UUID `json:"id"`
	Key   string    `json:"key"`
	Value string    `json:"value"`
	Index int       `json:"index"`
}

type instrumentSettingTO struct {
	ID                uuid.UUID `json:"id"`
	ProtocolSettingID uuid.UUID `json:"protocolSettingId"`
	Value             string    `json:"value"`
}

type protocolAbilityTO struct {
	ConnectionMode          ConnectionMode `json:"connectionMode"`
	Abilities               []Ability      `json:"abilities"`
	RequestMappingAvailable bool           `json:"requestMappingAvailable"`
}

type protocolSettingTO struct {
	ID          uuid.UUID           `json:"id"`
	Key         string              `json:"key"`
	Description *string             `json:"description"`
	Type        ProtocolSettingType `json:"type"`
}

type supportedProtocolTO struct {
	ID                uuid.UUID           `json:"id"`
	Name              Protocol            `json:"name"`
	Description       *string             `json:"description"`
	ProtocolAbilities []protocolAbilityTO `json:"protocolAbilities"`
	ProtocolSettings  []protocolSettingTO `json:"protocolSettings"`
}

type supportedManufacturerTestTO struct {
	TestName          string   `json:"testName"`
	Channels          []string `json:"channels"`
	ValidResultValues []string `json:"validResultValues"`
}

type analysisRequestInfoTO struct {
	RequestID uuid.UUID `json:"requestId"`
	//ResultID          *uuid.UUID `json:"-"`
	SampleCode string    `json:"sampleCode"`
	AnalyteID  uuid.UUID `json:"analyteId"`
	//AnalyteMappingsID *uuid.UUID `json:"-"`
	WorkItemID       uuid.UUID  `json:"workitemId"` // Todo
	RequestCreatedAt time.Time  `json:"requestCreatedAt"`
	TestName         *string    `json:"testName"`
	TestResult       *string    `json:"testResult"`
	BatchCreatedAt   *time.Time `json:"transmissionDate"`
	ResultCreatedAt  *time.Time `json:"resultCreatedAt"`
	Status           string     `json:"status"`
	//SentToCerberusAt  *time.Time `json:"-"`
	SourceIP string `json:"sourceIP"` // Todo
	//InstrumentID      *uuid.UUID `json:"-"`
	MappingError bool `json:"mappingError"`
}

type analysisResultInfoTO struct {
	ID               uuid.UUID  `json:"id"`
	RequestCreatedAt *time.Time `json:"requestCreatedAt"`
	WorkItemID       *uuid.UUID `json:"workItemId"`
	SampleCode       string     `json:"sampleCode"`
	AnalyteID        uuid.UUID  `json:"analyteId"`
	ResultCreatedAt  time.Time  `json:"resultCreatedAt"`
	TestName         *string    `json:"testName"`
	TestResult       *string    `json:"testResult"`
	Status           string     `json:"status"`
}

type analysisBatchTO struct {
	ID      uuid.UUID `json:"batchId"`
	Results Page      `json:"requests"`
}

type batchRetransmitTO struct {
	BatchIDs []uuid.UUID `json:"batchIds"`
}

type reprocessTO struct {
	SampleCode string `json:"sampleCode"`
}

type expectedControlResultTO struct {
	ID               uuid.UUID         `json:"id"`
	AnalyteMappingId uuid.UUID         `json:"analyteMappingId"`
	SampleCode       string            `json:"sampleCode"`
	Operator         ConditionOperator `json:"operator"`
	ExpectedValue    string            `json:"expectedValue"`
	ExpectedValue2   *string           `json:"expectedValue2"`
	CreatedBy        *uuid.UUID        `json:"createdBy"`
	CreatedAt        *time.Time        `json:"createdAt"`
}

type notSpecifiedExpectedControlResultTO struct {
	AnalyteMappingId uuid.UUID `json:"analyteMappingId"`
	SampleCode       string    `json:"sampleCode"`
}

const (
	MsgReprocessInstrumentDataFailed           = "Reprocessing instrument data failed!"
	keyReprocessInstrumentDataFailed           = "reprocessInstrumentDataFailed"
	keyExpectedControlResultCreateFailed       = "expectedControlResultCreateFailed"
	msgExpectedControlResultCreateFailed       = "Expected control result create failed!"
	keyExpectedControlResultUpdateFailed       = "expectedControlResultUpdateFailed"
	msgExpectedControlResultUpdateFailed       = "Expected control result update failed!"
	keyInvalidExpectedControlResultValue       = "invalidExpectedControlResultValue"
	keyExpectedControlResultValidationError    = "expectedControlResultValidationError"
	msgExpectedControlResultValidationError    = "Invalid request body!"
	keyAnalyteNotFoundForExpectedControlResult = "analyteNotFoundForExpectedControlResult"
	msgAnalyteNotFoundForExpectedControlResult = "Unexpected analyte for expected control result!"
	keyGetInstrumentByIdFailed                 = "getInstrumentByIdFailed"
	msgGetInstrumentByIdFailed                 = "Get specific instrument data failed!"
	keyInstrumentControlAnalyteAlreadyInUse    = "instrumentControlAnalyteAlreadyInUse"
	msgInstrumentControlAnalyteAlreadyInUse    = "Control testcode already in use!"
	keyInstrumentAnalyteAlreadyInUse           = "instrumentAnalyteAlreadyInUse"
	msgInstrumentAnalyteAlreadyInUse           = "Manufacturer's testcode already in use!"
)

type sortingRuleGroupTO struct {
	Name         string          `json:"name"`
	SortingRules []sortingRuleTO `json:"sortingRules"`
}

type sortingRuleTO struct {
	ID        uuid.UUID    `json:"id"`
	Condition *conditionTO `json:"condition"`
	Target    string       `json:"target"`
	Priority  int          `json:"priority"`
}

type conditionTO struct {
	ID                  uuid.UUID           `json:"id"`
	Name                *string             `json:"name,omitempty"`
	Operator            ConditionOperator   `json:"operator"`
	SubCondition1       *conditionTO        `json:"subCondition1"`
	SubCondition2       *conditionTO        `json:"subCondition2"`
	SubCondition1ID     *uuid.UUID          `json:"subCondition1Id"`
	SubCondition2ID     *uuid.UUID          `json:"subCondition2Id"`
	NegateSubCondition1 bool                `json:"negateSubCondition1"`
	NegateSubCondition2 bool                `json:"negateSubCondition2"`
	Operand1            *conditionOperandTO `json:"operand1"`
	Operand2            *conditionOperandTO `json:"operand2"`
}

type conditionOperandTO struct {
	ID            uuid.UUID            `json:"id"`
	Name          *string              `json:"name"`
	Type          ConditionOperandType `json:"type"`
	ConstantValue *string              `json:"constantValue"`
	ExtraValueKey *string              `json:"extraValueKey"`
	AnalyteID     *uuid.UUID           `json:"analyteId"`
}

func (api *api) GetInstruments(c *gin.Context) {
	instruments, err := api.instrumentService.GetInstruments(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, middleware.ClientError{
			MessageKey: "getInstrumentsFailed",
			Message:    "Getting instruments failed!",
		})
		return
	}

	instrumentTOs := make([]listInstrumentTO, len(instruments))

	for i := range instruments {
		instrumentTOs[i] = convertInstrumentToListInstrumentTO(instruments[i])
	}

	c.JSON(http.StatusOK, instrumentTOs)
}

func (api *api) GetInstrumentByID(c *gin.Context) {
	id, err := uuid.Parse(c.Param("instrumentId"))
	if err != nil {
		clientError := middleware.ErrInvalidOrMissingRequestParameter
		clientError.MessageParams = map[string]string{"param": "instrumentId"}
		c.AbortWithStatusJSON(http.StatusBadRequest, clientError)
		return
	}

	instrument, err := api.instrumentService.GetInstrumentByID(c, nil, id, false)
	if err != nil {
		if err == ErrInstrumentNotFound {
			c.AbortWithStatus(http.StatusNotFound)
		}
		c.AbortWithStatusJSON(http.StatusInternalServerError, middleware.ClientError{
			MessageKey: keyGetInstrumentByIdFailed,
			Message:    msgGetInstrumentByIdFailed,
		})
		return
	}
	err = api.instrumentService.HidePassword(c, &instrument)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, middleware.ClientError{
			MessageKey: keyGetInstrumentByIdFailed,
			Message:    msgGetInstrumentByIdFailed,
		})
		return
	}
	c.JSON(http.StatusOK, convertInstrumentToInstrumentTO(instrument))
}

func (api *api) CreateInstrument(c *gin.Context) {
	instrumentTO := instrumentTO{
		Type: Analyzer,
	}
	err := c.ShouldBindJSON(&instrumentTO)
	if err != nil {
		log.Error().Err(err).Msg("Create instrument failed! Can't parse request body!")
		c.AbortWithStatusJSON(http.StatusBadRequest, middleware.ErrInvalidRequestBody)
		return
	}

	instrument := convertInstrumentTOToInstrument(instrumentTO)

	if !isRequestMappingValid(instrument) {
		log.Error().Msg("RequestMapping is not Valid")
		c.AbortWithStatusJSON(http.StatusBadRequest, middleware.ClientError{
			MessageKey: "instrumentValidationFailed",
			Message:    "Instrument validation failed!",
		})
		return
	}

	savedInstrumentID, err := api.instrumentService.CreateInstrument(c, instrument)
	if err != nil {
		log.Error().Err(err).Interface("instrument", instrumentTO).Msg("CreateInstrument failed")
		if errors.Is(err, ErrUniqueViolationInstrumentControlAnalyte) {
			c.AbortWithStatusJSON(http.StatusBadRequest, middleware.ClientError{
				MessageKey: keyInstrumentControlAnalyteAlreadyInUse,
				Message:    msgInstrumentControlAnalyteAlreadyInUse,
			})
			return
		} else if errors.Is(err, ErrUniqueViolationInstrumentAnalyte) {
			c.AbortWithStatusJSON(http.StatusBadRequest, middleware.ClientError{
				MessageKey: keyInstrumentAnalyteAlreadyInUse,
				Message:    msgInstrumentAnalyteAlreadyInUse,
			})
			return
		} else {
			c.AbortWithStatusJSON(http.StatusInternalServerError, middleware.ClientError{
				MessageKey: "createInstrumentFailed",
				Message:    "Instrument creation failed!",
			})
			return
		}
	}

	c.JSON(http.StatusOK, savedInstrumentID)
}

func (api *api) UpdateInstrument(c *gin.Context) {
	instrumentTO := instrumentTO{}

	err := c.ShouldBindJSON(&instrumentTO)
	if err != nil {
		log.Error().Err(err).Msg("Update instrument failed! Can't bind request body!")
		c.AbortWithStatusJSON(http.StatusBadRequest, middleware.ErrInvalidRequestBody)
		return
	}

	instrument := convertInstrumentTOToInstrument(instrumentTO)

	user, ok := c.Get("User")
	if !ok {
		c.AbortWithStatusJSON(http.StatusUnauthorized, middleware.ErrInvalidToken)
		return
	}
	userId := user.(middleware.UserToken).UserID

	err = api.instrumentService.UpdateInstrument(c, instrument, userId)
	if err != nil {
		if errors.Is(err, ErrUniqueViolationInstrumentControlAnalyte) {
			c.AbortWithStatusJSON(http.StatusBadRequest, middleware.ClientError{
				MessageKey: keyInstrumentControlAnalyteAlreadyInUse,
				Message:    msgInstrumentControlAnalyteAlreadyInUse,
			})
			return
		} else if errors.Is(err, ErrUniqueViolationInstrumentAnalyte) {
			c.AbortWithStatusJSON(http.StatusBadRequest, middleware.ClientError{
				MessageKey: keyInstrumentAnalyteAlreadyInUse,
				Message:    msgInstrumentAnalyteAlreadyInUse,
			})
			return
		} else {
			c.AbortWithStatusJSON(http.StatusInternalServerError, middleware.ClientError{
				MessageKey: "updateInstrumentFailed",
				Message:    "Instrument update failed!",
			})
			return
		}
	}

	c.Status(http.StatusNoContent)
}

func (api *api) DeleteInstrument(c *gin.Context) {
	id, err := uuid.Parse(c.Param("instrumentId"))
	if err != nil {
		clientError := middleware.ErrInvalidOrMissingRequestParameter
		clientError.MessageParams = map[string]string{"param": "instrumentId"}
		c.AbortWithStatusJSON(http.StatusBadRequest, clientError)
		return
	}

	err = api.instrumentService.DeleteInstrument(c, id)
	if err != nil {
		if err == ErrInstrumentNotFound {
			c.AbortWithStatus(http.StatusNotFound)
		}
		c.AbortWithStatusJSON(http.StatusInternalServerError, middleware.ClientError{
			MessageKey: "instrumentDeletionFailed",
			Message:    "Instrument deletion failed!",
		})
		return
	}

	c.Status(http.StatusNoContent)
}

func (api *api) GetExpectedControlResultsByInstrumentId(c *gin.Context) {
	instrumentId, err := uuid.Parse(c.Param("instrumentId"))
	if err != nil {
		clientError := middleware.ErrInvalidOrMissingRequestParameter
		clientError.MessageParams = map[string]string{"param": "instrumentId"}
		c.AbortWithStatusJSON(http.StatusBadRequest, clientError)
		return
	}

	expectedControlResults, err := api.instrumentService.GetExpectedControlResultsByInstrumentId(c, instrumentId)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, middleware.ClientError{
			MessageKey: "getExpectedControlResultsByInstrumentIdFailed",
			Message:    "Gathering expected control results failed!",
		})
		return
	}

	c.JSON(http.StatusOK, convertExpectedControlResultListToTOList(expectedControlResults))
}

func (api *api) GetNotSpecifiedExpectedControlResultsByInstrumentId(c *gin.Context) {
	instrumentId, err := uuid.Parse(c.Param("instrumentId"))
	if err != nil {
		clientError := middleware.ErrInvalidOrMissingRequestParameter
		clientError.MessageParams = map[string]string{"param": "instrumentId"}
		c.AbortWithStatusJSON(http.StatusBadRequest, clientError)
		return
	}

	notSpecifiedExpectedControlResults, err := api.instrumentService.GetNotSpecifiedExpectedControlResultsByInstrumentId(c, instrumentId)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, middleware.ClientError{
			MessageKey: "getNotSpecifiedExpectedControlResultsByInstrumentIdFailed",
			Message:    "Gathering not specified expected control results failed!",
		})
		return
	}

	c.JSON(http.StatusOK, convertNotSpecifiedExpectedControlResultListToTOList(notSpecifiedExpectedControlResults))
}

func (api *api) CreateExpectedControlResults(c *gin.Context) {
	user, ok := c.Get("User")
	if !ok {
		c.AbortWithStatusJSON(http.StatusUnauthorized, middleware.ErrInvalidToken)
		return
	}
	userId := user.(middleware.UserToken).UserID

	instrumentId, err := uuid.Parse(c.Param("instrumentId"))
	if err != nil {
		clientError := middleware.ErrInvalidOrMissingRequestParameter
		clientError.MessageParams = map[string]string{"param": "instrumentId"}
		c.AbortWithStatusJSON(http.StatusBadRequest, clientError)
		return
	}

	var expectedControlResultTos []expectedControlResultTO
	err = c.ShouldBindJSON(&expectedControlResultTos)
	if err != nil {
		log.Error().Err(err).Msg("Create expected control results failed! Can't bind request body!")
		c.AbortWithStatusJSON(http.StatusBadRequest, middleware.ErrUnableToParseRequestBody)
		return
	}

	err = api.instrumentService.CreateExpectedControlResults(c, instrumentId, convertTOsToExpectedControlResults(expectedControlResultTos), userId)
	if err != nil {
		if errors.Is(err, ErrAnalyteMappingNotFound) {
			c.AbortWithStatusJSON(http.StatusNotFound, middleware.ClientError{
				MessageKey: keyAnalyteNotFoundForExpectedControlResult,
				Message:    msgAnalyteNotFoundForExpectedControlResult,
			})
		} else {
			parameterizedErrors, ok := err.(ParameterizedErrors)
			if ok {
				resp := middleware.ClientError{
					MessageKey: keyExpectedControlResultValidationError,
					Message:    msgExpectedControlResultValidationError,
				}
				for _, customError := range parameterizedErrors {
					resp.Errors = append(resp.Errors, middleware.ClientError{
						MessageKey:    keyInvalidExpectedControlResultValue,
						MessageParams: customError.Params,
						Message:       customError.Error(),
					})
				}
				c.AbortWithStatusJSON(http.StatusBadRequest, resp)
			} else {
				c.AbortWithStatusJSON(http.StatusInternalServerError, middleware.ClientError{
					MessageKey: keyExpectedControlResultCreateFailed,
					Message:    msgExpectedControlResultCreateFailed,
				})
			}
		}
		return
	}

	c.Status(http.StatusOK)
}

func (api *api) UpdateExpectedControlResults(c *gin.Context) {
	user, ok := c.Get("User")
	if !ok {
		c.AbortWithStatusJSON(http.StatusUnauthorized, middleware.ErrInvalidToken)
		return
	}
	userId := user.(middleware.UserToken).UserID

	instrumentId, err := uuid.Parse(c.Param("instrumentId"))
	if err != nil {
		clientError := middleware.ErrInvalidOrMissingRequestParameter
		clientError.MessageParams = map[string]string{"param": "instrumentId"}
		c.AbortWithStatusJSON(http.StatusBadRequest, clientError)
		return
	}

	var expectedControlResultTos []expectedControlResultTO
	err = c.ShouldBindJSON(&expectedControlResultTos)
	if err != nil {
		log.Error().Err(err).Msg("Create expected control results failed! Can't bind request body!")
		c.AbortWithStatusJSON(http.StatusBadRequest, middleware.ErrUnableToParseRequestBody)
		return
	}

	err = api.instrumentService.UpdateExpectedControlResults(c, instrumentId, convertTOsToExpectedControlResults(expectedControlResultTos), userId)
	if err != nil {
		if errors.Is(err, ErrAnalyteMappingNotFound) {
			c.AbortWithStatusJSON(http.StatusNotFound, middleware.ClientError{
				MessageKey: keyAnalyteNotFoundForExpectedControlResult,
				Message:    msgAnalyteNotFoundForExpectedControlResult,
			})
		} else {
			parameterizedErrors, ok := err.(ParameterizedErrors)
			if ok {
				resp := middleware.ClientError{
					MessageKey: keyExpectedControlResultValidationError,
					Message:    msgExpectedControlResultValidationError,
				}
				for _, customError := range parameterizedErrors {
					resp.Errors = append(resp.Errors, middleware.ClientError{
						MessageKey:    keyInvalidExpectedControlResultValue,
						MessageParams: customError.Params,
						Message:       customError.Error(),
					})
				}
				c.AbortWithStatusJSON(http.StatusBadRequest, resp)
			} else {
				c.AbortWithStatusJSON(http.StatusInternalServerError, middleware.ClientError{
					MessageKey: keyExpectedControlResultUpdateFailed,
					Message:    msgExpectedControlResultUpdateFailed,
				})
			}
		}
		return
	}

	c.Status(http.StatusOK)
}

func (api *api) DeleteExpectedControlResult(c *gin.Context) {
	user, ok := c.Get("User")
	if !ok {
		c.AbortWithStatusJSON(http.StatusUnauthorized, middleware.ErrInvalidToken)
		return
	}
	userId := user.(middleware.UserToken).UserID

	expectedControlResultId, err := uuid.Parse(c.Param("expectedControlResultId"))
	if err != nil {
		clientError := middleware.ErrInvalidOrMissingRequestParameter
		clientError.MessageParams = map[string]string{"param": "expectedControlResultId"}
		c.AbortWithStatusJSON(http.StatusBadRequest, clientError)
		return
	}

	err = api.instrumentService.DeleteExpectedControlResult(c, expectedControlResultId, userId)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, middleware.ClientError{
			MessageKey: "expectedControlResultDeletionFailed",
			Message:    "Expected control result deletion failed!",
		})
		return
	}

	c.Status(http.StatusNoContent)
}

func (api *api) GetSupportedProtocols(c *gin.Context) {
	supportedInstruments, err := api.instrumentService.GetSupportedProtocols(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, middleware.ClientError{
			MessageKey: "getSupportedProtocolsFailed",
			Message:    "Getting supported protocols failed!",
		})
		return
	}

	c.JSON(http.StatusOK, convertSupportedProtocolsToSupportedProtocolTOs(supportedInstruments))
}

func (api *api) GetProtocolAbilities(c *gin.Context) {
	protocolID, err := uuid.Parse(c.Param("protocolVersionId"))
	if err != nil {
		clientError := middleware.ErrInvalidOrMissingRequestParameter
		clientError.MessageParams = map[string]string{"param": "protocolVersionId"}
		c.AbortWithStatusJSON(http.StatusBadRequest, clientError)
		return
	}

	protocolAbilities, err := api.instrumentService.GetProtocolAbilities(c, protocolID)
	if err != nil {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}

	c.JSON(http.StatusOK, convertProtocolAbilitiesToProtocolAbilitiesTOs(protocolAbilities))
}

func (api *api) GetManufacturerTests(c *gin.Context) {
	protocolID, err := uuid.Parse(c.Param("protocolVersionId"))
	if err != nil {
		clientError := middleware.ErrInvalidOrMissingRequestParameter
		clientError.MessageParams = map[string]string{"param": "protocolVersionId"}
		c.AbortWithStatusJSON(http.StatusBadRequest, clientError)
		return
	}

	instrumentID := uuid.Nil
	instrumentIDString, ok := c.GetQuery("instrumentId")
	if ok {
		instrumentID, err = uuid.Parse(instrumentIDString)
		if err != nil {
			clientError := middleware.ErrInvalidOrMissingRequestParameter
			clientError.MessageParams = map[string]string{"param": "instrumentId"}
			c.AbortWithStatusJSON(http.StatusBadRequest, clientError)
			return
		}
	}

	tests, err := api.instrumentService.GetManufacturerTests(c, instrumentID, protocolID)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, middleware.ClientError{
			MessageKey: "getManufacturerTestsFailed",
			Message:    "Getting manufacturer tests failed!",
		})
		return
	}

	c.JSON(http.StatusOK, convertSupportedManufacturerTestsToSupportedManufacturerTestTOs(tests))
}

func (api *api) GetAnalysisRequestsInfo(c *gin.Context) {
	instrumentID, err := uuid.Parse(c.Param("instrumentId"))
	if err != nil {
		clientError := middleware.ErrInvalidOrMissingRequestParameter
		clientError.MessageParams = map[string]string{"param": "instrumentId"}
		c.AbortWithStatusJSON(http.StatusBadRequest, clientError)
		return
	}

	var filter Filter
	err = c.ShouldBindQuery(&filter)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, middleware.ErrUnableToParseRequestBody)
		return
	}

	log.Trace().Str("instrumentID", instrumentID.String()).Interface("filter", filter).Msg("GetAnalysisRequestsInfo")

	analysisRequestInfoList, totalCount, err := api.analysisService.GetAnalysisRequestsInfo(c, instrumentID, filter)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, middleware.ClientError{
			MessageKey: "getAnalysisRequestsInfoFailed",
			Message:    "Getting analysis requests info failed!",
		})
		return
	}

	c.JSON(http.StatusOK, NewPage(filter.Pageable, totalCount, convertAnalysisRequestInfoListToAnalysisRequestInfoTOList(analysisRequestInfoList)))
}

func (api *api) GetAnalysisResultsInfo(c *gin.Context) {
	instrumentID, err := uuid.Parse(c.Param("instrumentId"))
	if err != nil {
		clientError := middleware.ErrInvalidOrMissingRequestParameter
		clientError.MessageParams = map[string]string{"param": "instrumentId"}
		c.AbortWithStatusJSON(http.StatusBadRequest, clientError)
		return
	}

	var filter Filter
	err = c.ShouldBindQuery(&filter)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, middleware.ErrUnableToParseRequestBody)
		return
	}

	log.Trace().Str("instrumentID", instrumentID.String()).Interface("filter", filter).Msg("GetAnalysisResultsInfo")

	analysisResultInfoList, totalCount, err := api.analysisService.GetAnalysisResultsInfo(c, instrumentID, filter)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, middleware.ClientError{
			MessageKey: "getAnalysisResultsInfoFailed",
			Message:    "Getting analysis results info failed!",
		})
		return
	}

	c.JSON(http.StatusOK, NewPage(filter.Pageable, totalCount, convertAnalysisResultInfoListToAnalysisResultInfoTOList(analysisResultInfoList)))
}

func (api *api) GetAnalysisBatches(c *gin.Context) {
	instrumentID, err := uuid.Parse(c.Param("instrumentId"))
	if err != nil {
		clientError := middleware.ErrInvalidOrMissingRequestParameter
		clientError.MessageParams = map[string]string{"param": "instrumentId"}
		c.AbortWithStatusJSON(http.StatusBadRequest, clientError)
		return
	}

	var filter Filter
	err = c.ShouldBindQuery(&filter)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, middleware.ErrUnableToParseRequestBody)
		return
	}

	log.Trace().Str("instrumentID", instrumentID.String()).Interface("filter", filter).Msg("GetAnalysisBatches")

	analysisBatchList, totalCount, err := api.analysisService.GetAnalysisBatches(c, instrumentID, filter)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, middleware.ClientError{
			MessageKey: "getAnalysisBatchesFailed",
			Message:    "Getting analysis batches failed!",
		})
		return
	}

	c.JSON(http.StatusOK, NewPage(filter.Pageable, totalCount, convertAnalysisBatchListToAnalysisBatchTOList(analysisBatchList)))
}

func (api *api) RetransmitResult(c *gin.Context) {
	resultID, err := uuid.Parse(c.Param("resultID"))
	if err != nil {
		clientError := middleware.ErrInvalidOrMissingRequestParameter
		clientError.MessageParams = map[string]string{"param": "resultID"}
		c.AbortWithStatusJSON(http.StatusBadRequest, clientError)
		return
	}

	err = api.analysisService.RetransmitResult(c, resultID)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, middleware.ClientError{
			MessageKey: "retransmitResultFailed",
			Message:    "Retransmission of results failed!",
		})
		return
	}

	c.Status(http.StatusNoContent)
}

func (api *api) RetransmitResultBatches(c *gin.Context) {
	var to batchRetransmitTO
	err := c.ShouldBindJSON(&to)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, middleware.ErrUnableToParseRequestBody)
		return
	}

	err = api.analysisService.RetransmitResultBatches(c, to.BatchIDs)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, middleware.ClientError{
			MessageKey: "retransmitResultBatchesFailed",
			Message:    "Retransmission of results batches failed!",
		})
		return
	}

	c.Status(http.StatusNoContent)
}

func (api *api) ReprocessInstrumentData(c *gin.Context) {
	var batchIDs []uuid.UUID
	err := c.ShouldBindJSON(&batchIDs)
	if err != nil {
		log.Error().Err(err).Msg(MsgCanNotBindRequestBody)
		c.AbortWithStatusJSON(http.StatusBadRequest, middleware.ErrUnableToParseRequestBody)
		return
	}

	if len(batchIDs) > 0 {
		err = api.instrumentService.ReprocessInstrumentData(c, batchIDs)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, middleware.ClientError{
				Message:    MsgReprocessInstrumentDataFailed,
				MessageKey: keyReprocessInstrumentDataFailed,
			})
			return
		}
	}

	c.Status(http.StatusNoContent)
}

func (api *api) ReprocessInstrumentDataBySampleCode(c *gin.Context) {
	var to reprocessTO
	err := c.ShouldBindJSON(&to)
	if err != nil {
		log.Error().Err(err).Msg(MsgCanNotBindRequestBody)
		c.AbortWithStatusJSON(http.StatusBadRequest, middleware.ErrUnableToParseRequestBody)
		return
	}

	err = api.instrumentService.ReprocessInstrumentDataBySampleCode(c, to.SampleCode)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, middleware.ClientError{
			Message:    MsgReprocessInstrumentDataFailed,
			MessageKey: keyReprocessInstrumentDataFailed,
		})
		return
	}

	c.Status(http.StatusNoContent)
}

func (api *api) GetMessages(c *gin.Context) {
	instrumentID, err := uuid.Parse(c.Param("instrumentId"))
	if err != nil {
		clientError := middleware.ErrInvalidOrMissingRequestParameter
		clientError.MessageParams = map[string]string{"param": "instrumentId"}
		c.AbortWithStatusJSON(http.StatusBadRequest, clientError)
		return
	}

	logs := api.consoleLogService.GetConsoleLogs(instrumentID)

	c.JSON(http.StatusOK, logs)
}

func (api *api) GetEncodings(c *gin.Context) {
	protocolID, err := uuid.Parse(c.Param("protocolId"))
	if err != nil {
		clientError := middleware.ErrInvalidOrMissingRequestParameter
		clientError.MessageParams = map[string]string{"param": "protocolId"}
		c.AbortWithStatusJSON(http.StatusBadRequest, clientError)
		return
	}

	encodings, err := api.instrumentService.GetEncodings(c, protocolID)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, middleware.ClientError{
			MessageKey: "getEncodingsFailed",
			Message:    "Getting encodings failed!",
		})
		return
	}

	c.JSON(http.StatusOK, encodings)
}

func convertInstrumentToListInstrumentTO(instrument Instrument) listInstrumentTO {
	return listInstrumentTO{
		ID:           instrument.ID,
		Name:         instrument.Name,
		ProtocolName: instrument.ProtocolName,
		Status:       instrument.Status,
		ResultMode:   instrument.ResultMode,
	}
}

func convertInstrumentTOToInstrument(instrumentTO instrumentTO) Instrument {
	model := Instrument{
		ID:                 instrumentTO.ID,
		Type:               instrumentTO.Type,
		Name:               instrumentTO.Name,
		ProtocolID:         instrumentTO.ProtocolID,
		ProtocolName:       instrumentTO.ProtocolName,
		Enabled:            instrumentTO.Enabled,
		ConnectionMode:     instrumentTO.ConnectionMode,
		ResultMode:         instrumentTO.ResultMode,
		CaptureResults:     instrumentTO.CaptureResults,
		CaptureDiagnostics: instrumentTO.CaptureDiagnostics,
		ReplyToQuery:       instrumentTO.ReplyToQuery,
		FileEncoding:       instrumentTO.FileEncoding,
		Timezone:           instrumentTO.Timezone,
		Hostname:           instrumentTO.Hostname,
		ClientPort:         instrumentTO.ClientPort,
		AnalyteMappings:    make([]AnalyteMapping, len(instrumentTO.AnalyteMappings)),
		RequestMappings:    make([]RequestMapping, len(instrumentTO.RequestMappings)),
		SortingRules:       make([]SortingRule, 0),
		Settings:           convertInstrumentSettingTOsToInstrumentSettings(instrumentTO.Settings),
	}

	if instrumentTO.ConnectionMode == FTP {
		model.FTPConfig = &FTPConfig{
			InstrumentId:     instrumentTO.ID,
			Username:         stringPointerToString(instrumentTO.FtpUsername),
			Password:         stringPointerToString(instrumentTO.FtpPassword),
			OrderPath:        stringPointerToStringWithDefault(instrumentTO.FtpOrderPath, "/"),
			OrderFileMask:    stringPointerToString(instrumentTO.FtpOrderFileMask),
			OrderFileSuffix:  stringPointerToString(instrumentTO.FtpOrderFileSuffix),
			ResultPath:       stringPointerToStringWithDefault(instrumentTO.FtpResultPath, "/"),
			ResultFileMask:   stringPointerToString(instrumentTO.FtpResultFileMask),
			ResultFileSuffix: stringPointerToString(instrumentTO.FtpResultFileSuffix),
			FtpServerType:    stringPointerToString(instrumentTO.FtpServerType),
		}
	}

	if instrumentTO.Status == "" {
		model.Status = string(InstrumentReady)
	}

	for i, analyteMapping := range instrumentTO.AnalyteMappings {
		model.AnalyteMappings[i] = convertAnalyteMappingTOToAnalyteMapping(analyteMapping)
	}

	for i, requestMapping := range instrumentTO.RequestMappings {
		model.RequestMappings[i] = convertRequestMappingTOToRequestMapping(requestMapping)
	}

	model.SortingRules = convertSortingRuleGroupTOsToSortingRules(instrumentTO.SortingRuleGroups, instrumentTO.ID)

	return model
}

func convertInstrumentToInstrumentTO(instrument Instrument) instrumentTO {
	model := instrumentTO{
		ID:                 instrument.ID,
		Type:               instrument.Type,
		Name:               instrument.Name,
		ProtocolID:         instrument.ProtocolID,
		ProtocolName:       instrument.ProtocolName,
		Enabled:            instrument.Enabled,
		ConnectionMode:     instrument.ConnectionMode,
		ResultMode:         instrument.ResultMode,
		CaptureResults:     instrument.CaptureResults,
		CaptureDiagnostics: instrument.CaptureDiagnostics,
		ReplyToQuery:       instrument.ReplyToQuery,
		Status:             instrument.Status,
		FileEncoding:       instrument.FileEncoding,
		Timezone:           instrument.Timezone,
		Hostname:           instrument.Hostname,
		ClientPort:         instrument.ClientPort,
		AnalyteMappings:    make([]analyteMappingTO, len(instrument.AnalyteMappings)),
		RequestMappings:    make([]requestMappingTO, len(instrument.RequestMappings)),
		SortingRuleGroups:  make([]sortingRuleGroupTO, len(instrument.SortingRules)),
		Settings:           convertInstrumentSettingsToSettingsTOs(instrument.Settings),
	}

	if instrument.ConnectionMode == FTP && instrument.FTPConfig != nil {
		model.FtpServerType = &instrument.FTPConfig.FtpServerType
		model.FtpUsername = &instrument.FTPConfig.Username
		model.FtpPassword = &instrument.FTPConfig.Password
		model.FtpOrderPath = &instrument.FTPConfig.OrderPath
		model.FtpOrderFileMask = &instrument.FTPConfig.OrderFileMask
		model.FtpOrderFileSuffix = &instrument.FTPConfig.OrderFileSuffix
		model.FtpResultPath = &instrument.FTPConfig.ResultPath
		model.FtpResultFileMask = &instrument.FTPConfig.ResultFileMask
		model.FtpResultFileSuffix = &instrument.FTPConfig.ResultFileSuffix
	}

	for i, analyteMapping := range instrument.AnalyteMappings {
		model.AnalyteMappings[i] = convertAnalyteMappingToAnalyteMappingTO(analyteMapping)
	}

	for i, requestMapping := range instrument.RequestMappings {
		model.RequestMappings[i] = convertRequestMappingToRequestMappingTO(requestMapping)
	}

	model.SortingRuleGroups = convertSortingRulesToSortingRuleGroupTO(instrument.SortingRules)

	return model
}

func convertInstrumentSettingsToSettingsTOs(settings []InstrumentSetting) []instrumentSettingTO {
	settingTOs := make([]instrumentSettingTO, len(settings))
	for i := range settings {
		settingTOs[i] = convertInstrumentSettingToSettingTO(settings[i])
	}
	return settingTOs
}

func convertInstrumentSettingToSettingTO(setting InstrumentSetting) instrumentSettingTO {
	return instrumentSettingTO{
		ID:                setting.ID,
		ProtocolSettingID: setting.ProtocolSettingID,
		Value:             setting.Value,
	}
}

func convertInstrumentSettingTOsToInstrumentSettings(settingTOs []instrumentSettingTO) []InstrumentSetting {
	settings := make([]InstrumentSetting, len(settingTOs))
	for i := range settings {
		settings[i] = convertInstrumentSettingTOToInstrumentSetting(settingTOs[i])
	}
	return settings
}

func convertInstrumentSettingTOToInstrumentSetting(settingTO instrumentSettingTO) InstrumentSetting {
	return InstrumentSetting{
		ID:                settingTO.ID,
		ProtocolSettingID: settingTO.ProtocolSettingID,
		Value:             settingTO.Value,
	}
}

func convertAnalyteMappingTOToAnalyteMapping(analyteMappingTO analyteMappingTO) AnalyteMapping {
	model := AnalyteMapping{
		ID:                       analyteMappingTO.ID,
		InstrumentAnalyte:        analyteMappingTO.InstrumentAnalyte,
		AnalyteID:                analyteMappingTO.AnalyteID,
		ChannelMappings:          make([]ChannelMapping, len(analyteMappingTO.ChannelMappings)),
		ResultMappings:           make([]ResultMapping, len(analyteMappingTO.ResultMappings)),
		ResultType:               analyteMappingTO.ResultType,
		ControlResultRequired:    analyteMappingTO.ControlRequired,
		ControlInstrumentAnalyte: analyteMappingTO.InstrumentControlAnalyte,
	}

	for i, channelMapping := range analyteMappingTO.ChannelMappings {
		model.ChannelMappings[i] = convertChannelMappingTOToChannelMapping(channelMapping)
	}

	for i, resultMapping := range analyteMappingTO.ResultMappings {
		model.ResultMappings[i] = convertResultMappingTOToResultMapping(resultMapping)
	}

	return model
}

func convertAnalyteMappingToAnalyteMappingTO(analyteMapping AnalyteMapping) analyteMappingTO {
	model := analyteMappingTO{
		ID:                       analyteMapping.ID,
		InstrumentAnalyte:        analyteMapping.InstrumentAnalyte,
		AnalyteID:                analyteMapping.AnalyteID,
		ChannelMappings:          make([]channelMappingTO, len(analyteMapping.ChannelMappings)),
		ResultMappings:           make([]resultMappingTO, len(analyteMapping.ResultMappings)),
		ResultType:               analyteMapping.ResultType,
		ControlRequired:          analyteMapping.ControlResultRequired,
		InstrumentControlAnalyte: analyteMapping.ControlInstrumentAnalyte,
	}

	for i, channelMapping := range analyteMapping.ChannelMappings {
		model.ChannelMappings[i] = convertChannelMappingToChannelMappingTO(channelMapping)
	}

	for i, resultMapping := range analyteMapping.ResultMappings {
		model.ResultMappings[i] = convertResultMappingToResultMappingTO(resultMapping)
	}

	return model
}

func convertRequestMappingTOToRequestMapping(requestMappingTO requestMappingTO) RequestMapping {
	return RequestMapping{
		ID:         requestMappingTO.ID,
		Code:       requestMappingTO.Code,
		IsDefault:  requestMappingTO.IsDefault,
		AnalyteIDs: requestMappingTO.AnalyteIDs,
	}
}

func convertRequestMappingToRequestMappingTO(requestMapping RequestMapping) requestMappingTO {
	return requestMappingTO{
		ID:         requestMapping.ID,
		Code:       requestMapping.Code,
		IsDefault:  requestMapping.IsDefault,
		AnalyteIDs: requestMapping.AnalyteIDs,
	}
}

func convertChannelMappingTOToChannelMapping(channelMappingTO channelMappingTO) ChannelMapping {
	return ChannelMapping{
		ID:                channelMappingTO.ID,
		InstrumentChannel: channelMappingTO.InstrumentChannel,
		ChannelID:         channelMappingTO.ChannelID,
	}
}

func convertChannelMappingToChannelMappingTO(channelMapping ChannelMapping) channelMappingTO {
	return channelMappingTO{
		ID:                channelMapping.ID,
		InstrumentChannel: channelMapping.InstrumentChannel,
		ChannelID:         channelMapping.ChannelID,
	}
}

func convertResultMappingTOToResultMapping(resultMappingTO resultMappingTO) ResultMapping {
	return ResultMapping{
		ID:    resultMappingTO.ID,
		Key:   resultMappingTO.Key,
		Value: resultMappingTO.Value,
		Index: resultMappingTO.Index,
	}
}

func convertResultMappingToResultMappingTO(resultMapping ResultMapping) resultMappingTO {
	return resultMappingTO{
		ID:    resultMapping.ID,
		Key:   resultMapping.Key,
		Value: resultMapping.Value,
		Index: resultMapping.Index,
	}
}

func convertSupportedProtocolToSupportedProtocolTO(supportedProtocol SupportedProtocol) supportedProtocolTO {
	to := supportedProtocolTO{
		ID:                supportedProtocol.ID,
		Name:              supportedProtocol.Name,
		Description:       supportedProtocol.Description,
		ProtocolAbilities: convertProtocolAbilitiesToProtocolAbilitiesTOs(supportedProtocol.ProtocolAbilities),
		ProtocolSettings:  convertProtocolSettingsToProtocolSettingsTOs(supportedProtocol.ProtocolSettings),
	}
	return to
}

func convertSupportedProtocolsToSupportedProtocolTOs(supportedProtocols []SupportedProtocol) []supportedProtocolTO {
	tos := make([]supportedProtocolTO, len(supportedProtocols))
	for i := range supportedProtocols {
		tos[i] = convertSupportedProtocolToSupportedProtocolTO(supportedProtocols[i])
	}
	return tos
}

func convertProtocolAbilityToProtocolAbilityTO(protocolAbility ProtocolAbility) protocolAbilityTO {
	to := protocolAbilityTO{
		ConnectionMode:          protocolAbility.ConnectionMode,
		Abilities:               protocolAbility.Abilities,
		RequestMappingAvailable: protocolAbility.RequestMappingAvailable,
	}
	return to
}

func convertProtocolAbilitiesToProtocolAbilitiesTOs(protocolAbilities []ProtocolAbility) []protocolAbilityTO {
	tos := make([]protocolAbilityTO, len(protocolAbilities))
	for i := range protocolAbilities {
		tos[i] = convertProtocolAbilityToProtocolAbilityTO(protocolAbilities[i])
	}
	return tos
}

func convertProtocolSettingToProtocolSettingTO(setting ProtocolSetting) protocolSettingTO {
	return protocolSettingTO{
		ID:          setting.ID,
		Key:         setting.Key,
		Description: setting.Description,
		Type:        setting.Type,
	}
}

func convertProtocolSettingsToProtocolSettingsTOs(settings []ProtocolSetting) []protocolSettingTO {
	tos := make([]protocolSettingTO, len(settings))
	for i := range settings {
		tos[i] = convertProtocolSettingToProtocolSettingTO(settings[i])
	}
	return tos
}

func convertSupportedManufacturerTestToSupportedManufacturerTestTO(supportedManufacturerTest SupportedManufacturerTests) supportedManufacturerTestTO {
	return supportedManufacturerTestTO{
		TestName:          supportedManufacturerTest.TestName,
		Channels:          supportedManufacturerTest.Channels,
		ValidResultValues: supportedManufacturerTest.ValidResultValues,
	}
}

func convertSupportedManufacturerTestsToSupportedManufacturerTestTOs(supportedManufacturerTests []SupportedManufacturerTests) []supportedManufacturerTestTO {
	tos := make([]supportedManufacturerTestTO, len(supportedManufacturerTests))
	for i := range supportedManufacturerTests {
		tos[i] = convertSupportedManufacturerTestToSupportedManufacturerTestTO(supportedManufacturerTests[i])
	}
	return tos
}

func convertAnalysisRequestInfoToAnalysisRequestInfoTO(analysisRequestInfo AnalysisRequestInfo) analysisRequestInfoTO {
	return analysisRequestInfoTO{
		RequestID:        analysisRequestInfo.ID,
		SampleCode:       analysisRequestInfo.SampleCode,
		AnalyteID:        analysisRequestInfo.AnalyteID,
		WorkItemID:       analysisRequestInfo.WorkItemID,
		TestName:         analysisRequestInfo.TestName,
		TestResult:       analysisRequestInfo.TestResult,
		RequestCreatedAt: analysisRequestInfo.RequestCreatedAt,
		BatchCreatedAt:   analysisRequestInfo.BatchCreatedAt,
		ResultCreatedAt:  analysisRequestInfo.ResultCreatedAt,
		Status:           analysisRequestInfo.Status,
		SourceIP:         analysisRequestInfo.SourceIP,
		MappingError:     analysisRequestInfo.MappingError,
	}
}

func convertAnalysisResultInfoToAnalysisResultInfoTO(analysisResultInfo AnalysisResultInfo) analysisResultInfoTO {
	return analysisResultInfoTO{
		ID:               analysisResultInfo.ID,
		RequestCreatedAt: analysisResultInfo.RequestCreatedAt,
		WorkItemID:       analysisResultInfo.WorkItemID,
		SampleCode:       analysisResultInfo.SampleCode,
		AnalyteID:        analysisResultInfo.AnalyteID,
		ResultCreatedAt:  analysisResultInfo.ResultCreatedAt,
		TestName:         analysisResultInfo.TestName,
		TestResult:       analysisResultInfo.TestResult,
		Status:           analysisResultInfo.Status,
	}
}

func convertAnalysisBatchToAnalysisBatchTO(analysisBatch AnalysisBatch) analysisBatchTO {
	to := analysisBatchTO{
		ID:      analysisBatch.ID,
		Results: NewPage(Pageable{PageSize: 0}, len(analysisBatch.Results), convertAnalysisResultInfoListToAnalysisResultInfoTOList(analysisBatch.Results)),
	}

	return to
}

func convertAnalysisRequestInfoListToAnalysisRequestInfoTOList(analysisRequestInfoList []AnalysisRequestInfo) []analysisRequestInfoTO {
	tos := make([]analysisRequestInfoTO, len(analysisRequestInfoList))
	for i := range analysisRequestInfoList {
		tos[i] = convertAnalysisRequestInfoToAnalysisRequestInfoTO(analysisRequestInfoList[i])
	}
	return tos
}

func convertAnalysisResultInfoListToAnalysisResultInfoTOList(analysisResultInfoList []AnalysisResultInfo) []analysisResultInfoTO {
	tos := make([]analysisResultInfoTO, len(analysisResultInfoList))
	for i := range analysisResultInfoList {
		tos[i] = convertAnalysisResultInfoToAnalysisResultInfoTO(analysisResultInfoList[i])
	}
	return tos
}

func convertAnalysisBatchListToAnalysisBatchTOList(analysisBatchList []AnalysisBatch) []analysisBatchTO {
	tos := make([]analysisBatchTO, len(analysisBatchList))
	for i := range analysisBatchList {
		tos[i] = convertAnalysisBatchToAnalysisBatchTO(analysisBatchList[i])
	}
	return tos
}

func convertTOToSortingRule(to sortingRuleTO, programme string, instrumentID uuid.UUID) SortingRule {
	rule := SortingRule{
		ID:           to.ID,
		InstrumentID: instrumentID,
		Target:       to.Target,
		Programme:    programme,
		Priority:     to.Priority,
	}

	if to.Condition != nil {
		condition := convertTOToCondition(*to.Condition)
		rule.Condition = &condition
	}

	return rule
}

func convertSortingRuleGroupTOsToSortingRules(tos []sortingRuleGroupTO, instrumentID uuid.UUID) []SortingRule {
	sortingRules := make([]SortingRule, 0)
	for i := range tos {
		for _, sortingRule := range tos[i].SortingRules {
			sortingRules = append(sortingRules, convertTOToSortingRule(sortingRule, tos[i].Name, instrumentID))
		}
	}

	return sortingRules
}

func convertSortingRulesToSortingRuleGroupTO(sortingRules []SortingRule) []sortingRuleGroupTO {
	sortingRulesByGroupNameMap := make(map[string][]SortingRule)
	for i := range sortingRules {
		if _, ok := sortingRulesByGroupNameMap[sortingRules[i].Programme]; !ok {
			sortingRulesByGroupNameMap[sortingRules[i].Programme] = make([]SortingRule, 0)
		}

		sortingRulesByGroupNameMap[sortingRules[i].Programme] = append(sortingRulesByGroupNameMap[sortingRules[i].Programme], sortingRules[i])
	}

	tos := make([]sortingRuleGroupTO, 0)
	for groupName, sortingRules := range sortingRulesByGroupNameMap {
		tos = append(tos, sortingRuleGroupTO{
			Name:         groupName,
			SortingRules: convertSortingRulesToTOs(sortingRules),
		})
	}

	return tos
}

func convertSortingRulesToTOs(sortingRules []SortingRule) []sortingRuleTO {
	tos := make([]sortingRuleTO, len(sortingRules))
	for i := range sortingRules {
		tos[i] = convertSortingRuleToTO(sortingRules[i])
	}

	return tos
}

func convertSortingRuleToTO(sortingRule SortingRule) sortingRuleTO {
	to := sortingRuleTO{
		ID:       sortingRule.ID,
		Target:   sortingRule.Target,
		Priority: sortingRule.Priority,
	}
	if sortingRule.Condition != nil {
		conditionTO := convertConditionToTO(*sortingRule.Condition)
		to.Condition = &conditionTO
	}
	return to
}
func convertTOToCondition(to conditionTO) Condition {
	condition := Condition{
		ID:                  to.ID,
		Name:                to.Name,
		Operator:            to.Operator,
		NegateSubCondition1: to.NegateSubCondition1,
		NegateSubCondition2: to.NegateSubCondition1,
	}
	if to.SubCondition1 != nil {
		subCondition1 := convertTOToCondition(*to.SubCondition1)
		condition.SubCondition1 = &subCondition1
	}
	if to.SubCondition2 != nil {
		subCondition2 := convertTOToCondition(*to.SubCondition2)
		condition.SubCondition2 = &subCondition2
	}
	if to.Operand1 != nil {
		operand1 := convertTOToConditionOperand(*to.Operand1)
		condition.Operand1 = &operand1
	}
	if to.Operand2 != nil {
		operand2 := convertTOToConditionOperand(*to.Operand2)
		condition.Operand2 = &operand2
	}

	return condition
}
func convertConditionToTO(condition Condition) conditionTO {
	to := conditionTO{
		ID:                  condition.ID,
		Name:                condition.Name,
		Operator:            condition.Operator,
		NegateSubCondition1: condition.NegateSubCondition1,
		NegateSubCondition2: condition.NegateSubCondition2,
	}
	if condition.Operand1 != nil {
		operand1TO := convertConditionOperandToTO(*condition.Operand1)
		to.Operand1 = &operand1TO
	}
	if condition.Operand2 != nil {
		operand2TO := convertConditionOperandToTO(*condition.Operand2)
		to.Operand2 = &operand2TO
	}
	if condition.SubCondition1 != nil {
		subCondition1 := convertConditionToTO(*condition.SubCondition1)
		to.SubCondition1 = &subCondition1
		to.SubCondition1ID = &condition.SubCondition1.ID
	}
	if condition.SubCondition2 != nil {
		subCondition2 := convertConditionToTO(*condition.SubCondition2)
		to.SubCondition2 = &subCondition2
		to.SubCondition2ID = &condition.SubCondition2.ID
	}

	return to
}

func convertConditionOperandToTO(conditionOperand ConditionOperand) conditionOperandTO {
	return conditionOperandTO{
		ID:            conditionOperand.ID,
		Name:          conditionOperand.Name,
		ConstantValue: conditionOperand.ConstantValue,
		ExtraValueKey: conditionOperand.ExtraValueKey,
		Type:          conditionOperand.Type,
	}
}

func convertTOToConditionOperand(to conditionOperandTO) ConditionOperand {
	return ConditionOperand{
		ID:            to.ID,
		Name:          to.Name,
		Type:          to.Type,
		ConstantValue: to.ConstantValue,
		ExtraValueKey: to.ExtraValueKey,
	}
}

func convertExpectedControlResultListToTOList(expectedControlResults []ExpectedControlResult) []expectedControlResultTO {
	tos := make([]expectedControlResultTO, 0)
	for _, expectedControlResult := range expectedControlResults {
		tos = append(tos, convertExpectedControlResultToExpectedControlResultTO(expectedControlResult))
	}
	return tos
}

func convertExpectedControlResultToExpectedControlResultTO(expectedControlResult ExpectedControlResult) expectedControlResultTO {
	return expectedControlResultTO{
		ID:               expectedControlResult.ID,
		AnalyteMappingId: expectedControlResult.AnalyteMappingId,
		SampleCode:       expectedControlResult.SampleCode,
		Operator:         expectedControlResult.Operator,
		ExpectedValue:    expectedControlResult.ExpectedValue,
		ExpectedValue2:   expectedControlResult.ExpectedValue2,
		CreatedBy:        &expectedControlResult.CreatedBy,
		CreatedAt:        &expectedControlResult.CreatedAt,
	}
}

func convertNotSpecifiedExpectedControlResultListToTOList(notSpecifiedExpectedControlResults []NotSpecifiedExpectedControlResult) []notSpecifiedExpectedControlResultTO {
	tos := make([]notSpecifiedExpectedControlResultTO, 0)
	for _, notSpecifiedExpectedControlResult := range notSpecifiedExpectedControlResults {
		tos = append(tos, notSpecifiedExpectedControlResultTO{
			AnalyteMappingId: notSpecifiedExpectedControlResult.AnalyteMappingId,
			SampleCode:       notSpecifiedExpectedControlResult.SampleCode,
		})
	}
	return tos
}

func convertTOsToExpectedControlResults(expectedControlResultTOs []expectedControlResultTO) []ExpectedControlResult {
	expectedControlResults := make([]ExpectedControlResult, 0)
	for i := range expectedControlResultTOs {
		expectedControlResults = append(expectedControlResults, convertTOToExpectedControlResult(expectedControlResultTOs[i]))
	}
	return expectedControlResults
}

func convertTOToExpectedControlResult(expectedControlResultTO expectedControlResultTO) ExpectedControlResult {
	expectedControlResult := ExpectedControlResult{
		ID:               expectedControlResultTO.ID,
		SampleCode:       expectedControlResultTO.SampleCode,
		AnalyteMappingId: expectedControlResultTO.AnalyteMappingId,
		Operator:         expectedControlResultTO.Operator,
		ExpectedValue:    expectedControlResultTO.ExpectedValue,
		ExpectedValue2:   expectedControlResultTO.ExpectedValue2,
	}

	if expectedControlResultTO.CreatedBy != nil {
		expectedControlResult.CreatedBy = *expectedControlResultTO.CreatedBy
	}

	if expectedControlResultTO.CreatedAt != nil {
		expectedControlResult.CreatedAt = *expectedControlResultTO.CreatedAt
	}

	return expectedControlResult
}

// Todo ZsN - Improve this
func isRequestMappingValid(instrument Instrument) bool {
	requestMappings := instrument.RequestMappings
	codes := make([]string, 0)
	for _, requestMapping := range requestMappings {
		if len(requestMapping.AnalyteIDs) < 1 {
			return false
		}

		// Check if only each code is once created
		if utils.SliceContains(requestMapping.Code, codes) {
			return false
		}
		codes = append(codes, requestMapping.Code)
	}
	return true
}
