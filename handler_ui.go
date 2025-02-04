package skeleton

import (
	"errors"
	"github.com/blutspende/skeleton/middleware"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

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
	keyExpectedControlResultCreateFailed       = "expectedControlResultCreateFailed"
	msgExpectedControlResultCreateFailed       = "Expected control result create failed!"
	keyExpectedControlResultUpdateFailed       = "expectedControlResultUpdateFailed"
	msgExpectedControlResultUpdateFailed       = "Expected control result update failed!"
	keyInvalidExpectedControlResultValue       = "invalidExpectedControlResultValue"
	keyExpectedControlResultValidationError    = "expectedControlResultValidationError"
	msgExpectedControlResultValidationError    = "Invalid request body!"
	keyAnalyteNotFoundForExpectedControlResult = "analyteNotFoundForExpectedControlResult"
	msgAnalyteNotFoundForExpectedControlResult = "Unexpected analyte for expected control result!"
)

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
