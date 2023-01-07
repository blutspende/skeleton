package api

import (
	"database/sql"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// AddRequestToTransferQueue
// @Summary Add Request to Transfer Queue
// @Description Add a Request to the transfer queue to retransmit again
// @Tags AnalysisRequest
// @Produce json
// @Param requestID path string true "ID of the Request"
// @Success 200 "OK"
// @Failure 400 "Bad Request"
// @Failure 500 {object} model.HTTPError "Internal Server Error"
// @Router /v1/instruments/request/{requestID}/add-to-queue [GET]
func (h *api) AddRequestToTransferQueue(c *gin.Context) {
	requestID, err := uuid.Parse(c.Param("requestID"))
	if err != nil {
		log.Error().Err(err).Msg("AddRequestToTransferQueue: invalid requestID parameter")
		c.JSON(http.StatusBadRequest, api_errors.ErrInvalidIDParameter)
		return
	}

	request, err := h.analysisRequestService.GetRequestByID(requestID)
	if err != nil {
		if err == sql.ErrNoRows {
			c.Status(http.StatusOK)
			return
		}

		log.Error().Err(err).Msg("Can not fetch requestID")
		c.JSON(http.StatusInternalServerError, api_errors.InternalServerError)
		return
	}

	skeletonRequest := mapAnalysisRequestToSkeletonAnalysisRequest(request)
	h.analysisRequestService.RetriggerTransferOfRequest(skeletonRequest)

	c.Status(http.StatusOK)
}

// GetChannelResultsForRequest
// @Summary Get Channel Results for requestID
// @Description Get Channel Results for requestID
// @Tags AnalysisRequest
// @Produce json
// @Param requestID path string true "ID of the Request"
// @Success 200 {object} []model.ChannelResultDetailTO "OK"
// @Failure 400 "Bad Request"
// @Failure 500 string string "Internal Server Error"
// @Router /v1/instruments/channel-results/{requestID} [GET]
func (h *api) GetChannelResultsForRequest(c *gin.Context) {
	requestID, err := uuid.Parse(c.Param("requestID"))
	if err != nil {
		log.Error().Err(err).Msg("GetCIAHTTPRequestHistory: invalid requestID parameter")
		c.Status(http.StatusBadRequest)
		return
	}

	_, err = h.analysisRequestService.GetRequestByID(requestID)
	if err != nil {
		if err == sql.ErrNoRows {
			c.Status(http.StatusNotFound)
			return
		}
		log.Error().Err(err).Msg("Can not get request by given ID")
		c.JSON(http.StatusBadRequest, "Can not get request by ID")
		return
	}

	c.JSON(http.StatusOK, []model.ChannelResultDetailTO{})
}

func (h *api) AddTransmissionsBatchToTransferQueue(c *gin.Context) {
	var transmissionBatchData apiModel.TransmissionBatch

	err := c.ShouldBindJSON(&transmissionBatchData)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, api_errors.InvalidRequestBody)
		return
	}

	sampleCodes, err := h.analysisResultService.GetSampleCodesByBatchIDs(transmissionBatchData.TransmissionIDs)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, api_errors.InternalServerError)
		return
	}

	if len(sampleCodes) > 0 {
		err = h.analysisRequestService.RetriggerResultTransferBySampleCodes(sampleCodes)
		if err != nil {
			log.Error().Err(err).Msg("Can not retrigger Transfer for sampleCodes")
			c.AbortWithStatusJSON(http.StatusBadRequest, api_errors.InternalServerError)
			return
		}
	}

	c.Status(http.StatusOK)
}
