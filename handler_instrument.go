package skeleton

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"net/http"
)

// Reprocess instrument data
// @Summary Reprocess instrument data by batch ids
// @Description Reprocess instrument data by batch ids
// @Tags Instrument
// @Produce json
// @Accept json
// @Param BatchID body []uuid.UUID true "Batch ids"
// @Success 204 "No Content"
// @Router /v1/instruments/reprocess [POST]
func (api *api) ReprocessInstrumentData(c *gin.Context) {
	var batchIDs []uuid.UUID
	err := c.ShouldBindJSON(&batchIDs)
	if err != nil {
		log.Error().Err(err).Msg(MsgCanNotBindRequestBody)
		c.AbortWithStatusJSON(http.StatusBadRequest, clientError{
			Message:    MsgCanNotBindRequestBody,
			MessageKey: keyBadRequest,
		})
		return
	}

	if len(batchIDs) > 0 {
		api.instrumentService.ReprocessInstrumentData(c, batchIDs)
	}

	c.Status(http.StatusNoContent)
}
