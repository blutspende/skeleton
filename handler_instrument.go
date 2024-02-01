package skeleton

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"net/http"
)

// Reprocess instrument data
// @Summary Reprocess instrument data by batch id
// @Description Reprocess instrument data by batch id
// @Tags Instrument
// @Produce json
// @Accept json
// @Param BatchID query uuid.UUID true "Batch id"
// @Success 204 "No Content"
// @Router /v1/instruments/reprocess [POST]
func (api *api) ReprocessInstrumentData(c *gin.Context) {
	batchID := uuid.Nil
	batchIDString, ok := c.GetQuery("batchId")
	if ok {
		var err error
		batchID, err = uuid.Parse(batchIDString)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, "malformed batch ID")
			return
		}
	} else {
		c.AbortWithStatusJSON(http.StatusBadRequest, "missing batch ID")
		return
	}

	api.instrumentService.ReprocessInstrumentData(c, batchID)

	c.Status(http.StatusNoContent)
}
