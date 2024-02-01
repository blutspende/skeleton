package skeleton

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"net/http"
)

// Retrigger examination
// @Summary Retrigger examination by batch id
// @Description Retrigger examination by batch id
// @Tags Examination
// @Produce json
// @Accept json
// @Param BatchID query uuid.UUID true "Batch id"
// @Success 204 "No Content"
// @Router /v1/examination/batch [GET]
func (api *api) RetriggerExamination(c *gin.Context) {
	batchID := uuid.Nil
	batchIDString, ok := c.GetQuery("batchId")
	if ok {
		var err error
		batchID, err = uuid.Parse(batchIDString)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, "malformed batch ID")
			return
		}
	}

	api.analysisService.RetriggerExamination(c, batchID)

	c.Status(http.StatusNoContent)
}
