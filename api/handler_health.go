package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type buildInfoStruct struct {
	BuildId       string `json:"buildId"`
	BuildNr       string `json:"buildNr"`
	SourceVersion string `json:"sourceVersion"`
}

type healthCheck struct {
	Service    string   `json:"service"`
	Status     string   `json:"status"`
	ApiVersion []string `json:"apiVersion"`
}

func (h *api) GetHealth(c *gin.Context) {
	defaultInfo := healthCheck{
		Service: "astm",
		Status:  "running",
	}

	c.JSON(http.StatusOK, defaultInfo)
}
