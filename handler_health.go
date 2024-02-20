package skeleton

import (
	"fmt"
	"net/http"
	"runtime"

	"github.com/gin-gonic/gin"
)

// BuildVersion - will be filled at build process in pipeline
var BuildVersion string

// ServiceName - will be filled at build process in pipeline
var ServiceName string

type healthCheck struct {
	Service      string   `json:"service"`
	Status       string   `json:"status"`
	ApiVersion   []string `json:"apiVersion"`
	BuildVersion string   `json:"buildVersion"` // Docker build version
	MemStats     memStats `json:"memStats"`
}

type memStats struct {
	Alloc              string `json:"alloc"`
	TotalAlloc         string `json:"totalAlloc"`
	Sys                string `json:"sys"`
	HeapInUse          string `json:"heapInUse"`
	HeapAlloc          string `json:"headAlloc"`
	StackInUse         string `json:"stackInUse"`
	NumberOfGoRoutines int    `json:"numberOfGoRoutines"`
}

func (api *api) GetHealth(c *gin.Context) {
	defaultInfo := healthCheck{
		Service:      ServiceName,
		Status:       "running",
		ApiVersion:   []string{"v1"},
		BuildVersion: BuildVersion,
	}

	var memStat runtime.MemStats
	runtime.ReadMemStats(&memStat)

	defaultInfo.MemStats.Alloc = fmt.Sprintf("%v MiB", memStat.Alloc/1024/1024)
	defaultInfo.MemStats.TotalAlloc = fmt.Sprintf("%v MiB", memStat.TotalAlloc/1024/1024)
	defaultInfo.MemStats.Sys = fmt.Sprintf("%v MiB", memStat.Sys/1024/1024)
	defaultInfo.MemStats.HeapInUse = fmt.Sprintf("%v MiB", memStat.HeapInuse/1024/1024)
	defaultInfo.MemStats.HeapAlloc = fmt.Sprintf("%v MiB", memStat.HeapAlloc/1024/1024)
	defaultInfo.MemStats.StackInUse = fmt.Sprintf("%v MiB", memStat.StackInuse/1024/1024)
	defaultInfo.MemStats.NumberOfGoRoutines = runtime.NumGoroutine()

	c.JSON(http.StatusOK, defaultInfo)
}
