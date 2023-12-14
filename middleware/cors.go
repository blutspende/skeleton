package middleware

import (
	"github.com/blutspende/skeleton/config"
	"strings"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

func CreateCorsMiddleware(config *config.Configuration) gin.HandlerFunc {
	corsConfig := cors.DefaultConfig()
	corsConfig.AllowAllOrigins = !config.Authorization
	corsConfig.AllowCredentials = true

	if config.Authorization {
		origins := strings.Split(config.PermittedOrigin, ",")
		corsConfig.AllowOrigins = origins
	}

	corsConfig.AllowHeaders = []string{
		"Content-Type",
		"Content-Length",
		"Accept-Encoding",
		"X-CSRF-Token",
		"Authorization",
		"accept",
		"origin",
		"Cache-Control",
		"X-Requested-With",
	}

	corsConfig.AllowMethods = []string{
		"GET",
		"POST",
		"PUT",
		"PATCH",
		"DELETE",
	}

	return cors.New(corsConfig)
}
