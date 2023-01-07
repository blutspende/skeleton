package middleware

import (
	"astm/skeleton/auth"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v4"
)

// CheckAuth - Token Validator for api requests
func CheckAuth(authManager auth.AuthManager) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.Request.Header.Get("Authorization")
		token := strings.Split(authHeader, "Bearer ")

		if len(token) < 2 || token[1] == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, InvalidTokenResponse)
			return
		}

		jwks, err := authManager.GetJWKS()
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, ErrOpenIDConfiguration)
			return
		}

		userToken := UserToken{}
		_, err = jwt.ParseWithClaims(token[1], &userToken, jwks.Keyfunc)

		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, InvalidTokenResponse)
			return
		}

		if !userToken.VerifyExpiresAt(time.Now(), true) {
			c.AbortWithStatusJSON(http.StatusUnauthorized, TokenExpiredResponse)
			return
		}

		c.Set("User", userToken)
		c.Next()
	}
}
