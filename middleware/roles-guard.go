package middleware

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
)

// RoleProtection - Checks if user has roles
//
// @var strict bool - if strict is true, the user must have all the roles
func RoleProtection(roles []UserRole, strict, authMode bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		// For development, you can turn of the roleProtection
		if !authMode {
			c.Next()
			return
		}

		userObj, ok := c.Get("User")
		if !ok {
			log.Error().Msg(ErrInvalidToken.Message)
			c.AbortWithStatusJSON(http.StatusUnauthorized, ErrInvalidToken)
			return
		}

		user, ok := userObj.(UserToken)
		if !ok {
			log.Error().Msg(ErrInvalidToken.Message)
			c.AbortWithStatusJSON(http.StatusUnauthorized, ErrInvalidToken)
			return
		}

		if !strict {
			for _, role := range roles {
				if contains(user.RealmAccess.Roles, role) {
					c.Next()
					return
				}
			}
			log.Error().Str("user", user.Email).Msg(fmt.Sprintf("%s. roles=%v", ErrNoPrivileges.Message, roles))
			c.AbortWithStatusJSON(http.StatusUnauthorized, ErrNoPrivileges)
			return
		}

		if !containsAll(user.RealmAccess.Roles, roles) {
			log.Error().Str("user", user.Email).Msg(fmt.Sprintf("%s. roles=%v", ErrNoPrivileges.Message, roles))
			c.AbortWithStatusJSON(http.StatusUnauthorized, ErrNoPrivileges)
			return
		}

		c.Next()
	}
}

// Contains - String Slice contains a string. Return true of false
func contains[T comparable](set []T, target T) bool {
	for i := 0; i < len(set); i++ {
		if set[i] == target {
			return true
		}
	}
	return false
}

// ContainsAll - Check if all targets are contains in the Array/Slice
func containsAll[T comparable](set []T, targets []T) bool {
	allFound := true
	for i := 0; i < len(targets); i++ {
		if !contains(set, targets[i]) {
			allFound = false
			break
		}
	}
	return allFound
}
