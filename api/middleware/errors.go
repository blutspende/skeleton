package middleware

import (
	"astm/skeleton/i18n"
	"errors"
)

// TODO: Update new Error Structure
type HTTPError struct {
	MessageKey    string            `json:"messageKey"`
	MessageParams map[string]string `json:"messageParams"`
	Message       string            `json:"message"`
	Errors        []HTTPError       `json:"errors"`
}

var (
	InvalidTokenResponse = HTTPError{
		MessageKey: "40100",
		Message:    i18n.InvalidTokenMsg,
	}
	ErrOpenIDConfiguration = HTTPError{
		MessageKey: "40099",
		Message:    i18n.CanNotGetOIDCConfig,
	}
	TokenExpiredResponse = HTTPError{
		MessageKey: "40101",
		Message:    i18n.ExpiredTokenMsg,
	}
	ErrInvalidToken = HTTPError{
		MessageKey: "40100",
		Message:    i18n.InvalidTokenMsg,
	}
	ErrNoPrivileges = HTTPError{
		MessageKey: "40102",
		Message:    i18n.NoPrivilegeMsg,
	}
)

var (
	ErrFailedToLoadJwks = errors.New(i18n.FailedToLoadJwksMsg)
)
