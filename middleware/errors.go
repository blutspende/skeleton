package middleware

import "errors"

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
		Message:    "Invalid Token",
	}
	ErrOpenIDConfiguration = HTTPError{
		MessageKey: "40099",
		Message:    "OIDC .well-known/configuration could not be retrieved",
	}
	TokenExpiredResponse = HTTPError{
		MessageKey: "40101",
		Message:    "Token expired",
	}
	ErrInvalidToken = HTTPError{
		MessageKey: "40100",
		Message:    "Invalid Token",
	}
	ErrNoPrivileges = HTTPError{
		MessageKey: "40102",
		Message:    "Not authorized",
	}
)

var (
	ErrFailedToLoadJwks = errors.New("Failed to load JWKS")
)
