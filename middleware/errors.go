package middleware

import "errors"

type ClientError struct {
	MessageKey    string            `json:"messageKey"`
	MessageParams map[string]string `json:"messageParams"`
	Message       string            `json:"message"`
	Errors        []ClientError     `json:"errors"`
}

var (
	InvalidTokenResponse = ClientError{
		MessageKey: "invalidTokenResponse",
		Message:    "Invalid Token Response",
	}
	ErrOpenIDConfiguration = ClientError{
		MessageKey: "40099",
		Message:    "OIDC .well-known/configuration could not be retrieved",
	}
	TokenExpiredResponse = ClientError{
		MessageKey: "tokenExpired",
		Message:    "Token expired",
	}
	ErrInvalidToken = ClientError{
		MessageKey: "invalidToken",
		Message:    "Invalid Token",
	}
	ErrNoPrivileges = ClientError{
		MessageKey: "unauthorized",
		Message:    "Not authorized",
	}
	ErrInvalidRequestBody = ClientError{
		MessageKey: "invalidRequestBody",
		Message:    "Invalid request body",
	}
	ErrUnableToParseRequestBody = ClientError{
		MessageKey: "unableToParseRequestBody",
		Message:    "Unable to parse request body",
	}
	ErrInvalidOrMissingRequestParameter = ClientError{
		MessageKey: "invalidOrMissingRequestParameter",
		Message:    "Invalid or missing request parameter: {{param}}",
	}
)

var (
	ErrFailedToLoadJwks = errors.New("Failed to load JWKS")
)
