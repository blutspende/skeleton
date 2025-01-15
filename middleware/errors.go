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
		MessageKey: "InvalidTokenResponse",
		Message:    "Invalid Token Response",
	}
	ErrOpenIDConfiguration = ClientError{
		MessageKey: "40099",
		Message:    "OIDC .well-known/configuration could not be retrieved",
	}
	TokenExpiredResponse = ClientError{
		MessageKey: "TokenExpired",
		Message:    "Token expired",
	}
	ErrInvalidToken = ClientError{
		MessageKey: "InvalidToken",
		Message:    "Invalid Token",
	}
	ErrNoPrivileges = ClientError{
		MessageKey: "Unauthorized",
		Message:    "Not authorized",
	}
	ErrInvalidRequestBody = ClientError{
		MessageKey: "InvalidRequestBody",
		Message:    "Invalid request body",
	}
	ErrUnableToParseRequestBody = ClientError{
		MessageKey: "UnableToParseRequestBody",
		Message:    "Unable to parse request body",
	}
	ErrInvalidOrMissingRequestParameter = ClientError{
		MessageKey: "InvalidOrMissingRequestParameter",
		Message:    "Invalid or missing request parameter",
	}
)

var (
	ErrFailedToLoadJwks = errors.New("Failed to load JWKS")
)
