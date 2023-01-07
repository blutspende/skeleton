package api

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

// TODO: Rework the messagekeys: they are redundant and often missing
// TODO: Check each for use and delete those that are not used
var (
	ErrCanNotGetOIDCConfig = HTTPError{
		MessageKey: "40100",
		Message:    i18n.CanNotGetOIDCConfig,
	}
	ErrInvalidToken = HTTPError{
		MessageKey: "40100",
		Message:    i18n.InvalidTokenMsg,
	}
	ErrExpiredToken = HTTPError{
		MessageKey: "40101",
		Message:    i18n.InvalidTokenMsg,
	}
	ErrInvalidIDParameter = HTTPError{
		MessageKey: "",
		Message:    i18n.InvalidIdParameterMsg,
	}
	ErrInvalidIDParameterRequestMapping = HTTPError{
		MessageKey: "",
		Message:    i18n.InvalidIdParameterRequestMappingMsg,
	}
	ErrInvalidIDParameterInstrument = HTTPError{
		MessageKey: "",
		Message:    i18n.InvalidIdParameterInstrumentMsg,
	}
	ErrInvalidRequestBody = HTTPError{
		MessageKey: "40012",
		Message:    i18n.InvalidBodyInRequest,
	}
	ErrInternalServerError = HTTPError{
		MessageKey: "50000",
		Message:    i18n.InternalServerError,
	}
	InvalidValidUntil = HTTPError{
		MessageKey: "5555",
		Message:    i18n.InvalidValidUntil,
	}
	InvalidDataOfRequestMapping = HTTPError{
		MessageKey: "99999",
		Message:    i18n.InvalidDataOfRequestMapping,
	}
	ErrInvalidIDParameterAnalyteMappingID = HTTPError{
		MessageKey: "",
		Message:    i18n.InvalidAnalyteMappingID,
	}
	CanNotInsertData = HTTPError{
		MessageKey: "",
		Message:    i18n.CanNotInsertData,
	}
	CanNotConvertStringToNumber = HTTPError{
		MessageKey: "",
		Message:    i18n.CanNotConvertStringToNumber,
	}
	InvalidQueryParamTimefrom = HTTPError{
		MessageKey: "",
		Message:    i18n.InvalidQueryParamTimeFrom,
	}
	InvalidTimeFormatOfTimeStringISO8601 = HTTPError{
		MessageKey: "",
		Message:    i18n.InvalidTimeFormatOfTimeStringISO8601,
	}
	InvalidQueryParams = HTTPError{
		MessageKey: "",
		Message:    i18n.InvalidQueryParams,
	}
	ErrOpenIDConfiguration = HTTPError{
		MessageKey: "40099",
		Message:    i18n.CanNotGetOIDCConfig,
	}
	InvalidTokenResponse = HTTPError{
		MessageKey: "40100",
		Message:    i18n.InvalidTokenMsg,
	}
	TokenExpiredResponse = HTTPError{
		MessageKey: "40101",
		Message:    i18n.ExpiredTokenMsg,
	}

	NoPrivilegesResponse = HTTPError{
		MessageKey: "40102",
		Message:    i18n.NoPrivilegeMsg,
	}
	ErrInvalidSubjectTypeProvidedInAnalysisRequest = HTTPError{
		MessageKey: "40103",
		Message:    "Invalid subject type provided in analysisRequest",
	}
)

var (
	ErrFailedToLoadJwks = errors.New(i18n.FailedToLoadJwksMsg)
)
