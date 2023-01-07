package web

// TODO: Update new Error Structure
type HTTPError struct {
	MessageKey    string            `json:"messageKey"`
	MessageParams map[string]string `json:"messageParams"`
	Message       string            `json:"message"`
	Errors        []HTTPError       `json:"errors"`
}

var (
	ErrInvalidRequestBody = HTTPError{
		MessageKey: "41100",
		Message:    "Invalid request body",
	}
	ErrInvalidSubjectTypeProvidedInAnalysisRequest = HTTPError{
		MessageKey: "41101",
		Message:    "Invalid subject Type provided",
	}
	ErrInternalServerError = HTTPError{
		MessageKey: "500",
		Message:    "Internal Server Error",
	}
)
