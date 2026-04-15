package skeleton

import (
	"bytes"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"
)

const (
	ForeignKeyViolationErrorCode = pq.ErrorCode("23503")
	UniqueViolationErrorCode     = pq.ErrorCode("23505")
)

// ErrSkipAnalysisRequests is to be used by instrument drivers in the HandleAnalysisRequests callback when the
// analysis request batch cannot be processed due to some internal logic or rule set.
var ErrSkipAnalysisRequests = errors.New("skip analysis requests")

type ParameterizedError struct {
	error
	Params map[string]string
}

func (pe ParameterizedError) Error() string {
	buff := bytes.NewBufferString("")

	buff.WriteString(pe.error.Error())
	for key, value := range pe.Params {
		buff.WriteString(" " + key + ": " + value)
	}
	buff.WriteString("\n")

	return strings.TrimSpace(buff.String())
}

func IsErrorCode(err error, errCode pq.ErrorCode) bool {
	pgErr, ok := err.(*pq.Error)
	if ok {
		return pgErr.Code == errCode
	}

	pgxErr, ok := err.(*pgconn.PgError)
	if ok {
		currentCode := pq.ErrorCode(pgxErr.Code)
		return currentCode == errCode
	}

	return false
}
