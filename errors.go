package skeleton

import (
	"bytes"
	"errors"
	"strings"

	pgx "github.com/jackc/pgconn"
	"github.com/lib/pq"
)

const (
	ForeignKeyViolationErrorCode = pq.ErrorCode("23503")
	UniqueViolationErrorCode     = pq.ErrorCode("23505")
	MsgFailedToAudit             = "failed to audit"
)

var (
	ErrFailedToAudit = errors.New(MsgFailedToAudit)
)

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

type ParameterizedErrors []ParameterizedError

func (e ParameterizedErrors) Error() string {

	buff := bytes.NewBufferString("")

	for i := range e {
		buff.WriteString(e[i].Error() + "\n")
	}

	return strings.TrimSpace(buff.String())
}

func NewParameterizedError(err error, params map[string]string) ParameterizedError {
	return ParameterizedError{
		error:  err,
		Params: params,
	}
}

func IsErrorCode(err error, errCode pq.ErrorCode) bool {
	pgErr, ok := err.(*pq.Error)
	if ok {
		return pgErr.Code == errCode
	}

	pgxErr, ok := err.(*pgx.PgError)
	if ok {
		currentCode := pq.ErrorCode(pgxErr.Code)
		return currentCode == errCode
	}

	return false
}
