package skeleton

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	pgx "github.com/jackc/pgconn"
	"github.com/lib/pq"
)

var (
	CanNotValidateResult = fmt.Errorf("Can not validate Result")
	InvalidResultType    = fmt.Errorf("Invalid result type")
	OrderAlreadySent     = fmt.Errorf("Orders already sent")

	ErrSendResultBatchPartiallyFailed = fmt.Errorf(MsgSendResultBatchPartiallyFailed)
	ErrUnmarshalResponseFailed        = fmt.Errorf(MsgUnmarshalResponseFailed)
	ErrUnmarshalErrorResponseFailed   = fmt.Errorf(MsgUnmarshalErrorResponseFailed)
	ReceiveError                      = fmt.Errorf(MsgErrorInSkeleton)
)

const (
	ForeignKeyViolationErrorCode = pq.ErrorCode("23503")

	ApiStartMsg           = "API server astm has been started"
	ApiEndedGracefullyMsg = "API server astm ended gracefully"
	ApiFailedToStartMsg   = "Failed to start API server astm"

	InvalidTokenMsg       = "invalid token"
	ExpiredTokenMsg       = "expired token"
	NoPrivilegeMsg        = "user has no privileges to use this route"
	FailedToGetOIDCConfig = "can not get OIDC config"

	MissingIdParameterMsg               = "missing id parameter"
	InvalidIdParameterMsg               = "invalid id parameter"
	InvalidIdParameterRequestMappingMsg = "invalid id parameter for request mapping"
	InvalidIdParameterInstrumentMsg     = "invalid id parameter for instrumentID"

	FailedToLoadJwksMsg = "failed to load jwks from userservice"

	CanNotGetSavedUserFromTokenMsg           = "can not get saved user from token"
	CanNotParseUserObjectInUserTokenModelMsg = "can not parse user object in userToken model"
	UserMissingOneOfThisPrivilegesMsg        = "user missing one of this privileges: %v"
	UserMissingAllPrivilegesMsg              = "user missing privileges: %v all of these must assign to the user!"

	InvalidBodyInRequest                 = "can't not bind request body!"
	InvalidValidUntil                    = "valid until is in the Past!"
	InvalidDateTimeForValidUntil         = "invalid Datetime for ValidUntilTime"
	InvalidAnalyteMappingID              = "invalid param analyteMappingID"
	CanNotGetInstrumentIDFromParam       = "can not getting instrumentID from params"
	CanNotInsertData                     = "can not insert Data"
	CanNotConvertStringToNumber          = "can not convert string to number"
	InvalidQueryParamTimeFrom            = "invalid or missing queryParam timefrom"
	InvalidTimeFormatOfTimeStringISO8601 = "invalid time format of string. This string should be in format of ISO8601"
	InvalidQueryParams                   = "invalid query params"

	FailedToConnectToPostgresDbMsg = "failed to connect to postgres db"
	UnavailablePostgresDbMsg       = "the postgres db is unavailable"
	PostgresAvailableConnectedMsg  = "postgres available, connected to %s / %s"
	HandlerFailedToReadInfoFileMsg = "failed to read info file: %s"

	FailedToRollBackTransaction  = "can not roll-back transaction"
	FailedToCommitTransaction    = "can not commit transaction"
	InternalServerError          = "Unexpected error."
	FailedToStartTransaction     = "can not start transaction"
	InvalidDataOfRequestMapping  = "the data of request mapping is invalid"
	OldChannelMappingFoundInList = "old channel mapping not found in given list"

	CanNotGetOIDCConfig = "can not get OIDC config"

	MsgSendResultBatchPartiallyFailed = "send result batch to cerberus partially Failed"
	MsgUnmarshalResponseFailed        = "unmarshal send result batch response failed"
	MsgUnmarshalErrorResponseFailed   = "unmarshal send result batch error response failed"

	MsgErrorInSkeleton = "error in skeletonhandler"
	MsgFailedToAudit   = "failed to audit"
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

	buff.WriteString(pe.Error())
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

func IsErrorCode(err error, errcode pq.ErrorCode) bool {
	pgErr, ok := err.(*pq.Error)
	if ok {
		return pgErr.Code == errcode
	}

	pgxErr, ok := err.(*pgx.PgError)
	if ok {
		currentCode := pq.ErrorCode(pgxErr.Code)
		return currentCode == errcode
	}

	return false
}
