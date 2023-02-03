package db

import (
	"errors"
)

const (
	MsgBeginTransactionFailed    = "begin transaction failed"
	MsgCommitTransactionFailed   = "commit transaction failed"
	MsgRollbackTransactionFailed = "revert transaction failed"
)

var (
	ErrBeginTransactionFailed    = errors.New(MsgBeginTransactionFailed)
	ErrCommitTransactionFailed   = errors.New(MsgCommitTransactionFailed)
	ErrRollbackTransactionFailed = errors.New(MsgRollbackTransactionFailed)
)
