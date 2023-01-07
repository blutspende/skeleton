package db

import (
    "errors"
)

const (
    MsgBeginTransactionTransactionFailed    = "begin transaction failed"
    MsgCommitTransactionTransactionFailed   = "commit transaction failed"
    MsgRollbackTransactionTransactionFailed = "revert transaction failed"
)

var (
    ErrBeginTransactionTransactionFailed    = errors.New(MsgBeginTransactionTransactionFailed)
    ErrCommitTransactionTransactionFailed   = errors.New(MsgCommitTransactionTransactionFailed)
    ErrRollbackTransactionTransactionFailed = errors.New(MsgRollbackTransactionTransactionFailed)
)
