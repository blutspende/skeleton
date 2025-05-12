package db

import (
	"context"
	"database/sql"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"

	// Need this import for sqlx
	_ "github.com/lib/pq"
)

type DbConnector interface {
	CreateTransactionConnector() (DbConnector, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	NamedExec(query string, arg interface{}) (sql.Result, error)
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
	NamedQuery(query string, arg interface{}) (*sqlx.Rows, error)
	NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error)
	Queryx(query string, args ...interface{}) (*sqlx.Rows, error)
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
	QueryRowx(query string, args ...interface{}) *sqlx.Row
	QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row
	PrepareNamed(query string) (*sqlx.NamedStmt, error)
	Rebind(query string) string
	Commit() error
	Rollback() error
	Ping() error
}

type dbConnector struct {
	pg Postgres
	db *sqlx.DB
	tx *sqlx.Tx
}

func NewDbConnector(pg Postgres) DbConnector {
	return &dbConnector{
		pg: pg,
	}
}

func (c *dbConnector) CreateTransactionConnector() (DbConnector, error) {
	dbConn, err := c.pg.GetDbConnection()
	if err != nil {
		log.Error().Err(err).Msg(ErrDbConnectionNotAvailable.Error())
		return nil, ErrDbConnectionNotAvailable
	}
	c.db = dbConn

	c.tx, err = c.db.Beginx()
	if err != nil {
		log.Error().Err(err).Msg(MsgBeginTransactionFailed)
		return nil, ErrBeginTransactionFailed
	}

	return c, err
}

func (c *dbConnector) Exec(query string, args ...interface{}) (sql.Result, error) {
	if c.tx != nil {
		return c.tx.Exec(query, args...)
	} else {
		return c.db.Exec(query, args...)
	}
}

func (c *dbConnector) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if c.tx != nil {
		return c.tx.ExecContext(ctx, query, args...)
	} else {
		return c.db.ExecContext(ctx, query, args...)
	}
}

func (c *dbConnector) NamedExec(query string, arg interface{}) (sql.Result, error) {
	if c.tx != nil {
		return c.tx.NamedExec(query, arg)
	} else {
		return c.db.NamedExec(query, arg)
	}
}

func (c *dbConnector) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	if c.tx != nil {
		return c.tx.NamedExecContext(ctx, query, arg)
	} else {
		return c.db.NamedExecContext(ctx, query, arg)
	}
}

func (c *dbConnector) NamedQuery(query string, arg interface{}) (*sqlx.Rows, error) {
	if c.tx != nil {
		return c.tx.NamedQuery(query, arg)
	} else {
		return c.db.NamedQuery(query, arg)
	}
}

func (c *dbConnector) NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error) {
	if c.tx != nil {
		return c.tx.NamedQuery(query, arg)
	}
	return c.db.NamedQueryContext(ctx, query, arg)
}

func (c *dbConnector) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	if c.tx != nil {
		return c.tx.Queryx(query, args...)
	} else {
		return c.db.Queryx(query, args...)
	}
}

func (c *dbConnector) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	if c.tx != nil {
		return c.tx.QueryxContext(ctx, query, args...)
	} else {
		return c.db.QueryxContext(ctx, query, args...)
	}
}

func (c *dbConnector) QueryRowx(query string, args ...interface{}) *sqlx.Row {
	if c.tx != nil {
		return c.tx.QueryRowx(query, args...)
	} else {
		return c.db.QueryRowx(query, args...)
	}
}

func (c *dbConnector) QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row {
	if c.tx != nil {
		return c.tx.QueryRowxContext(ctx, query, args...)
	} else {
		return c.db.QueryRowxContext(ctx, query, args...)
	}
}

func (c *dbConnector) Commit() error {
	if c.tx != nil {
		err := c.tx.Commit()
		if err != nil {
			log.Error().Err(err).Msg(MsgCommitTransactionFailed)
			return ErrCommitTransactionFailed
		}
	}
	return nil
}

func (c *dbConnector) Rollback() error {
	if c.tx != nil {
		err := c.tx.Rollback()
		if err != nil {
			log.Error().Err(err).Msg(MsgRollbackTransactionFailed)
			return ErrRollbackTransactionFailed
		}
	}
	return nil
}

func (c *dbConnector) Rebind(query string) string {
	if c.tx != nil {
		return c.tx.Rebind(query)
	}
	return c.db.Rebind(query)
}

func (c *dbConnector) PrepareNamed(query string) (*sqlx.NamedStmt, error) {
	if c.tx != nil {
		return c.tx.PrepareNamed(query)
	} else {
		return c.db.PrepareNamed(query)
	}
}

func (c *dbConnector) Ping() error {
	return c.db.Ping()
}
