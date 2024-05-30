package skeleton

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/blutspende/skeleton/db"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"time"
)

type SortingRuleRepository interface {
	Create(ctx context.Context, sortingRule SortingRule) (uuid.UUID, error)
	GetByInstrumentIDs(ctx context.Context, instrumentIDs []uuid.UUID) (map[uuid.UUID][]SortingRule, error)
	GetByInstrumentIDAndProgramme(ctx context.Context, instrumentID uuid.UUID, programme string) ([]SortingRule, error)
	Update(ctx context.Context, rule SortingRule) error
	Delete(ctx context.Context, sortingRuleIDs []uuid.UUID) error

	CreateTransaction() (db.DbConnector, error)
	WithTransaction(tx db.DbConnector) SortingRuleRepository
}

type sortingRuleRepository struct {
	db       db.DbConnector
	dbSchema string
}

func (r *sortingRuleRepository) Create(ctx context.Context, sortingRule SortingRule) (uuid.UUID, error) {
	if sortingRule.ID == uuid.Nil {
		sortingRule.ID = uuid.New()
	}

	query := fmt.Sprintf("INSERT INTO %s.sk_sorting_rules (id, instrument_id, priority, target, condition_id, programme)"+
		" VALUES (:id, :instrument_id, :priority, :target, :condition_id, :programme);", r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertSortingRuleToDAO(sortingRule))
	if err != nil {
		log.Error().Err(err).Msg(msgCreateSortingRuleFailed)
		return uuid.Nil, ErrCreateSortingRuleFailed
	}

	return sortingRule.ID, nil
}

func (r *sortingRuleRepository) GetByInstrumentIDs(ctx context.Context, instrumentIDs []uuid.UUID) (map[uuid.UUID][]SortingRule, error) {
	query := fmt.Sprintf("SELECT * FROM %s.sk_sorting_rules WHERE deleted_at IS NULL AND instrument_id IN (?) ORDER BY priority ASC;", r.dbSchema)
	query, args, err := sqlx.In(query, instrumentIDs)
	query = r.db.Rebind(query)
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgGetSortingRuleByInstrumentIDFailed)
		return nil, ErrGetSortingRuleByInstrumentIDFailed
	}

	defer rows.Close()
	sortingRulesMap := make(map[uuid.UUID][]SortingRule)
	for rows.Next() {
		var dao sortingRuleDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetSortingRuleByInstrumentIDFailed)
			return nil, ErrGetSortingRuleByInstrumentIDFailed
		}
		if _, ok := sortingRulesMap[dao.ID]; !ok {
			sortingRulesMap[dao.InstrumentID] = make([]SortingRule, 0)
		}
		sortingRulesMap[dao.InstrumentID] = append(sortingRulesMap[dao.InstrumentID], convertDAOToSortingRule(dao))
	}

	return sortingRulesMap, nil
}

func (r *sortingRuleRepository) GetByInstrumentIDAndProgramme(ctx context.Context, instrumentID uuid.UUID, programme string) ([]SortingRule, error) {
	query := fmt.Sprintf("SELECT * FROM %s.sk_sorting_rules WHERE deleted_at IS NULL AND instrument_id = $1", r.dbSchema)
	queryArgs := []interface{}{instrumentID}
	if len(programme) == 0 {
		query += " AND programme IS NULL"
	} else {
		queryArgs = append(queryArgs, programme)
		query += " AND programme = $2"
	}
	query += " ORDER BY priority ASC;"
	rows, err := r.db.QueryxContext(ctx, query, queryArgs...)
	if err != nil {
		log.Error().Err(err).Msg(msgGetSortingRuleByInstrumentIDFailed)
		return nil, ErrGetSortingRuleByInstrumentIDFailed
	}

	defer rows.Close()
	sortingRules := make([]SortingRule, 0)
	for rows.Next() {
		var dao sortingRuleDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetSortingRuleByInstrumentIDFailed)
			return nil, ErrGetSortingRuleByInstrumentIDFailed
		}

		sortingRules = append(sortingRules, convertDAOToSortingRule(dao))
	}

	return sortingRules, nil
}

func (r *sortingRuleRepository) Update(ctx context.Context, rule SortingRule) error {
	query := fmt.Sprintf("UPDATE %s.sk_sorting_rules "+
		"SET condition_id = :condition_id, target = :target, programme = :programme, modified_at = timezone('utc', now()) WHERE id = :id", r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertSortingRuleToDAO(rule))
	if err != nil {
		log.Error().Err(err).Msg(msgUpdateSortingRuleFailed)
		return ErrUpdateSortingRuleFailed
	}

	return nil
}

func (r *sortingRuleRepository) Delete(ctx context.Context, sortingRuleIDs []uuid.UUID) error {
	if len(sortingRuleIDs) == 0 {
		return nil
	}

	query := fmt.Sprintf("UPDATE %s.sk_sorting_rules SET deleted_at = timezone('utc', now()) WHERE id IN (?);", r.dbSchema)
	query, args, _ := sqlx.In(query, sortingRuleIDs)
	query = r.db.Rebind(query)
	_, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgDeleteSortingRulesFailed)
		return ErrDeleteSortingRulesFailed
	}

	return nil
}

func (r *sortingRuleRepository) CreateTransaction() (db.DbConnector, error) {
	return r.db.CreateTransactionConnector()
}

func (r *sortingRuleRepository) WithTransaction(tx db.DbConnector) SortingRuleRepository {
	txRepo := *r
	txRepo.db = tx
	return &txRepo
}
func NewSortingRuleRepository(db db.DbConnector, dbSchema string) SortingRuleRepository {
	return &sortingRuleRepository{
		db:       db,
		dbSchema: dbSchema,
	}
}

type sortingRuleDAO struct {
	ID           uuid.UUID      `db:"id"`
	InstrumentID uuid.UUID      `db:"instrument_id"`
	Priority     int            `db:"priority"`
	Target       string         `db:"target"`
	ConditionID  uuid.NullUUID  `db:"condition_id""`
	Programme    sql.NullString `db:"programme"`
	CreatedAt    time.Time      `db:"created_at"`
	ModifiedAt   sql.NullTime   `db:"modified_at"`
	DeletedAt    sql.NullTime   `db:"deleted_at"`
}

func convertSortingRuleToDAO(sortingRule SortingRule) sortingRuleDAO {
	dao := sortingRuleDAO{
		ID:           sortingRule.ID,
		InstrumentID: sortingRule.InstrumentID,
		Target:       sortingRule.Target,
		Priority:     0,
	}
	if sortingRule.Programme != nil {
		dao.Programme = sql.NullString{
			String: *sortingRule.Programme,
			Valid:  len(*sortingRule.Programme) > 0,
		}

	}
	if sortingRule.Condition != nil {
		dao.ConditionID = uuid.NullUUID{
			UUID:  sortingRule.Condition.ID,
			Valid: true,
		}
	}

	return dao
}

func convertDAOToSortingRule(dao sortingRuleDAO) SortingRule {
	sortingRule := SortingRule{
		ID:           dao.ID,
		InstrumentID: dao.InstrumentID,
		Target:       dao.Target,
		Priority:     dao.Priority,
	}

	if dao.ConditionID.Valid {
		sortingRule.Condition = &Condition{
			ID: dao.ConditionID.UUID,
		}
	}

	if dao.Programme.Valid {
		sortingRule.Programme = &dao.Programme.String
	}

	return sortingRule
}

const (
	msgCreateSortingRuleFailed            = "create sorting rule failed"
	msgGetSortingRuleByInstrumentIDFailed = "get sorting rule by instrument ID failed"
	msgUpdateSortingRuleFailed            = "update sorting rule failed"
	msgDeleteSortingRulesFailed           = "delete sorting rules failed"
)

var (
	ErrCreateSortingRuleFailed            = errors.New(msgCreateSortingRuleFailed)
	ErrGetSortingRuleByInstrumentIDFailed = errors.New(msgGetSortingRuleByInstrumentIDFailed)
	ErrUpdateSortingRuleFailed            = errors.New(msgUpdateSortingRuleFailed)
	ErrDeleteSortingRulesFailed           = errors.New(msgDeleteSortingRulesFailed)
)
