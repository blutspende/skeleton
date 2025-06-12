package skeleton

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/blutspende/skeleton/db"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
	"strings"
	"time"
)

type SortingRuleRepository interface {
	Upsert(ctx context.Context, sortingRule SortingRule) (uuid.UUID, error)
	GetById(ctx context.Context, id uuid.UUID) (*SortingRule, error)
	GetByInstrumentIDs(ctx context.Context, instrumentIDs []uuid.UUID) (map[uuid.UUID][]SortingRule, error)
	GetByInstrumentIDAndProgramme(ctx context.Context, instrumentID uuid.UUID, programme string) ([]SortingRule, error)
	GetAppliedSortingRuleTargets(ctx context.Context, instrumentID uuid.UUID, programme string, sampleCodes []string) ([]string, error)
	ApplySortingRuleTarget(ctx context.Context, instrumentID uuid.UUID, programme, sampleCode, target string, validUntil time.Time) error
	GetSampleSequenceNumber(ctx context.Context, sampleCode string) (int, error)
	Update(ctx context.Context, rule SortingRule) error
	Delete(ctx context.Context, sortingRuleIDs []uuid.UUID) error

	CreateTransaction() (db.DbConnection, error)
	WithTransaction(tx db.DbConnection) SortingRuleRepository
}

type sortingRuleRepository struct {
	db       db.DbConnection
	dbSchema string
}

func (r *sortingRuleRepository) Upsert(ctx context.Context, sortingRule SortingRule) (uuid.UUID, error) {
	if sortingRule.ID == uuid.Nil {
		sortingRule.ID = uuid.New()
	}

	query := fmt.Sprintf(`INSERT INTO %s.sk_sorting_rules (id, instrument_id, priority, target, condition_id, programme)
		VALUES (:id, :instrument_id, :priority, :target, :condition_id, :programme)
		ON CONFLICT (id) DO UPDATE SET instrument_id = EXCLUDED.instrument_id, priority = EXCLUDED.priority,
		target = EXCLUDED.target, condition_id = EXCLUDED.condition_id, programme = EXCLUDED.programme;`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertSortingRuleToDAO(sortingRule))
	if err != nil {
		log.Error().Err(err).Msg(msgCreateSortingRuleFailed)
		return uuid.Nil, ErrCreateSortingRuleFailed
	}

	return sortingRule.ID, nil
}

func (r *sortingRuleRepository) GetById(ctx context.Context, id uuid.UUID) (*SortingRule, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_sorting_rules WHERE id = $1 AND deleted_at IS NULL;`, r.dbSchema)
	var dao sortingRuleDAO
	err := r.db.QueryRowxContext(ctx, query, id).StructScan(&dao)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		log.Error().Err(err).Msg(msgGetSortingRuleByIDFailed)
		return nil, ErrGetSortingRuleByIDFailed
	}

	sortingRule := convertDAOToSortingRule(dao)
	return &sortingRule, nil
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
		if _, ok := sortingRulesMap[dao.InstrumentID]; !ok {
			sortingRulesMap[dao.InstrumentID] = make([]SortingRule, 0)
		}
		sortingRulesMap[dao.InstrumentID] = append(sortingRulesMap[dao.InstrumentID], convertDAOToSortingRule(dao))
	}

	return sortingRulesMap, nil
}

func (r *sortingRuleRepository) GetByInstrumentIDAndProgramme(ctx context.Context, instrumentID uuid.UUID, programme string) ([]SortingRule, error) {
	query := fmt.Sprintf("SELECT * FROM %s.sk_sorting_rules WHERE deleted_at IS NULL AND instrument_id = $1 AND programme = $2 ORDER BY priority ASC;", r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query, instrumentID, programme)
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

func (r *sortingRuleRepository) ApplySortingRuleTarget(ctx context.Context, instrumentID uuid.UUID, programme, sampleCode, target string, validUntil time.Time) error {
	preparedValues := map[string]any{
		"instrument_id": instrumentID,
		"programme":     programme,
		"sample_code":   sampleCode,
		"target":        sql.NullString{String: target, Valid: len(target) > 0},
		"valid_until":   validUntil,
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_applied_sorting_rule_targets (
									instrument_id, sample_code, target, programme, valid_until) 
									VALUES (:instrument_id, :sample_code, :target, :programme, :valid_until)`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, preparedValues)
	if err != nil {
		log.Error().Err(err).Msg(msgApplySortingRuleTargetFailed)
		return ErrApplySortingRuleTargetFailed
	}

	return nil
}

func (r *sortingRuleRepository) GetSampleSequenceNumber(ctx context.Context, sampleCode string) (int, error) {
	query := strings.ReplaceAll(`SELECT COUNT(DISTINCT sasrt.sample_code)+1 FROM %s.sk_applied_sorting_rule_targets sasrt WHERE
    sasrt.created_at < (SELECT COALESCE(MIN(created_at), timezone('utc', now())) FROM %s.sk_applied_sorting_rule_targets sasrt WHERE sample_code = $1) AND sasrt.sample_code IN
    (
    SELECT DISTINCT sample_code from %s.sk_analysis_requests sar
        INNER JOIN %s.sk_analysis_request_extra_values sarev ON sar.id = sarev.analysis_request_id
            WHERE valid_until_time >= timezone('utc', now())
            AND sarev.key = 'OrderID'
            AND sample_code <> $1
            AND sarev.value = (SELECT sarev2.value FROM %s.sk_analysis_requests sar2
                                INNER JOIN %s.sk_analysis_request_extra_values sarev2 ON sar2.id = sarev2.analysis_request_id
                                    WHERE sarev2.key = 'OrderID' and sar2.sample_code = $1)
    );`, "%s", r.dbSchema)

	rows, err := r.db.QueryxContext(ctx, query, sampleCode)
	if err != nil {
		log.Error().Err(err).Msg(msgGetSampleSequenceNumberFailed)
		return 0, ErrGetSampleSequenceNumberFailed
	}
	defer rows.Close()

	sequenceNumber := 1
	if rows.Next() {
		err := rows.Scan(&sequenceNumber)
		if err != nil {
			log.Error().Err(err).Msg(msgGetSampleSequenceNumberFailed)
			return 0, ErrGetSampleSequenceNumberFailed
		}
	}

	return sequenceNumber, nil
}
func (r *sortingRuleRepository) GetAppliedSortingRuleTargets(ctx context.Context, instrumentID uuid.UUID, programme string, sampleCodes []string) ([]string, error) {
	if len(sampleCodes) == 0 {
		return nil, nil
	}
	query := fmt.Sprintf(`SELECT DISTINCT target FROM %s.sk_applied_sorting_rule_targets 
				WHERE instrument_id = ? AND sample_code IN (?) AND valid_until >= timezone('utc', NOW()) AND programme = ?`, r.dbSchema)
	query, args, _ := sqlx.In(query, instrumentID, sampleCodes, programme)
	query = r.db.Rebind(query)
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgGetAppliedSortingRuleTargetsFailed)
		return nil, ErrGetAppliedSortingRuleTargetsFailed
	}
	defer rows.Close()

	appliedSortingRuleTargets := make([]string, 0)
	for rows.Next() {
		var target string
		err = rows.Scan(&target)
		if err != nil {
			log.Error().Err(err).Msg(msgGetAppliedSortingRuleTargetsFailed)
			return nil, ErrGetAppliedSortingRuleTargetsFailed
		}
		appliedSortingRuleTargets = append(appliedSortingRuleTargets, target)
	}

	return appliedSortingRuleTargets, nil
}

func (r *sortingRuleRepository) Update(ctx context.Context, rule SortingRule) error {
	query := fmt.Sprintf("UPDATE %s.sk_sorting_rules "+
		"SET condition_id = :condition_id, target = :target, programme = :programme, priority = :priority, modified_at = timezone('utc', now()) WHERE id = :id", r.dbSchema)
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

func (r *sortingRuleRepository) CreateTransaction() (db.DbConnection, error) {
	return r.db.CreateTransactionConnector()
}

func (r *sortingRuleRepository) WithTransaction(tx db.DbConnection) SortingRuleRepository {
	if tx == nil {
		return r
	}
	txRepo := *r
	txRepo.db = tx
	return &txRepo
}
func NewSortingRuleRepository(db db.DbConnection, dbSchema string) SortingRuleRepository {
	return &sortingRuleRepository{
		db:       db,
		dbSchema: dbSchema,
	}
}

type sortingRuleDAO struct {
	ID           uuid.UUID     `db:"id"`
	InstrumentID uuid.UUID     `db:"instrument_id"`
	Priority     int           `db:"priority"`
	Target       string        `db:"target"`
	ConditionID  uuid.NullUUID `db:"condition_id"`
	Programme    string        `db:"programme"`
	CreatedAt    time.Time     `db:"created_at"`
	ModifiedAt   sql.NullTime  `db:"modified_at"`
	DeletedAt    sql.NullTime  `db:"deleted_at"`
}

func convertSortingRuleToDAO(sortingRule SortingRule) sortingRuleDAO {
	dao := sortingRuleDAO{
		ID:           sortingRule.ID,
		InstrumentID: sortingRule.InstrumentID,
		Target:       sortingRule.Target,
		Priority:     sortingRule.Priority,
		Programme:    sortingRule.Programme,
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
		Programme:    dao.Programme,
	}

	if dao.ConditionID.Valid {
		sortingRule.Condition = &Condition{
			ID: dao.ConditionID.UUID,
		}
	}

	return sortingRule
}

const (
	msgApplySortingRuleTargetFailed           = "apply sorting rule target failed"
	msgCreateSortingRuleFailed                = "create sorting rule failed"
	msgGetSortingRuleByInstrumentIDFailed     = "get sorting rule by instrument ID failed"
	msgGetAppliedSortingRuleTargetsFailed     = "get applied sorting rule targets failed"
	msgGetSampleSequenceNumberFailed          = "get sample sequence number failed"
	msgUpdateSortingRuleFailed                = "update sorting rule failed"
	msgDeleteSortingRulesFailed               = "delete sorting rules failed"
	msgGetSortingRuleByIDFailed               = "get sorting rule by ID failed"
	msgRequiredSortingRuleTransactionNotFound = "required transaction not found when handling sorting rules"
)

var (
	ErrApplySortingRuleTargetFailed             = errors.New(msgApplySortingRuleTargetFailed)
	ErrCreateSortingRuleFailed                  = errors.New(msgCreateSortingRuleFailed)
	ErrGetSortingRuleByInstrumentIDFailed       = errors.New(msgGetSortingRuleByInstrumentIDFailed)
	ErrGetAppliedSortingRuleTargetsFailed       = errors.New(msgGetAppliedSortingRuleTargetsFailed)
	ErrGetSampleSequenceNumberFailed            = errors.New(msgGetSampleSequenceNumberFailed)
	ErrUpdateSortingRuleFailed                  = errors.New(msgUpdateSortingRuleFailed)
	ErrDeleteSortingRulesFailed                 = errors.New(msgDeleteSortingRulesFailed)
	ErrGetSortingRuleByIDFailed                 = errors.New(msgGetSortingRuleByIDFailed)
	ErrorRequiredSortingRuleTransactionNotFound = errors.New(msgRequiredSortingRuleTransactionNotFound)
)
