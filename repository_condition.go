package skeleton

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/blutspende/skeleton/db"
	"github.com/pkg/errors"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/google/uuid"
)

type ConditionRepository interface {
	UpsertCondition(ctx context.Context, condition Condition) (uuid.UUID, error)
	UpsertConditionOperand(ctx context.Context, condition ConditionOperand) (uuid.UUID, error)
	DeleteCondition(ctx context.Context, id uuid.UUID) error
	DeleteConditionOperand(ctx context.Context, id uuid.UUID) error
	GetConditionByID(ctx context.Context, id uuid.UUID) (Condition, error)
	GetConditionOperandByID(ctx context.Context, id uuid.UUID) (ConditionOperand, error)
	GetNamedConditions(ctx context.Context) ([]Condition, error)
	IsConditionReferenced(ctx context.Context, id uuid.UUID) (bool, error)
	IsConditionOperandReferenced(ctx context.Context, id uuid.UUID) (bool, error)
	UpdateCondition(ctx context.Context, condition Condition) error
	UpdateConditionOperand(ctx context.Context, condition ConditionOperand) error

	CreateTransaction() (db.DbConnector, error)
	WithTransaction(db db.DbConnector) ConditionRepository
}

type conditionRepository struct {
	db       db.DbConnector
	dbSchema string
}

func (r *conditionRepository) UpsertCondition(ctx context.Context, condition Condition) (uuid.UUID, error) {
	if condition.ID == uuid.Nil {
		condition.ID = uuid.New()
	}

	query := fmt.Sprintf(`INSERT INTO %s.sk_conditions(id, name, operator, subcondition_1_id, subcondition_2_id, negate_subcondition_1, negate_subcondition_2, operand_1_id, operand_2_id)
				VALUES (:id,:name,:operator,:subcondition_1_id,:subcondition_2_id,:negate_subcondition_1,:negate_subcondition_2,:operand_1_id,:operand_2_id)
				ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, operator = EXCLUDED.operator, subcondition_1_id = EXCLUDED.subcondition_1_id, subcondition_2_id = EXCLUDED.subcondition_2_id,
				negate_subcondition_1 = EXCLUDED.negate_subcondition_1, negate_subcondition_2 = EXCLUDED.negate_subcondition_2, operand_1_id = EXCLUDED.operand_1_id,
				operand_2_id = EXCLUDED.operand_2_id, modified_at = timezone('utc', now());`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertConditionToDAO(condition))
	if err != nil {
		log.Error().Err(err).Msg(msgCreateConditionFailed)
		return uuid.Nil, ErrCreateConditionFailed
	}
	return condition.ID, nil
}

func (r *conditionRepository) GetConditionByID(ctx context.Context, id uuid.UUID) (Condition, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_conditions where id = $1 AND deleted_at IS NULL;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query, id)
	if err != nil {
		log.Error().Err(err).Msg(msgGetConditionFailed)
		return Condition{}, ErrGetConditionFailed
	}
	defer rows.Close()
	if rows.Next() {
		var dao conditionDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetConditionFailed)
			return Condition{}, ErrGetConditionFailed
		}
		return convertDAOToCondition(dao), nil
	}
	return Condition{}, ErrConditionNotFound
}

func (r *conditionRepository) GetNamedConditions(ctx context.Context) ([]Condition, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_conditions where name IS NOT NULL AND deleted_at IS NULL ORDER BY name;`, r.dbSchema)
	conditions := make([]Condition, 0)
	rows, err := r.db.QueryxContext(ctx, query)
	if err != nil {
		log.Error().Err(err).Msg(msgGetNamedConditionsFailed)
		return nil, ErrGetNamedConditionsFailed
	}
	defer rows.Close()
	for rows.Next() {
		var dao conditionDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetNamedConditionsFailed)
			return nil, ErrGetNamedConditionsFailed
		}
		conditions = append(conditions, convertDAOToCondition(dao))
	}

	return conditions, nil
}

func (r *conditionRepository) UpdateCondition(ctx context.Context, condition Condition) error {
	query := fmt.Sprintf(`UPDATE %s.sk_conditions SET name = :name, operator = :operator, 
				subcondition_1_id = :subcondition_1_id, subcondition_2_id = :subcondition_2_id, 
				negate_subcondition_1 = :negate_subcondition_1, negate_subcondition_2 = :negate_subcondition_2, 
				operand_1_id = :operand_1_id, operand_2_id = :operand_2_id,  
				modified_at = timezone('utc', now()) 
				WHERE id = :id;`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertConditionToDAO(condition))
	if err != nil {
		log.Error().Err(err).Msg(msgUpdateConditionFailed)
		return ErrUpdateConditionFailed
	}
	return nil
}

func (r *conditionRepository) DeleteCondition(ctx context.Context, id uuid.UUID) error {
	query := fmt.Sprintf(`UPDATE %s.sk_conditions SET deleted_at = timezone('utc', now()) WHERE id = $1;`, r.dbSchema)
	_, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		log.Error().Err(err).Msg(msgDeleteConditionFailed)
		return ErrDeleteConditionFailed
	}
	return nil
}

func (r *conditionRepository) IsConditionReferenced(ctx context.Context, id uuid.UUID) (bool, error) {
	query := fmt.Sprintf(`SELECT id FROM %s.sk_conditions where subcondition_1_id = $1 OR subcondition_2_id = $1 AND deleted_at IS NULL LIMIT 1;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query, id)
	if err != nil {
		log.Error().Err(err).Msg(msgIsConditionReferencedFailed)
		return false, ErrIsConditionReferencedFailed
	}
	defer rows.Close()
	return rows.Next(), nil
}

func (r *conditionRepository) UpsertConditionOperand(ctx context.Context, conditionOperand ConditionOperand) (uuid.UUID, error) {
	if conditionOperand.ID == uuid.Nil {
		conditionOperand.ID = uuid.New()
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_condition_operands(id, name, type, constant_value, extra_value_key)
			VALUES (:id, :name, :type, :constant_value, :extra_value_key)
			ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, type = EXCLUDED.type, constant_value = EXCLUDED.constant_value, extra_value_key = EXCLUDED.extra_value_key, modified_at = timezone('utc', now());`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertConditionOperandToDAO(conditionOperand))
	if err != nil {
		log.Error().Err(err).Msg(msgCreateConditionOperandFailed)
		return uuid.Nil, ErrCreateConditionOperandFailed
	}
	return conditionOperand.ID, nil
}

func (r *conditionRepository) GetConditionOperandByID(ctx context.Context, id uuid.UUID) (ConditionOperand, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_condition_operands where id = $1 AND deleted_at IS NULL;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query, id)
	if err != nil {
		log.Error().Err(err).Msg(msgGetConditionOperandFailed)
		return ConditionOperand{}, ErrGetConditionOperandFailed
	}
	defer rows.Close()

	if rows.Next() {
		var dao conditionOperandDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetConditionOperandFailed)
			return ConditionOperand{}, ErrGetConditionOperandFailed
		}
		return convertDAOToConditionOperand(dao), nil
	}

	return ConditionOperand{}, ErrConditionOperandNotFound
}

func (r *conditionRepository) UpdateConditionOperand(ctx context.Context, conditionOperand ConditionOperand) error {
	query := fmt.Sprintf(`UPDATE %s.sk_condition_operands SET
				name = :name, type = :type, constant_value =:constant_value, extra_value_key = :extra_value_key, modified_at = timezone('utc', now()) WHERE id = :id;`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertConditionOperandToDAO(conditionOperand))
	if err != nil {
		log.Error().Err(err).Msg(msgUpdateConditionOperandFailed)
		return ErrUpdateConditionOperandFailed
	}
	return nil
}

func (r *conditionRepository) DeleteConditionOperand(ctx context.Context, id uuid.UUID) error {
	query := fmt.Sprintf(`UPDATE %s.sk_condition_operands SET deleted_at = timezone('utc', now()) WHERE id = $1;`, r.dbSchema)
	_, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		log.Error().Err(err).Msg(msgDeleteConditionOperandFailed)
		return ErrDeleteConditionOperandFailed
	}
	return nil
}

func (r *conditionRepository) IsConditionOperandReferenced(ctx context.Context, id uuid.UUID) (bool, error) {
	query := fmt.Sprintf(`SELECT id FROM %s.sk_conditions where operand_1_id = $1 OR operand_2_id = $1 AND deleted_at IS NULL LIMIT 1;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query, id)
	if err != nil {
		log.Error().Err(err).Msg(msgIsConditionOperandReferencedFailed)
		return false, ErrIsConditionOperandReferencedFailed
	}
	defer rows.Close()
	return rows.Next(), nil
}

type conditionOperandDAO struct {
	ID            uuid.UUID            `db:"id"`
	Name          sql.NullString       `db:"name"`
	Type          ConditionOperandType `db:"type"`
	ConstantValue sql.NullString       `db:"constant_value"`
	ExtraValueKey sql.NullString       `db:"extra_value_key"`
	CreatedAt     time.Time            `db:"created_at"`
	ModifiedAt    sql.NullTime         `db:"modified_at"`
	DeletedAt     sql.NullTime         `db:"deleted_at"`
}

func convertConditionOperandToDAO(conditionOperand ConditionOperand) conditionOperandDAO {
	dao := conditionOperandDAO{
		ID:   conditionOperand.ID,
		Type: conditionOperand.Type,
	}

	if conditionOperand.Name != nil {
		dao.Name = sql.NullString{
			String: *conditionOperand.Name,
			Valid:  len(*conditionOperand.Name) > 0,
		}
	}

	if conditionOperand.ConstantValue != nil {
		dao.ConstantValue = sql.NullString{
			String: *conditionOperand.ConstantValue,
			Valid:  len(*conditionOperand.ConstantValue) > 0,
		}
	}

	if conditionOperand.ExtraValueKey != nil {
		dao.ExtraValueKey = sql.NullString{
			String: *conditionOperand.ExtraValueKey,
			Valid:  len(*conditionOperand.ExtraValueKey) > 0,
		}
	}

	return dao
}

func convertDAOToConditionOperand(dao conditionOperandDAO) ConditionOperand {
	conditionOperand := ConditionOperand{
		ID:   dao.ID,
		Type: dao.Type,
	}

	if dao.Name.Valid {
		conditionOperand.Name = &dao.Name.String
	}
	if dao.ConstantValue.Valid {
		conditionOperand.ConstantValue = &dao.ConstantValue.String
	}
	if dao.ExtraValueKey.Valid {
		conditionOperand.ExtraValueKey = &dao.ExtraValueKey.String
	}

	return conditionOperand
}

func (r *conditionRepository) CreateTransaction() (db.DbConnector, error) {
	return r.db.CreateTransactionConnector()
}

func (r *conditionRepository) WithTransaction(tx db.DbConnector) ConditionRepository {
	if tx == nil {
		return r
	}
	txRepo := *r
	txRepo.db = tx
	return &txRepo
}

func NewConditionRepository(db db.DbConnector, dbSchema string) ConditionRepository {
	return &conditionRepository{
		db:       db,
		dbSchema: dbSchema,
	}
}

type conditionDAO struct {
	ID                  uuid.UUID         `db:"id"`
	Name                sql.NullString    `db:"name"`
	Operator            ConditionOperator `db:"operator"`
	SubCondition1ID     uuid.NullUUID     `db:"subcondition_1_id"`
	SubCondition2ID     uuid.NullUUID     `db:"subcondition_2_id"`
	NegateSubCondition1 bool              `db:"negate_subcondition_1"`
	NegateSubCondition2 bool              `db:"negate_subcondition_2"`
	Operand1ID          uuid.NullUUID     `db:"operand_1_id"`
	Operand2ID          uuid.NullUUID     `db:"operand_2_id"`
	CreatedAt           time.Time         `db:"created_at"`
	ModifiedAt          sql.NullTime      `db:"modified_at"`
	DeletedAt           sql.NullTime      `db:"deleted_at"`
}

func convertConditionToDAO(condition Condition) conditionDAO {
	dao := conditionDAO{
		ID:                  condition.ID,
		Operator:            condition.Operator,
		NegateSubCondition1: condition.NegateSubCondition1,
		NegateSubCondition2: condition.NegateSubCondition2,
	}
	if condition.Operand1 != nil {
		dao.Operand1ID = uuid.NullUUID{
			UUID:  condition.Operand1.ID,
			Valid: true,
		}
	}

	if condition.Operand2 != nil {
		dao.Operand2ID = uuid.NullUUID{
			UUID:  condition.Operand2.ID,
			Valid: true,
		}
	}

	if condition.SubCondition1 != nil {
		dao.SubCondition1ID = uuid.NullUUID{
			UUID:  condition.SubCondition1.ID,
			Valid: true,
		}
	}

	if condition.SubCondition2 != nil {
		dao.SubCondition2ID = uuid.NullUUID{
			UUID:  condition.SubCondition2.ID,
			Valid: true,
		}
	}

	if condition.Name != nil {
		dao.Name = sql.NullString{
			String: *condition.Name,
			Valid:  len(*condition.Name) > 0,
		}
	}

	return dao
}

func convertDAOToCondition(dao conditionDAO) Condition {
	condition := Condition{
		ID:                  dao.ID,
		Operator:            dao.Operator,
		NegateSubCondition1: dao.NegateSubCondition1,
		NegateSubCondition2: dao.NegateSubCondition2,
	}
	if dao.Name.Valid {
		condition.Name = &dao.Name.String
	}
	if dao.SubCondition1ID.Valid {
		condition.SubCondition1 = &Condition{
			ID: dao.SubCondition1ID.UUID,
		}
	}
	if dao.SubCondition2ID.Valid {
		condition.SubCondition2 = &Condition{
			ID: dao.SubCondition2ID.UUID,
		}
	}
	if dao.Operand1ID.Valid {
		condition.Operand1 = &ConditionOperand{
			ID: dao.Operand1ID.UUID,
		}
	}
	if dao.Operand2ID.Valid {
		condition.Operand2 = &ConditionOperand{
			ID: dao.Operand2ID.UUID,
		}
	}

	return condition
}

const (
	msgConditionNotFound                    = "condition not found"
	msgConditionOperandNotFound             = "condition operand not found"
	msgCreateConditionFailed                = "create condition failed"
	msgCreateConditionOperandFailed         = "create condition operand failed"
	msgDecimalFieldValueParseFailed         = "parse fieldvalue as a decimal number failed"
	msgDeleteConditionFailed                = "delete condition failed"
	msgDeleteConditionOperandFailed         = "delete condition operand failed"
	msgGetConditionFailed                   = "get condition failed"
	msgGetConditionOperandFailed            = "get condition operand failed"
	msgGetNamedConditionsFailed             = "get named condition failed"
	msgIsConditionOperandReferencedFailed   = "check if condition operand is referenced failed"
	msgIsConditionReferencedFailed          = "check if condition is referenced failed"
	msgUpdateConditionFailed                = "update condition failed"
	msgUpdateConditionOperandFailed         = "update condition operand failed"
	msgRequiredConditionTransactionNotFound = "required transaction not found when handling conditions"
)

var (
	ErrConditionNotFound                      = errors.New(msgConditionNotFound)
	ErrConditionOperandNotFound               = errors.New(msgConditionOperandNotFound)
	ErrCreateConditionFailed                  = errors.New(msgCreateConditionFailed)
	ErrCreateConditionOperandFailed           = errors.New(msgCreateConditionOperandFailed)
	ErrDeleteConditionFailed                  = errors.New(msgDeleteConditionFailed)
	ErrDeleteConditionOperandFailed           = errors.New(msgDeleteConditionOperandFailed)
	ErrGetConditionFailed                     = errors.New(msgGetConditionFailed)
	ErrGetConditionOperandFailed              = errors.New(msgGetConditionOperandFailed)
	ErrGetNamedConditionsFailed               = errors.New(msgGetNamedConditionsFailed)
	ErrIsConditionOperandReferencedFailed     = errors.New(msgIsConditionOperandReferencedFailed)
	ErrIsConditionReferencedFailed            = errors.New(msgIsConditionReferencedFailed)
	ErrUpdateConditionFailed                  = errors.New(msgUpdateConditionFailed)
	ErrUpdateConditionOperandFailed           = errors.New(msgUpdateConditionOperandFailed)
	ErrorRequiredConditionTransactionNotFound = errors.New(msgRequiredConditionTransactionNotFound)
)

const (
	msgInvalidConditionIDUpdate                     = "invalid condition id update"
	msgInvalidConditionOperandIDUpdate              = "invalid condition operand id update"
	msgInvalidOperandForExistOrNotExist             = "invalid operand for exists or not exists operator"
	msgInvalidOperatorForArithmeticFilter           = "invalid operator for arithmetic filter"
	msgInvalidOperatorForLogicFilter                = "invalid operator for logic filter"
	msgMissingExtraValueKey                         = "extravaluekey missing from condition"
	msgOperand1Missing                              = "operand 1 missing"
	msgOperand2Missing                              = "operand 2 missing"
	msgOperand2NotDecimal                           = "second operand must be a decimal number"
	msgOperand2NotRegex                             = "second operand must be a valid regex"
	msgSubCondition1Required                        = "first subcondition is required"
	msgSubCondition2Required                        = "second subcondition is required"
	msgUndecidableBoundedDecimalArithmeticOperation = "undecidable arithmetic operation on a bounded decimal value"
	msgAnalysisRequestExtraValueNotFound            = "extravalue not found for analysisRequest"
	msgInvalidValueForNthSample                     = "invalid value for 'n' in is nth sample operator"
	msgInvalidValueForNPercentChance                = "invalid value for 'n' in is nth sample operator"
)

var (
	ErrDecimalFieldValueParseFailed                 = errors.New(msgDecimalFieldValueParseFailed)
	ErrInvalidConditionIDUpdate                     = errors.New(msgInvalidConditionIDUpdate)
	ErrInvalidConditionOperandIDUpdate              = errors.New(msgInvalidConditionOperandIDUpdate)
	ErrInvalidOperandForExistOrNotExist             = errors.New(msgInvalidOperandForExistOrNotExist)
	ErrInvalidOperatorForArithmeticFilter           = errors.New(msgInvalidOperatorForArithmeticFilter)
	ErrInvalidOperatorForLogicFilter                = errors.New(msgInvalidOperatorForLogicFilter)
	ErrMissingExtraValueKey                         = errors.New(msgMissingExtraValueKey)
	ErrOperand1Missing                              = errors.New(msgOperand1Missing)
	ErrOperand2Missing                              = errors.New(msgOperand2Missing)
	ErrOperand2NotDecimal                           = errors.New(msgOperand2NotDecimal)
	ErrOperand2NotRegex                             = errors.New(msgOperand2NotRegex)
	ErrSubCondition1Required                        = errors.New(msgSubCondition1Required)
	ErrSubCondition2Required                        = errors.New(msgSubCondition2Required)
	ErrUndecidableBoundedDecimalArithmeticOperation = errors.New(msgUndecidableBoundedDecimalArithmeticOperation)
	ErrAnalysisRequestExtraValueNotFound            = errors.New(msgAnalysisRequestExtraValueNotFound)
	ErrInvalidValueForNthSample                     = errors.New(msgInvalidValueForNthSample)
	ErrInvalidValueForNPercentChance                = errors.New(msgInvalidValueForNPercentChance)
)
