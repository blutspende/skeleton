package skeleton

import (
	"context"
	"github.com/blutspende/skeleton/db"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"math/rand/v2"
	"regexp"
	"strconv"
	"strings"
)

type ConditionService interface {
	UpsertConditionWithTx(ctx context.Context, condition Condition) (uuid.UUID, error)
	GetCondition(ctx context.Context, id uuid.UUID) (Condition, error)
	DeleteConditionWithTx(ctx context.Context, id uuid.UUID) error
	WithTransaction(tx db.DbConnection) ConditionService
}

type conditionService struct {
	conditionRepository ConditionRepository
	externalTx          db.DbConnection
}

func (s *conditionService) UpsertConditionWithTx(ctx context.Context, condition Condition) (uuid.UUID, error) {
	tx := s.getTransaction()
	if tx == nil {
		log.Error().Msg(msgRequiredConditionTransactionNotFound)
		return uuid.Nil, ErrorRequiredConditionTransactionNotFound
	}

	id, err := s.upsertConditionWithTx(ctx, tx, condition)
	if err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

func (s *conditionService) upsertConditionWithTx(ctx context.Context, tx db.DbConnection, condition Condition) (uuid.UUID, error) {
	if condition.SubCondition1 != nil {
		subCondition1ID, err := s.upsertConditionWithTx(ctx, tx, *condition.SubCondition1)
		if err != nil {
			return uuid.Nil, err
		}
		condition.SubCondition1.ID = subCondition1ID
	}
	if condition.SubCondition2 != nil {
		subCondition2ID, err := s.upsertConditionWithTx(ctx, tx, *condition.SubCondition2)
		if err != nil {
			return uuid.Nil, err
		}
		condition.SubCondition2.ID = subCondition2ID
	}

	if condition.Operand1 != nil {
		operand1ID, err := s.conditionRepository.WithTransaction(tx).UpsertConditionOperand(ctx, *condition.Operand1)
		if err != nil {
			return uuid.Nil, err
		}
		condition.Operand1.ID = operand1ID
	}
	if condition.Operand2 != nil {
		operand2ID, err := s.conditionRepository.WithTransaction(tx).UpsertConditionOperand(ctx, *condition.Operand2)
		if err != nil {
			return uuid.Nil, err
		}
		condition.Operand2.ID = operand2ID
	}

	return s.conditionRepository.WithTransaction(tx).UpsertCondition(ctx, condition)
}

func (s *conditionService) GetCondition(ctx context.Context, id uuid.UUID) (condition Condition, err error) {
	tx := s.getTransaction()

	condition, err = s.getConditionRecursively(ctx, tx, id)
	if err != nil {
		return condition, err
	}

	return condition, nil
}

func (s *conditionService) getConditionRecursively(ctx context.Context, tx db.DbConnection, id uuid.UUID) (Condition, error) {
	condition, err := s.conditionRepository.WithTransaction(tx).GetConditionByID(ctx, id)
	if err != nil {
		return Condition{}, err
	}

	var subCondition1, subCondition2 Condition
	if condition.SubCondition1 != nil {
		subCondition1, err = s.getConditionRecursively(ctx, tx, condition.SubCondition1.ID)
		if err != nil {
			return Condition{}, err
		}
		condition.SubCondition1 = &subCondition1
	}
	if condition.SubCondition2 != nil {
		subCondition2, err = s.getConditionRecursively(ctx, tx, condition.SubCondition2.ID)
		if err != nil {
			return Condition{}, err
		}
		condition.SubCondition2 = &subCondition2
	}

	var operand1, operand2 ConditionOperand
	if condition.Operand1 != nil {
		operand1, err = s.conditionRepository.WithTransaction(tx).GetConditionOperandByID(ctx, condition.Operand1.ID)
		if err != nil {
			return Condition{}, err
		}
		condition.Operand1 = &operand1
	}
	if condition.Operand2 != nil {
		operand2, err = s.conditionRepository.WithTransaction(tx).GetConditionOperandByID(ctx, condition.Operand2.ID)
		if err != nil {
			return condition, err
		}

		condition.Operand2 = &operand2
	}

	return condition, nil
}

func ConditionHasOperator(condition *Condition, operators ...ConditionOperator) bool {
	if condition == nil {
		return false
	}
	for _, operator := range operators {
		if condition.Operator == operator {
			return true
		}
	}
	if ConditionHasOperator(condition.SubCondition1, operators...) {
		return true
	}
	if ConditionHasOperator(condition.SubCondition2, operators...) {
		return true
	}

	return false
}

func (s *conditionService) DeleteConditionWithTx(ctx context.Context, id uuid.UUID) error {
	tx := s.getTransaction()
	if tx == nil {
		log.Error().Msg(msgRequiredConditionTransactionNotFound)
		return ErrorRequiredConditionTransactionNotFound
	}

	err := s.deleteConditionWithTx(ctx, tx, id)

	if err != nil {
		return err
	}

	return nil
}

func (s *conditionService) deleteConditionWithTx(ctx context.Context, tx db.DbConnection, id uuid.UUID) error {
	conditionTxRepo := s.conditionRepository.WithTransaction(tx)
	err := s.deleteConditionRecursively(ctx, conditionTxRepo, id)
	if err != nil {
		return err
	}
	return nil
}

func (s *conditionService) deleteConditionRecursively(ctx context.Context, conditionTxRepo ConditionRepository, id uuid.UUID) error {
	isReferenced, err := conditionTxRepo.IsConditionReferenced(ctx, id)
	if err != nil {
		return err
	}
	if isReferenced {
		return nil
	}

	condition, err := conditionTxRepo.GetConditionByID(ctx, id)
	if err != nil {
		return err
	}

	if condition.SubCondition1 != nil {
		err = s.deleteConditionRecursively(ctx, conditionTxRepo, condition.SubCondition1.ID)
		if err != nil {
			return err
		}
	}

	if condition.SubCondition2 != nil {
		err = s.deleteConditionRecursively(ctx, conditionTxRepo, condition.SubCondition2.ID)
		if err != nil {
			return err
		}
	}

	if condition.Operand1 != nil {
		isReferenced, err = conditionTxRepo.IsConditionOperandReferenced(ctx, condition.Operand1.ID)
		if err != nil {
			return err
		}
		if !isReferenced {
			err = conditionTxRepo.DeleteConditionOperand(ctx, condition.Operand1.ID)
			if err != nil {
				return err
			}
		}
	}

	if condition.Operand2 != nil {
		isReferenced, err = conditionTxRepo.IsConditionOperandReferenced(ctx, condition.Operand2.ID)
		if err != nil {
			return err
		}
		if !isReferenced {
			err = conditionTxRepo.DeleteConditionOperand(ctx, condition.Operand2.ID)
			if err != nil {
				return err
			}
		}
	}

	return conditionTxRepo.DeleteCondition(ctx, id)
}

func (s *conditionService) getTransaction() db.DbConnection {
	return s.externalTx
}

func (s *conditionService) WithTransaction(tx db.DbConnection) ConditionService {
	if tx == nil {
		return s
	}
	txSvc := *s
	txSvc.externalTx = tx
	return &txSvc
}

func NewConditionService(conditionRepository ConditionRepository) ConditionService {
	return &conditionService{
		conditionRepository: conditionRepository,
	}
}

type ConditionEvaluatorFunc func(operand1AnalysisRequest AnalysisRequest, operand2AnalysisRequest AnalysisRequest, relatedAnalysisRequests []AnalysisRequest) (bool, error)

func NewConditionEvaluator(condition Condition, appliedTargets []string, sampleSequenceNumber int) (ConditionEvaluatorFunc, []ConditionError) {
	index := 0
	conditionErrors := make([]ConditionError, 0)
	return createConditionEvaluator(condition, &index, &conditionErrors, appliedTargets, sampleSequenceNumber), conditionErrors
}

func createConditionEvaluator(condition Condition, index *int, conditionErrors *[]ConditionError, appliedTargets []string, sampleSequenceNumber int) ConditionEvaluatorFunc {
	var evalFunc ConditionEvaluatorFunc
	switch condition.Operator {
	case And, Or:
		if condition.SubCondition1 == nil {
			*conditionErrors = append(*conditionErrors, ConditionError{
				ConditionNodeIndex: *index,
				Error:              ErrSubCondition1Required,
			})
			return nil
		}

		if condition.SubCondition2 == nil {
			*conditionErrors = append(*conditionErrors, ConditionError{
				ConditionNodeIndex: *index,
				Error:              ErrSubCondition2Required,
			})
			return nil
		}
	case Greater, GreaterOrEqual, Less, LessOrEqual:
		if condition.Operand2.Type == Constant {
			if condition.Operand2.ConstantValue == nil {
				*conditionErrors = append(*conditionErrors, ConditionError{
					ConditionNodeIndex: *index,
					Error:              ErrOperand2NotDecimal,
				})
				return nil
			}
			if _, err := decimal.NewFromString(*condition.Operand2.ConstantValue); err != nil {
				*conditionErrors = append(*conditionErrors, ConditionError{
					ConditionNodeIndex: *index,
					Error:              ErrOperand2NotDecimal,
				})
				return nil
			}
		}
	case MatchRegex:
		if condition.Operand2.Type == Constant {
			if condition.Operand2.ConstantValue == nil {
				*conditionErrors = append(*conditionErrors, ConditionError{
					ConditionNodeIndex: *index,
					Error:              ErrOperand2NotRegex,
				})
				return nil
			}
			if _, err := regexp.Compile(*condition.Operand2.ConstantValue); err != nil {
				*conditionErrors = append(*conditionErrors, ConditionError{
					ConditionNodeIndex: *index,
					Error:              ErrOperand2NotRegex,
				})
				return nil
			}
		}
	case Exists:
	case NotExists:
		if condition.Operand1.Type == Constant ||
			condition.Operand1.Type == Analyte ||
			condition.Operand1.Type == Laboratory ||
			condition.Operand1.Type == SampleCode ||
			condition.Operand1.Type == Order {
			*conditionErrors = append(*conditionErrors, ConditionError{
				ConditionNodeIndex: *index,
				Error:              ErrInvalidOperandForExistOrNotExist,
			})
			return nil
		}
	case MatchAny, MatchAll:
		if condition.SubCondition2 == nil {
			*conditionErrors = append(*conditionErrors, ConditionError{
				ConditionNodeIndex: *index,
				Error:              ErrOperand2Missing,
			})
			return nil
		}
	}

	if condition.Operand1 != nil &&
		condition.Operand1.ExtraValueKey == nil &&
		condition.Operand1.Type == AnalysisRequestExtraValue {
		*conditionErrors = append(*conditionErrors, ConditionError{
			ConditionNodeIndex: *index,
			Error:              ErrMissingExtraValueKey,
		})
		return nil
	}

	switch condition.Operator {
	case And, Or:
		evalFunc = createLogicConditionEvaluator(condition, index, conditionErrors, appliedTargets, sampleSequenceNumber)
	case Contains:
		evalFunc = createContainsConditionEvaluator(condition)
	case NotContains:
		evalFunc = createNotContainsConditionEvaluator(condition)
	case Equals:
		evalFunc = createEqualsConditionEvaluator(condition)
	case NotEquals:
		evalFunc = createNotEqualsConditionEvaluator(condition)
	case Greater,
		GreaterOrEqual,
		Less,
		LessOrEqual:
		evalFunc = createArithmeticConditionEvaluator(condition)
	case MatchRegex:
		evalFunc = createMatchRegexCondition(condition)
	case Exists:
		evalFunc = createExistsCondition(condition)
	case NotExists:
		evalFunc = createNotExistsCondition(condition)
	case TargetApplied:
		evalFunc = createTargetAppliedCondition(condition, appliedTargets)
	case TargetNotApplied:
		evalFunc = createTargetNotAppliedCondition(condition, appliedTargets)
	case MatchAll:
		evalFunc = createMatchAllCondition(condition, index, conditionErrors, appliedTargets, sampleSequenceNumber)
	case MatchAny:
		evalFunc = createMatchAnyCondition(condition, index, conditionErrors, appliedTargets, sampleSequenceNumber)
	case IsNthSample:
		evalFunc = createIsNthSampleCondition(condition, sampleSequenceNumber)
	case HasNPercentProbability:
		evalFunc = createHasNPercentCondition(condition)
	case Default:
		evalFunc = func(operand1AnalysisRequest AnalysisRequest, operand2AnalysisRequest AnalysisRequest, relatedAnalysisRequests []AnalysisRequest) (bool, error) {
			return true, nil
		}
	}
	return evalFunc
}

func createLogicConditionEvaluator(condition Condition, index *int, checkRuleConditionErrors *[]ConditionError, appliedTargets []string, sampleSequenceNumber int) ConditionEvaluatorFunc {
	*index = *index + 1
	subCondition1Evaluator := createConditionEvaluator(*condition.SubCondition1, index, checkRuleConditionErrors, appliedTargets, sampleSequenceNumber)

	*index = *index + 1
	subCondition2Evaluator := createConditionEvaluator(*condition.SubCondition2, index, checkRuleConditionErrors, appliedTargets, sampleSequenceNumber)

	return func(operand1AnalysisRequest AnalysisRequest, operand2AnalysisRequest AnalysisRequest, relatedAnalysisRequests []AnalysisRequest) (bool, error) {
		subcondition1Evaluation, err := subCondition1Evaluator(operand1AnalysisRequest, operand2AnalysisRequest, relatedAnalysisRequests)

		if err != nil {
			return false, err
		}

		if condition.Operator == And && !subcondition1Evaluation {
			return false, nil
		}

		subcondition2Evaluation, err := subCondition2Evaluator(operand1AnalysisRequest, operand2AnalysisRequest, relatedAnalysisRequests)
		if err != nil {
			return false, err
		}

		if condition.Operator == And {
			return subcondition1Evaluation && subcondition2Evaluation, nil
		} else if condition.Operator == Or {
			return subcondition1Evaluation || subcondition2Evaluation, nil
		}
		return false, ErrInvalidOperatorForLogicFilter
	}
}

func createContainsConditionEvaluator(condition Condition) ConditionEvaluatorFunc {
	return func(operand1AnalysisRequest AnalysisRequest, operand2AnalysisRequest AnalysisRequest, relatedAnalysisRequests []AnalysisRequest) (bool, error) {
		operand1Value, _ := getFieldValueByOperand(operand1AnalysisRequest, *condition.Operand1)
		operand2Value, _ := getFieldValueByOperand(operand2AnalysisRequest, *condition.Operand2)
		return strings.Contains(operand1Value, operand2Value), nil
	}
}

func createNotContainsConditionEvaluator(condition Condition) ConditionEvaluatorFunc {
	containsEvalFunc := createContainsConditionEvaluator(condition)
	return func(operand1AnalysisRequest AnalysisRequest, operand2AnalysisRequest AnalysisRequest, relatedAnalysisRequests []AnalysisRequest) (bool, error) {
		res, err := containsEvalFunc(operand1AnalysisRequest, operand2AnalysisRequest, relatedAnalysisRequests)
		return !res, err
	}
}

func createEqualsConditionEvaluator(condition Condition) ConditionEvaluatorFunc {
	return func(operand1AnalysisRequest AnalysisRequest, operand2AnalysisRequest AnalysisRequest, relatedAnalysisRequests []AnalysisRequest) (bool, error) {
		var operand1Value, operand2Value string
		if condition.Operand1 != nil {
			operand1Value, _ = getFieldValueByOperand(operand1AnalysisRequest, *condition.Operand1)
		} else {
			return false, ErrOperand1Missing
		}
		if condition.Operand2 != nil {
			operand2Value, _ = getFieldValueByOperand(operand2AnalysisRequest, *condition.Operand2)
		} else {
			return false, ErrOperand2Missing
		}

		//Bounded decimals
		if (strings.HasPrefix(operand1Value, "<") || strings.HasPrefix(operand1Value, ">")) &&
			(!strings.HasPrefix(operand2Value, "<") && !strings.HasPrefix(operand2Value, ">")) {
			return false, nil
		}

		return strings.ToLower(strings.TrimSpace(operand1Value)) == strings.ToLower(strings.TrimSpace(operand2Value)), nil
	}
}

func createNotEqualsConditionEvaluator(condition Condition) ConditionEvaluatorFunc {
	equalsEvalFunc := createEqualsConditionEvaluator(condition)
	return func(operand1AnalysisRequest AnalysisRequest, operand2AnalysisRequest AnalysisRequest, relatedAnalysisRequests []AnalysisRequest) (bool, error) {
		res, err := equalsEvalFunc(operand1AnalysisRequest, operand2AnalysisRequest, relatedAnalysisRequests)
		return !res, err
	}
}

func createMatchRegexCondition(condition Condition) ConditionEvaluatorFunc {
	return func(operand1AnalysisRequest AnalysisRequest, operand2AnalysisRequest AnalysisRequest, relatedAnalysisRequests []AnalysisRequest) (bool, error) {
		var operand1Value, operand2Value string
		if condition.Operand1 != nil {
			operand1Value, _ = getFieldValueByOperand(operand1AnalysisRequest, *condition.Operand1)
		} else {
			return false, ErrOperand1Missing
		}
		if condition.Operand2 != nil {
			operand2Value, _ = getFieldValueByOperand(operand2AnalysisRequest, *condition.Operand2)
		} else {
			return false, ErrOperand2Missing
		}
		matched, err := regexp.MatchString(operand2Value, operand1Value)
		if err != nil {
			return false, err
		}
		return matched, nil
	}
}

func createArithmeticConditionEvaluator(condition Condition) ConditionEvaluatorFunc {
	return func(operand1AnalysisRequest AnalysisRequest, operand2AnalysisRequest AnalysisRequest, relatedAnalysisRequests []AnalysisRequest) (bool, error) {
		var operand1Str, operand2Str string
		var err error
		if condition.Operand1 != nil {
			operand1Str, _ = getFieldValueByOperand(operand1AnalysisRequest, *condition.Operand1)
		} else {
			return false, ErrOperand1Missing
		}
		if condition.Operand2 != nil {
			operand2Str, _ = getFieldValueByOperand(operand2AnalysisRequest, *condition.Operand2)
		} else {
			return false, ErrOperand2Missing
		}

		lowerBound := false
		upperBound := false
		if strings.HasPrefix(operand1Str, "<") {
			lowerBound = true
			operand1Str = operand1Str[1:]
		} else if strings.HasPrefix(operand1Str, ">") {
			upperBound = true
			operand1Str = operand1Str[1:]
		}
		operand1Str = strings.ReplaceAll(operand1Str, ",", ".")
		operand2Str = strings.ReplaceAll(operand2Str, ",", ".")
		operand1Value, err := decimal.NewFromString(operand1Str)
		if err != nil {
			log.Error().Err(err).Str("value", operand1Str).Msg(msgDecimalFieldValueParseFailed)
			return false, ErrDecimalFieldValueParseFailed
		}

		operand2Value, err := decimal.NewFromString(operand2Str)
		if err != nil {
			log.Error().Err(err).Str("value", operand2Str).Msg(msgDecimalFieldValueParseFailed)
			return false, ErrDecimalFieldValueParseFailed
		}

		if lowerBound {
			switch condition.Operator {
			case Less, LessOrEqual:
				if operand1Value.Equal(operand2Value) {
					return true, nil
				} else if operand1Value.GreaterThan(operand2Value) {
					return false, ErrUndecidableBoundedDecimalArithmeticOperation
				}
			case Greater, GreaterOrEqual:
				if operand1Value.GreaterThan(operand2Value) {
					return false, ErrUndecidableBoundedDecimalArithmeticOperation
				}
				return false, nil
			case Equals:
				return false, nil
			}
		} else if upperBound {
			switch condition.Operator {
			case Less, LessOrEqual:
				if operand1Value.LessThan(operand2Value) {
					return false, ErrUndecidableBoundedDecimalArithmeticOperation
				}
				return false, nil
			case Greater, GreaterOrEqual:
				if operand1Value.Equal(operand2Value) {
					return true, nil
				} else if operand1Value.LessThan(operand2Value) {
					return false, ErrUndecidableBoundedDecimalArithmeticOperation
				}
			case Equals:
				return false, nil
			}
		}

		switch condition.Operator {
		case Greater:
			return operand1Value.GreaterThan(operand2Value), nil
		case GreaterOrEqual:
			return operand1Value.GreaterThanOrEqual(operand2Value), nil
		case Less:
			return operand1Value.LessThan(operand2Value), nil
		case LessOrEqual:
			return operand1Value.LessThanOrEqual(operand2Value), nil
		default:
			return false, ErrInvalidOperatorForArithmeticFilter
		}
	}
}

func createExistsCondition(condition Condition) ConditionEvaluatorFunc {
	return func(operand1AnalysisRequest AnalysisRequest, operand2AnalysisRequest AnalysisRequest, relatedAnalysisRequests []AnalysisRequest) (bool, error) {
		var err error
		if condition.Operand1 != nil {
			_, err = getFieldValueByOperand(operand1AnalysisRequest, *condition.Operand1)
			if err == ErrAnalysisRequestExtraValueNotFound || err == ErrConditionOperandNotFound {
				return false, nil
			}
			return true, nil
		} else {
			return false, ErrOperand1Missing
		}
	}
}

func createTargetAppliedCondition(condition Condition, appliedTargets []string) ConditionEvaluatorFunc {
	return func(operand1AnalysisRequest AnalysisRequest, operand2AnalysisRequest AnalysisRequest, relatedAnalysisRequests []AnalysisRequest) (bool, error) {
		var err error
		var value string
		if condition.Operand2 != nil {
			value, err = getFieldValueByOperand(operand1AnalysisRequest, *condition.Operand2)
			if err == ErrConditionOperandNotFound {
				return false, ErrOperand1Missing
			}
			for _, appliedTarget := range appliedTargets {
				if appliedTarget == value {
					return true, nil
				}
			}
			return false, nil
		} else {
			return false, ErrOperand2Missing
		}
	}
}

func createTargetNotAppliedCondition(condition Condition, appliedTargets []string) ConditionEvaluatorFunc {
	return func(operand1AnalysisRequest AnalysisRequest, operand2AnalysisRequest AnalysisRequest, relatedAnalysisRequests []AnalysisRequest) (bool, error) {
		evalFunc := createTargetAppliedCondition(condition, appliedTargets)
		res, err := evalFunc(operand1AnalysisRequest, operand2AnalysisRequest, relatedAnalysisRequests)
		return !res, err
	}
}

func createIsNthSampleCondition(condition Condition, sampleSequenceNumber int) ConditionEvaluatorFunc {
	return func(operand1AnalysisRequest AnalysisRequest, operand2AnalysisRequest AnalysisRequest, relatedAnalysisRequests []AnalysisRequest) (bool, error) {
		var err error
		var value string
		if condition.Operand2 != nil {
			value, err = getFieldValueByOperand(operand1AnalysisRequest, *condition.Operand2)
			if err == ErrConditionOperandNotFound {
				return false, ErrOperand2Missing
			}
			valueNum, err := strconv.Atoi(value)
			if err != nil {
				return false, ErrInvalidValueForNthSample
			}
			return sampleSequenceNumber == valueNum, nil
		} else {
			return false, ErrOperand2Missing
		}
	}
}

func createHasNPercentCondition(condition Condition) ConditionEvaluatorFunc {
	return func(operand1AnalysisRequest AnalysisRequest, operand2AnalysisRequest AnalysisRequest, relatedAnalysisRequests []AnalysisRequest) (bool, error) {
		var err error
		var value string
		if condition.Operand2 != nil {
			value, err = getFieldValueByOperand(operand1AnalysisRequest, *condition.Operand2)
			if err == ErrConditionOperandNotFound {
				return false, ErrOperand2Missing
			}
			valueNum, err := strconv.Atoi(value)
			if err != nil {
				return false, ErrInvalidValueForNPercentChance
			}
			return rand.IntN(100) < valueNum, nil
		} else {
			return false, ErrOperand2Missing
		}
	}
}

func createNotExistsCondition(condition Condition) ConditionEvaluatorFunc {
	return func(operand1AnalysisRequest AnalysisRequest, operand2AnalysisRequest AnalysisRequest, relatedAnalysisRequests []AnalysisRequest) (bool, error) {
		_, err := getFieldValueByOperand(operand1AnalysisRequest, *condition.Operand1)
		return err != nil, nil
	}
}

func createMatchAnyCondition(condition Condition, index *int, checkRuleConditionErrors *[]ConditionError, appliedTargets []string, sampleSequenceNumber int) ConditionEvaluatorFunc {
	lambdaEvaluator := createConditionEvaluator(*condition.SubCondition2, index, checkRuleConditionErrors, appliedTargets, sampleSequenceNumber)
	if lambdaEvaluator == nil {
		return nil
	}

	return func(operand1AnalysisRequest AnalysisRequest, operand2AnalysisRequest AnalysisRequest, relatedAnalysisRequests []AnalysisRequest) (bool, error) {
		match, err := lambdaEvaluator(operand1AnalysisRequest, operand2AnalysisRequest, relatedAnalysisRequests)
		if err != nil {
			return false, err
		}
		if match {
			return true, nil
		}
		for _, ar := range relatedAnalysisRequests {
			match, err := lambdaEvaluator(ar, operand1AnalysisRequest, nil)
			if err != nil {
				return false, err
			}
			if match {
				return true, nil
			}
		}
		return false, nil
	}
}

func createMatchAllCondition(condition Condition, index *int, checkRuleConditionErrors *[]ConditionError, appliedTargets []string, sampleSequenceNumber int) ConditionEvaluatorFunc {
	lambdaEvaluator := createConditionEvaluator(*condition.SubCondition2, index, checkRuleConditionErrors, appliedTargets, sampleSequenceNumber)
	if lambdaEvaluator == nil {
		return nil
	}

	return func(operand1AnalysisRequest AnalysisRequest, operand2AnalysisRequest AnalysisRequest, relatedAnalysisRequests []AnalysisRequest) (bool, error) {
		match, err := lambdaEvaluator(operand1AnalysisRequest, operand2AnalysisRequest, relatedAnalysisRequests)
		if err != nil {
			return false, err
		}
		if !match {
			return false, nil
		}
		for _, wi := range relatedAnalysisRequests {
			match, err := lambdaEvaluator(wi, operand1AnalysisRequest, nil)
			if err != nil {
				return false, err
			}
			if !match {
				return false, nil
			}
		}
		return true, nil
	}
}

func getFieldValueByOperand(analysisRequest AnalysisRequest, operand ConditionOperand) (string, error) {
	switch operand.Type {
	case SampleCode:
		return analysisRequest.SampleCode, nil
	case AnalysisRequestExtraValue:
		for _, extraValue := range analysisRequest.ExtraValues {
			if extraValue.Key == *operand.ExtraValueKey {
				return extraValue.Value, nil
			}
		}
		return "", ErrAnalysisRequestExtraValueNotFound
	case Constant:
		if operand.ConstantValue != nil {
			return *operand.ConstantValue, nil
		}
		return "", nil
	case Analyte:
		return analysisRequest.AnalyteID.String(), nil
	case Laboratory:
		return analysisRequest.LaboratoryID.String(), nil
	default:
		return "", ErrConditionOperandNotFound
	}
}
