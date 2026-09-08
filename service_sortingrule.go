package skeleton

import (
	"context"
	"fmt"
	"time"

	"github.com/blutspende/bloodlab-common/db"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type SortingRuleService interface {
	ApplySortingRuleTarget(ctx context.Context, instrumentID uuid.UUID, programme, sampleCode, target string, validUntil time.Time) error
	UpsertWithTx(ctx context.Context, sortingRule *SortingRule) error
	GetAppliedSortingRuleTargets(ctx context.Context, instrumentID uuid.UUID, programme string, analysisRequest AnalysisRequest) ([]string, error)
	GetByInstrumentIDAndProgramme(ctx context.Context, instrumentID uuid.UUID, programme string) ([]SortingRule, error)
	GetByInstrumentIDs(ctx context.Context, instrumentIDs []uuid.UUID) (map[uuid.UUID][]SortingRule, error)
	GetSampleSequenceNumber(ctx context.Context, sampleCode string) (int, error)
	DeleteSortingRulesWithTx(ctx context.Context, sortingRules []SortingRule) error
	WithTransaction(tx db.DbConnection) SortingRuleService
}

type sortingRuleService struct {
	analysisRepository    AnalysisRepository
	conditionService      ConditionService
	sortingRuleRepository SortingRuleRepository
	externalTx            db.DbConnection
}

func (s *sortingRuleService) getTransaction() db.DbConnection {
	return s.externalTx
}

func (s *sortingRuleService) ApplySortingRuleTarget(ctx context.Context, instrumentID uuid.UUID, programme, sampleCode, target string, validUntil time.Time) error {
	return s.sortingRuleRepository.ApplySortingRuleTarget(ctx, instrumentID, programme, sampleCode, target, validUntil)
}

func (s *sortingRuleService) GetAppliedSortingRuleTargets(ctx context.Context, instrumentID uuid.UUID, programme string, analysisRequest AnalysisRequest) ([]string, error) {
	var orderID uuid.UUID
	var err error
	for _, extraValue := range analysisRequest.ExtraValues {
		if extraValue.Key == "OrderID" {
			orderID, err = uuid.Parse(extraValue.Value)
			if err != nil {
				log.Error().Err(err).Str("orderID", extraValue.Value).Msg("invalid order ID in extra values")
				return nil, err
			}
			break
		}
	}

	if orderID == uuid.Nil {
		return nil, nil
	}

	sampleCodes, err := s.analysisRepository.GetSampleCodesByOrderID(ctx, orderID)
	if err != nil {
		return nil, err
	}
	return s.sortingRuleRepository.GetAppliedSortingRuleTargets(ctx, instrumentID, programme, sampleCodes)
}

func (s *sortingRuleService) GetSampleSequenceNumber(ctx context.Context, sampleCode string) (int, error) {
	return s.sortingRuleRepository.GetSampleSequenceNumber(ctx, sampleCode)
}

func (s *sortingRuleService) UpsertWithTx(ctx context.Context, sortingRule *SortingRule) error {
	tx := s.getTransaction()
	if tx == nil {
		log.Error().Msg(msgRequiredSortingRuleTransactionNotFound)
		return ErrorRequiredSortingRuleTransactionNotFound
	}
	shouldUpdateCondition := true
	existingSortingRule, err := s.sortingRuleRepository.WithTransaction(tx).GetById(ctx, sortingRule.ID)
	if err != nil {
		return err
	}

	if existingSortingRule != nil && existingSortingRule.Condition != nil {
		existingCondition, err := s.conditionService.WithTransaction(tx).GetCondition(ctx, existingSortingRule.Condition.ID)
		if err != nil {
			return err
		}
		shouldUpdateCondition = !areConditionsEquivalent(&existingCondition, sortingRule.Condition)
		if shouldUpdateCondition {
			err = s.conditionService.WithTransaction(tx).DeleteConditionWithTx(ctx, existingSortingRule.Condition.ID)
			if err != nil {
				return err
			}
		}
	}

	if sortingRule.Condition != nil && shouldUpdateCondition {
		conditionID, err := s.conditionService.WithTransaction(tx).CreateConditionWithTx(ctx, *sortingRule.Condition)
		if err != nil {
			return err
		}
		sortingRule.Condition.ID = conditionID
	}

	sortingRule.ID, err = s.sortingRuleRepository.WithTransaction(tx).Upsert(ctx, *sortingRule)
	if err != nil {
		return err
	}

	return nil
}

func GetSortingTargetForAnalysisRequestAndCondition(analysisRequests []AnalysisRequest, sortingRule SortingRule, appliedTargets []string, sampleSequenceNumber int) (string, error) {
	if sortingRule.Condition == nil {
		return sortingRule.Target, nil
	}
	if len(analysisRequests) == 0 {
		return "", fmt.Errorf("no analysis requests to evaluate")
	}
	evaluatorFunc, conditionErrors := NewConditionEvaluator(*sortingRule.Condition, appliedTargets, sampleSequenceNumber)
	if len(conditionErrors) > 0 || evaluatorFunc == nil {
		log.Error().Interface("sortingRuleID", sortingRule.ID).Interface("conditionID", sortingRule.Condition.ID).Msg("invalid condition")
		return "", fmt.Errorf("invalid condition")
	}
	result, err := evaluatorFunc(analysisRequests[0], analysisRequests[0], analysisRequests)
	if err != nil {
		log.Error().Err(err).Interface("sortingRuleID", sortingRule.ID).Interface("conditionID", sortingRule.Condition.ID).Msg("invalid condition")
		return "", err
	}
	if result {
		return sortingRule.Target, nil
	}

	return "", fmt.Errorf("no target found")
}

func (s *sortingRuleService) GetByInstrumentIDs(ctx context.Context, instrumentIDs []uuid.UUID) (map[uuid.UUID][]SortingRule, error) {
	tx := s.getTransaction()
	sortingRulesMap, err := s.sortingRuleRepository.WithTransaction(tx).GetByInstrumentIDs(ctx, instrumentIDs)
	if err != nil {
		return nil, err
	}

	for instrumentID := range sortingRulesMap {
		for i := range sortingRulesMap[instrumentID] {
			if sortingRulesMap[instrumentID][i].Condition != nil {
				condition, err := s.conditionService.WithTransaction(tx).GetCondition(ctx, sortingRulesMap[instrumentID][i].Condition.ID)
				if err != nil {
					return nil, err
				}
				sortingRulesMap[instrumentID][i].Condition = &condition
			}
		}
	}

	return sortingRulesMap, nil
}

func (s *sortingRuleService) GetByInstrumentIDAndProgramme(ctx context.Context, instrumentID uuid.UUID, programme string) ([]SortingRule, error) {
	sortingRules, err := s.sortingRuleRepository.GetByInstrumentIDAndProgramme(ctx, instrumentID, programme)
	if err != nil {
		return nil, err
	}

	for i := range sortingRules {
		if sortingRules[i].Condition != nil {
			condition, err := s.conditionService.GetCondition(ctx, sortingRules[i].Condition.ID)
			if err != nil {
				return nil, err
			}
			sortingRules[i].Condition = &condition
		}
	}

	return sortingRules, nil
}

func (s *sortingRuleService) DeleteSortingRulesWithTx(ctx context.Context, sortingRules []SortingRule) error {
	tx := s.getTransaction()
	if tx == nil {
		log.Error().Msg(msgRequiredSortingRuleTransactionNotFound)
		return ErrorRequiredSortingRuleTransactionNotFound
	}

	sortingRuleIDs := make([]uuid.UUID, len(sortingRules))
	for i := range sortingRules {
		sortingRuleIDs = append(sortingRuleIDs, sortingRules[i].ID)
		if sortingRules[i].Condition == nil {
			continue
		}

		err := s.conditionService.WithTransaction(tx).DeleteConditionWithTx(ctx, sortingRules[i].Condition.ID)
		if err != nil {
			return err
		}
	}
	err := s.sortingRuleRepository.WithTransaction(tx).Delete(ctx, sortingRuleIDs)
	if err != nil {
		return err
	}

	return nil
}

func (s *sortingRuleService) WithTransaction(tx db.DbConnection) SortingRuleService {
	if tx == nil {
		return s
	}
	txSvc := *s
	txSvc.externalTx = tx
	return &txSvc
}

func areConditionsEquivalent(cond1, cond2 *Condition) bool {
	if cond1 == nil && cond2 == nil {
		return true
	}
	if cond1 == nil || cond2 == nil {
		return false
	}
	if !areOperandsEquivalent(cond1.Operand1, cond2.Operand1) || !areOperandsEquivalent(cond1.Operand2, cond2.Operand2) {
		return false
	}
	if cond1.Operator != cond2.Operator {
		return false
	}
	if !isEquivalent(cond1.Name, cond2.Name) || cond1.NegateSubCondition1 != cond2.NegateSubCondition1 ||
		cond1.NegateSubCondition2 != cond2.NegateSubCondition2 {
		return false
	}

	return areConditionsEquivalent(cond1.SubCondition1, cond2.SubCondition1) && areConditionsEquivalent(cond1.SubCondition2, cond2.SubCondition2)
}

func areOperandsEquivalent(operand1, operand2 *ConditionOperand) bool {
	if operand1 == nil && operand2 == nil {
		return true
	}
	if operand1 == nil || operand2 == nil {
		return false
	}
	return isEquivalent(operand1.ConstantValue, operand2.ConstantValue) && isEquivalent(operand1.Name, operand2.Name) &&
		isEquivalent(operand1.ExtraValueKey, operand2.ExtraValueKey) &&
		isEquivalent(&operand1.Type, &operand2.Type)
}

func isEquivalent[T comparable](a, b *T) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func NewSortingRuleService(analysisRepository AnalysisRepository, conditionService ConditionService, sortingRuleRepository SortingRuleRepository) SortingRuleService {
	return &sortingRuleService{
		analysisRepository:    analysisRepository,
		conditionService:      conditionService,
		sortingRuleRepository: sortingRuleRepository,
	}
}
