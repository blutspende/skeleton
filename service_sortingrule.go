package skeleton

import (
	"context"
	"fmt"
	"github.com/blutspende/skeleton/db"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type SortingRuleService interface {
	Create(ctx context.Context, sortingRule *SortingRule) error
	GetByInstrumentIDAndProgramme(ctx context.Context, instrumentID uuid.UUID, programme string) ([]SortingRule, error)
	GetByInstrumentIDs(ctx context.Context, instrumentIDs []uuid.UUID) (map[uuid.UUID][]SortingRule, error)
	Update(ctx context.Context, rule *SortingRule) error
	Delete(ctx context.Context, sortingRuleIDs []uuid.UUID) error
	WithTransaction(tx db.DbConnector) SortingRuleService
}

type sortingRuleService struct {
	conditionService      ConditionService
	sortingRuleRepository SortingRuleRepository
	externalTx            db.DbConnector
}

func (s *sortingRuleService) getTransaction() (db.DbConnector, error) {
	if s.externalTx != nil {
		return s.externalTx, nil
	}

	return s.sortingRuleRepository.CreateTransaction()
}

func (s *sortingRuleService) Create(ctx context.Context, sortingRule *SortingRule) error {
	tx, err := s.getTransaction()
	if err != nil {
		return err
	}

	if sortingRule.Condition != nil {
		conditionID, err := s.conditionService.WithTransaction(tx).CreateCondition(ctx, *sortingRule.Condition)
		if err != nil {
			if s.externalTx == nil {
				_ = tx.Rollback()
			}
			return err
		}
		sortingRule.Condition.ID = conditionID
	}

	sortingRule.ID, err = s.sortingRuleRepository.WithTransaction(tx).Create(ctx, *sortingRule)

	if err != nil {
		if s.externalTx == nil {
			_ = tx.Rollback()
		}
		return err
	}

	if err != nil {
		if s.externalTx == nil {
			_ = tx.Rollback()
		}
		return err
	}

	if s.externalTx == nil {
		err = tx.Commit()
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return nil
}

func GetSortingTargetForAnalysisRequestAndCondition(analysisRequests []AnalysisRequest, sortingRule SortingRule) (string, error) {
	if sortingRule.Condition == nil {
		return sortingRule.Target, nil
	}
	if len(analysisRequests) == 0 {
		return "", fmt.Errorf("no analysis requests to evaluate")
	}
	evaluatorFunc, conditionErrors := NewConditionEvaluator(*sortingRule.Condition)
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
	sortingRulesMap, err := s.sortingRuleRepository.GetByInstrumentIDs(ctx, instrumentIDs)
	if err != nil {
		return nil, err
	}

	for instrumentID := range sortingRulesMap {
		for i := range sortingRulesMap[instrumentID] {
			if sortingRulesMap[instrumentID][i].Condition != nil {
				condition, err := s.conditionService.GetCondition(ctx, sortingRulesMap[instrumentID][i].Condition.ID)
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

func (s *sortingRuleService) Update(ctx context.Context, rule *SortingRule) error {
	tx, err := s.getTransaction()
	if err != nil {
		return err
	}

	if rule.Condition != nil {
		err = s.conditionService.WithTransaction(tx).UpdateCondition(ctx, *rule.Condition)
		if err != nil {
			if err == ErrConditionNotFound {
				conditionID, err := s.conditionService.CreateCondition(ctx, *rule.Condition)
				if err != nil {
					if s.externalTx == nil {
						_ = tx.Rollback()
					}
					return err
				}
				rule.Condition.ID = conditionID
			} else {
				if s.externalTx == nil {
					_ = tx.Rollback()
				}
				return err
			}
		}
	}
	err = s.sortingRuleRepository.WithTransaction(tx).Update(ctx, *rule)
	if err != nil {
		if s.externalTx == nil {
			_ = tx.Rollback()
		}
		return err
	}
	if s.externalTx == nil {
		err = tx.Commit()
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	return nil
}

func (s *sortingRuleService) Delete(ctx context.Context, sortingRuleIDs []uuid.UUID) error {
	return s.sortingRuleRepository.Delete(ctx, sortingRuleIDs)
}

func (s *sortingRuleService) WithTransaction(tx db.DbConnector) SortingRuleService {
	txSvc := *s
	txSvc.externalTx = tx
	return &txSvc
}

func IsSortingRuleUpdated(originalRule SortingRule, newRule SortingRule) bool {
	if newRule.Target != originalRule.Target {
		return true
	}
	if newRule.Programme != originalRule.Programme {
		return true
	}
	if newRule.Condition == nil && originalRule.Condition != nil {
		return true
	}
	if newRule.Condition != nil && originalRule.Condition == nil {
		return true
	}
	if newRule.Condition != nil && originalRule.Condition != nil && newRule.Condition.ID != originalRule.Condition.ID {
		return true
	}

	return false
}
func NewSortingRuleService(conditionService ConditionService, sortingRuleRepository SortingRuleRepository) SortingRuleService {
	return &sortingRuleService{
		conditionService:      conditionService,
		sortingRuleRepository: sortingRuleRepository,
	}
}
