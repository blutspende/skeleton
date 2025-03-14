package skeleton

import (
	"context"
	"fmt"
	"github.com/blutspende/skeleton/db"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"time"
)

type SortingRuleService interface {
	ApplySortingRuleTarget(ctx context.Context, instrumentID uuid.UUID, programme, sampleCode, target string, validUntil time.Time) error
	Create(ctx context.Context, sortingRule *SortingRule) error
	GetAppliedSortingRuleTargets(ctx context.Context, instrumentID uuid.UUID, programme string, analysisRequest AnalysisRequest) ([]string, error)
	GetByInstrumentIDAndProgramme(ctx context.Context, instrumentID uuid.UUID, programme string) ([]SortingRule, error)
	GetByInstrumentIDs(ctx context.Context, instrumentIDs []uuid.UUID) (map[uuid.UUID][]SortingRule, error)
	GetSampleSequenceNumber(ctx context.Context, sampleCode string) (int, error)
	Update(ctx context.Context, rule *SortingRule) error
	DeleteSortingRules(ctx context.Context, sortingRules []SortingRule) error
	WithTransaction(tx db.DbConnector) SortingRuleService
}

type sortingRuleService struct {
	analysisRepository    AnalysisRepository
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

	if s.externalTx == nil {
		err = tx.Commit()
		if err != nil {
			_ = tx.Rollback()
			return err
		}
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

func (s *sortingRuleService) DeleteSortingRules(ctx context.Context, sortingRules []SortingRule) error {
	tx, err := s.getTransaction()
	if err != nil {
		return err
	}
	sortingRuleIDs := make([]uuid.UUID, len(sortingRules))
	for i := range sortingRules {
		sortingRuleIDs = append(sortingRuleIDs, sortingRules[i].ID)
		if sortingRules[i].Condition == nil {
			continue
		}

		err = s.conditionService.WithTransaction(tx).DeleteCondition(ctx, sortingRules[i].Condition.ID)
		if err != nil {
			if s.externalTx == nil {
				_ = tx.Rollback()
			}
			return err
		}
	}
	err = s.sortingRuleRepository.Delete(ctx, sortingRuleIDs)
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
	if newRule.Priority != originalRule.Priority {
		return true
	}

	return false
}
func NewSortingRuleService(analysisRepository AnalysisRepository, conditionService ConditionService, sortingRuleRepository SortingRuleRepository) SortingRuleService {
	return &sortingRuleService{
		analysisRepository:    analysisRepository,
		conditionService:      conditionService,
		sortingRuleRepository: sortingRuleRepository,
	}
}
