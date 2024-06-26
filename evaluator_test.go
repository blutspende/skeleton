package skeleton

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	CustomerFacilityKey = "CustomerFacilityID"
	DonationTypeKey     = "DonationType"
	FirstTimeDonorType  = "E"
	SecondTimeDonorType = "2"
	MultiTimeDonorType  = "M"
)

var (
	trueStr       = "true"
	falseStr      = "false"
	trueCondition = &Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &trueStr,
		},
		Operator: Equals,
		Operand2: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &trueStr,
		},
	}
	falseCondition = &Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &trueStr,
		},
		Operator: Equals,
		Operand2: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &falseStr,
		},
	}
)

func TestAndOperator(t *testing.T) {
	conditionTrueAndTrue := Condition{
		SubCondition1: trueCondition,
		Operator:      And,
		SubCondition2: trueCondition,
	}
	conditionTrueAndFalse := Condition{
		SubCondition1: trueCondition,
		Operator:      And,
		SubCondition2: falseCondition,
	}
	analysisRequest := AnalysisRequest{}
	evalFunc, _ := NewConditionEvaluator(conditionTrueAndTrue, nil)
	result, err := evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.True(t, result)
	evalFunc, _ = NewConditionEvaluator(conditionTrueAndFalse, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestOrOperator(t *testing.T) {
	conditionFalseOrTrue := Condition{
		SubCondition1: falseCondition,
		Operator:      Or,
		SubCondition2: trueCondition,
	}
	conditionTrueOrFalse := Condition{
		SubCondition1: trueCondition,
		Operator:      Or,
		SubCondition2: falseCondition,
	}
	conditionFalseOrFalse := Condition{
		SubCondition1: falseCondition,
		Operator:      Or,
		SubCondition2: falseCondition,
	}
	analysisRequest := AnalysisRequest{}
	evalFunc, _ := NewConditionEvaluator(conditionFalseOrTrue, nil)
	result, err := evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.True(t, result)
	evalFunc, _ = NewConditionEvaluator(conditionTrueOrFalse, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.True(t, result)
	evalFunc, _ = NewConditionEvaluator(conditionFalseOrFalse, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestContainsOperator(t *testing.T) {
	subStr := "ue"
	conditionContainsTrue := Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &trueStr,
		},
		Operator: Contains,
		Operand2: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &subStr,
		},
	}
	conditionContainsFalse := Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &trueStr,
		},
		Operator: Contains,
		Operand2: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &falseStr,
		},
	}
	var evalFunc ConditionEvaluatorFunc
	var err error
	var result bool
	analysisRequest := AnalysisRequest{}
	evalFunc, _ = NewConditionEvaluator(conditionContainsTrue, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.True(t, result)
	evalFunc, _ = NewConditionEvaluator(conditionContainsFalse, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestNotContainsOperator(t *testing.T) {
	subStr := "ue"
	conditionNotContainsFalse := Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &trueStr,
		},
		Operator: NotContains,
		Operand2: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &subStr,
		},
	}
	conditionNotContainsTrue := Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &trueStr,
		},
		Operator: NotContains,
		Operand2: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &falseStr,
		},
	}
	var evalFunc ConditionEvaluatorFunc
	var err error
	var result bool
	analysisRequest := AnalysisRequest{}
	evalFunc, _ = NewConditionEvaluator(conditionNotContainsTrue, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.True(t, result)
	evalFunc, _ = NewConditionEvaluator(conditionNotContainsFalse, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestEqualsOperator(t *testing.T) {
	conditionTrueEqualsTrue := Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &trueStr,
		},
		Operator: Equals,
		Operand2: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &trueStr,
		},
	}
	conditionTrueEqualsFalse := Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &trueStr,
		},
		Operator: Equals,
		Operand2: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &falseStr,
		},
	}
	var evalFunc ConditionEvaluatorFunc
	var err error
	var result bool
	analysisRequest := AnalysisRequest{}
	evalFunc, _ = NewConditionEvaluator(conditionTrueEqualsTrue, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.True(t, result)
	evalFunc, _ = NewConditionEvaluator(conditionTrueEqualsFalse, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestNotEqualsOperator(t *testing.T) {
	conditionTrueNotEqualsTrue := Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &trueStr,
		},
		Operator: NotEquals,
		Operand2: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &trueStr,
		},
	}
	conditionTrueNotEqualsFalse := Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &trueStr,
		},
		Operator: NotEquals,
		Operand2: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &falseStr,
		},
	}
	var evalFunc ConditionEvaluatorFunc
	var err error
	var result bool
	analysisRequest := AnalysisRequest{}
	evalFunc, _ = NewConditionEvaluator(conditionTrueNotEqualsFalse, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.True(t, result)
	evalFunc, _ = NewConditionEvaluator(conditionTrueNotEqualsTrue, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestGreaterOperator(t *testing.T) {
	value1 := "32"
	value2 := "42"
	condition2GT1 := Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value2,
		},
		Operator: Greater,
		Operand2: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value1,
		},
	}
	condition1GT2 := Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value1,
		},
		Operator: Greater,
		Operand2: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value2,
		},
	}
	condition1GT1 := Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value1,
		},
		Operator: Greater,
		Operand2: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value1,
		},
	}
	var evalFunc ConditionEvaluatorFunc
	var err error
	var result bool
	analysisRequest := AnalysisRequest{}
	evalFunc, _ = NewConditionEvaluator(condition2GT1, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.True(t, result)
	evalFunc, _ = NewConditionEvaluator(condition1GT2, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.False(t, result)
	evalFunc, _ = NewConditionEvaluator(condition1GT1, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestGreaterOrEqualOperator(t *testing.T) {
	value1 := "32"
	value2 := "42"
	condition2GTE1 := Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value2,
		},
		Operator: GreaterOrEqual,
		Operand2: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value1,
		},
	}
	condition1GTE2 := Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value1,
		},
		Operator: GreaterOrEqual,
		Operand2: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value2,
		},
	}
	condition1GTE1 := Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value1,
		},
		Operator: GreaterOrEqual,
		Operand2: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value1,
		},
	}
	var evalFunc ConditionEvaluatorFunc
	var err error
	var result bool
	analysisRequest := AnalysisRequest{}
	evalFunc, _ = NewConditionEvaluator(condition2GTE1, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.True(t, result)
	evalFunc, _ = NewConditionEvaluator(condition1GTE1, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.True(t, result)
	evalFunc, _ = NewConditionEvaluator(condition1GTE2, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.False(t, result)

}

func TestLessOperator(t *testing.T) {
	value1 := "32"
	value2 := "42"
	condition1LT2 := Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value1,
		},
		Operator: Less,
		Operand2: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value2,
		},
	}
	condition2LT1 := Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value2,
		},
		Operator: Less,
		Operand2: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value1,
		},
	}
	condition1LT1 := Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value1,
		},
		Operator: Less,
		Operand2: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value1,
		},
	}
	var evalFunc ConditionEvaluatorFunc
	var err error
	var result bool
	analysisRequest := AnalysisRequest{}
	evalFunc, _ = NewConditionEvaluator(condition1LT2, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.True(t, result)
	evalFunc, _ = NewConditionEvaluator(condition2LT1, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.False(t, result)
	evalFunc, _ = NewConditionEvaluator(condition1LT1, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestLessOrEqualOperator(t *testing.T) {
	value1 := "32"
	value2 := "42"
	condition1LTE2 := Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value1,
		},
		Operator: LessOrEqual,
		Operand2: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value2,
		},
	}
	condition2LTE1 := Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value2,
		},
		Operator: LessOrEqual,
		Operand2: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value1,
		},
	}
	condition1LTE1 := Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value1,
		},
		Operator: LessOrEqual,
		Operand2: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value1,
		},
	}
	var evalFunc ConditionEvaluatorFunc
	var err error
	var result bool
	analysisRequest := AnalysisRequest{}
	evalFunc, _ = NewConditionEvaluator(condition1LTE2, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.True(t, result)
	evalFunc, _ = NewConditionEvaluator(condition1LTE1, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.True(t, result)
	evalFunc, _ = NewConditionEvaluator(condition2LTE1, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestMatchRegexOperator(t *testing.T) {
	value1 := "AASAMPLE1"
	value2 := "CLSAMPLE1"
	conditionMatchRegexTrue := Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value1,
		},
		Operator: MatchRegex,
		Operand2: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &aaPrefixRegex,
		},
	}
	conditionMatchRegexFalse := Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &value2,
		},
		Operator: MatchRegex,
		Operand2: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &aaPrefixRegex,
		},
	}
	var evalFunc ConditionEvaluatorFunc
	var err error
	var result bool
	analysisRequest := AnalysisRequest{}
	evalFunc, _ = NewConditionEvaluator(conditionMatchRegexTrue, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.True(t, result)
	evalFunc, _ = NewConditionEvaluator(conditionMatchRegexFalse, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestExistsOperator(t *testing.T) {
	conditionExists := Condition{
		Operand1: &ConditionOperand{
			Type:          AnalysisRequestExtraValue,
			ExtraValueKey: &CustomerFacilityKey,
		},
		Operator: Exists,
	}
	var evalFunc ConditionEvaluatorFunc
	var err error
	var result bool
	analysisRequest := AnalysisRequest{
		ExtraValues: []ExtraValue{
			{
				Key:   DonationTypeKey,
				Value: MultiTimeDonorType,
			},
			{
				Key:   CustomerFacilityKey,
				Value: "customerfacility1",
			},
		},
	}
	evalFunc, _ = NewConditionEvaluator(conditionExists, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.True(t, result)
	analysisRequest.ExtraValues = []ExtraValue{{Key: DonationTypeKey, Value: MultiTimeDonorType}}
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestNotExistsOperator(t *testing.T) {
	conditionNotExists := Condition{
		Operand1: &ConditionOperand{
			Type:          AnalysisRequestExtraValue,
			ExtraValueKey: &CustomerFacilityKey,
		},
		Operator: NotExists,
	}
	var evalFunc ConditionEvaluatorFunc
	var err error
	var result bool
	analysisRequest := AnalysisRequest{
		ExtraValues: []ExtraValue{
			{
				Key:   DonationTypeKey,
				Value: MultiTimeDonorType,
			},
		},
	}
	evalFunc, _ = NewConditionEvaluator(conditionNotExists, nil)
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.True(t, result)
	analysisRequest.ExtraValues = append(analysisRequest.ExtraValues, ExtraValue{
		Key:   CustomerFacilityKey,
		Value: "customerfacility1",
	})
	result, err = evalFunc(analysisRequest, analysisRequest, nil)
	assert.Nil(t, err)
	assert.False(t, result)
}

func TestMatchAllOperator(t *testing.T) {
	conditionMatchAll := Condition{
		Operand1: &ConditionOperand{
			Type: Order,
		},
		Operator: MatchAll,
		SubCondition2: &Condition{
			Operand1: &ConditionOperand{
				Type:          AnalysisRequestExtraValue,
				ExtraValueKey: &CustomerFacilityKey,
			},
			Operator: Exists,
		},
	}
	analysisRequests := []AnalysisRequest{
		{
			ExtraValues: []ExtraValue{{Key: DonationTypeKey, Value: MultiTimeDonorType}},
		},
		{},
	}
	var evalFunc ConditionEvaluatorFunc
	var err error
	var result bool
	evalFunc, _ = NewConditionEvaluator(conditionMatchAll, nil)
	result, err = evalFunc(analysisRequests[0], analysisRequests[0], analysisRequests)
	assert.Nil(t, err)
	assert.False(t, result)

	for i := range analysisRequests {
		analysisRequests[i].ExtraValues = append(analysisRequests[i].ExtraValues, ExtraValue{Key: CustomerFacilityKey, Value: "customer1"})
	}

	result, err = evalFunc(analysisRequests[0], analysisRequests[0], analysisRequests)
	assert.Nil(t, err)
	assert.True(t, result)
}

func TestMatchAnyOperator(t *testing.T) {
	conditionMatchAny := Condition{
		Operand1: &ConditionOperand{
			Type: Order,
		},
		Operator: MatchAny,
		SubCondition2: &Condition{
			Operand1: &ConditionOperand{
				Type:          AnalysisRequestExtraValue,
				ExtraValueKey: &CustomerFacilityKey,
			},
			Operator: Exists,
		},
	}
	analysisRequests := []AnalysisRequest{
		{},
		{},
		{},
	}
	var evalFunc ConditionEvaluatorFunc
	var err error
	var result bool
	evalFunc, _ = NewConditionEvaluator(conditionMatchAny, nil)
	result, err = evalFunc(analysisRequests[0], analysisRequests[0], analysisRequests)
	assert.Nil(t, err)
	assert.False(t, result)

	analysisRequests[2].ExtraValues = append(analysisRequests[2].ExtraValues, ExtraValue{Key: CustomerFacilityKey, Value: "customer1"})

	result, err = evalFunc(analysisRequests[0], analysisRequests[0], analysisRequests)
	assert.Nil(t, err)
	assert.True(t, result)
}

func TestTargetAppliedOperator(t *testing.T) {
	target1 := "PK7400_MF"
	conditionApplied := Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &target1,
		},
		Operator: TargetApplied,
	}
	var evalFunc ConditionEvaluatorFunc
	var err error
	var result bool
	evalFunc, _ = NewConditionEvaluator(conditionApplied, []string{"PK7400_ES", "HAM_ESP"})
	result, err = evalFunc(AnalysisRequest{}, AnalysisRequest{}, nil)
	assert.Nil(t, err)
	assert.False(t, result)

	evalFunc, _ = NewConditionEvaluator(conditionApplied, []string{"PK7400_MF", "HAM_ESP"})
	result, err = evalFunc(AnalysisRequest{}, AnalysisRequest{}, nil)
	assert.Nil(t, err)
	assert.True(t, result)
}

func TestTargetNotAppliedOperator(t *testing.T) {
	target1 := "PK7400_MF"
	conditionNotApplied := Condition{
		Operand1: &ConditionOperand{
			Type:          Constant,
			ConstantValue: &target1,
		},
		Operator: TargetNotApplied,
	}
	var evalFunc ConditionEvaluatorFunc
	var err error
	var result bool
	evalFunc, _ = NewConditionEvaluator(conditionNotApplied, []string{"PK7400_ES", "HAM_ESP"})
	result, err = evalFunc(AnalysisRequest{}, AnalysisRequest{}, nil)
	assert.Nil(t, err)
	assert.True(t, result)

	evalFunc, _ = NewConditionEvaluator(conditionNotApplied, []string{"PK7400_MF", "HAM_ESP"})
	result, err = evalFunc(AnalysisRequest{}, AnalysisRequest{}, nil)
	assert.Nil(t, err)
	assert.False(t, result)
}
