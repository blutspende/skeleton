package skeleton

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetSortingTargetRun1FirstSecondTimeDonor(t *testing.T) {
	var target string
	var err error
	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{firstTimeAnalysis}, programme1SortingRules[i])
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "HAM_ESP", target)
	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{secondTimeAnalysis}, programme1SortingRules[i])
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "HAM_ESP", target)
}

func TestGetSortingTargetRun1MultiTimeDonorWithCMV(t *testing.T) {
	var target string
	var err error
	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{multiTimeAnalysis1, multiTimeAnalysis2, multiTimeCMV}, programme1SortingRules[i])
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "HAM_CMV", target)
}

func TestGetSortingTargetRun1MultiTimeDonorWithoutCMV(t *testing.T) {
	var target string
	var err error
	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{multiTimeAnalysis1, multiTimeAnalysis2}, programme1SortingRules[i])
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "HAM_OCMV", target)
}

func TestGetSortingTargetRun1Prefix(t *testing.T) {
	var target string
	var err error
	aaAnalysis := AnalysisRequest{
		SampleCode: "AASAMPLE",
	}
	clAnalysis := AnalysisRequest{
		SampleCode: "CLSAMPLE",
	}
	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{aaAnalysis}, programme1SortingRules[i])
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "HAM_CMV", target)

	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{clAnalysis}, programme1SortingRules[i])
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "HAM_ESP", target)
}

func TestSortingTargetPriority(t *testing.T) {
	// CL prefix sample, but multiple time donor
	// since donation type rules have higher priority than prefix rules, result should be HAM_OCMV, not HAM_ESP
	// here the ordering is static, but in practice the executed sorting rules are ordered by priority
	analysisRequest := AnalysisRequest{
		SampleCode: "CLSAMPLE",
		ExtraValues: []ExtraValue{
			{
				Key:   DonationTypeKey,
				Value: MultiTimeDonorType,
			},
		},
	}
	var target string
	var err error
	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{analysisRequest}, programme1SortingRules[i])
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "HAM_OCMV", target)
}

func TestGetSortingTargetRun2FirstSecondTimeDonor(t *testing.T) {
	var target string
	var err error
	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{firstTimeAnalysis}, programme2SortingRules[i])
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "PK7400_ES", target)

	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{secondTimeAnalysis}, programme2SortingRules[i])
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "PK7400_ES", target)
}

func TestGetSortingTargetRun2MultiTimeDonor(t *testing.T) {
	var target string
	var err error
	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{multiTimeAnalysis1}, programme2SortingRules[i])
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "PK7400_MF", target)
}

func TestGetSortingTargetRun2AACLPrefix(t *testing.T) {
	var target string
	var err error
	aaAnalysis := AnalysisRequest{
		SampleCode: "AASAMPLE",
	}
	clAnalysis := AnalysisRequest{
		SampleCode: "CLSAMPLE",
	}
	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{aaAnalysis}, programme2SortingRules[i])
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "PK7400_MF", target)

	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{clAnalysis}, programme2SortingRules[i])
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "PK7400_ES", target)
}

func TestGetSortingTargetRun2PSuffix(t *testing.T) {
	var target string
	var err error
	pSuffixAnalysis := AnalysisRequest{
		SampleCode: "PSUFFIXSAMPLEP",
	}
	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{pSuffixAnalysis}, programme2SortingRules[i])
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "PK7400_MF", target)
}

func TestGetSortingTargetRun2Default(t *testing.T) {
	var target string
	var err error
	analysis := AnalysisRequest{
		SampleCode: "UNKNOWNSAMPLE",
	}
	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{analysis}, programme2SortingRules[i])
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "ERROR", target)
}

var (
	aaPrefixRegex      = "^AA"
	clPrefixRegex      = "^CL"
	iSuffixRegex       = "I$"
	pSuffixRegex       = "P$"
	cmvAnalyteIDString = "91d5bf56-2bfd-470f-b8e0-92535391ee82"
	analyte1IDString   = "79e7222a-dc73-44e0-9369-d9d890bda8ce"
	analyte2IDString   = "155afc8a-12f9-475e-823a-a7fa501f7d58"
	firstTimeAnalysis  = AnalysisRequest{
		SampleCode: "FIRSTTIME",
		ExtraValues: []ExtraValue{
			{
				Key:   "DonationType",
				Value: "E",
			},
		},
		AnalyteID: uuid.MustParse(cmvAnalyteIDString),
	}
	secondTimeAnalysis = AnalysisRequest{
		SampleCode: "SECONDTIME",
		ExtraValues: []ExtraValue{
			{
				Key:   DonationTypeKey,
				Value: "2",
			},
		},
		AnalyteID: uuid.MustParse(cmvAnalyteIDString),
	}
	multiTimeAnalysis1 = AnalysisRequest{
		SampleCode: "MULTITIME",
		ExtraValues: []ExtraValue{
			{
				Key:   DonationTypeKey,
				Value: "M",
			},
		},
		AnalyteID: uuid.MustParse(analyte1IDString),
	}
	multiTimeAnalysis2 = AnalysisRequest{
		SampleCode: "MULTITIME",
		ExtraValues: []ExtraValue{
			{
				Key:   DonationTypeKey,
				Value: "M",
			},
		},
		AnalyteID: uuid.MustParse(analyte2IDString),
	}
	multiTimeCMV = AnalysisRequest{
		SampleCode: "MULTITIME",
		ExtraValues: []ExtraValue{
			{
				Key:   DonationTypeKey,
				Value: "M",
			},
		},
		AnalyteID: uuid.MustParse(cmvAnalyteIDString),
	}
)

var programme1SortingRules = []SortingRule{
	{
		Target:   "HAM_CMV",
		Priority: 1,
		Condition: &Condition{
			SubCondition1: &Condition{
				Operand1: &ConditionOperand{
					Type:          AnalysisRequestExtraValue,
					ExtraValueKey: &DonationTypeKey,
				},
				Operator: Equals,
				Operand2: &ConditionOperand{
					Type:          Constant,
					ConstantValue: &MultiTimeDonorType,
				},
			},
			Operator: And,
			SubCondition2: &Condition{
				Operand1: &ConditionOperand{
					Type: Order,
				},
				Operator: MatchAny,
				SubCondition2: &Condition{
					Operand1: &ConditionOperand{
						Type: Analyte,
					},
					Operator: Equals,
					Operand2: &ConditionOperand{
						Type:          Constant,
						ConstantValue: &cmvAnalyteIDString,
					},
				},
			},
		},
	},
	{
		Target:   "HAM_OCMV",
		Priority: 2,
		Condition: &Condition{
			SubCondition1: &Condition{
				Operand1: &ConditionOperand{
					Type:          AnalysisRequestExtraValue,
					ExtraValueKey: &DonationTypeKey,
				},
				Operator: Equals,
				Operand2: &ConditionOperand{
					Type:          Constant,
					ConstantValue: &MultiTimeDonorType,
				},
			},
			Operator: And,
			SubCondition2: &Condition{
				Operand1: &ConditionOperand{
					Type: Order,
				},
				Operator: MatchAll,
				SubCondition2: &Condition{
					Operand1: &ConditionOperand{
						Type: Analyte,
					},
					Operator: NotEquals,
					Operand2: &ConditionOperand{
						Type:          Constant,
						ConstantValue: &cmvAnalyteIDString,
					},
				},
			},
		},
	},
	{
		Target:   "HAM_ESP",
		Priority: 3,
		Condition: &Condition{
			SubCondition1: &Condition{
				Operand1: &ConditionOperand{
					Type:          AnalysisRequestExtraValue,
					ExtraValueKey: &DonationTypeKey,
				},
				Operator: Equals,
				Operand2: &ConditionOperand{
					Type:          Constant,
					ConstantValue: &FirstTimeDonorType,
				},
			},
			Operator: Or,
			SubCondition2: &Condition{
				Operand1: &ConditionOperand{
					Type:          AnalysisRequestExtraValue,
					ExtraValueKey: &DonationTypeKey,
				},
				Operator: Equals,
				Operand2: &ConditionOperand{
					Type:          Constant,
					ConstantValue: &SecondTimeDonorType,
				},
			},
		},
	},
	{
		Target:   "HAM_CMV",
		Priority: 4,
		Condition: &Condition{
			Operand1: &ConditionOperand{
				Type: SampleCode,
			},
			Operator: MatchRegex,
			Operand2: &ConditionOperand{
				Type:          Constant,
				ConstantValue: &pSuffixRegex,
			},
		},
	},
	{
		Target:   "ROCHE",
		Priority: 5,
		Condition: &Condition{
			Operand1: &ConditionOperand{
				Type: SampleCode,
			},
			Operator: MatchRegex,
			Operand2: &ConditionOperand{
				Type:          Constant,
				ConstantValue: &iSuffixRegex,
			},
		},
	},
	{
		Target:   "HAM_ESP",
		Priority: 6,
		Condition: &Condition{
			Operand1: &ConditionOperand{
				Type: SampleCode,
			},
			Operator: MatchRegex,
			Operand2: &ConditionOperand{
				Type:          Constant,
				ConstantValue: &clPrefixRegex,
			},
		},
	},
	{
		Target:   "HAM_CMV",
		Priority: 7,
		Condition: &Condition{
			Operand1: &ConditionOperand{
				Type: SampleCode,
			},
			Operator: MatchRegex,
			Operand2: &ConditionOperand{
				Type:          Constant,
				ConstantValue: &aaPrefixRegex,
			},
		},
	},
}

var programme2SortingRules = []SortingRule{
	{
		Target:   "PK7400_MF",
		Priority: 1,
		Condition: &Condition{
			Operand1: &ConditionOperand{
				Type:          AnalysisRequestExtraValue,
				ExtraValueKey: &DonationTypeKey,
			},
			Operator: Equals,
			Operand2: &ConditionOperand{
				Type:          Constant,
				ConstantValue: &MultiTimeDonorType,
			},
		},
	},
	{
		Target:   "PK7400_ES",
		Priority: 2,
		Condition: &Condition{
			SubCondition1: &Condition{
				Operand1: &ConditionOperand{
					Type:          AnalysisRequestExtraValue,
					ExtraValueKey: &DonationTypeKey,
				},
				Operator: Equals,
				Operand2: &ConditionOperand{
					Type:          Constant,
					ConstantValue: &FirstTimeDonorType,
				},
			},
			Operator: Or,
			SubCondition2: &Condition{
				Operand1: &ConditionOperand{
					Type:          AnalysisRequestExtraValue,
					ExtraValueKey: &DonationTypeKey,
				},
				Operator: Equals,
				Operand2: &ConditionOperand{
					Type:          Constant,
					ConstantValue: &SecondTimeDonorType,
				},
			},
		},
	},
	{
		Target:   "PK7400_MF",
		Priority: 3,
		Condition: &Condition{
			SubCondition1: &Condition{
				Operand1: &ConditionOperand{
					Type: SampleCode,
				},
				Operator: MatchRegex,
				Operand2: &ConditionOperand{
					Type:          Constant,
					ConstantValue: &aaPrefixRegex,
				},
			},
			Operator: Or,
			SubCondition2: &Condition{
				Operand1: &ConditionOperand{
					Type: SampleCode,
				},
				Operator: MatchRegex,
				Operand2: &ConditionOperand{
					Type:          Constant,
					ConstantValue: &pSuffixRegex,
				},
			},
		},
	},
	{
		Target:   "PK7400_ES",
		Priority: 4,
		Condition: &Condition{
			Operand1: &ConditionOperand{
				Type: SampleCode,
			},
			Operator: MatchRegex,
			Operand2: &ConditionOperand{
				Type:          Constant,
				ConstantValue: &clPrefixRegex,
			},
		},
	},
	{
		Priority: 5,
		Target:   "ERROR",
	},
}
