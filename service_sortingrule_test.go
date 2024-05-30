package skeleton

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetSortingTargetProgramme1FirstSecondTimeDonor(t *testing.T) {
	var target string
	var err error
	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{firstTimeAnalysis}, programme1SortingRules[i], nil, 0)
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "HAM_ESP", target)
	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{secondTimeAnalysis}, programme1SortingRules[i], nil, 0)
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "HAM_ESP", target)
}

func TestGetSortingTargetProgramme1MultiTimeDonorChanceBased(t *testing.T) {
	var target string
	var err error
	cmvCounter := 0
	ocmvCounter := 0
	for i := 0; i < 100; i++ {
		for j := range programme1SortingRules {
			target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{multiTimeAnalysis1, multiTimeAnalysis2}, programme1SortingRules[j], nil, 0)
			if err == nil {
				if target == "HAM_CMV" {
					cmvCounter++
				} else if target == "HAM_OCMV" {
					ocmvCounter++
				}
				break
			}
		}
	}
	assert.Nil(t, err)
	// 10%-ish margin of error
	assert.GreaterOrEqual(t, cmvCounter, 70)
	assert.LessOrEqual(t, ocmvCounter, 30)
}

func TestGetSortingTargetProgramme1Prefix(t *testing.T) {
	var target string
	var err error
	aaAnalysis := AnalysisRequest{
		SampleCode: "AASAMPLE",
	}
	clAnalysis := AnalysisRequest{
		SampleCode: "CLSAMPLE",
	}
	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{aaAnalysis}, programme1SortingRules[i], nil, 0)
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "HAM_CMV", target)

	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{clAnalysis}, programme1SortingRules[i], nil, 0)
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "HAM_ESP", target)
}

func TestProgramme1FirstSecondSample(t *testing.T) {
	var target string
	var err error
	aaAnalysis := AnalysisRequest{
		SampleCode: "AASAMPLE",
	}
	clAnalysis := AnalysisRequest{
		SampleCode: "CLSAMPLE",
	}
	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{aaAnalysis}, programme1SortingRules[i], nil, 1)
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "HAM_CMV", target)

	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{clAnalysis}, programme1SortingRules[i], nil, 2)
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "DOPPELT", target)
}

func TestSortingTargetPriority(t *testing.T) {
	// AA prefix sample, but second time donor
	// since donation type rules have higher priority than prefix rules, result should be HAM_ESP, not HAM_CMV
	// here the ordering is static, but in practice the executed sorting rules are ordered by priority
	analysisRequest := AnalysisRequest{
		SampleCode: "AASAMPLE",
		ExtraValues: []ExtraValue{
			{
				Key:   DonationTypeKey,
				Value: SecondTimeDonorType,
			},
		},
	}
	var target string
	var err error
	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{analysisRequest}, programme1SortingRules[i], nil, 0)
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "HAM_ESP", target)
}

func TestGetSortingTargetProgramme2FirstSecondTimeDonor(t *testing.T) {
	var target string
	var err error
	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{firstTimeAnalysis}, programme2SortingRules[i], nil, 0)
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "PK7400_ES", target)

	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{secondTimeAnalysis}, programme2SortingRules[i], nil, 0)
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "PK7400_ES", target)
}

func TestGetSortingTargetProgramme2MultiTimeDonor(t *testing.T) {
	var target string
	var err error
	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{multiTimeAnalysis1}, programme2SortingRules[i], nil, 0)
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "PK7400_MF", target)
}

func TestGetSortingTargetProgramme2AACLPrefix(t *testing.T) {
	var target string
	var err error
	aaAnalysis := AnalysisRequest{
		SampleCode: "AASAMPLE",
	}
	clAnalysis := AnalysisRequest{
		SampleCode: "CLSAMPLE",
	}
	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{aaAnalysis}, programme2SortingRules[i], nil, 0)
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "PK7400_MF", target)

	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{clAnalysis}, programme2SortingRules[i], nil, 0)
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "PK7400_ES", target)
}

func TestGetSortingTargetProgramme2PSuffix(t *testing.T) {
	var target string
	var err error
	pSuffixAnalysis := AnalysisRequest{
		SampleCode: "PSUFFIXSAMPLEP",
	}
	for i := range programme1SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{pSuffixAnalysis}, programme2SortingRules[i], nil, 0)
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "PK7400_MF", target)
}

func TestGetSortingTargetProgramme2Default(t *testing.T) {
	var target string
	var err error
	analysis := AnalysisRequest{
		SampleCode: "UNKNOWNSAMPLE",
	}
	for i := range programme2SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{analysis}, programme2SortingRules[i], nil, 0)
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "ERROR", target)
}

func TestProgramme3WithoutMalaria(t *testing.T) {
	var target string
	var err error
	for i := range programme3SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{{}}, programme3SortingRules[i], nil, 0)
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "ARCHIV", target)
}

func TestProgramme3WithMalaria(t *testing.T) {
	var target string
	var err error
	analysisRequest := AnalysisRequest{
		SampleCode: "MALARIA",
		AnalyteID:  uuid.MustParse(malariaAnalyteIDString),
	}
	for i := range programme3SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{analysisRequest}, programme3SortingRules[i], nil, 0)
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "STUDIE_1", target)
}

func TestProgramme4(t *testing.T) {
	var target string
	var err error
	analysis1 := AnalysisRequest{
		SampleCode:  "SAMPLECODE1",
		AnalyteID:   uuid.MustParse(iggAnalyteIDString),
		ExtraValues: []ExtraValue{{Key: "OrderID", Value: "order1"}, {Key: CustomerFacilityKey, Value: programme4FacilityID}},
	}
	analysis2 := AnalysisRequest{
		SampleCode:  "SAMPLECODE1",
		AnalyteID:   uuid.MustParse(gewAnalyteIDString),
		ExtraValues: []ExtraValue{{Key: "OrderID", Value: "order1"}, {Key: CustomerFacilityKey, Value: programme4FacilityID}},
	}
	appliedTargets := make([]string, 0)
	for i := range programme4SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{analysis1, analysis2}, programme4SortingRules[i], appliedTargets, 0)
		if err == nil {
			appliedTargets = append(appliedTargets, target)
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "ROCHE", target)

	for i := range programme4SortingRules {
		target, err = GetSortingTargetForAnalysisRequestAndCondition([]AnalysisRequest{analysis1, analysis2}, programme4SortingRules[i], appliedTargets, 0)
		if err == nil {
			break
		}
	}
	assert.Nil(t, err)
	assert.Equal(t, "???", target)
}

var (
	aaPrefixRegex          = "^AA"
	cmvAnalyteIDString     = "91d5bf56-2bfd-470f-b8e0-92535391ee82"
	iggAnalyteIDString     = "15c984d8-f253-4f02-931c-4b197e184181"
	gewAnalyteIDString     = "1e590b57-b296-441b-9af6-a5ec58d2baf0"
	analyte1IDString       = "79e7222a-dc73-44e0-9369-d9d890bda8ce"
	analyte2IDString       = "155afc8a-12f9-475e-823a-a7fa501f7d58"
	malariaAnalyteIDString = "b9ed2cfa-9e46-41b4-af26-da0c31df175a"
	firstTimeAnalysis      = AnalysisRequest{
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
)

var programme1SortingRules = []SortingRule{
	{
		Condition: &Condition{
			Operator: "isNthSample",
			Operand1: &ConditionOperand{
				Type: "sample",
			},
			Operand2: &ConditionOperand{
				Type:          "constant",
				ConstantValue: &SecondTimeDonorType,
			},
		},
		Target:   "DOPPELT",
		Priority: 0,
	},
	{
		Condition: &Condition{
			Operator: "or",
			SubCondition1: &Condition{
				Operator: "==",
				Operand1: &ConditionOperand{
					Type:          "extraValue",
					ExtraValueKey: &DonationTypeKey,
				},
				Operand2: &ConditionOperand{
					Type:          "constant",
					ConstantValue: &FirstTimeDonorType,
				},
			},
			SubCondition2: &Condition{
				Operator: "==",
				Operand1: &ConditionOperand{
					Type:          "extraValue",
					ExtraValueKey: &DonationTypeKey,
				},
				Operand2: &ConditionOperand{
					Type:          "constant",
					ConstantValue: &SecondTimeDonorType,
				},
			},
		},
		Target:   "HAM_ESP",
		Priority: 1,
	},
	{
		Condition: &Condition{
			Operator: "and",
			SubCondition1: &Condition{
				Operator: "hasNPercentProbability",
				Operand1: &ConditionOperand{
					Type: "sample",
				},
				Operand2: &ConditionOperand{
					Type:          "constant",
					ConstantValue: strToPtr("80"),
				},
			},
			SubCondition2: &Condition{
				Operator: "==",
				Operand1: &ConditionOperand{
					Type:          "extraValue",
					ExtraValueKey: &DonationTypeKey,
				},
				Operand2: &ConditionOperand{
					Type:          "constant",
					ConstantValue: &MultiTimeDonorType,
				},
			},
		},
		Target:   "HAM_CMV",
		Priority: 2,
	},
	{
		Condition: &Condition{
			Operator: "==",
			Operand1: &ConditionOperand{
				Type:          "extraValue",
				ExtraValueKey: &DonationTypeKey,
			},
			Operand2: &ConditionOperand{
				Type:          "constant",
				ConstantValue: &MultiTimeDonorType,
			},
		},
		Target:   "HAM_OCMV",
		Priority: 3,
	},
	{
		Condition: &Condition{
			Operator: "regex",
			Operand1: &ConditionOperand{
				Type: "sampleCode",
			},
			Operand2: &ConditionOperand{
				Type:          "constant",
				ConstantValue: strToPtr("I$"),
			},
		},
		Target:   "ROCHE",
		Priority: 4,
	},
	{
		Condition: &Condition{
			Operator: "or",
			SubCondition1: &Condition{
				Operator: "regex",
				Operand1: &ConditionOperand{
					Type: "sampleCode",
				},
				Operand2: &ConditionOperand{
					Type:          "constant",
					ConstantValue: strToPtr("P$"),
				},
			},
			SubCondition2: &Condition{
				Operator: "regex",
				Operand1: &ConditionOperand{
					Type: "sampleCode",
				},
				Operand2: &ConditionOperand{
					Type:          "constant",
					ConstantValue: strToPtr("^AA"),
				},
			},
		},
		Target:   "HAM_CMV",
		Priority: 5,
	},
	{
		Condition: &Condition{
			Operator: "regex",
			Operand1: &ConditionOperand{
				Type: "sampleCode",
			},
			Operand2: &ConditionOperand{
				Type:          "constant",
				ConstantValue: strToPtr("^CL"),
			},
		},
		Target:   "HAM_ESP",
		Priority: 6,
	},
}

var programme2SortingRules = []SortingRule{
	{
		Condition: &Condition{
			Operator: "regex",
			Operand1: &ConditionOperand{
				Type: "sampleCode",
			},
			Operand2: &ConditionOperand{
				Type:          "constant",
				ConstantValue: strToPtr("^(585|586|587|588|589|590|591|592|593|594|595|596|597|598)"),
			},
		},
		Target:   "ARCHIV",
		Priority: 0,
	},
	{
		Condition: &Condition{
			Operator: "or",
			SubCondition1: &Condition{
				Operator: "==",
				Operand1: &ConditionOperand{
					Type:          "extraValue",
					ExtraValueKey: &DonationTypeKey,
				},
				Operand2: &ConditionOperand{
					Type:          "constant",
					ConstantValue: &FirstTimeDonorType,
				},
			},
			SubCondition2: &Condition{
				Operator: "==",
				Operand1: &ConditionOperand{
					Type:          "extraValue",
					ExtraValueKey: &DonationTypeKey,
				},
				Operand2: &ConditionOperand{
					Type:          "constant",
					ConstantValue: &SecondTimeDonorType,
				},
			},
		},
		Target:   "PK7400_ES",
		Priority: 1,
	},
	{
		Condition: &Condition{
			Operator: "==",
			Operand1: &ConditionOperand{
				Type:          "extraValue",
				ExtraValueKey: &DonationTypeKey,
			},
			Operand2: &ConditionOperand{
				Type:          "constant",
				ConstantValue: &MultiTimeDonorType,
			},
		},
		Target:   "PK7400_MF",
		Priority: 2,
	},
	{
		Condition: &Condition{
			Operator: "regex",
			Operand1: &ConditionOperand{
				Type: "sampleCode",
			},
			Operand2: &ConditionOperand{
				Type:          "constant",
				ConstantValue: strToPtr("^CL"),
			},
		},
		Target:   "PK7400_ES",
		Priority: 3,
	},
	{
		Condition: &Condition{
			Operator: "regex",
			Operand1: &ConditionOperand{
				Type: "sampleCode",
			},
			Operand2: &ConditionOperand{
				Type:          "constant",
				ConstantValue: strToPtr("(^AA|P$)"),
			},
		},
		Target:   "PK7400_MF",
		Priority: 4,
	},
	{
		Condition: &Condition{
			Operator: Default,
			Operand1: &ConditionOperand{
				Type: DefaultOperand,
			},
		},

		Priority: 5,
		Target:   "ERROR",
	},
}

var programme3SortingRules = []SortingRule{
	{
		Condition: &Condition{
			Operator: "matchAny",
			SubCondition2: &Condition{
				Operator: "==",
				Operand1: &ConditionOperand{
					Type: "analyte",
				},
				Operand2: &ConditionOperand{
					Type:          "constant",
					ConstantValue: &malariaAnalyteIDString,
				},
			},
			Operand1: &ConditionOperand{
				Type: "order",
			},
		},
		Target:   "STUDIE_1",
		Priority: 0,
	},
	{
		Condition: &Condition{
			Operator: "default",
			Operand1: &ConditionOperand{
				Type: "default",
			},
		},
		Target:   "ARCHIV",
		Priority: 1,
	},
}

func strToPtr(str string) *string {
	return &str
}

var programme4FacilityID = "77d8d66c-d6e3-4dc9-96f2-305d9fdab0a7"
var rocheTarget = "ROCHE"
var programme4SortingRules = []SortingRule{
	{
		Target:   "ROCHE",
		Priority: 1,
		Condition: &Condition{
			SubCondition1: &Condition{
				Operand1: &ConditionOperand{
					Type:          AnalysisRequestExtraValue,
					ExtraValueKey: &CustomerFacilityKey,
				},
				Operator: Equals,
				Operand2: &ConditionOperand{
					Type:          Constant,
					ConstantValue: &programme4FacilityID,
				},
			},
			Operator: And,
			SubCondition2: &Condition{
				SubCondition1: &Condition{
					SubCondition1: &Condition{
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
								ConstantValue: &iggAnalyteIDString,
							},
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
								ConstantValue: &gewAnalyteIDString,
							},
						},
					},
				},
				Operator: And,
				SubCondition2: &Condition{
					Operand1: &ConditionOperand{
						Type: Target,
					},
					Operator: TargetNotApplied,
					Operand2: &ConditionOperand{
						Type:          Constant,
						ConstantValue: &rocheTarget,
					},
				},
			},
		},
	},
	{
		Target:   "???",
		Priority: 2,
		Condition: &Condition{
			SubCondition1: &Condition{
				Operand1: &ConditionOperand{
					Type:          AnalysisRequestExtraValue,
					ExtraValueKey: &CustomerFacilityKey,
				},
				Operator: Equals,
				Operand2: &ConditionOperand{
					Type:          Constant,
					ConstantValue: &programme4FacilityID,
				},
			},
			Operator: And,
			SubCondition2: &Condition{
				SubCondition1: &Condition{
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
							ConstantValue: &iggAnalyteIDString,
						},
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
							ConstantValue: &gewAnalyteIDString,
						},
					},
				},
			},
		},
	},
}
