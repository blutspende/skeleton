package middleware

import (
	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"
)

type UserToken struct {
	RealmAccess RealmAccess `json:"realm_access"`
	Roles       []string    `json:"role"`
	Email       string      `json:"email"`
	ClientID    string      `json:"azp"`
	UserID      uuid.UUID   `json:"sub"`
	Scopes      string      `json:"scope"`
	jwt.RegisteredClaims
}

type RealmAccess struct {
	Roles []string `json:"roles"`
}

type UserRole string

const (
	Admin              UserRole = "admin"
	OrderEntry         UserRole = "orderinput"
	LabManager         UserRole = "lab_manager"
	LabDirector        UserRole = "lab_director"
	LabCoordinator     UserRole = "lab_coordinator"
	LabAssistant       UserRole = "lab_assistant"
	LabAnalyst         UserRole = "lab_analyst"
	LabOperations      UserRole = "lab_operations"
	LabTechnician      UserRole = "lab_technician"
	ITSupport          UserRole = "it_support"
	ITSupportAssistant UserRole = "it_support_assistant"
	ReportViewer       UserRole = "viewer"
	MedicalDirector    UserRole = "medical_director"
	QAManager          UserRole = "qa_manager"
	FinancePrincipal   UserRole = "finance_principal"
	FinanceAssistant   UserRole = "finance_assistant"
	RapidTester        UserRole = "rapid_tester"
	Doctor             UserRole = "doctor"
)

func (s UserRole) ToString() string {
	return string(s)
}
