package middleware

import (
	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"
)

type UserToken struct {
	RealmAccess RealmAccess `json:"realm_access"`
	Roles       []UserRole  `json:"role"`
	Email       string      `json:"email"`
	ClientID    string      `json:"azp"`
	UserID      uuid.UUID   `json:"sub"`
	Scopes      string      `json:"scope"`
	jwt.RegisteredClaims
}

type RealmAccess struct {
	Roles []UserRole `json:"roles"`
}

type UserRole string

const (
	Admin        UserRole = "admin"
	MedLabSuper  UserRole = "medlabsuper"
	MedLabHead   UserRole = "medlabhead"
	MedLabDoc    UserRole = "medlabdoc"
	MedLabAssist UserRole = "medlabassist"
	ITSupport    UserRole = "it_support"
	Service      UserRole = "service"
)

func (s UserRole) ToString() string {
	return string(s)
}
