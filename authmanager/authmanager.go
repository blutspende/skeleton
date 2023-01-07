package authmanager

import (
	"context"
	"errors"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/config"
	"strings"
	"sync"
	"time"

	"github.com/MicahParks/keyfunc"
	"github.com/go-resty/resty/v2"
	"github.com/rs/zerolog/log"
)

const oidcURLPart = "/.well-known/openid-configuration"

type AuthManager interface {
	GetJWKS() (*keyfunc.JWKS, error)
	GetClientCredential() (string, error)
	StartClientCredentialTask(ctx context.Context)
}

type authManager struct {
	configuration         *config.Configuration
	restClient            *resty.Client
	jwks                  *keyfunc.JWKS
	oidc                  *OpenIDConfiguration
	oidcMutex             sync.Mutex
	tokenEndpointResponse *TokenEndpointResponse
}

func NewAuthManager(configuration *config.Configuration, restClient *resty.Client) AuthManager {
	keycloakManager := &authManager{
		configuration: configuration,
		restClient:    restClient,
	}

	err := keycloakManager.loadJWKS()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load OIDC from Keycloak!")
		return nil
	}

	return keycloakManager
}

func (m *authManager) GetJWKS() (*keyfunc.JWKS, error) {
	if m.jwks == nil {
		if err := m.loadJWKS(); err != nil {
			return nil, err
		}
	}
	return m.jwks, nil
}

func (m *authManager) GetClientCredential() (string, error) {
	if m.tokenEndpointResponse == nil {
		return "", errors.New("no client credential")
	}
	return m.tokenEndpointResponse.AccessToken, nil
}

func (m *authManager) loadJWKS() error {
	err := m.ensureOIDC()
	if err != nil {
		return err
	}

	m.jwks, err = keyfunc.Get(m.oidc.JwksURI, keyfunc.Options{
		Client:              m.restClient.GetClient(),
		RefreshErrorHandler: m.refreshErrorHandler,
		RefreshUnknownKID:   true,
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to get JWKS from KeyCloak!")
		return err
	}

	return nil
}

func (m *authManager) StartClientCredentialTask(ctx context.Context) {
	log.Info().Msg("Starting client credential synchronizer task")
	actualPeriod := 1 * time.Minute
	ticker := time.NewTicker(actualPeriod)
	m.refreshClientCredentialAuthToken(&actualPeriod)
	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				m.refreshClientCredentialAuthToken(&actualPeriod)
				ticker.Reset(actualPeriod)
			}
		}
	}()
}

func (m *authManager) refreshErrorHandler(err error) {
	log.Error().Err(err).Msg("Failed to get JWKS from KeyCloak!")
}

func (m *authManager) callKeycloakOIDCEndpoint() (*OpenIDConfiguration, error) {
	response, err := m.restClient.R().
		SetHeader("Content-Type", "application/json").
		SetResult(&OpenIDConfiguration{}).
		Get(strings.TrimRight(m.configuration.OIDCBaseURL, "/") + oidcURLPart)

	if err != nil {
		log.Error().Err(err).Msg("Failed to get OIDC from Keycloak")
		return nil, err
	}

	if !response.IsSuccess() {
		log.Error().Err(err).Msgf("Failed to get OIDC from Keycloak: %v", response.Error())
		return nil, err
	}

	oidc := response.Result().(*OpenIDConfiguration)

	return oidc, nil
}

func (m *authManager) callKeycloakTokenEndpoint() (*TokenEndpointResponse, error) {
	response, err := m.restClient.R().
		SetHeader("Content-Type", "application/x-www-form-urlencoded").
		SetHeader("Cache-Control", "no-cache").
		SetAuthScheme("Basic").
		SetAuthToken(m.configuration.ClientCredentialAuthHeaderValue).
		SetResult(&TokenEndpointResponse{}).
		SetFormData(map[string]string{"grant_type": "client_credentials"}).
		Post(m.oidc.TokenEndpoint)

	if err != nil {
		log.Error().Err(err).Msg("Failed to get JWT token from Keycloak token endpoint")
		return nil, err
	}

	if !response.IsSuccess() {
		log.Error().Err(err).Msgf("Failed to get JWT token from Keycloak token endpoint: %v", response.Status())
		return nil, err
	}

	tokenEndpointResponse := response.Result().(*TokenEndpointResponse)

	return tokenEndpointResponse, nil
}

func (m *authManager) refreshClientCredentialAuthToken(actualPeriod *time.Duration) {
	if err := m.ensureOIDC(); err != nil {
		return
	}

	tokenEndpointResponse, err := m.callKeycloakTokenEndpoint()
	if tokenEndpointResponse == nil || err != nil {
		log.Error().Err(err).Msg("Failed to load JWT token from Keycloak token endpoint")
		return
	}

	if tokenEndpointResponse.TokenType != "Bearer" {
		log.Error().Msg("Invalid token type")
		return
	}

	expiresIn := time.Duration(tokenEndpointResponse.ExpiresIn) * time.Second
	if actualPeriod.Milliseconds() != expiresIn.Milliseconds() {
		*actualPeriod = expiresIn
	}

	m.tokenEndpointResponse = tokenEndpointResponse
}

func (m *authManager) ensureOIDC() error {
	if m.oidc == nil {
		oidc, err := m.callKeycloakOIDCEndpoint()
		if err != nil {
			log.Error().Err(err).Msg("Failed to load OIDC")
			return err
		}

		m.updateOIDC(oidc)
	}

	return nil
}

func (m *authManager) updateOIDC(oidc *OpenIDConfiguration) {
	m.oidcMutex.Lock()
	m.oidc = oidc
	m.oidcMutex.Unlock()
}
