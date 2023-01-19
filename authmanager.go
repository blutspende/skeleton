package skeleton

import (
	"errors"
	"strings"
	"sync"

	"github.com/DRK-Blutspende-BaWueHe/skeleton/config"
	"github.com/MicahParks/keyfunc"
	"github.com/go-resty/resty/v2"
	"github.com/rs/zerolog/log"
)

const oidcURLPart = "/.well-known/openid-configuration"

type AuthManager interface {
	GetJWKS() (*keyfunc.JWKS, error)
	GetClientCredential() (string, error)
	RefreshClientCredential() error
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
	authenticationManager := &authManager{
		configuration: configuration,
		restClient:    restClient,
	}

	err := authenticationManager.loadJWKS()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load OIDC from the authentication provider")
		return nil
	}

	return authenticationManager
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
		err := m.RefreshClientCredential()
		if err != nil || m.tokenEndpointResponse == nil {
			return "", errors.New("no client credential")
		}
	}
	return m.tokenEndpointResponse.AccessToken, nil
}

func (m *authManager) RefreshClientCredential() error {
	if err := m.ensureOIDC(); err != nil {
		return err
	}

	tokenEndpointResponse, err := m.callAuthProviderTokenEndpoint()
	if tokenEndpointResponse == nil || err != nil {
		log.Error().Err(err).Msg("Failed to load JWT token from the authentication provider")
		return err
	}

	if tokenEndpointResponse.TokenType != "Bearer" {
		log.Error().Msg("Got invalid token type from the authentication provider")
		return errors.New("invalid token type")
	}

	m.tokenEndpointResponse = tokenEndpointResponse

	return nil
}

func (m *authManager) loadJWKS() error {
	if err := m.ensureOIDC(); err != nil {
		return err
	}

	jwks, err := keyfunc.Get(m.oidc.JwksURI, keyfunc.Options{
		Client:              m.restClient.GetClient(),
		RefreshErrorHandler: m.refreshErrorHandler,
		RefreshUnknownKID:   true,
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to get JWKS from the authentication provider")
		return err
	}

	m.jwks = jwks

	return nil
}

func (m *authManager) refreshErrorHandler(err error) {
	log.Error().Err(err).Msg("Failed to get and refresh JWKS from the authentication provider")
}

func (m *authManager) callAuthProviderOIDCEndpoint() (*OpenIDConfiguration, error) {
	response, err := m.restClient.R().
		SetHeader("Content-Type", "application/json").
		SetResult(&OpenIDConfiguration{}).
		Get(strings.TrimRight(m.configuration.OIDCBaseURL, "/") + oidcURLPart)

	if err != nil {
		log.Error().Err(err).Msg("Failed to get OIDC from the authentication provider")
		return nil, err
	}

	if !response.IsSuccess() {
		log.Error().Err(err).Msgf("Failed to get OIDC from the authentication provider: %s", response.Status())
		return nil, err
	}

	oidc := response.Result().(*OpenIDConfiguration)

	return oidc, nil
}

func (m *authManager) callAuthProviderTokenEndpoint() (*TokenEndpointResponse, error) {
	response, err := m.restClient.R().
		SetHeader("Content-Type", "application/x-www-form-urlencoded").
		SetHeader("Cache-Control", "no-cache").
		SetAuthScheme("Basic").
		SetAuthToken(m.configuration.ClientCredentialAuthHeaderValue).
		SetResult(&TokenEndpointResponse{}).
		SetFormData(map[string]string{"grant_type": "client_credentials"}).
		Post(m.oidc.TokenEndpoint)

	if err != nil {
		log.Error().Err(err).Msg("Failed to get JWT token from the authentication provider's token endpoint")
		return nil, err
	}

	if !response.IsSuccess() {
		log.Error().Err(err).Msgf("Failed to get JWT token from the authentication provider's token endpoint: %s", response.Status())
		return nil, err
	}

	tokenEndpointResponse := response.Result().(*TokenEndpointResponse)

	return tokenEndpointResponse, nil
}

func (m *authManager) ensureOIDC() error {
	if m.oidc == nil {
		oidc, err := m.callAuthProviderOIDCEndpoint()
		if err != nil {
			log.Error().Err(err).Msg("Failed to load OIDC from the authentication provider")
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
