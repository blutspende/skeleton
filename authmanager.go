package skeleton

import (
	"context"
	"sync"

	"github.com/MicahParks/keyfunc"
	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/go-resty/resty/v2"
	"github.com/rs/zerolog/log"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

type AuthManager interface {
	GetClientCredential() (string, error)
	GetJWKS() (*keyfunc.JWKS, error)
	GetTokenSource() (oauth2.TokenSource, error)
}
type authManager struct {
	oidcClientContext context.Context
	oidcBaseURL       string
	clientId          string
	clientSecret      string
	oidc              *openIDConfiguration
	jwks              *keyfunc.JWKS
	tokenSource       oauth2.TokenSource
	oidcMutex         sync.Mutex
	jwksMutex         sync.Mutex
	tokenSourceMutex  sync.Mutex
	restyClient       *resty.Client
}

type openIDConfiguration struct {
	JwksURI       string `json:"jwks_uri"`
	TokenEndpoint string `json:"token_endpoint"`
}

func (m *authManager) GetClientCredential() (string, error) {
	var err error
	if m.oidc == nil {
		err = m.loadOIDCConfig()
		if err != nil {
			return "", err
		}
	}
	if m.tokenSource == nil {
		m.loadTokenSource()
	}
	token, err := m.tokenSource.Token()
	if err != nil {
		return "", err
	}
	return token.AccessToken, nil
}

func (m *authManager) GetJWKS() (*keyfunc.JWKS, error) {
	var err error
	if m.oidc == nil {
		err = m.loadOIDCConfig()
		if err != nil {
			return nil, err
		}
	}
	if m.jwks == nil {
		if err = m.loadJWKS(); err != nil {
			return nil, err
		}
	}
	return m.jwks, nil
}

func (m *authManager) GetTokenSource() (oauth2.TokenSource, error) {
	var err error
	if m.oidc == nil {
		err = m.loadOIDCConfig()
		if err != nil {
			return nil, err
		}
	}
	if m.tokenSource == nil {
		m.loadTokenSource()
	}
	return m.tokenSource, nil
}

func (m *authManager) loadOIDCConfig() error {
	m.oidcMutex.Lock()
	defer m.oidcMutex.Unlock()
	if m.oidc == nil {
		provider, err := oidc.NewProvider(m.oidcClientContext, m.oidcBaseURL)
		if err != nil {
			return err
		}
		var config openIDConfiguration
		err = provider.Claims(&config)
		if err != nil {
			log.Error().Err(err).Msg("Failed to load OIDC from the authentication provider")
			return err
		}
		m.oidc = &config
	}
	return nil
}

func (m *authManager) loadJWKS() error {
	m.jwksMutex.Lock()
	defer m.jwksMutex.Unlock()
	if m.jwks == nil {
		jwks, err := keyfunc.Get(m.oidc.JwksURI, keyfunc.Options{
			Client: m.restyClient.GetClient(),
			RefreshErrorHandler: func(err error) {
				log.Error().Err(err).Msg("Failed to get and refresh JWKS from the authentication provider")
			},
			RefreshUnknownKID: true,
		})
		if err != nil {
			log.Error().Err(err).Msg("Failed to get JWKS from the authentication provider")
			return err
		}
		m.jwks = jwks
	}
	return nil
}

func (m *authManager) loadTokenSource() {
	m.tokenSourceMutex.Lock()
	defer m.tokenSourceMutex.Unlock()
	if m.tokenSource == nil {
		credentialsConfig := clientcredentials.Config{
			ClientID:     m.clientId,
			ClientSecret: m.clientSecret,
			TokenURL:     m.oidc.TokenEndpoint,
		}

		m.tokenSource = credentialsConfig.TokenSource(m.oidcClientContext)
	}
}

func NewAuthManager(ctx context.Context, unauthorizedRestyClient *resty.Client, OIDCBaseURL, clientId, clientSecret string) AuthManager {
	//Skip validation of OIDC issuer
	//In standard deployments, services communicate with the identity provider (keycloak) through k8s namespace-internal network (f.e.: http://keycloak)
	//OIDC issuer always contains the public address of the deployed identity provider (f.e.: https://latest1.bloodlab.org/identity)
	//Making it comparable would cause extra network traffic, vulnerability to cloudflare issues, etc..
	oidcContext := oidc.InsecureIssuerURLContext(ctx, OIDCBaseURL)

	return &authManager{
		oidcClientContext: oidc.ClientContext(oidcContext, unauthorizedRestyClient.GetClient()),
		clientId:          clientId,
		clientSecret:      clientSecret,
		oidcBaseURL:       OIDCBaseURL,
		restyClient:       unauthorizedRestyClient,
	}
}
