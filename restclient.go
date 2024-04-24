package skeleton

import (
	"context"
	"crypto/tls"
	"net/http"

	"github.com/blutspende/skeleton/config"
	"github.com/go-resty/resty/v2"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func NewRestyClient(ctx context.Context, configuration *config.Configuration, useProxy bool) *resty.Client {
	client := resty.New().
		OnBeforeRequest(configureRequest(ctx, configuration))

	if configuration.Development {
		client = client.SetTLSClientConfig(&tls.Config{
			InsecureSkipVerify: true,
		})
	}
	if useProxy && configuration.Proxy != "" {
		client.SetProxy(configuration.Proxy)
	}

	return client
}

func NewRestyClientWithAuthManager(ctx context.Context, configuration *config.Configuration, authManager AuthManager) *resty.Client {
	client := resty.New().
		SetRetryCount(2).
		AddRetryCondition(configureRetryMechanismForService2ServiceCalls(authManager)).
		OnBeforeRequest(configureRequest(ctx, configuration)).
		OnBeforeRequest(setService2ServiceAuthToken(authManager))

	if configuration.Development {
		client = client.SetTLSClientConfig(&tls.Config{
			InsecureSkipVerify: true,
		})
	}

	return client
}

func configureRequest(ctx context.Context, configuration *config.Configuration) resty.RequestMiddleware {
	return func(client *resty.Client, request *resty.Request) error {
		request.SetContext(ctx)

		if configuration.LogLevel <= zerolog.DebugLevel {
			request.EnableTrace()
		}

		return nil
	}
}

func configureRetryMechanismForService2ServiceCalls(authManager AuthManager) resty.RetryConditionFunc {
	return func(response *resty.Response, err error) bool {
		if response == nil {
			return true
		}

		if response.StatusCode() == http.StatusUnauthorized {
			authManager.InvalidateClientCredential()
			return true
		}

		return false
	}
}

func setService2ServiceAuthToken(authManager AuthManager) resty.RequestMiddleware {
	return func(client *resty.Client, request *resty.Request) error {
		authToken, err := authManager.GetClientCredential()
		if err != nil {
			log.Error().Err(err).Msg("refresh internal api client auth token failed")
			return err
		}
		client.SetAuthToken(authToken)
		return nil
	}
}
