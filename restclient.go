package skeleton

import (
	"context"
	"crypto/tls"
	"net/http"
	"time"

	"github.com/blutspende/skeleton/config"
	"github.com/go-resty/resty/v2"
	"github.com/rs/zerolog"
	"golang.org/x/oauth2"
	"golang.org/x/time/rate"
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

func NewAuthorizedRestyClient(ctx context.Context, configuration *config.Configuration, tokenSource oauth2.TokenSource, rateLimiter *rate.Limiter, timeoutSeconds uint) *resty.Client {
	client := resty.New().
		SetRetryCount(2).
		AddRetryCondition(retryConditionFunc).
		OnBeforeRequest(configureRequest(ctx, configuration)).
		OnBeforeRequest(func(client *resty.Client, request *resty.Request) error {
			token, err := tokenSource.Token()
			if err != nil {
				return err
			}
			request.SetAuthToken(token.AccessToken)
			return nil
		}).
		OnBeforeRequest(func(client *resty.Client, request *resty.Request) error {
			return rateLimiter.Wait(ctx)
		})
	if timeoutSeconds > 0 {
		client = client.SetTimeout(time.Second * time.Duration(timeoutSeconds))
	}
	if configuration.Development {
		client = client.SetTLSClientConfig(&tls.Config{
			InsecureSkipVerify: true,
		})
	}

	return client
}

var retryConditionFunc = func(response *resty.Response, err error) bool { // retry on 401, 503 ...
	if response != nil &&
		(response.StatusCode() == http.StatusUnauthorized || response.StatusCode() == http.StatusServiceUnavailable) {
		return true
	}
	return false
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
