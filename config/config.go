package config

import (
	"encoding/base64"
	"github.com/rs/zerolog"

	"github.com/pkg/errors"

	"github.com/kelseyhightower/envconfig"
	"github.com/rs/zerolog/log"
)

const MsgFailedToReadConfiguration = "failed to read configuration"

var ErrFailedToReadConfiguration = errors.New(MsgFailedToReadConfiguration)

type Configuration struct {
	InstrumentSettings struct {
		MemoryLogSize int `envconfig:"TCP_MEMORY_LOG_SIZE" required:"true" default:"500"`
	}
	APIPort                           int           `envconfig:"API_PORT" default:"8080"`
	Authorization                     bool          `envconfig:"AUTHORIZATION" default:"true"`
	ClientID                          string        `envconfig:"CLIENT_ID" required:"true"`
	ClientSecret                      string        `envconfig:"CLIENT_SECRET" required:"true"`
	EnableTLS                         bool          `envconfig:"ENABLE_TLS" default:"false"`
	BloodlabCertPath                  string        `envconfig:"BLOODLAB_CERT_PATH" default:"../bloodlab_cert.pem"`
	BloodlabKeyPath                   string        `envconfig:"BLOODLAB_KEY_PATH" default:"../bloodlab_key.pem"`
	Development                       bool          `envconfig:"DEVELOPMENT" default:"false"`
	PermittedOrigin                   string        `envconfig:"PERMITTED_ORIGIN_URL" default:"*"`
	OIDCBaseURL                       string        `envconfig:"OIDC_BASE_URL" default:"https://iam.bloodlab.org/realms/test.bloodlab.org"`
	LogLevel                          zerolog.Level `envconfig:"LOG_LEVEL" default:"-1"`
	ApplicationName                   string        `envconfig:"APPLICATION_NAME" default:"astm"`
	TCPListenerPort                   int           `envconfig:"TCP_LISTENER_PORT" required:"true" default:"5000"`
	LocalEnvironment                  bool          `envconfig:"LOCAL_ENVIRONMENT" default:"false"`
	InstrumentTransferRetryDelay      int           `envconfig:"INSTRUMENT_TRANSFER_DELAY" default:"10"`
	CerberusURL                       string        `envconfig:"CERBERUS_URL" required:"true" default:"localhost"`
	DeaURL                            string        `envconfig:"DEA_URL" required:"true" default:"localhost"`
	LookBackDays                      int           `envconfig:"LOOK_BACK_DAYS" default:"14"`
	TCPServerMaxConnections           int           `envconfig:"MAX_TCP_CONNECTIONS" required:"true" default:"50"`
	Proxy                             string        `envconfig:"PROXY" default:""`
	RequestTransferSleepTimer         int           `envconfig:"REQUEST_TRANSFER_SLEEP_TIME" default:"500"`
	ResultTransferBatchSize           int           `envconfig:"RESULT_TRANSFER_BATCH_SIZE" default:"100"`
	ResultTransferFlushTimeout        int           `envconfig:"RESULT_TRANSFER_FLUSH_TIMEOUT" default:"5"`
	ResultTransferRetryTimeout        int           `envconfig:"RESULT_TRANSFER_RETRY_TIMEOUT" default:"5"`
	AnalysisRequestsChannelBufferSize int           `envconfig:"ANALYSIS_REQUESTS_CHANNEL_BUFFER_SIZE" default:"1024"`
	AnalysisResultsChannelBufferSize  int           `envconfig:"ANALYSIS_RESULTS_CHANNEL_BUFFER_SIZE" default:"1024"`

	ClientCredentialAuthHeaderValue string
}

var Settings Configuration

func ReadConfiguration() (Configuration, error) {
	var config Configuration
	err := envconfig.Process("", &config)
	if err != nil {
		err = errors.Wrap(err, MsgFailedToReadConfiguration)
		log.Error().Err(err).Msgf("%s\n", ErrFailedToReadConfiguration)
		return config, err
	}
	config.ClientCredentialAuthHeaderValue = base64.StdEncoding.EncodeToString([]byte(config.ClientID + ":" + config.ClientSecret))
	return config, nil
}
