package config

import (
	"encoding/base64"
	"errors"
	"github.com/rs/zerolog"
	"golang.org/x/time/rate"

	"github.com/kelseyhightower/envconfig"
	"github.com/rs/zerolog/log"
)

const MsgFailedToReadConfiguration = "failed to read configuration"

var ErrFailedToReadConfiguration = errors.New(MsgFailedToReadConfiguration)

type Configuration struct {
	InstrumentSettings struct {
		MemoryLogSize int `envconfig:"TCP_MEMORY_LOG_SIZE" required:"true" default:"500"`
	}
	PostgresDB struct {
		Host     string `envconfig:"DB_SERVER" required:"true" default:"127.0.0.1"`
		Port     uint32 `envconfig:"DB_PORT" required:"true" default:"5432"`
		User     string `envconfig:"DB_USER" required:"true" default:"postgres"`
		Pass     string `envconfig:"DB_PASS" required:"true"`
		Database string `envconfig:"DB_DATABASE" required:"true"`
		SSLMode  string `envconfig:"DB_SSL_MODE" required:"true"`
	}
	APIPort                                    uint16        `envconfig:"API_PORT" default:"8080"`
	Authorization                              bool          `envconfig:"AUTHORIZATION" default:"true"`
	ClientID                                   string        `envconfig:"CLIENT_ID" required:"true"`
	ClientSecret                               string        `envconfig:"CLIENT_SECRET" required:"true"`
	EnableTLS                                  bool          `envconfig:"ENABLE_TLS" default:"false"`
	BloodlabCertPath                           string        `envconfig:"BLOODLAB_CERT_PATH" default:"../bloodlab_cert.pem"`
	BloodlabKeyPath                            string        `envconfig:"BLOODLAB_KEY_PATH" default:"../bloodlab_key.pem"`
	Development                                bool          `envconfig:"DEVELOPMENT" default:"false"`
	PermittedOrigin                            string        `envconfig:"PERMITTED_ORIGIN_URL" default:"*"`
	OIDCBaseURL                                string        `envconfig:"OIDC_BASE_URL" default:"https://iam.bloodlab.org/realms/test.bloodlab.org"`
	LogLevel                                   zerolog.Level `envconfig:"LOG_LEVEL" default:"1"`
	ApplicationName                            string        `envconfig:"APPLICATION_NAME" default:"astm"`
	TCPListenerPort                            int           `envconfig:"TCP_LISTENER_PORT" required:"true" default:"5000"`
	LocalEnvironment                           bool          `envconfig:"LOCAL_ENVIRONMENT" default:"false"`
	InstrumentTransferRetryDelayInMs           int           `envconfig:"INSTRUMENT_TRANSFER_DELAY" default:"600000"`
	CerberusURL                                string        `envconfig:"CERBERUS_URL" required:"true" default:"http://cerberus"`
	DeaURL                                     string        `envconfig:"DEA_URL" required:"true" default:"http://dea"`
	LookBackDays                               int           `envconfig:"LOOK_BACK_DAYS" default:"14"`
	TCPServerMaxConnections                    int           `envconfig:"MAX_TCP_CONNECTIONS" required:"true" default:"50"`
	Proxy                                      string        `envconfig:"PROXY" default:""`
	RequestTransferSleepTimer                  int           `envconfig:"REQUEST_TRANSFER_SLEEP_TIME" default:"500"`
	ResultTransferBatchSize                    int           `envconfig:"RESULT_TRANSFER_BATCH_SIZE" default:"100"`
	ResultTransferFlushTimeout                 int           `envconfig:"RESULT_TRANSFER_FLUSH_TIMEOUT" default:"5"`
	ResultTransferRetryTimeout                 int           `envconfig:"RESULT_TRANSFER_RETRY_TIMEOUT" default:"5"`
	ImageRetrySeconds                          int           `envconfig:"IMAGE_RETRY_SECONDS" default:"60"`
	AnalysisRequestsChannelBufferSize          int           `envconfig:"ANALYSIS_REQUESTS_CHANNEL_BUFFER_SIZE" default:"1024"`
	AnalysisResultsChannelBufferSize           int           `envconfig:"ANALYSIS_RESULTS_CHANNEL_BUFFER_SIZE" default:"1024"`
	AnalysisRequestWorkerPoolSize              int           `envconfig:"ANALYSIS_REQUEST_WORKER_POOL_SIZE" default:"3"`
	InstrumentDriverRegistrationTimeoutSeconds int           `envconfig:"INSTRUMENT_DRIVER_REGISTRATION_RETRY_TIMEOUT" default:"10"`
	InstrumentDriverRegistrationMaxRetry       int           `envconfig:"INSTRUMENT_DRIVER_REGISTRATION_MAX_RETRY" default:"20"`
	CleanupDays                                int           `envconfig:"CLEANUP_DAYS" default:"90"`
	CleanupJobRunIntervalHours                 int           `envconfig:"CLEANUP_JOB_RUN_INTERVAL_HOURS" default:"4"`
	UnprocessedAnalysisRequestErrorRetryMinute int           `envconfig:"UNPROCESSED_ANALYSIS_REQUEST_ERROR_RETRY" default:"5"`
	UnprocessedAnalysisResultErrorRetryMinute  int           `envconfig:"UNPROCESSED_ANALYSIS_RESULT_ERROR_RETRY" default:"5"`
	GetUnprocessedAnalysisRequestRetryMinute   int           `envconfig:"GET_UNPROCESSED_ANALYSIS_REQUEST_RETRY" default:"5"`
	GetUnprocessedAnalysisResultIDsRetryMinute int           `envconfig:"GET_UNPROCESSED_ANALYSIS_RESULT_IDS_RETRY" default:"5"`
	StandardAPIClientTimeoutSeconds            uint          `envconfig:"STANDARD_API_CLIENT_TIMEOUT_SECONDS" default:"10"`
	LongPollingAPIClientTimeoutSeconds         uint          `envconfig:"LONG_POLLING_API_CLIENT_TIMEOUT_SECONDS" default:"80"`
	LongPollingReattemptWaitSeconds            uint          `envconfig:"LONG_POLLING_REATTEMPT_WAIT_SECONDS" default:"5"`
	LongPollingLoggingEnabled                  bool          `envconfig:"LONG_POLLING_LOGGING_ENABLED" default:"false"`
	LongPollingRetrySeconds                    int           `envconfig:"LONG_POLLING_RETRY_SECONDS" default:"30"`
	MessageMaxRetries                          int           `envconfig:"MESSAGE_MAX_RETRIES" default:"25"`
	BlackListedTCPClientIPAddresses            string        `envconfig:"BLACKLISTED_TCP_CLIENT_IP_ADDRESSES" default:""`
	MaxRequestsPerSecond                       rate.Limit    `envconfig:"MAX_REQUESTS_PER_SECOND" default:"15"`
	ClientCredentialAuthHeaderValue            string
}

var Settings Configuration

func ReadConfiguration() (Configuration, error) {
	var config Configuration
	err := envconfig.Process("", &config)
	if err != nil {
		log.Error().Err(err).Msgf("%s\n", ErrFailedToReadConfiguration)
		return config, errors.New(MsgFailedToReadConfiguration)
	}
	config.ClientCredentialAuthHeaderValue = base64.StdEncoding.EncodeToString([]byte(config.ClientID + ":" + config.ClientSecret))
	return config, nil
}
