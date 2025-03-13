package skeleton

import (
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/rs/zerolog"
	"os"
	"testing"

	"github.com/rs/zerolog/log"
)

func TestMain(m *testing.M) {
	postgres := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().Port(5551))
	err := postgres.Start()
	if err != nil {
		log.Error().Err(err).Msg("starting embedded postgres failed")
	}

	configureLogger()

	code := m.Run()

	err = postgres.Stop()
	if err != nil {
		log.Error().Err(err).Msg("stopping embedded postgres failed")
	}

	os.Exit(code)
}

func configureLogger() {
	consoleWriter := zerolog.NewConsoleWriter()
	consoleWriter.TimeFormat = "2006-01-02T15:04:05Z07:00"
	log.Logger = zerolog.New(consoleWriter).With().Caller().Stack().Timestamp().Logger()
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
}
