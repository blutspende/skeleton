package db

import (
	"fmt"
	"skeleton/config"

	"github.com/jmoiron/sqlx"
	// Need this import for sqlx
	_ "github.com/lib/pq"
	"github.com/rs/zerolog/log"
)

func CreatePostgresConnectionNew(config *config.Configuration) (*sqlx.DB, error) {

	url := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s application_name=%s",
		config.PostgresDB.Host, config.PostgresDB.Port, config.PostgresDB.User,
		config.PostgresDB.Pass, config.PostgresDB.Database, config.PostgresDB.SSLMode, config.ApplicationName)

	pgDB, pgErr := sqlx.Connect("postgres", url)
	if pgErr != nil {
		return nil, pgErr
	} else {
		pgErr = pgDB.Ping()
		if pgErr != nil {
			return nil, pgErr
		}
		log.Info().Msgf("Postgres available, connected to %s / %s", config.PostgresDB.Host, config.PostgresDB.Database)
	}

	return pgDB, nil
}
