package utils

import (
	"github.com/rs/zerolog/log"
	"time"
)

func FormatTimeStringToBerlinTime(timeString, format string) (time.Time, error) {
	location, err := time.LoadLocation(string("Europe/Berlin"))
	if err != nil {
		log.Error().Err(err).Msg("Can not load Location")
		return time.Time{}, err
	}

	return time.ParseInLocation(format, timeString, location)
}
