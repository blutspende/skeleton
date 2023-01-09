package skeleton

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/db"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"time"
)

const (
	msgInvalidConnectionMode              = "invalid connection mode"
	msgInvalidResultMode                  = "invalid result mode"
	msgCreateInstrumentFailed             = "create instrument failed"
	msgGetInstrumentsFailed               = "get instruments failed"
	msgGetInstrumentChangesFailed         = "get changed instruments failed"
	msgGetInstrumentByIDFailed            = "get instrument by id failed"
	msgInstrumentNotFound                 = "instrument not found"
	msgUpdateInstrumentFailed             = "update instrument failed"
	msgCreateAnalyteMappingsFailed        = "create analyte mappings failed"
	msgGetAnalyteMappingsFailed           = "get analyte mappings failed"
	msgUpdateAnalyteMappingFailed         = "update analyte mapping failed"
	msgDeleteAnalyteMappingFailed         = "delete analyte mapping failed"
	msgCreateChannelMappingsFailed        = "create channel mappings failed"
	msgGetChannelMappingsFailed           = "get channel mappings failed"
	msgUpdateChannelMappingFailed         = "update channel mapping failed"
	msgDeleteChannelMappingFailed         = "delete channel mapping failed"
	msgGetResultMappingsFailed            = "get result mappings failed"
	msgDeleteResultMappingFailed          = "delete result mapping failed"
	msgUpdateResultMappingFailed          = "update result mapping failed"
	msgGetRequestMappingsFailed           = "get request mappings failed"
	msgGetRequestMappingAnalytesFailed    = "get request mapping analytes failed"
	msgDeleteRequestMappingsFailed        = "delete request mappings failed"
	msgUpdateRequestMappingFailed         = "update request mapping failed"
	msgDeleteRequestMappingAnalytesFailed = "delete request mapping analytes failed"
)

var (
	ErrInvalidConnectionMode              = errors.New(msgInvalidConnectionMode)
	ErrInvalidResultMode                  = errors.New(msgInvalidResultMode)
	ErrCreateInstrumentFailed             = errors.New(msgCreateInstrumentFailed)
	ErrGetInstrumentsFailed               = errors.New(msgGetInstrumentsFailed)
	ErrGetInstrumentChangesFailed         = errors.New(msgGetInstrumentChangesFailed)
	ErrGetInstrumentByIDFailed            = errors.New(msgGetInstrumentByIDFailed)
	ErrInstrumentNotFound                 = errors.New(msgInstrumentNotFound)
	ErrUpdateInstrumentFailed             = errors.New(msgUpdateInstrumentFailed)
	ErrCreateAnalyteMappingsFailed        = errors.New(msgCreateAnalyteMappingsFailed)
	ErrGetAnalyteMappingsFailed           = errors.New(msgGetAnalyteMappingsFailed)
	ErrUpdateAnalyteMappingFailed         = errors.New(msgUpdateAnalyteMappingFailed)
	ErrDeleteAnalyteMappingFailed         = errors.New(msgDeleteAnalyteMappingFailed)
	ErrCreateChannelMappingsFailed        = errors.New(msgCreateChannelMappingsFailed)
	ErrGetChannelMappingsFailed           = errors.New(msgGetChannelMappingsFailed)
	ErrUpdateChannelMappingFailed         = errors.New(msgUpdateChannelMappingFailed)
	ErrDeleteChannelMappingFailed         = errors.New(msgDeleteChannelMappingFailed)
	ErrGetResultMappingsFailed            = errors.New(msgGetResultMappingsFailed)
	ErrDeleteResultMappingFailed          = errors.New(msgDeleteResultMappingFailed)
	ErrUpdateResultMappingFailed          = errors.New(msgUpdateResultMappingFailed)
	ErrGetRequestMappingsFailed           = errors.New(msgGetRequestMappingsFailed)
	ErrGetRequestMappingAnalytesFailed    = errors.New(msgGetRequestMappingAnalytesFailed)
	ErrDeleteRequestMappingsFailed        = errors.New(msgDeleteRequestMappingsFailed)
	ErrUpdateRequestMappingFailed         = errors.New(msgUpdateRequestMappingFailed)
	ErrDeleteRequestMappingAnalytesFailed = errors.New(msgDeleteRequestMappingAnalytesFailed)
)

type instrumentDAO struct {
	ID                 uuid.UUID     `db:"id"`
	ProtocolID         uuid.UUID     `db:"protocol_id"`
	Name               string        `db:"name"`
	HostName           string        `db:"hostname"`
	ClientPort         sql.NullInt32 `db:"client_port"`
	Enabled            bool          `db:"enabled"`
	ConnectionMode     string        `db:"connection_mode"`
	RunningMode        string        `db:"running_mode"`
	CaptureResults     bool          `db:"captureresults"`
	CaptureDiagnostics bool          `db:"capturediagnostics"`
	ReplyToQuery       bool          `db:"replytoquery"`
	Status             string        `db:"status"`
	SentToCerberus     bool          `db:"sent_to_cerberus"`
	Timezone           string        `db:"timezone"`
	FileEncoding       string        `db:"file_encoding"`
	CreatedAt          time.Time     `db:"created_at"`
	ModifiedAt         sql.NullTime  `db:"modified_at"`
	DeletedAt          sql.NullTime  `db:"deleted_at"`
}

type analyteMappingDAO struct {
	ID                uuid.UUID    `db:"id"`
	InstrumentID      uuid.UUID    `db:"instrument_id"`
	InstrumentAnalyte string       `db:"instrument_analyte"`
	AnalyteID         uuid.UUID    `db:"analyte_id"`
	CreatedAt         time.Time    `db:"created_at"`
	ModifiedAt        sql.NullTime `db:"modified_at"`
	DeletedAt         sql.NullTime `db:"deleted_at"`
}

type channelMappingDAO struct {
	ID                uuid.UUID    `db:"id"`
	ChannelID         uuid.UUID    `db:"channel_id"`
	AnalyteMappingID  uuid.UUID    `db:"analyte_mapping_id"`
	InstrumentChannel string       `db:"instrument_channel"`
	CreatedAt         time.Time    `db:"created_at"`
	ModifiedAt        sql.NullTime `db:"modified_at"`
	DeletedAt         sql.NullTime `db:"deleted_at"`
}

type resultMappingDAO struct {
	ID               uuid.UUID    `db:"id"`
	AnalyteMappingID uuid.UUID    `db:"analyte_mapping_id"`
	Key              string       `db:"key"`
	Value            string       `db:"value"`
	Index            int          `db:"index"`
	CreatedAt        time.Time    `db:"created_at"`
	ModifiedAt       sql.NullTime `db:"modified_at"`
	DeletedAt        sql.NullTime `db:"deleted_at"`
}

type requestMappingDAO struct {
	ID           uuid.UUID    `db:"id"`
	InstrumentID uuid.UUID    `db:"instrument_id"`
	Code         string       `db:"code"`
	CreatedAt    time.Time    `db:"created_at"`
	ModifiedAt   sql.NullTime `db:"modified_at"`
	DeletedAt    sql.NullTime `db:"deleted_at"`
}

type requestMappingAnalyteDAO struct {
	ID               uuid.UUID    `db:"id"`
	RequestMappingID uuid.UUID    `db:"request_mapping_id"`
	AnalyteID        uuid.UUID    `db:"analyte_id"`
	CreatedAt        time.Time    `db:"created_at"`
	ModifiedAt       sql.NullTime `db:"modified_at"`
	DeletedAt        sql.NullTime `db:"deleted_at"`
}

type instrumentRepository struct {
	db       db.DbConnector
	dbSchema string
}

type InstrumentRepository interface {
	UpsertSupportedProtocol(ctx context.Context, id uuid.UUID, name string, description string) error
}

func (r *instrumentRepository) CreateInstrument(ctx context.Context, instrument Instrument) (uuid.UUID, error) {
	id, err := r.createInstrument(ctx, instrument)
	if err != nil {
		return uuid.Nil, err
	}
	analyteMappingIDs, err := r.createAnalyteMappings(ctx, instrument.AnalyteMappings, id)
	for i, analyteMappingID := range analyteMappingIDs {
		_, err = r.createChannelMappings(ctx, instrument.AnalyteMappings[i].ChannelMappings, analyteMappingID)
		if err != nil {
			return uuid.Nil, err
		}
		_, err = r.createResultMappings(ctx, instrument.AnalyteMappings[i].ResultMappings, analyteMappingID)
		if err != nil {
			return uuid.Nil, err
		}
	}
	requestMappingIDs, err := r.createRequestMappings(ctx, instrument.RequestMappings, id)
	if err != nil {
		return uuid.Nil, err
	}
	analyteIDsByRequestMappingIDs := make(map[uuid.UUID][]uuid.UUID)
	for i, requestMappingID := range requestMappingIDs {
		analyteIDsByRequestMappingIDs[requestMappingID] = instrument.RequestMappings[i].AnalyteIDs
	}
	err = r.upsertRequestMappingAnalytes(ctx, analyteIDsByRequestMappingIDs)
	if err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

func (r *instrumentRepository) GetInstruments(ctx context.Context) ([]Instrument, error) {
	instruments, err := r.getInstruments(ctx)
	if err != nil {
		return nil, err
	}
	instrumentIDs := make([]uuid.UUID, len(instruments))
	instrumentsByIDs := make(map[uuid.UUID]*Instrument)
	analyteMappingsByIDs := make(map[uuid.UUID]*AnalyteMapping)
	for i := range instruments {
		instrumentIDs[i] = instruments[i].ID
		instrumentsByIDs[instruments[i].ID] = &instruments[i]
	}
	analyteMappingsByInstrumentID, err := r.getAnalyteMappings(ctx, instrumentIDs)
	if err != nil {
		return nil, err
	}
	analyteMappingsIDs := make([]uuid.UUID, 0)
	analyteMappingIDInstrumentIDMap := make(map[uuid.UUID]uuid.UUID)

	for instrumentID, analyteMappings := range analyteMappingsByInstrumentID {
		instrumentsByIDs[instrumentID].AnalyteMappings = analyteMappings
		for i := range instrumentsByIDs[instrumentID].AnalyteMappings {
			analyteMappingsIDs = append(analyteMappingsIDs, analyteMappings[i].ID)
			analyteMappingIDInstrumentIDMap[analyteMappings[i].ID] = instrumentID
			analyteMappingsByIDs[analyteMappings[i].ID] = &instrumentsByIDs[instrumentID].AnalyteMappings[i]
		}
	}
	channelMappingsByAnalyteMappingID, err := r.getChannelMappings(ctx, analyteMappingsIDs)
	if err != nil {
		return nil, err
	}
	for analyteMappingID, channelMappings := range channelMappingsByAnalyteMappingID {
		analyteMappingsByIDs[analyteMappingID].ChannelMappings = channelMappings
	}

	resultMappingsByAnalyteMappingID, err := r.getResultMappings(ctx, analyteMappingsIDs)
	if err != nil {
		return nil, err
	}
	for analyteMappingID, resultMappings := range resultMappingsByAnalyteMappingID {
		analyteMappingsByIDs[analyteMappingID].ResultMappings = resultMappings
	}

	requestMappingsByInstrumentID, err := r.getRequestMappings(ctx, instrumentIDs)
	if err != nil {
		return nil, err
	}
	requestMappingIDs := make([]uuid.UUID, 0)
	requestMappingsByIDs := make(map[uuid.UUID]*RequestMapping)
	for instrumentID, requestMappings := range requestMappingsByInstrumentID {
		instrumentsByIDs[instrumentID].RequestMappings = requestMappings
		for i := range instrumentsByIDs[instrumentID].RequestMappings {
			requestMappingIDs = append(requestMappingIDs, requestMappings[i].ID)
			requestMappingsByIDs[requestMappings[i].ID] = &instrumentsByIDs[instrumentID].RequestMappings[i]
		}
	}

	requestMappingAnalyteIDs, err := r.getRequestMappingAnalytes(ctx, requestMappingIDs)
	if err != nil {
		return nil, err
	}

	for requestMappingID, analyteIDs := range requestMappingAnalyteIDs {
		requestMappingsByIDs[requestMappingID].AnalyteIDs = analyteIDs
	}
	return instruments, nil
}

func (r *instrumentRepository) GetInstrumentByID(ctx context.Context, id uuid.UUID) (Instrument, error) {
	instrument, err := r.getInstrumentByID(ctx, id)
	if err != nil {
		return instrument, err
	}
	instrumentIDs := []uuid.UUID{id}
	analyteMappingsByInstrumentID, err := r.getAnalyteMappings(ctx, instrumentIDs)
	if err != nil {
		return instrument, err
	}
	analyteMappingsIDs := make([]uuid.UUID, 0)
	analyteMappingIDInstrumentIDMap := make(map[uuid.UUID]uuid.UUID)
	analyteMappingsByIDs := make(map[uuid.UUID]*AnalyteMapping)

	for instrumentID, analyteMappings := range analyteMappingsByInstrumentID {
		instrument.AnalyteMappings = analyteMappings
		for i := range instrument.AnalyteMappings {
			analyteMappingsIDs = append(analyteMappingsIDs, analyteMappings[i].ID)
			analyteMappingIDInstrumentIDMap[analyteMappings[i].ID] = instrumentID
			analyteMappingsByIDs[analyteMappings[i].ID] = &instrument.AnalyteMappings[i]
		}
	}
	channelMappingsByAnalyteMappingID, err := r.getChannelMappings(ctx, analyteMappingsIDs)
	if err != nil {
		return instrument, err
	}
	for analyteMappingID, channelMappings := range channelMappingsByAnalyteMappingID {
		analyteMappingsByIDs[analyteMappingID].ChannelMappings = channelMappings
	}

	resultMappingsByAnalyteMappingID, err := r.getResultMappings(ctx, analyteMappingsIDs)
	if err != nil {
		return instrument, err
	}
	for analyteMappingID, resultMappings := range resultMappingsByAnalyteMappingID {
		analyteMappingsByIDs[analyteMappingID].ResultMappings = resultMappings
	}

	requestMappingsByInstrumentID, err := r.getRequestMappings(ctx, instrumentIDs)
	if err != nil {
		return instrument, err
	}
	requestMappingIDs := make([]uuid.UUID, 0)
	requestMappingsByIDs := make(map[uuid.UUID]*RequestMapping)
	for _, requestMappings := range requestMappingsByInstrumentID {
		instrument.RequestMappings = requestMappings
		for i := range instrument.RequestMappings {
			requestMappingIDs = append(requestMappingIDs, requestMappings[i].ID)
			requestMappingsByIDs[requestMappings[i].ID] = &instrument.RequestMappings[i]
		}
	}

	requestMappingAnalyteIDs, err := r.getRequestMappingAnalytes(ctx, requestMappingIDs)
	if err != nil {
		return instrument, err
	}

	for requestMappingID, analyteIDs := range requestMappingAnalyteIDs {
		requestMappingsByIDs[requestMappingID].AnalyteIDs = analyteIDs
	}
	return instrument, nil
}

func (r *instrumentRepository) UpdateInstrument(ctx context.Context, instrument Instrument) error {
	oldInstrument, err := r.GetInstrumentByID(ctx, instrument.ID)
	if err != nil {
		return err
	}
	err = r.updateInstrument(ctx, instrument)
	if err != nil {
		return err
	}
	deletedAnalyteMappingIDs := make([]uuid.UUID, 0)
	deletedChannelMappingIDs := make([]uuid.UUID, 0)
	deletedResultMappingIDs := make([]uuid.UUID, 0)
	deletedRequestMappingIDs := make([]uuid.UUID, 0)

	for _, oldAnalyteMapping := range oldInstrument.AnalyteMappings {
		analyteMappingFound := false
		for _, newAnalyteMapping := range instrument.AnalyteMappings {
			if oldAnalyteMapping.ID == newAnalyteMapping.ID {
				for _, oldChannelMapping := range oldAnalyteMapping.ChannelMappings {
					channelMappingFound := false
					for _, newChannelMapping := range newAnalyteMapping.ChannelMappings {
						if oldChannelMapping.ID == newChannelMapping.ID {
							channelMappingFound = true
							break
						}
					}
					if !channelMappingFound {
						deletedChannelMappingIDs = append(deletedChannelMappingIDs, oldChannelMapping.ID)
					}
				}
				for _, oldResultMapping := range oldAnalyteMapping.ResultMappings {
					resultMappingFound := false
					for _, newResultMapping := range newAnalyteMapping.ResultMappings {
						if oldResultMapping.ID == newResultMapping.ID {
							resultMappingFound = true
							break
						}
					}
					if !resultMappingFound {
						deletedResultMappingIDs = append(deletedResultMappingIDs, oldResultMapping.ID)
					}
				}
				analyteMappingFound = true
				break
			}
		}
		if !analyteMappingFound {
			deletedAnalyteMappingIDs = append(deletedAnalyteMappingIDs, oldAnalyteMapping.ID)
		}
	}
	for _, oldRequestMapping := range oldInstrument.RequestMappings {
		found := false
		for _, newRequestMapping := range instrument.RequestMappings {
			if oldRequestMapping.ID == newRequestMapping.ID {
				found = true
				break
			}
		}
		if !found {
			deletedRequestMappingIDs = append(deletedRequestMappingIDs, oldRequestMapping.ID)
		}
	}

	err = r.deleteAnalyteMappings(ctx, deletedAnalyteMappingIDs)
	if err != nil {
		return err
	}
	err = r.deleteChannelMappings(ctx, deletedChannelMappingIDs)
	if err != nil {
		return err
	}
	err = r.deleteResultMappings(ctx, deletedResultMappingIDs)
	if err != nil {
		return err
	}
	err = r.deleteRequestMappings(ctx, deletedRequestMappingIDs)
	if err != nil {
		return err
	}

	newAnalyteMappings := make([]AnalyteMapping, 0)
	for _, analyteMapping := range instrument.AnalyteMappings {
		if analyteMapping.ID == uuid.Nil {
			newAnalyteMappings = append(newAnalyteMappings, analyteMapping)
		} else {
			err = r.updateAnalyteMapping(ctx, analyteMapping)
			if err != nil {
				return err
			}
			newChannelMappings := make([]ChannelMapping, 0)
			for _, channelMapping := range analyteMapping.ChannelMappings {
				if channelMapping.ID == uuid.Nil {
					newChannelMappings = append(newChannelMappings, channelMapping)
				} else {
					err = r.updateChannelMapping(ctx, channelMapping)
					if err != nil {
						return err
					}
				}
			}
			_, err = r.createChannelMappings(ctx, newChannelMappings, analyteMapping.ID)
			if err != nil {
				return err
			}

			newResultMappings := make([]ResultMapping, 0)
			for _, resultMapping := range analyteMapping.ResultMappings {
				if resultMapping.ID == uuid.Nil {
					newResultMappings = append(newResultMappings, resultMapping)
				} else {
					err = r.updateResultMapping(ctx, resultMapping)
					if err != nil {
						return err
					}
				}
			}
			_, err = r.createResultMappings(ctx, newResultMappings, analyteMapping.ID)
			if err != nil {
				return err
			}
		}
	}
	analyteMappingIDs, err := r.createAnalyteMappings(ctx, newAnalyteMappings, instrument.ID)
	if err != nil {
		return err
	}
	for i, analyteMappingID := range analyteMappingIDs {
		_, err = r.createChannelMappings(ctx, newAnalyteMappings[i].ChannelMappings, analyteMappingID)
		if err != nil {
			return err
		}
		_, err = r.createResultMappings(ctx, newAnalyteMappings[i].ResultMappings, analyteMappingID)
		if err != nil {
			return err
		}
	}
	newRequestMappings := make([]RequestMapping, 0)
	for _, requestMapping := range instrument.RequestMappings {
		if requestMapping.ID == uuid.Nil {
			newRequestMappings = append(newRequestMappings, requestMapping)
		} else {
			err = r.updateRequestMapping(ctx, requestMapping)
			if err != nil {
				return err
			}
			err = r.upsertRequestMappingAnalytes(ctx, map[uuid.UUID][]uuid.UUID{
				requestMapping.ID: requestMapping.AnalyteIDs,
			})
		}
	}
	requestMappingIDs, err := r.createRequestMappings(ctx, newRequestMappings, instrument.ID)
	if err != nil {
		return err
	}
	requestMappingsAnalytes := make(map[uuid.UUID][]uuid.UUID)
	for i := range requestMappingIDs {
		requestMappingsAnalytes[requestMappingIDs[i]] = newRequestMappings[i].AnalyteIDs
	}
	err = r.upsertRequestMappingAnalytes(ctx, requestMappingsAnalytes)
	if err != nil {
		return err
	}
	return nil
}

func (r *instrumentRepository) DeleteInstrument(ctx context.Context, id uuid.UUID) error {
	query := fmt.Sprintf(`UPDATE %s.sk_instruments SET deleted_at = timezone('utc', now()) WHERE id = $1;`, r.dbSchema)
	_, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		log.Error().Err(err).Msg(msgUpdateInstrumentFailed)
		return ErrUpdateInstrumentFailed
	}
	return nil
}

func (r *instrumentRepository) createInstrument(ctx context.Context, instrument Instrument) (uuid.UUID, error) {
	query := fmt.Sprintf(`INSERT INTO %s.sk_instruments(id, protocol_id, "name", hostname, client_port, enabled, connection_mode, running_mode, captureresults, capturediagnostics, replytoquery, status, sent_to_cerberus, timezone, file_encoding) 
		VALUES(id, :protocol_id, :name, :hostname, :client_port, :enabled, :connection_mode, :running_mode, :captureresults, :capturediagnostics, :replytoquery, :status, :sent_to_cerberus, :timezone, :file_encoding);`, r.dbSchema)
	instrument.ID = uuid.New()

	dao, err := convertInstrumentToDAO(instrument)
	if err != nil {
		return uuid.Nil, err
	}
	_, err = r.db.NamedExecContext(ctx, query, dao)
	if err != nil {
		log.Error().Err(err).Msg(msgCreateInstrumentFailed)
		return uuid.Nil, ErrCreateInstrumentFailed
	}
	return instrument.ID, nil
}

func (r *instrumentRepository) getInstruments(ctx context.Context) ([]Instrument, error) {
	query := `SELECT * FROM %s.sk_instruments WHERE deleted_at IS NULL ORDER BY name;`
	rows, err := r.db.QueryxContext(ctx, query)
	if err != nil {
		log.Error().Err(err).Msg(msgGetInstrumentsFailed)
		return nil, ErrGetInstrumentsFailed
	}
	defer rows.Close()
	instruments := make([]Instrument, 0)
	for rows.Next() {
		var dao instrumentDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetInstrumentsFailed)
			return nil, ErrGetInstrumentsFailed
		}
		var instrument Instrument
		instrument, err = convertInstrumentDaoToInstrument(dao)
		if err != nil {
			return nil, err
		}
		instruments = append(instruments, instrument)
	}
	return instruments, nil
}

func (r *instrumentRepository) getInstrumentChanges(ctx context.Context, timeFrom time.Time) ([]Instrument, error) {
	query := `SELECT * FROM %s.sk_instruments WHERE deleted_at >= $1 OR modified_at >= $1 ORDER BY name;`
	rows, err := r.db.QueryxContext(ctx, query, timeFrom)
	if err != nil {
		log.Error().Err(err).Msg(msgGetInstrumentChangesFailed)
		return nil, ErrGetInstrumentChangesFailed
	}
	defer rows.Close()
	instruments := make([]Instrument, 0)
	for rows.Next() {
		var dao instrumentDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetInstrumentChangesFailed)
			return nil, ErrGetInstrumentChangesFailed
		}
		var instrument Instrument
		instrument, err = convertInstrumentDaoToInstrument(dao)
		if err != nil {
			return nil, err
		}
		instruments = append(instruments, instrument)
	}
	return instruments, nil
}

func (r *instrumentRepository) getInstrumentByID(ctx context.Context, id uuid.UUID) (Instrument, error) {
	query := `SELECT * FROM %s.sk_instruments WHERE id = $1 AND deleted_at IS NULL;`
	var instrument Instrument
	row := r.db.QueryRowxContext(ctx, query, id)
	if row.Err() != nil {
		if row.Err() == sql.ErrNoRows {
			return instrument, ErrInstrumentNotFound
		}
		log.Error().Err(row.Err()).Msg(msgGetInstrumentByIDFailed)
		return instrument, ErrGetInstrumentByIDFailed
	}
	var dao instrumentDAO
	err := row.StructScan(&dao)
	if err != nil {
		log.Error().Err(row.Err()).Msg(msgGetInstrumentByIDFailed)
		return instrument, ErrGetInstrumentByIDFailed
	}
	instrument, err = convertInstrumentDaoToInstrument(dao)
	if err != nil {
		return instrument, err
	}
	return instrument, nil
}

func (r *instrumentRepository) updateInstrument(ctx context.Context, instrument Instrument) error {
	query := fmt.Sprintf(`UPDATE %s.sk_instruments SET protocol_id = :protocol_id, "name" = :name, hostname = :hostname, 
		 	client_port = :client_port, enabled = :enabled, connection_mode = :connection_mode, running_mode = :running_mode, 
		 	captureresults = :captureresults, capturediagnostics = :capturediagnostics, replytoquery = :replytoquery, status = :status,
		 	sent_to_cerberus = :sent_to_cerberus, timezone = :timezone, file_encoding = :file_encoding, modified_at = timezone('utc',now()) 
		 WHERE id = :id`, r.dbSchema)
	dao, err := convertInstrumentToDAO(instrument)
	if err != nil {
		return err
	}
	_, err = r.db.NamedExecContext(ctx, query, dao)
	if err != nil {
		log.Error().Err(err).Msg(msgUpdateInstrumentFailed)
		return ErrUpdateInstrumentFailed
	}
	return nil
}

func (r *instrumentRepository) createAnalyteMappings(ctx context.Context, analyteMappings []AnalyteMapping, instrumentID uuid.UUID) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, len(analyteMappings))
	if len(analyteMappings) == 0 {
		return ids, nil
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_analyte_mappings(:id, instrument_id, instrument_analyte, analyte_id) VALUES(:id, :instrument_id, :instrument_analyte, :analyte_id);`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertAnalyteMappingsToDAOs(analyteMappings, instrumentID))
	if err != nil {
		log.Error().Err(err).Msg(msgCreateAnalyteMappingsFailed)
		return ids, ErrCreateAnalyteMappingsFailed
	}
	return ids, nil
}

func (r *instrumentRepository) getAnalyteMappings(ctx context.Context, instrumentIDs []uuid.UUID) (map[uuid.UUID][]AnalyteMapping, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_analyte_mappings WHERE instrument_id IN (?) AND deleted_at IS NULL;`, r.dbSchema)
	query, args, _ := sqlx.In(query, instrumentIDs)
	query = r.db.Rebind(query)
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgGetAnalyteMappingsFailed)
		return nil, ErrGetAnalyteMappingsFailed
	}
	defer rows.Close()
	analyteMappingsByInstrumentID := make(map[uuid.UUID][]AnalyteMapping)
	for rows.Next() {
		var dao analyteMappingDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetAnalyteMappingsFailed)
			return nil, ErrGetAnalyteMappingsFailed
		}
		analyteMappingsByInstrumentID[dao.InstrumentID] = append(analyteMappingsByInstrumentID[dao.InstrumentID], convertAnalyteMappingDaoToAnalyteMapping(dao))
	}
	return analyteMappingsByInstrumentID, nil
}

func (r *instrumentRepository) updateAnalyteMapping(ctx context.Context, analyteMapping AnalyteMapping) error {
	query := fmt.Sprintf(`UDPATE %s.sk_analyte_mappings SET instrument_analyte = :instrument_analyte, analyte_id = :analyte_id, modified_at = timezone('utc', now()) WHERE id = :id`, r.dbSchema)
	dao := convertAnalyteMappingToDAO(analyteMapping, uuid.Nil)
	_, err := r.db.ExecContext(ctx, query, dao)
	if err != nil {
		log.Error().Err(err).Msg(msgUpdateAnalyteMappingFailed)
		return ErrUpdateAnalyteMappingFailed
	}
	return nil
}

func (r *instrumentRepository) deleteAnalyteMappings(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	query := fmt.Sprintf(`UDPATE %s.sk_analyte_mappings SET deleted_at = timezone('utc', now()) WHERE id IN (?);`, r.dbSchema)
	query, args, _ := sqlx.In(query, ids)
	_, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgDeleteAnalyteMappingFailed)
		return ErrDeleteAnalyteMappingFailed
	}
	return nil
}

func (r *instrumentRepository) createChannelMappings(ctx context.Context, channelMappings []ChannelMapping, analyteMappingID uuid.UUID) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, len(channelMappings))
	if len(channelMappings) == 0 {
		return ids, nil
	}
	for i := range channelMappings {
		channelMappings[i].ID = uuid.New()
		ids[i] = channelMappings[i].ID
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_channel_mappings(id, instrument_channel, channel_id, analyte_mapping_id, created_at) 
		VALUES(:id, :instrument_channel, :channel_id, :analyte_mapping_id, timezone('utc',now()));`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertChannelMappingsToDAOs(channelMappings, analyteMappingID))
	if err != nil {
		log.Error().Err(err).Msg(msgCreateChannelMappingsFailed)
		return ids, ErrCreateChannelMappingsFailed
	}
	return ids, nil
}

func (r *instrumentRepository) getChannelMappings(ctx context.Context, analyteMappingIDs []uuid.UUID) (map[uuid.UUID][]ChannelMapping, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_channel_mappings WHERE analyte_mapping_id IN (?) AND deleted_at IS NULL;`, r.dbSchema)
	query, args, _ := sqlx.In(query, analyteMappingIDs)
	query = r.db.Rebind(query)
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgGetChannelMappingsFailed)
		return nil, ErrGetChannelMappingsFailed
	}
	defer rows.Close()
	channelMappingsByAnalyteMappingID := make(map[uuid.UUID][]ChannelMapping)
	for rows.Next() {
		var dao channelMappingDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetChannelMappingsFailed)
			return nil, ErrGetChannelMappingsFailed
		}
		channelMappingsByAnalyteMappingID[dao.AnalyteMappingID] = append(channelMappingsByAnalyteMappingID[dao.AnalyteMappingID], convertChannelMappingDaoToChannelMapping(dao))
	}
	return channelMappingsByAnalyteMappingID, nil
}

func (r *instrumentRepository) updateChannelMapping(ctx context.Context, channelMapping ChannelMapping) error {
	query := fmt.Sprintf(`UDPATE %s.sk_channel_mappings SET instrument_channel = :instrument_channel, channel_id = :channel_id, modified_at = timezone('utc', now()) WHERE id = :id`, r.dbSchema)
	dao := convertChannelMappingToDAO(channelMapping, uuid.Nil)
	_, err := r.db.ExecContext(ctx, query, dao)
	if err != nil {
		log.Error().Err(err).Msg(msgUpdateChannelMappingFailed)
		return ErrUpdateChannelMappingFailed
	}
	return nil
}

func (r *instrumentRepository) deleteChannelMappings(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	query := fmt.Sprintf(`UDPATE %s.sk_channel_mappings SET deleted_at = timezone('utc', now()) WHERE id IN (?);`, r.dbSchema)
	query, args, _ := sqlx.In(query, ids)
	_, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgDeleteChannelMappingFailed)
		return ErrDeleteChannelMappingFailed
	}
	return nil
}

func (r *instrumentRepository) createResultMappings(ctx context.Context, resultMappings []ResultMapping, analyteMappingID uuid.UUID) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, len(resultMappings))
	if len(resultMappings) == 0 {
		return ids, nil
	}
	for i := range resultMappings {
		resultMappings[i].ID = uuid.New()
		ids[i] = resultMappings[i].ID
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_result_mappings(id, analyte_mapping_id, key, value, index, created_at) 
		VALUES(:id, :analyte_mapping_id, :key, :value, :index, timezone('utc', now()));`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertResultMappingsToDAOs(resultMappings, analyteMappingID))
	if err != nil {
		log.Error().Err(err).Msg(msgCreateChannelMappingsFailed)
		return ids, ErrCreateChannelMappingsFailed
	}
	return ids, nil
}

func (r *instrumentRepository) getResultMappings(ctx context.Context, analyteMappingIDs []uuid.UUID) (map[uuid.UUID][]ResultMapping, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_result_mappings WHERE analyte_mapping_id IN (?) AND deleted_at IS NULL;`, r.dbSchema)
	query, args, _ := sqlx.In(query, analyteMappingIDs)
	query = r.db.Rebind(query)
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgGetResultMappingsFailed)
		return nil, ErrGetResultMappingsFailed
	}
	defer rows.Close()
	resultMappingsByAnalyteMappingID := make(map[uuid.UUID][]ResultMapping)
	for rows.Next() {
		var dao resultMappingDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetResultMappingsFailed)
			return nil, ErrGetResultMappingsFailed
		}
		resultMappingsByAnalyteMappingID[dao.AnalyteMappingID] = append(resultMappingsByAnalyteMappingID[dao.AnalyteMappingID], convertResultMappingDaoToChannelMapping(dao))
	}
	return resultMappingsByAnalyteMappingID, nil
}

func (r *instrumentRepository) updateResultMapping(ctx context.Context, resultMapping ResultMapping) error {
	query := fmt.Sprintf(`UDPATE %s.sk_result_mappings SET key = :key, value = :value, index = :index, modified_at = timezone('utc', now()) WHERE id = :id`, r.dbSchema)
	dao := convertResultMappingToDAO(resultMapping, uuid.Nil)
	_, err := r.db.ExecContext(ctx, query, dao)
	if err != nil {
		log.Error().Err(err).Msg(msgUpdateResultMappingFailed)
		return ErrUpdateResultMappingFailed
	}
	return nil
}

func (r *instrumentRepository) deleteResultMappings(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	query := fmt.Sprintf(`UDPATE %s.sk_result_mappings SET deleted_at = timezone('utc', now()) WHERE id IN (?);`, r.dbSchema)
	query, args, _ := sqlx.In(query, ids)
	_, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgDeleteResultMappingFailed)
		return ErrDeleteResultMappingFailed
	}
	return nil
}

func (r *instrumentRepository) createRequestMappings(ctx context.Context, requestMappings []RequestMapping, instrumentID uuid.UUID) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, len(requestMappings))
	if len(requestMappings) == 0 {
		return ids, nil
	}
	for i := range requestMappings {
		requestMappings[i].ID = uuid.New()
		ids[i] = requestMappings[i].ID
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_request_mappings(id, code, instrument_id, created_at) 
		VALUES(:id, :code, :instrument_id, timezone('utc', now()));`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertRequestMappingsToDAOs(requestMappings, instrumentID))
	if err != nil {
		log.Error().Err(err).Msg(msgCreateAnalyteMappingsFailed)
		return ids, ErrCreateAnalyteMappingsFailed
	}
	return ids, nil
}

func (r *instrumentRepository) upsertRequestMappingAnalytes(ctx context.Context, analyteIDsByRequestMappingID map[uuid.UUID][]uuid.UUID) error {
	requestMappingAnalyteDAOs := make([]requestMappingAnalyteDAO, 0)
	for requestMappingID, analyteIDs := range analyteIDsByRequestMappingID {
		for _, analyteID := range analyteIDs {
			requestMappingAnalyteDAOs = append(requestMappingAnalyteDAOs, requestMappingAnalyteDAO{
				ID:               uuid.New(),
				RequestMappingID: requestMappingID,
				AnalyteID:        analyteID,
				CreatedAt:        time.Time{},
				ModifiedAt:       sql.NullTime{},
				DeletedAt:        sql.NullTime{},
			})
		}
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_request_mapping_analytes(id, analyte_id, request_mapping_id, created_at) 
		VALUES(:id, :analyte_id, :request_mapping_id, :created_at) ON CONFLICT ON CONSTRAINT sk_unique_request_mapping_analytes DO NOTHING;`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, requestMappingAnalyteDAOs)
	if err != nil {
		log.Error().Err(err).Msg(msgCreateAnalyteMappingsFailed)
		return ErrCreateAnalyteMappingsFailed
	}
	return nil
}

func (r *instrumentRepository) updateRequestMapping(ctx context.Context, requestMapping RequestMapping) error {
	query := fmt.Sprintf(`UDPATE %s.sk_result_mappings SET code = :code, modified_at = timezone('utc', now()) WHERE id = :id`, r.dbSchema)
	dao := convertRequestMappingToDAO(requestMapping, uuid.Nil)
	_, err := r.db.ExecContext(ctx, query, dao)
	if err != nil {
		log.Error().Err(err).Msg(msgUpdateRequestMappingFailed)
		return ErrUpdateRequestMappingFailed
	}
	return nil
}

func (r *instrumentRepository) deleteRequestMappingAnalytes(ctx context.Context, requestMappingID uuid.UUID, analyteIDs []uuid.UUID) error {
	query := fmt.Sprintf(`UPDATE %s.sk_request_mapping_analytes SET deleted_at = timezone('utc', now()) WHERE request_mapping_id = ? AND analyte_id IN (?);`, r.dbSchema)
	query, args, _ := sqlx.In(query, requestMappingID, analyteIDs)
	query = r.db.Rebind(query)
	_, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgDeleteRequestMappingAnalytesFailed)
		return ErrDeleteRequestMappingAnalytesFailed
	}
	return nil
}

func (r *instrumentRepository) deleteRequestMappings(ctx context.Context, requestMappingIDs []uuid.UUID) error {
	query := fmt.Sprintf(`UPDATE %s.sk_request_mappings SET deleted_at = timezone('utc', now()) WHERE id IN (?);`, r.dbSchema)
	query, args, _ := sqlx.In(query, requestMappingIDs)
	_, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgDeleteRequestMappingsFailed)
		return ErrDeleteRequestMappingsFailed
	}
	return nil
}

func (r *instrumentRepository) getRequestMappings(ctx context.Context, instrumentIDs []uuid.UUID) (map[uuid.UUID][]RequestMapping, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_request_mappings WHERE instrument_id IN (?) AND deleted_at IS NULL;`, r.dbSchema)
	query, args, _ := sqlx.In(query, instrumentIDs)
	query = r.db.Rebind(query)
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgGetRequestMappingsFailed)
		return nil, ErrGetRequestMappingsFailed
	}
	defer rows.Close()
	analyteMappingsByInstrumentID := make(map[uuid.UUID][]RequestMapping)
	for rows.Next() {
		var dao requestMappingDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetRequestMappingsFailed)
			return nil, ErrGetRequestMappingsFailed
		}
		analyteMappingsByInstrumentID[dao.InstrumentID] = append(analyteMappingsByInstrumentID[dao.InstrumentID], convertRequestMappingDaoToAnalyteMapping(dao))
	}
	return analyteMappingsByInstrumentID, nil
}

func (r *instrumentRepository) getRequestMappingAnalytes(ctx context.Context, requestMappingIDs []uuid.UUID) (map[uuid.UUID][]uuid.UUID, error) {
	query := fmt.Sprintf(`SELECT request_mapping_id, analyte_id FROM %s.sk_request_mapping_analytes WHERE request_mapping_id IN (?);`, r.dbSchema)
	query, args, _ := sqlx.In(query, requestMappingIDs)
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgGetRequestMappingAnalytesFailed)
		return nil, ErrGetRequestMappingAnalytesFailed
	}
	defer rows.Close()
	analyteIDsByRequestMappingIDs := make(map[uuid.UUID][]uuid.UUID)
	for rows.Next() {
		var requestMappingID, analyteID uuid.UUID
		err = rows.Scan(&requestMappingID, &analyteID)
		if err != nil {
			log.Error().Err(err).Msg(msgGetRequestMappingAnalytesFailed)
			return nil, ErrGetRequestMappingAnalytesFailed
		}
		analyteIDsByRequestMappingIDs[requestMappingID] = append(analyteIDsByRequestMappingIDs[requestMappingID], analyteID)
	}
	return analyteIDsByRequestMappingIDs, nil
}

func convertRequestMappingDaoToAnalyteMapping(dao requestMappingDAO) RequestMapping {
	return RequestMapping{
		ID:         dao.ID,
		Code:       dao.Code,
		AnalyteIDs: nil,
	}
}

func (r *instrumentRepository) UpsertSupportedProtocol(ctx context.Context, id uuid.UUID, name string, description string) error {
	query := fmt.Sprintf(`INSERT INTO %s.sk_supported_protocols(id, "name", description) VALUES($1, $2, $3) ON CONFLICT (id) DO UPDATE SET name = $2, description = $3;`, r.dbSchema)
	_, err := r.db.ExecContext(ctx, query, id, name, sql.NullString{
		String: description,
		Valid:  len(description) > 0,
	})
	if err != nil {
		log.Error().Err(err).Msg("upsert supported protocol failed")
		return err
	}
	return nil
}

func convertInstrumentToDAO(instrument Instrument) (instrumentDAO, error) {
	dao := instrumentDAO{
		ID:                 instrument.ID,
		ProtocolID:         instrument.ProtocolID,
		Name:               instrument.Name,
		HostName:           instrument.Hostname,
		ClientPort:         sql.NullInt32{},
		Enabled:            instrument.Enabled,
		CaptureResults:     instrument.CaptureResults,
		CaptureDiagnostics: instrument.CaptureDiagnostics,
		ReplyToQuery:       instrument.ReplyToQuery,
		Status:             instrument.Status,
		Timezone:           instrument.Timezone,
		FileEncoding:       instrument.FileEncoding,
	}
	switch instrument.ConnectionMode {
	case TCPClientMode, TCPServerMode, FTP, HTTP, TCPMixed:
		dao.ConnectionMode = string(instrument.ConnectionMode)
	default:
		return dao, ErrInvalidConnectionMode
	}
	switch instrument.ResultMode {
	case Simulation, Qualify, Production:
		dao.RunningMode = string(instrument.ResultMode)
	default:
		return dao, ErrInvalidResultMode
	}
	if instrument.ClientPort != nil {
		dao.ClientPort = sql.NullInt32{
			Int32: int32(*instrument.ClientPort),
			Valid: true,
		}
	}
	return dao, nil
}

func convertInstrumentDaoToInstrument(dao instrumentDAO) (Instrument, error) {
	instrument := Instrument{
		ID:                 dao.ID,
		ProtocolID:         dao.ProtocolID,
		Name:               dao.Name,
		Hostname:           dao.HostName,
		Enabled:            dao.Enabled,
		CaptureResults:     dao.CaptureResults,
		CaptureDiagnostics: dao.CaptureDiagnostics,
		ReplyToQuery:       dao.ReplyToQuery,
		Status:             dao.Status,
		Timezone:           dao.Timezone,
		FileEncoding:       dao.FileEncoding,
		CreatedAt:          dao.CreatedAt,
	}
	switch dao.ConnectionMode {
	case "TCP_CLIENT_ONLY":
		instrument.ConnectionMode = TCPClientMode
	case "TCP_SERVER_ONLY":
		instrument.ConnectionMode = TCPServerMode
	case "FTP_SFTP":
		instrument.ConnectionMode = FTP
	case "HTTP":
		instrument.ConnectionMode = HTTP
	case "TCP_MIXED":
		instrument.ConnectionMode = TCPMixed
	default:
		return instrument, ErrInvalidConnectionMode
	}
	switch dao.RunningMode {
	case "SIMULATION":
		instrument.ResultMode = Simulation
	case "QUALIFY":
		instrument.ResultMode = Qualify
	case "PRODUCTION":
		instrument.ResultMode = Production
	default:
		return instrument, ErrInvalidResultMode
	}
	if dao.ClientPort.Valid {
		clientPort := int(dao.ClientPort.Int32)
		instrument.ClientPort = &clientPort
	}

	return instrument, nil
}

func convertAnalyteMappingToDAO(analyteMapping AnalyteMapping, instrumentID uuid.UUID) analyteMappingDAO {
	return analyteMappingDAO{
		ID:                analyteMapping.ID,
		InstrumentID:      instrumentID,
		InstrumentAnalyte: analyteMapping.InstrumentAnalyte,
		AnalyteID:         analyteMapping.AnalyteID,
	}
}

func convertAnalyteMappingsToDAOs(analyteMappings []AnalyteMapping, instrumentID uuid.UUID) []analyteMappingDAO {
	analyteMappingDAOs := make([]analyteMappingDAO, len(analyteMappings))
	for i := range analyteMappings {
		analyteMappingDAOs[i] = convertAnalyteMappingToDAO(analyteMappings[i], instrumentID)
	}
	return analyteMappingDAOs
}

func convertAnalyteMappingDaoToAnalyteMapping(dao analyteMappingDAO) AnalyteMapping {
	return AnalyteMapping{
		ID:                dao.ID,
		InstrumentAnalyte: dao.InstrumentAnalyte,
		AnalyteID:         dao.AnalyteID,
	}
}

func convertChannelMappingToDAO(channelMapping ChannelMapping, analyteMappingID uuid.UUID) channelMappingDAO {
	return channelMappingDAO{
		ID:                channelMapping.ID,
		ChannelID:         channelMapping.ChannelID,
		AnalyteMappingID:  analyteMappingID,
		InstrumentChannel: channelMapping.InstrumentChannel,
	}
}

func convertChannelMappingsToDAOs(channelMappings []ChannelMapping, analyteMappingID uuid.UUID) []channelMappingDAO {
	channelMappingDAOs := make([]channelMappingDAO, len(channelMappings))
	for i := range channelMappings {
		channelMappingDAOs = append(channelMappingDAOs, convertChannelMappingToDAO(channelMappings[i], analyteMappingID))
	}
	return channelMappingDAOs
}

func convertChannelMappingDaoToChannelMapping(dao channelMappingDAO) ChannelMapping {
	return ChannelMapping{
		ID:                dao.ID,
		InstrumentChannel: dao.InstrumentChannel,
		ChannelID:         dao.ChannelID,
	}
}

func convertResultMappingToDAO(resultMapping ResultMapping, analyteMappingID uuid.UUID) resultMappingDAO {
	return resultMappingDAO{
		ID:               resultMapping.ID,
		AnalyteMappingID: analyteMappingID,
		Key:              resultMapping.Key,
		Value:            resultMapping.Value,
		Index:            resultMapping.Index,
	}
}

func convertResultMappingsToDAOs(resultMappings []ResultMapping, analyteMappingID uuid.UUID) []resultMappingDAO {
	resultMappingDAOs := make([]resultMappingDAO, len(resultMappings))
	for i := range resultMappings {
		resultMappingDAOs[i] = convertResultMappingToDAO(resultMappings[i], analyteMappingID)
	}
	return resultMappingDAOs
}

func convertResultMappingDaoToChannelMapping(dao resultMappingDAO) ResultMapping {
	return ResultMapping{
		ID:    dao.ID,
		Key:   dao.Key,
		Value: dao.Value,
		Index: dao.Index,
	}
}

func convertRequestMappingToDAO(requestMapping RequestMapping, instrumentID uuid.UUID) requestMappingDAO {
	return requestMappingDAO{
		ID:           requestMapping.ID,
		InstrumentID: instrumentID,
		Code:         requestMapping.Code,
	}
}

func convertRequestMappingsToDAOs(requestMappings []RequestMapping, instrumentID uuid.UUID) []requestMappingDAO {
	requestMappingDAOs := make([]requestMappingDAO, len(requestMappings))
	for i := range requestMappings {
		requestMappingDAOs[i] = convertRequestMappingToDAO(requestMappings[i], instrumentID)
	}
	return requestMappingDAOs
}

func NewInstrumentRepository(db db.DbConnector, dbSchema string) InstrumentRepository {
	return &instrumentRepository{
		db:       db,
		dbSchema: dbSchema,
	}
}
