package skeleton

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/blutspende/skeleton/utils"
	"strings"
	"time"

	"github.com/blutspende/skeleton/db"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

const (
	msgInvalidConnectionMode              = "invalid connection mode"
	msgInvalidResultMode                  = "invalid result mode"
	msgInvalidInstrumentType              = "invalid instrument type"
	msgCreateInstrumentFailed             = "create instrument failed"
	msgGetInstrumentsFailed               = "get instruments failed"
	msgGetInstrumentChangesFailed         = "get changed instruments failed"
	msgGetInstrumentByIDFailed            = "get instrument by id failed"
	msgGetInstrumentByIPFailed            = "get instrument by IP failed"
	msgInstrumentNotFound                 = "instrument not found"
	msgUpdateInstrumentFailed             = "update instrument failed"
	msgDeleteInstrumentFailed             = "delete instrument failed"
	msgMarkInstrumentSentToCerberusFailed = "mark instrument as sent to cerberus failed"
	msgCreateFtpConfigFailed              = "create FTP config failed"
	msgUpdateFtpConfigFailed              = "update FTP config failed"
	msgDeleteFtpConfigFailed              = "delete FTP config failed"
	msgGetFtpConfigFailed                 = "get ftp config by instrument id failed"
	msgFtpConfigNotFound                  = "ftp config not found"
	msgFtpConfigExistsFailed              = "failed to check whether ftp config exists"
	msgGetProtocolByIDFailed              = "get protocol by ID failed"
	msgUpsertSupportedProtocolFailed      = "upsert supported protocol failed"
	msgUpsertProtocolAbilitiesFailed      = "upsert protocol abilities failed"
	msgUpdateInstrumentStatusFailed       = "update instrument status failed"
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
	msgGetEncodingsFailed                 = "get encodings failed"
	msgGetProtocolSettingsFailed          = "get protocol settings failed"
	msgUpsertProtocolSettingsFailed       = "upsert protocol settings failed"
	msgDeleteProtocolMappingFailed        = "delete protocol settings failed"
	msgGetInstrumentsSettingsFailed       = "get instruments settings failed"
	msgUpsertInstrumentSettingsFailed     = "upsert instrument settings failed"
	msgDeleteInstrumentSettingsFailed     = "delete instrument settings failed"
	msgCheckAnalyteUsageFailed            = "check analyte usage failed"
)

var (
	ErrInvalidConnectionMode              = errors.New(msgInvalidConnectionMode)
	ErrInvalidResultMode                  = errors.New(msgInvalidResultMode)
	ErrInvalidInstrumentType              = errors.New(msgInvalidInstrumentType)
	ErrCreateInstrumentFailed             = errors.New(msgCreateInstrumentFailed)
	ErrGetInstrumentsFailed               = errors.New(msgGetInstrumentsFailed)
	ErrGetInstrumentChangesFailed         = errors.New(msgGetInstrumentChangesFailed)
	ErrGetInstrumentByIDFailed            = errors.New(msgGetInstrumentByIDFailed)
	ErrGetInstrumentByIPFailed            = errors.New(msgGetInstrumentByIPFailed)
	ErrInstrumentNotFound                 = errors.New(msgInstrumentNotFound)
	ErrUpdateInstrumentFailed             = errors.New(msgUpdateInstrumentFailed)
	ErrDeleteInstrumentFailed             = errors.New(msgDeleteInstrumentFailed)
	ErrMarkInstrumentSentToCerberusFailed = errors.New(msgMarkInstrumentSentToCerberusFailed)
	ErrCreateFtpConfigFailed              = errors.New(msgCreateFtpConfigFailed)
	ErrUpdateFtpConfigFailed              = errors.New(msgUpdateFtpConfigFailed)
	ErrDeleteFtpConfigFailed              = errors.New(msgDeleteFtpConfigFailed)
	ErrGetFtpConfigFailed                 = errors.New(msgGetFtpConfigFailed)
	ErrFtpConfigNotFound                  = errors.New(msgFtpConfigNotFound)
	ErrFtpConfigExistsFailed              = errors.New(msgFtpConfigExistsFailed)
	ErrGetProtocolByIDFailed              = errors.New(msgGetProtocolByIDFailed)
	ErrUpsertSupportedProtocolFailed      = errors.New(msgUpsertSupportedProtocolFailed)
	ErrUpsertProtocolAbilitiesFailed      = errors.New(msgUpsertProtocolAbilitiesFailed)
	ErrUpdateInstrumentStatusFailed       = errors.New(msgUpdateInstrumentStatusFailed)
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
	ErrGetEncodingsFailed                 = errors.New(msgGetEncodingsFailed)
	ErrGetProtocolSettingsFailed          = errors.New(msgGetProtocolSettingsFailed)
	ErrUpsertProtocolSettingsFailed       = errors.New(msgUpsertProtocolSettingsFailed)
	ErrDeleteProtocolMappingFailed        = errors.New(msgDeleteProtocolMappingFailed)
	ErrGetInstrumentsSettingsFailed       = errors.New(msgGetInstrumentsSettingsFailed)
	ErrUpsertInstrumentSettingsFailed     = errors.New(msgUpsertInstrumentSettingsFailed)
	ErrDeleteInstrumentSettingsFailed     = errors.New(msgDeleteInstrumentSettingsFailed)
	ErrCheckAnalyteUsageFailed            = errors.New(msgCheckAnalyteUsageFailed)
)

type instrumentDAO struct {
	ID                 uuid.UUID      `db:"id"`
	Type               InstrumentType `db:"type"`
	ProtocolID         uuid.UUID      `db:"protocol_id"`
	Name               string         `db:"name"`
	HostName           string         `db:"hostname"`
	ClientPort         sql.NullInt32  `db:"client_port"`
	Enabled            bool           `db:"enabled"`
	ConnectionMode     string         `db:"connection_mode"`
	RunningMode        ResultMode     `db:"running_mode"`
	CaptureResults     bool           `db:"captureresults"`
	CaptureDiagnostics bool           `db:"capturediagnostics"`
	ReplyToQuery       bool           `db:"replytoquery"`
	Status             string         `db:"status"`
	SentToCerberus     bool           `db:"sent_to_cerberus"`
	Timezone           string         `db:"timezone"`
	FileEncoding       string         `db:"file_encoding"`
	CreatedAt          time.Time      `db:"created_at"`
	ModifiedAt         sql.NullTime   `db:"modified_at"`
	DeletedAt          sql.NullTime   `db:"deleted_at"`
}

type ftpConfigDAO struct {
	ID               uuid.UUID    `db:"id"`
	InstrumentId     uuid.UUID    `db:"instrument_id"`
	Username         string       `db:"username"`
	Password         string       `db:"password"`
	RemotePath       string       `db:"remote_path"`
	FileMask         string       `db:"file_mask"`
	ResultRemotePath string       `db:"result_remote_path"`
	FileSuffix       string       `db:"file_suffix"`
	FtpServerType    string       `db:"ftp_server_type"`
	DeletedAt        sql.NullTime `db:"deleted_at"`
}

type analyteMappingDAO struct {
	ID                uuid.UUID    `db:"id"`
	InstrumentID      uuid.UUID    `db:"instrument_id"`
	InstrumentAnalyte string       `db:"instrument_analyte"`
	AnalyteID         uuid.UUID    `db:"analyte_id"`
	ResultType        ResultType   `db:"result_type"`
	CreatedAt         time.Time    `db:"created_at"`
	ModifiedAt        sql.NullTime `db:"modified_at"`
	DeletedAt         sql.NullTime `db:"deleted_at"`
	ChannelMapping    []channelMappingDAO
	ResultMapping     []resultMappingDAO
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
	IsDefault    bool         `db:"is_default"`
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

type instrumentSettingDao struct {
	ID                uuid.UUID    `db:"id"`
	InstrumentID      uuid.UUID    `db:"instrument_id"`
	ProtocolSettingID uuid.UUID    `db:"protocol_setting_id"`
	Value             string       `db:"value"`
	CreatedAt         time.Time    `db:"created_at"`
	ModifiedAt        sql.NullTime `db:"modified_at"`
	DeletedAt         sql.NullTime `db:"deleted_at"`
}

type supportedProtocolDAO struct {
	ID          uuid.UUID
	Name        Protocol
	Description sql.NullString
}

type protocolAbilityDAO struct {
	ID                      uuid.UUID    `db:"id"`
	ProtocolID              uuid.UUID    `db:"protocol_id"`
	ConnectionMode          string       `db:"connection_mode"`
	Abilities               string       `db:"abilities"`
	RequestMappingAvailable bool         `db:"request_mapping_available"`
	CreatedAt               time.Time    `db:"created_at"`
	ModifiedAt              sql.NullTime `db:"modified_at"`
	DeletedAt               sql.NullTime `db:"deleted_at"`
}

type protocolSettingDAO struct {
	ID          uuid.UUID           `db:"id"`
	ProtocolID  uuid.UUID           `db:"protocol_id"`
	Key         string              `db:"key"`
	Type        ProtocolSettingType `db:"type"`
	Description sql.NullString      `db:"description"`
	CreatedAt   time.Time           `db:"created_at"`
	ModifiedAt  sql.NullTime        `db:"modified_at"`
	DeletedAt   sql.NullTime        `db:"deleted_at"`
}

type instrumentRepository struct {
	db       db.DbConnector
	dbSchema string
}

type InstrumentRepository interface {
	CreateInstrument(ctx context.Context, instrument Instrument) (uuid.UUID, error)
	GetInstruments(ctx context.Context) ([]Instrument, error)
	GetInstrumentChanges(ctx context.Context, timeFrom time.Time) ([]Instrument, error)
	GetInstrumentByID(ctx context.Context, id uuid.UUID) (Instrument, error)
	GetInstrumentByIP(ctx context.Context, ip string) (Instrument, error)
	UpdateInstrument(ctx context.Context, instrument Instrument) error
	DeleteInstrument(ctx context.Context, id uuid.UUID) error
	CreateFtpConfig(ctx context.Context, ftpConfig FTPConfig) error
	GetFtpConfigByInstrumentId(ctx context.Context, instrumentId uuid.UUID) (FTPConfig, error)
	FtpConfigExists(ctx context.Context, instrumentId uuid.UUID) (bool, error)
	UpdateFtpConfig(ctx context.Context, ftpConfig FTPConfig) error
	DeleteFtpConfig(ctx context.Context, instrumentId uuid.UUID) error
	MarkAsSentToCerberus(ctx context.Context, id uuid.UUID) error
	GetUnsentToCerberus(ctx context.Context) ([]uuid.UUID, error)
	GetProtocolByID(ctx context.Context, id uuid.UUID) (SupportedProtocol, error)
	GetSupportedProtocols(ctx context.Context) ([]SupportedProtocol, error)
	UpsertSupportedProtocol(ctx context.Context, id uuid.UUID, name string, description string) error
	GetProtocolAbilities(ctx context.Context, protocolID uuid.UUID) ([]ProtocolAbility, error)
	UpsertProtocolAbilities(ctx context.Context, protocolID uuid.UUID, protocolAbilities []ProtocolAbility) error
	GetProtocolSettings(ctx context.Context, protocolID uuid.UUID) ([]ProtocolSetting, error)
	UpsertProtocolSetting(ctx context.Context, protocolID uuid.UUID, protocolSetting ProtocolSetting) error
	DeleteProtocolSettings(ctx context.Context, protocolSettingIDs []uuid.UUID) error
	UpdateInstrumentStatus(ctx context.Context, id uuid.UUID, status InstrumentStatus) error
	CreateAnalyteMappings(ctx context.Context, analyteMappings []AnalyteMapping, instrumentID uuid.UUID) ([]uuid.UUID, error)
	GetAnalyteMappings(ctx context.Context, instrumentIDs []uuid.UUID) (map[uuid.UUID][]AnalyteMapping, error)
	UpdateAnalyteMapping(ctx context.Context, analyteMapping AnalyteMapping) error
	DeleteAnalyteMappings(ctx context.Context, ids []uuid.UUID) error
	CreateChannelMappings(ctx context.Context, channelMappings []ChannelMapping, analyteMappingID uuid.UUID) ([]uuid.UUID, error)
	GetChannelMappings(ctx context.Context, analyteMappingIDs []uuid.UUID) (map[uuid.UUID][]ChannelMapping, error)
	UpdateChannelMapping(ctx context.Context, channelMapping ChannelMapping) error
	DeleteChannelMappings(ctx context.Context, ids []uuid.UUID) error
	CreateResultMappings(ctx context.Context, resultMappings []ResultMapping, analyteMappingID uuid.UUID) ([]uuid.UUID, error)
	GetResultMappings(ctx context.Context, analyteMappingIDs []uuid.UUID) (map[uuid.UUID][]ResultMapping, error)
	UpdateResultMapping(ctx context.Context, resultMapping ResultMapping) error
	DeleteResultMappings(ctx context.Context, ids []uuid.UUID) error
	CreateRequestMappings(ctx context.Context, requestMappings []RequestMapping, instrumentID uuid.UUID) ([]uuid.UUID, error)
	UpsertRequestMappingAnalytes(ctx context.Context, analyteIDsByRequestMappingID map[uuid.UUID][]uuid.UUID) error
	UpdateRequestMapping(ctx context.Context, requestMapping RequestMapping) error
	GetRequestMappings(ctx context.Context, instrumentIDs []uuid.UUID) (map[uuid.UUID][]RequestMapping, error)
	GetRequestMappingAnalytes(ctx context.Context, requestMappingIDs []uuid.UUID) (map[uuid.UUID][]uuid.UUID, error)
	DeleteRequestMappings(ctx context.Context, requestMappingIDs []uuid.UUID) error
	DeleteRequestMappingAnalytes(ctx context.Context, requestMappingID uuid.UUID, analyteIDs []uuid.UUID) error
	GetEncodings(ctx context.Context) ([]string, error)
	GetInstrumentsSettings(ctx context.Context, instrumentIDs []uuid.UUID) (map[uuid.UUID][]InstrumentSetting, error)
	UpsertInstrumentSetting(ctx context.Context, instrumentID uuid.UUID, setting InstrumentSetting) error
	DeleteInstrumentSettings(ctx context.Context, ids []uuid.UUID) error
	CheckAnalytesUsage(ctx context.Context, analyteIDs []uuid.UUID) (map[uuid.UUID][]Instrument, error)
	CreateTransaction() (db.DbConnector, error)
	WithTransaction(tx db.DbConnector) InstrumentRepository
}

func (r *instrumentRepository) CreateInstrument(ctx context.Context, instrument Instrument) (uuid.UUID, error) {
	query := fmt.Sprintf(`INSERT INTO %s.sk_instruments(id, protocol_id, "type", "name", hostname, client_port, enabled, connection_mode, running_mode, captureresults, capturediagnostics, replytoquery, status, sent_to_cerberus, timezone, file_encoding)
		VALUES(:id, :protocol_id, :type, :name, :hostname, :client_port, :enabled, :connection_mode, :running_mode, :captureresults, :capturediagnostics, :replytoquery, :status, :sent_to_cerberus, :timezone, :file_encoding);`, r.dbSchema)
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

func (r *instrumentRepository) GetInstruments(ctx context.Context) ([]Instrument, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_instruments WHERE deleted_at IS NULL ORDER BY name;`, r.dbSchema)
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

func (r *instrumentRepository) GetInstrumentChanges(ctx context.Context, timeFrom time.Time) ([]Instrument, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_instruments WHERE deleted_at >= $1 OR modified_at >= $1 ORDER BY name;`, r.dbSchema)
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

func (r *instrumentRepository) GetInstrumentByID(ctx context.Context, id uuid.UUID) (Instrument, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_instruments WHERE id = $1 AND deleted_at IS NULL;`, r.dbSchema)
	var instrument Instrument
	var dao instrumentDAO
	err := r.db.QueryRowxContext(ctx, query, id).StructScan(&dao)
	if err != nil {
		if err == sql.ErrNoRows {
			return instrument, ErrInstrumentNotFound
		}
		log.Error().Err(err).Msg(msgGetInstrumentByIDFailed)
		return instrument, ErrGetInstrumentByIDFailed
	}

	instrument, err = convertInstrumentDaoToInstrument(dao)
	if err != nil {
		return instrument, err
	}
	return instrument, nil
}

func (r *instrumentRepository) GetInstrumentByIP(ctx context.Context, ip string) (Instrument, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_instruments WHERE hostname = $1 AND deleted_at IS NULL;`, r.dbSchema)
	var instrument Instrument
	var dao instrumentDAO
	err := r.db.QueryRowxContext(ctx, query, ip).StructScan(&dao)
	if err != nil {
		if err == sql.ErrNoRows {
			return instrument, ErrInstrumentNotFound
		}
		log.Error().Err(err).Msg(msgGetInstrumentByIPFailed + " " + ip)
		return instrument, ErrGetInstrumentByIPFailed
	}

	instrument, err = convertInstrumentDaoToInstrument(dao)
	if err != nil {
		return instrument, err
	}
	return instrument, nil
}

func (r *instrumentRepository) UpdateInstrument(ctx context.Context, instrument Instrument) error {
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

func (r *instrumentRepository) DeleteInstrument(ctx context.Context, id uuid.UUID) error {
	query := fmt.Sprintf(`UPDATE %s.sk_instruments SET deleted_at = timezone('utc', now()) WHERE id = $1;`, r.dbSchema)
	_, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		log.Error().Err(err).Msg(msgDeleteInstrumentFailed)
		return ErrDeleteInstrumentFailed
	}
	return nil
}

func (r *instrumentRepository) CreateFtpConfig(ctx context.Context, ftpConfig FTPConfig) error {
	query := fmt.Sprintf(`INSERT INTO %s.sk_instrument_ftp_config(id, instrument_id, username, password, remote_path, file_mask, result_remote_path, file_suffix, ftp_server_type)
		VALUES(:id, :instrument_id, :username, :password, :remote_path, :file_mask, :result_remote_path, :file_suffix, :ftp_server_type)`, r.dbSchema)
	ftpConfig.ID = uuid.New()

	dao := convertFtpConfigToDao(ftpConfig)
	_, err := r.db.NamedExecContext(ctx, query, dao)
	if err != nil {
		log.Error().Err(err).Msg(msgCreateFtpConfigFailed)
		return ErrCreateFtpConfigFailed
	}
	return nil
}

func (r *instrumentRepository) GetFtpConfigByInstrumentId(ctx context.Context, instrumentId uuid.UUID) (FTPConfig, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_instrument_ftp_config WHERE instrument_id = $1 AND deleted_at is NULL;`, r.dbSchema)

	var ftpConfig FTPConfig
	var dao ftpConfigDAO
	err := r.db.QueryRowxContext(ctx, query, instrumentId).StructScan(&dao)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Warn().Interface("instrumentId", instrumentId).Msg("ftp config queried bot not found")
			return ftpConfig, ErrFtpConfigNotFound
		}
		log.Error().Err(err).Msg(msgGetFtpConfigFailed)
		return ftpConfig, ErrGetFtpConfigFailed
	}

	return convertFtpConfigDaoToFtpConfig(dao), nil
}

func (r *instrumentRepository) FtpConfigExists(ctx context.Context, instrumentId uuid.UUID) (bool, error) {
	query := fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM %s.sk_instrument_ftp_config WHERE instrument_id = $1 AND deleted_at is NULL);`, r.dbSchema)

	var ftpConfigExists bool
	err := r.db.QueryRowxContext(ctx, query, instrumentId).Scan(&ftpConfigExists)
	if err != nil {
		log.Error().Err(err).Msg(msgFtpConfigExistsFailed)
		return ftpConfigExists, ErrFtpConfigExistsFailed
	}

	return ftpConfigExists, nil
}

func (r *instrumentRepository) UpdateFtpConfig(ctx context.Context, ftpConfig FTPConfig) error {
	query := fmt.Sprintf(`UPDATE %s.sk_instrument_ftp_config SET username = :username, password = :password,
        remote_path = :remote_path, file_mask = :file_mask, result_remote_path = :result_remote_path, file_suffix = :file_suffix,
        ftp_server_type = :ftp_server_type WHERE instrument_id = :instrument_id AND deleted_at is NULL;`, r.dbSchema)

	dao := convertFtpConfigToDao(ftpConfig)
	_, err := r.db.NamedExecContext(ctx, query, dao)
	if err != nil {
		log.Error().Err(err).Msg(msgUpdateFtpConfigFailed)
		return ErrUpdateFtpConfigFailed
	}
	return nil
}

func (r *instrumentRepository) DeleteFtpConfig(ctx context.Context, instrumentId uuid.UUID) error {
	query := fmt.Sprintf(`UPDATE %s.sk_instrument_ftp_config SET deleted_at = timezone('utc', now()) WHERE instrument_id = $1 AND deleted_at is NULL;`, r.dbSchema)

	_, err := r.db.ExecContext(ctx, query, instrumentId)
	if err != nil {
		log.Error().Err(err).Msg(msgDeleteFtpConfigFailed)
		return ErrDeleteFtpConfigFailed
	}
	return nil
}

func (r *instrumentRepository) MarkAsSentToCerberus(ctx context.Context, id uuid.UUID) error {
	query := fmt.Sprintf(`UPDATE %s.sk_instruments SET sent_to_cerberus = TRUE WHERE id = $1;`, r.dbSchema)
	_, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		log.Error().Err(err).Msg(msgMarkInstrumentSentToCerberusFailed)
		return ErrMarkInstrumentSentToCerberusFailed
	}
	return nil
}

func (r *instrumentRepository) GetUnsentToCerberus(ctx context.Context) ([]uuid.UUID, error) {
	query := fmt.Sprintf(`SELECT id FROM %s.sk_instruments WHERE sent_to_cerberus = FALSE AND deleted_at IS NULL;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query)
	if err != nil {
		log.Error().Err(err).Msg("Failed to fetch unsent instrument IDs")
		return []uuid.UUID{}, err
	}
	defer rows.Close()

	instrumentIDs := make([]uuid.UUID, 0)
	for rows.Next() {
		instrumentID := uuid.UUID{}
		err = rows.Scan(&instrumentID)
		if err != nil {
			log.Error().Err(err).Msg("Failed to scan row")
			return nil, err
		}
		instrumentIDs = append(instrumentIDs, instrumentID)
	}

	return instrumentIDs, nil
}

func (r *instrumentRepository) GetProtocolByID(ctx context.Context, id uuid.UUID) (SupportedProtocol, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_supported_protocols WHERE id = $1;`, r.dbSchema)
	row := r.db.QueryRowxContext(ctx, query, id)
	var dao supportedProtocolDAO
	err := row.StructScan(&dao)
	if err != nil {
		log.Error().Err(err).Msg(msgGetProtocolByIDFailed)
		return SupportedProtocol{}, ErrGetProtocolByIDFailed
	}
	return convertSupportedProtocolDAOToSupportedProtocol(dao), nil
}

func (r *instrumentRepository) GetSupportedProtocols(ctx context.Context) ([]SupportedProtocol, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_supported_protocols ORDER BY name;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query)
	if err != nil {
		log.Error().Err(err).Msg(msgGetInstrumentsFailed)
		return nil, ErrGetInstrumentsFailed
	}
	defer rows.Close()
	supportedProtocols := make([]SupportedProtocol, 0)
	for rows.Next() {
		var dao supportedProtocolDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetInstrumentsFailed)
			return nil, ErrGetInstrumentsFailed
		}
		var supportedProtocol SupportedProtocol
		supportedProtocol = convertSupportedProtocolDAOToSupportedProtocol(dao)
		if err != nil {
			return nil, err
		}
		supportedProtocols = append(supportedProtocols, supportedProtocol)
	}
	return supportedProtocols, nil
}

func (r *instrumentRepository) UpsertSupportedProtocol(ctx context.Context, id uuid.UUID, name string, description string) error {
	query := fmt.Sprintf(`INSERT INTO %s.sk_supported_protocols(id, "name", description) VALUES($1, $2, $3) ON CONFLICT (id) DO UPDATE SET name = $2, description = $3;`, r.dbSchema)
	_, err := r.db.ExecContext(ctx, query, id, name, sql.NullString{
		String: description,
		Valid:  len(description) > 0,
	})
	if err != nil {
		log.Error().Err(err).Msg(msgUpsertSupportedProtocolFailed)
		return ErrUpsertSupportedProtocolFailed
	}
	return nil
}

func (r *instrumentRepository) GetProtocolAbilities(ctx context.Context, protocolID uuid.UUID) ([]ProtocolAbility, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_protocol_abilities WHERE protocol_id = $1 AND deleted_at IS NULL;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query, protocolID)
	if err != nil {
		log.Error().Err(err).Msg(msgGetInstrumentsFailed)
		return nil, ErrGetInstrumentsFailed
	}
	defer rows.Close()
	protocolAbilities := make([]ProtocolAbility, 0)
	for rows.Next() {
		var dao protocolAbilityDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetInstrumentsFailed)
			return nil, ErrGetInstrumentsFailed
		}
		var protocolAbility ProtocolAbility
		protocolAbility = convertProtocolAbilityDAOToProtocolAbility(dao)
		if err != nil {
			return nil, err
		}
		protocolAbilities = append(protocolAbilities, protocolAbility)
	}
	return protocolAbilities, nil
}

func (r *instrumentRepository) UpsertProtocolAbilities(ctx context.Context, protocolID uuid.UUID, protocolAbilities []ProtocolAbility) error {
	query := fmt.Sprintf(`INSERT INTO %s.sk_protocol_abilities(protocol_id, connection_mode, abilities, request_mapping_available)
		VALUES(:protocol_id, :connection_mode, :abilities, :request_mapping_available)
		ON CONFLICT (protocol_id, connection_mode) WHERE deleted_at IS NULL
		DO UPDATE SET abilities = :abilities, request_mapping_available = :request_mapping_available, modified_at = timezone('utc', now());`, r.dbSchema)
	protocolAbilityDAOs := convertProtocolAbilitiesToDAOs(protocolAbilities, protocolID)
	// Todo - check and improve it cuz gives error with batch insert. Old and fixed(?): https://github.com/jmoiron/sqlx/issues/505
	for _, protocolAbility := range protocolAbilityDAOs {
		_, err := r.db.NamedExecContext(ctx, query, protocolAbility)
		if err != nil {
			log.Error().Err(err).Msg(msgUpsertProtocolAbilitiesFailed)
			return ErrUpsertProtocolAbilitiesFailed
		}
	}
	return nil
}

func (r *instrumentRepository) GetProtocolSettings(ctx context.Context, protocolID uuid.UUID) ([]ProtocolSetting, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_protocol_settings WHERE protocol_id = $1 AND deleted_at IS NULL;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query, protocolID)
	if err != nil {
		log.Error().Err(err).Msg(msgGetProtocolSettingsFailed)
		return nil, ErrGetProtocolSettingsFailed
	}
	defer rows.Close()
	protocolSettings := make([]ProtocolSetting, 0)
	for rows.Next() {
		var psDao protocolSettingDAO
		err = rows.StructScan(&psDao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetProtocolSettingsFailed)
			return nil, ErrGetProtocolSettingsFailed
		}
		ps := ProtocolSetting{
			ID:   psDao.ID,
			Key:  psDao.Key,
			Type: psDao.Type,
		}
		if psDao.Description.Valid {
			ps.Description = &psDao.Description.String
		}
		protocolSettings = append(protocolSettings, ps)
	}
	return protocolSettings, nil
}

func (r *instrumentRepository) UpsertProtocolSetting(ctx context.Context, protocolID uuid.UUID, protocolSetting ProtocolSetting) error {
	psDao := protocolSettingDAO{
		ID:         protocolSetting.ID,
		ProtocolID: protocolID,
		Key:        protocolSetting.Key,
		Type:       protocolSetting.Type,
	}
	if protocolSetting.Description != nil && len(*protocolSetting.Description) > 0 {
		psDao.Description = sql.NullString{
			String: *protocolSetting.Description,
			Valid:  true,
		}
	}

	query := fmt.Sprintf(`INSERT INTO %s.sk_protocol_settings(id, protocol_id, "key", description, "type") VALUES(:id, :protocol_id, :key, :description, :type)
	ON CONFLICT (id) DO UPDATE SET "key" = :key, description = :description, "type" = :type, modified_at = now();`, r.dbSchema)

	_, err := r.db.NamedExecContext(ctx, query, psDao)
	if err != nil {
		log.Error().Err(err).Msg(msgUpsertProtocolSettingsFailed)
		return ErrUpsertProtocolSettingsFailed
	}
	return nil
}

func (r *instrumentRepository) DeleteProtocolSettings(ctx context.Context, protocolSettingIDs []uuid.UUID) error {
	if len(protocolSettingIDs) == 0 {
		return nil
	}
	query := fmt.Sprintf(`UPDATE %s.sk_protocol_settings SET deleted_at = now() WHERE id IN (?);`, r.dbSchema)
	query, args, _ := sqlx.In(query, protocolSettingIDs)
	query = r.db.Rebind(query)
	_, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgDeleteProtocolMappingFailed)
		return ErrDeleteProtocolMappingFailed
	}
	return nil
}

func (r *instrumentRepository) UpdateInstrumentStatus(ctx context.Context, id uuid.UUID, status InstrumentStatus) error {
	query := fmt.Sprintf(`UPDATE %s.sk_instruments SET status = $2 WHERE id = $1;`, r.dbSchema)
	_, err := r.db.ExecContext(ctx, query, id, status)
	if err != nil {
		log.Error().Err(err).Msg(msgUpdateInstrumentStatusFailed)
		return ErrUpdateInstrumentStatusFailed
	}
	return nil
}

func (r *instrumentRepository) CreateAnalyteMappings(ctx context.Context, analyteMappings []AnalyteMapping, instrumentID uuid.UUID) ([]uuid.UUID, error) {
	if len(analyteMappings) == 0 {
		return []uuid.UUID{}, nil
	}
	ids := make([]uuid.UUID, len(analyteMappings))
	for i := range analyteMappings {
		if (analyteMappings[i].ID == uuid.UUID{}) || (analyteMappings[i].ID == uuid.Nil) {
			analyteMappings[i].ID = uuid.New()
		}
		ids[i] = analyteMappings[i].ID
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_analyte_mappings(id, instrument_id, instrument_analyte, analyte_id, result_type) VALUES(:id, :instrument_id, :instrument_analyte, :analyte_id, :result_type);`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertAnalyteMappingsToDAOs(analyteMappings, instrumentID))
	if err != nil {
		log.Error().Err(err).Msg(msgCreateAnalyteMappingsFailed)
		return []uuid.UUID{}, ErrCreateAnalyteMappingsFailed
	}
	return ids, nil
}

func (r *instrumentRepository) GetAnalyteMappings(ctx context.Context, instrumentIDs []uuid.UUID) (map[uuid.UUID][]AnalyteMapping, error) {
	analyteMappingsByInstrumentID := make(map[uuid.UUID][]AnalyteMapping)
	if len(instrumentIDs) == 0 {
		return analyteMappingsByInstrumentID, nil
	}
	query := fmt.Sprintf(`SELECT * FROM %s.sk_analyte_mappings WHERE instrument_id IN (?) AND deleted_at IS NULL;`, r.dbSchema)
	query, args, _ := sqlx.In(query, instrumentIDs)
	query = r.db.Rebind(query)
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgGetAnalyteMappingsFailed)
		return nil, ErrGetAnalyteMappingsFailed
	}
	defer rows.Close()
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

func (r *instrumentRepository) UpdateAnalyteMapping(ctx context.Context, analyteMapping AnalyteMapping) error {
	query := fmt.Sprintf(`UPDATE %s.sk_analyte_mappings SET instrument_analyte = :instrument_analyte, analyte_id = :analyte_id, result_type = :result_type, modified_at = timezone('utc', now()) WHERE id = :id;`, r.dbSchema)
	dao := convertAnalyteMappingToDAO(analyteMapping, uuid.Nil)
	_, err := r.db.NamedExecContext(ctx, query, dao)
	if err != nil {
		log.Error().Err(err).Msg(msgUpdateAnalyteMappingFailed)
		return ErrUpdateAnalyteMappingFailed
	}
	return nil
}

func (r *instrumentRepository) DeleteAnalyteMappings(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	query := fmt.Sprintf(`UPDATE %s.sk_analyte_mappings SET deleted_at = timezone('utc', now()) WHERE id IN (?);`, r.dbSchema)
	query, args, err := sqlx.In(query, ids)
	query = r.db.Rebind(query)
	if err != nil {
		log.Error().Err(err).Msg(msgDeleteAnalyteMappingFailed)
		return ErrDeleteAnalyteMappingFailed
	}
	log.Trace().Str("query", query).Interface("arguments", args).Msg("Assembled query")
	_, err = r.db.ExecContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgDeleteAnalyteMappingFailed)
		return ErrDeleteAnalyteMappingFailed
	}
	return nil
}

func (r *instrumentRepository) CreateChannelMappings(ctx context.Context, channelMappings []ChannelMapping, analyteMappingID uuid.UUID) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, len(channelMappings))
	if len(channelMappings) == 0 {
		return ids, nil
	}
	for i := range channelMappings {
		if (channelMappings[i].ID == uuid.UUID{}) || (channelMappings[i].ID == uuid.Nil) {
			channelMappings[i].ID = uuid.New()
		}
		ids[i] = channelMappings[i].ID
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_channel_mappings(id, instrument_channel, channel_id, analyte_mapping_id) 
		VALUES(:id, :instrument_channel, :channel_id, :analyte_mapping_id);`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertChannelMappingsToDAOs(channelMappings, analyteMappingID))
	if err != nil {
		log.Error().Err(err).Msg(msgCreateChannelMappingsFailed)
		return ids, ErrCreateChannelMappingsFailed
	}
	return ids, nil
}

func (r *instrumentRepository) GetChannelMappings(ctx context.Context, analyteMappingIDs []uuid.UUID) (map[uuid.UUID][]ChannelMapping, error) {
	channelMappingsByAnalyteMappingID := make(map[uuid.UUID][]ChannelMapping)
	if len(analyteMappingIDs) == 0 {
		return channelMappingsByAnalyteMappingID, nil
	}
	query := fmt.Sprintf(`SELECT * FROM %s.sk_channel_mappings WHERE analyte_mapping_id IN (?) AND deleted_at IS NULL;`, r.dbSchema)
	query, args, _ := sqlx.In(query, analyteMappingIDs)
	query = r.db.Rebind(query)
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgGetChannelMappingsFailed)
		return nil, ErrGetChannelMappingsFailed
	}
	defer rows.Close()
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

func (r *instrumentRepository) UpdateChannelMapping(ctx context.Context, channelMapping ChannelMapping) error {
	query := fmt.Sprintf(`UPDATE %s.sk_channel_mappings SET instrument_channel = :instrument_channel, channel_id = :channel_id, modified_at = timezone('utc', now()) WHERE id = :id;`, r.dbSchema)
	dao := convertChannelMappingToDAO(channelMapping, uuid.Nil)
	_, err := r.db.NamedExecContext(ctx, query, dao)
	if err != nil {
		log.Error().Err(err).Msg(msgUpdateChannelMappingFailed)
		return ErrUpdateChannelMappingFailed
	}
	return nil
}

func (r *instrumentRepository) DeleteChannelMappings(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	query := fmt.Sprintf(`UPDATE %s.sk_channel_mappings SET deleted_at = timezone('utc', now()) WHERE id IN (?);`, r.dbSchema)
	query, args, _ := sqlx.In(query, ids)
	query = r.db.Rebind(query)
	_, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgDeleteChannelMappingFailed)
		return ErrDeleteChannelMappingFailed
	}
	return nil
}

func (r *instrumentRepository) CreateResultMappings(ctx context.Context, resultMappings []ResultMapping, analyteMappingID uuid.UUID) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, len(resultMappings))
	if len(resultMappings) == 0 {
		return ids, nil
	}
	for i := range resultMappings {
		if (resultMappings[i].ID == uuid.UUID{}) || (resultMappings[i].ID == uuid.Nil) {
			resultMappings[i].ID = uuid.New()
		}
		ids[i] = resultMappings[i].ID
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_result_mappings(id, analyte_mapping_id, "key", "value", "index") 
		VALUES(:id, :analyte_mapping_id, :key, :value, :index);`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertResultMappingsToDAOs(resultMappings, analyteMappingID))
	if err != nil {
		log.Error().Err(err).Msg(msgCreateChannelMappingsFailed)
		return ids, ErrCreateChannelMappingsFailed
	}
	return ids, nil
}

func (r *instrumentRepository) GetResultMappings(ctx context.Context, analyteMappingIDs []uuid.UUID) (map[uuid.UUID][]ResultMapping, error) {
	resultMappingsByAnalyteMappingID := make(map[uuid.UUID][]ResultMapping)
	if len(analyteMappingIDs) == 0 {
		return resultMappingsByAnalyteMappingID, nil
	}
	query := fmt.Sprintf(`SELECT * FROM %s.sk_result_mappings WHERE analyte_mapping_id IN (?) AND deleted_at IS NULL;`, r.dbSchema)
	query, args, _ := sqlx.In(query, analyteMappingIDs)
	query = r.db.Rebind(query)
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgGetResultMappingsFailed)
		return nil, ErrGetResultMappingsFailed
	}
	defer rows.Close()
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

func (r *instrumentRepository) UpdateResultMapping(ctx context.Context, resultMapping ResultMapping) error {
	query := fmt.Sprintf(`UPDATE %s.sk_result_mappings SET "key" = :key, "value" = :value, "index" = :index, modified_at = timezone('utc', now()) WHERE id = :id`, r.dbSchema)
	dao := convertResultMappingToDAO(resultMapping, uuid.Nil)
	_, err := r.db.NamedExecContext(ctx, query, dao)
	if err != nil {
		log.Error().Err(err).Msg(msgUpdateResultMappingFailed)
		return ErrUpdateResultMappingFailed
	}
	return nil
}

func (r *instrumentRepository) DeleteResultMappings(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	query := fmt.Sprintf(`UPDATE %s.sk_result_mappings SET deleted_at = timezone('utc', now()) WHERE id IN (?);`, r.dbSchema)
	query, args, _ := sqlx.In(query, ids)
	query = r.db.Rebind(query)
	_, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgDeleteResultMappingFailed)
		return ErrDeleteResultMappingFailed
	}
	return nil
}

func (r *instrumentRepository) CreateRequestMappings(ctx context.Context, requestMappings []RequestMapping, instrumentID uuid.UUID) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, len(requestMappings))
	if len(requestMappings) == 0 {
		return ids, nil
	}
	for i := range requestMappings {
		requestMappings[i].ID = uuid.New()
		ids[i] = requestMappings[i].ID
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_request_mappings(id, code, instrument_id, is_default) 
		VALUES(:id, :code, :instrument_id, :is_default);`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertRequestMappingsToDAOs(requestMappings, instrumentID))
	if err != nil {
		log.Error().Err(err).Msg(msgCreateAnalyteMappingsFailed)
		return ids, ErrCreateAnalyteMappingsFailed
	}
	return ids, nil
}

func (r *instrumentRepository) UpsertRequestMappingAnalytes(ctx context.Context, analyteIDsByRequestMappingID map[uuid.UUID][]uuid.UUID) error {
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
	if len(requestMappingAnalyteDAOs) < 1 {
		return nil
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_request_mapping_analytes(id, analyte_id, request_mapping_id) 
		VALUES(:id, :analyte_id, :request_mapping_id) ON CONFLICT ON CONSTRAINT sk_unique_request_mapping_analytes DO NOTHING;`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, requestMappingAnalyteDAOs)
	if err != nil {
		log.Error().Err(err).Msg(msgCreateAnalyteMappingsFailed)
		return ErrCreateAnalyteMappingsFailed
	}
	return nil
}

func (r *instrumentRepository) UpdateRequestMapping(ctx context.Context, requestMapping RequestMapping) error {
	query := fmt.Sprintf(`UPDATE %s.sk_request_mappings SET code = :code, is_default = :is_default, modified_at = timezone('utc', now()) WHERE id = :id;`, r.dbSchema)
	dao := convertRequestMappingToDAO(requestMapping, uuid.Nil)
	_, err := r.db.NamedExecContext(ctx, query, dao)
	if err != nil {
		log.Error().Err(err).Msg(msgUpdateRequestMappingFailed)
		return ErrUpdateRequestMappingFailed
	}
	return nil
}

func (r *instrumentRepository) GetRequestMappings(ctx context.Context, instrumentIDs []uuid.UUID) (map[uuid.UUID][]RequestMapping, error) {
	analyteMappingsByInstrumentID := make(map[uuid.UUID][]RequestMapping)
	if len(instrumentIDs) == 0 {
		return analyteMappingsByInstrumentID, nil
	}
	query := fmt.Sprintf(`SELECT * FROM %s.sk_request_mappings WHERE instrument_id IN (?) AND deleted_at IS NULL;`, r.dbSchema)
	query, args, _ := sqlx.In(query, instrumentIDs)
	query = r.db.Rebind(query)
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgGetRequestMappingsFailed)
		return nil, ErrGetRequestMappingsFailed
	}
	defer rows.Close()
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

func (r *instrumentRepository) GetRequestMappingAnalytes(ctx context.Context, requestMappingIDs []uuid.UUID) (map[uuid.UUID][]uuid.UUID, error) {
	query := fmt.Sprintf(`SELECT request_mapping_id, analyte_id FROM %s.sk_request_mapping_analytes WHERE request_mapping_id IN (?);`, r.dbSchema)
	query, args, _ := sqlx.In(query, requestMappingIDs)
	query = r.db.Rebind(query)
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

func (r *instrumentRepository) DeleteRequestMappings(ctx context.Context, requestMappingIDs []uuid.UUID) error {
	query := fmt.Sprintf(`UPDATE %s.sk_request_mappings SET deleted_at = timezone('utc', now()) WHERE id IN (?);`, r.dbSchema)
	query, args, _ := sqlx.In(query, requestMappingIDs)
	query = r.db.Rebind(query)
	_, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgDeleteRequestMappingsFailed)
		return ErrDeleteRequestMappingsFailed
	}
	return nil
}

func (r *instrumentRepository) DeleteRequestMappingAnalytes(ctx context.Context, requestMappingID uuid.UUID, analyteIDs []uuid.UUID) error {
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

func (r *instrumentRepository) GetEncodings(ctx context.Context) ([]string, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_encodings ORDER BY encoding;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query)
	if err != nil {
		log.Error().Err(err).Msg(msgGetEncodingsFailed)
		return nil, ErrGetEncodingsFailed
	}
	defer rows.Close()
	encodings := make([]string, 0)
	for rows.Next() {
		var encoding string
		err = rows.Scan(&encoding)
		if err != nil {
			log.Error().Err(err).Msg(msgGetEncodingsFailed)
			return nil, ErrGetEncodingsFailed
		}
		encodings = append(encodings, encoding)
	}
	return encodings, nil
}

func (r *instrumentRepository) GetInstrumentsSettings(ctx context.Context, instrumentIDs []uuid.UUID) (map[uuid.UUID][]InstrumentSetting, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_instrument_settings WHERE instrument_id IN (?) AND deleted_at IS NULL;`, r.dbSchema)
	query, args, _ := sqlx.In(query, instrumentIDs)
	query = r.db.Rebind(query)
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgGetInstrumentsSettingsFailed)
		return nil, ErrGetInstrumentsSettingsFailed
	}
	defer rows.Close()
	settingsMap := make(map[uuid.UUID][]InstrumentSetting)
	for rows.Next() {
		var settingDao instrumentSettingDao
		err = rows.StructScan(&settingDao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetInstrumentsSettingsFailed)
			return nil, ErrGetInstrumentsSettingsFailed
		}
		settingsMap[settingDao.InstrumentID] = append(settingsMap[settingDao.InstrumentID], InstrumentSetting{
			ID:                settingDao.ID,
			ProtocolSettingID: settingDao.ProtocolSettingID,
			Value:             settingDao.Value,
		})
	}
	return settingsMap, nil
}

func (r *instrumentRepository) UpsertInstrumentSetting(ctx context.Context, instrumentID uuid.UUID, setting InstrumentSetting) error {
	settingDao := instrumentSettingDao{
		ID:                setting.ID,
		InstrumentID:      instrumentID,
		ProtocolSettingID: setting.ProtocolSettingID,
		Value:             setting.Value,
	}
	if settingDao.ID == uuid.Nil {
		settingDao.ID = uuid.New()
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_instrument_settings(id, instrument_id, protocol_setting_id, "value") VALUES(:id, :instrument_id, :protocol_setting_id, :value)
	ON CONFLICT (id) DO UPDATE SET "value" = :value, modified_at = now();`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, settingDao)
	if err != nil {
		log.Error().Err(err).Msg(msgUpsertInstrumentSettingsFailed)
		return ErrUpsertInstrumentSettingsFailed
	}
	return nil
}

func (r *instrumentRepository) DeleteInstrumentSettings(ctx context.Context, ids []uuid.UUID) error {
	query := fmt.Sprintf(`UPDATE %s.sk_instrument_settings SET deleted_at = now() WHERE id IN (?);`, r.dbSchema)
	query, args, _ := sqlx.In(query, ids)
	query = r.db.Rebind(query)
	_, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgDeleteInstrumentSettingsFailed)
		return ErrDeleteInstrumentSettingsFailed
	}
	return nil
}

func (r *instrumentRepository) CheckAnalytesUsage(ctx context.Context, analyteIDs []uuid.UUID) (map[uuid.UUID][]Instrument, error) {
	query := fmt.Sprintf(`SELECT am.analyte_id, i.id, i.name FROM %s.sk_analyte_mappings am INNER JOIN %s.sk_instruments i ON am.instrument_id = i.id
	  WHERE am.analyte_id IN (?) AND am.deleted_at IS NULL AND i.deleted_at IS NULL;`, r.dbSchema, r.dbSchema)
	query, args, _ := sqlx.In(query, analyteIDs)
	query = r.db.Rebind(query)
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgCheckAnalyteUsageFailed)
		return nil, ErrCheckAnalyteUsageFailed
	}
	defer rows.Close()
	analyteUsageMap := make(map[uuid.UUID][]Instrument)
	for rows.Next() {
		var analyteID, instrumentID uuid.UUID
		var name string
		err = rows.Scan(&analyteID, &instrumentID, &name)
		if err != nil {
			log.Error().Err(err).Msg(msgCheckAnalyteUsageFailed)
			return nil, ErrCheckAnalyteUsageFailed
		}
		analyteUsageMap[analyteID] = append(analyteUsageMap[analyteID], Instrument{ID: instrumentID, Name: name})
	}
	return analyteUsageMap, nil
}

func (r *instrumentRepository) CreateTransaction() (db.DbConnector, error) {
	return r.db.CreateTransactionConnector()
}

func (r *instrumentRepository) WithTransaction(tx db.DbConnector) InstrumentRepository {
	if tx == nil {
		return r
	}

	txRepo := *r
	txRepo.db = tx
	return &txRepo
}

func convertRequestMappingDaoToAnalyteMapping(dao requestMappingDAO) RequestMapping {
	return RequestMapping{
		ID:         dao.ID,
		Code:       dao.Code,
		IsDefault:  dao.IsDefault,
		AnalyteIDs: nil,
	}
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
	switch instrument.Type {
	case Analyzer, Sorter:
		dao.Type = instrument.Type
	default:
		return dao, ErrInvalidInstrumentType
	}
	switch instrument.ConnectionMode {
	case TCPClientMode, TCPServerMode, FTP, HTTP, TCPMixed:
		dao.ConnectionMode = string(instrument.ConnectionMode)
	default:
		return dao, ErrInvalidConnectionMode
	}
	switch instrument.ResultMode {
	case Simulation, Qualification, Production:
		dao.RunningMode = instrument.ResultMode
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
	switch dao.Type {
	case Analyzer, Sorter:
		instrument.Type = dao.Type
	default:
		return instrument, ErrInvalidInstrumentType
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
	case Simulation, Qualification, Production:
		instrument.ResultMode = dao.RunningMode
	default:
		return instrument, ErrInvalidResultMode
	}
	if dao.ClientPort.Valid {
		clientPort := int(dao.ClientPort.Int32)
		instrument.ClientPort = &clientPort
	}

	return instrument, nil
}

func convertFtpConfigToDao(ftpConfig FTPConfig) ftpConfigDAO {
	return ftpConfigDAO{
		ID:               ftpConfig.ID,
		InstrumentId:     ftpConfig.InstrumentId,
		Username:         ftpConfig.Username,
		Password:         ftpConfig.Password,
		RemotePath:       ftpConfig.RemotePath,
		FileMask:         ftpConfig.FileMask,
		ResultRemotePath: ftpConfig.ResultRemotePath,
		FileSuffix:       ftpConfig.FileSuffix,
		FtpServerType:    ftpConfig.FtpServerType,
	}
}

func convertFtpConfigDaoToFtpConfig(dao ftpConfigDAO) FTPConfig {
	return FTPConfig{
		ID:               dao.ID,
		InstrumentId:     dao.InstrumentId,
		Username:         dao.Username,
		Password:         dao.Password,
		RemotePath:       dao.RemotePath,
		FileMask:         dao.FileMask,
		ResultRemotePath: dao.ResultRemotePath,
		FileSuffix:       dao.FileSuffix,
		FtpServerType:    dao.FtpServerType,
	}
}

func convertAnalyteMappingToDAO(analyteMapping AnalyteMapping, instrumentID uuid.UUID) analyteMappingDAO {
	return analyteMappingDAO{
		ID:                analyteMapping.ID,
		InstrumentID:      instrumentID,
		InstrumentAnalyte: analyteMapping.InstrumentAnalyte,
		AnalyteID:         analyteMapping.AnalyteID,
		ResultType:        analyteMapping.ResultType,
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
		ResultType:        dao.ResultType,
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
		channelMappingDAOs[i] = convertChannelMappingToDAO(channelMappings[i], analyteMappingID)
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
		IsDefault:    requestMapping.IsDefault,
	}
}

func convertRequestMappingsToDAOs(requestMappings []RequestMapping, instrumentID uuid.UUID) []requestMappingDAO {
	requestMappingDAOs := make([]requestMappingDAO, len(requestMappings))
	for i := range requestMappings {
		requestMappingDAOs[i] = convertRequestMappingToDAO(requestMappings[i], instrumentID)
	}
	return requestMappingDAOs
}

func convertSupportedProtocolDAOToSupportedProtocol(dao supportedProtocolDAO) SupportedProtocol {
	return SupportedProtocol{
		ID:          dao.ID,
		Name:        dao.Name,
		Description: utils.SqlNullStringToStringPointer(dao.Description),
	}
}

func convertProtocolAbilityToDAO(protocolAbility ProtocolAbility, protocolID uuid.UUID) protocolAbilityDAO {
	return protocolAbilityDAO{
		ProtocolID:              protocolID,
		ConnectionMode:          string(protocolAbility.ConnectionMode),
		Abilities:               utils.JoinEnumsAsString(protocolAbility.Abilities, ","),
		RequestMappingAvailable: protocolAbility.RequestMappingAvailable,
	}
}

func convertProtocolAbilitiesToDAOs(protocolAbilities []ProtocolAbility, protocolID uuid.UUID) []protocolAbilityDAO {
	requestMappingDAOs := make([]protocolAbilityDAO, len(protocolAbilities))
	for i := range protocolAbilities {
		requestMappingDAOs[i] = convertProtocolAbilityToDAO(protocolAbilities[i], protocolID)
	}
	return requestMappingDAOs
}

func convertProtocolAbilityDAOToProtocolAbility(dao protocolAbilityDAO) ProtocolAbility {
	return ProtocolAbility{
		ConnectionMode:          ConnectionMode(dao.ConnectionMode),
		Abilities:               splitStringToEnumArray(dao.Abilities, ","),
		RequestMappingAvailable: dao.RequestMappingAvailable,
	}
}

func NewInstrumentRepository(db db.DbConnector, dbSchema string) InstrumentRepository {
	return &instrumentRepository{
		db:       db,
		dbSchema: dbSchema,
	}
}

func splitStringToEnumArray(value string, separator string) []Ability {
	stringItems := strings.Split(value, separator)
	items := make([]Ability, len(stringItems))
	for i := range stringItems {
		items[i] = Ability(stringItems[i])
	}
	return items
}
