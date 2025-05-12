package skeleton

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/blutspende/bloodlab-common/util"
	"strings"
	"time"

	"github.com/blutspende/skeleton/db"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

const (
	msgInvalidConnectionMode                       = "invalid connection mode"
	msgInvalidResultMode                           = "invalid result mode"
	msgInvalidInstrumentType                       = "invalid instrument type"
	msgCreateInstrumentFailed                      = "create instrument failed"
	msgGetInstrumentsFailed                        = "get instruments failed"
	msgGetInstrumentChangesFailed                  = "get changed instruments failed"
	msgGetInstrumentByIDFailed                     = "get instrument by id failed"
	msgGetInstrumentByIPFailed                     = "get instrument by IP failed"
	msgInstrumentNotFound                          = "instrument not found"
	msgUpdateInstrumentFailed                      = "update instrument failed"
	msgDeleteInstrumentFailed                      = "delete instrument failed"
	msgCreateFtpConfigFailed                       = "create FTP config failed"
	msgDeleteFtpConfigFailed                       = "delete FTP config failed"
	msgGetFtpConfigFailed                          = "get ftp config by instrument id failed"
	msgFtpConfigNotFound                           = "ftp config not found"
	msgGetProtocolByIDFailed                       = "get protocol by CerberusID failed"
	msgUpsertSupportedProtocolFailed               = "upsert supported protocol failed"
	msgUpsertManufacturerTestsFailed               = "upsert manufacturer tests failed"
	msgGetManufacturerTestsFailed                  = "get manufacturer tests failed"
	msgUpsertProtocolAbilitiesFailed               = "upsert protocol abilities failed"
	msgUpdateInstrumentStatusFailed                = "update instrument status failed"
	msgUpsertAnalyteMappingsFailed                 = "upsert analyte mappings failed"
	msgGetAnalyteMappingsFailed                    = "get analyte mappings failed"
	msgUpdateAnalyteMappingFailed                  = "update analyte mapping failed"
	msgDeleteAnalyteMappingFailed                  = "delete analyte mapping failed"
	msgCreateChannelMappingsFailed                 = "create channel mappings failed"
	msgGetChannelMappingsFailed                    = "get channel mappings failed"
	msgUpdateChannelMappingFailed                  = "update channel mapping failed"
	msgDeleteChannelMappingFailed                  = "delete channel mapping failed"
	msgGetResultMappingsFailed                     = "get result mappings failed"
	msgDeleteResultMappingFailed                   = "delete result mapping failed"
	msgUpdateResultMappingFailed                   = "update result mapping failed"
	msgCreateExpectedControlResultsFailed          = "create expected control results failed"
	msgGetExpectedControlResultsFailed             = "get expected control results failed"
	msgGetNotSpecifiedExpectedControlResultsFailed = "get not specified expected control results failed"
	msgDeleteExpectedControlResultFailed           = "delete expected control result failed"
	msgGetRequestMappingsFailed                    = "get request mappings failed"
	msgGetRequestMappingAnalytesFailed             = "get request mapping analytes failed"
	msgDeleteRequestMappingsFailed                 = "delete request mappings failed"
	msgUpdateRequestMappingFailed                  = "update request mapping failed"
	msgDeleteRequestMappingAnalytesFailed          = "delete request mapping analytes failed"
	msgGetEncodingsFailed                          = "get encodings failed"
	msgGetProtocolSettingsFailed                   = "get protocol settings failed"
	msgUpsertProtocolSettingsFailed                = "upsert protocol settings failed"
	msgDeleteProtocolMappingFailed                 = "delete protocol settings failed"
	msgGetInstrumentsSettingsFailed                = "get instruments settings failed"
	msgUpsertInstrumentSettingsFailed              = "upsert instrument settings failed"
	msgDeleteInstrumentSettingsFailed              = "delete instrument settings failed"
	msgCheckAnalyteUsageFailed                     = "check analyte usage failed"
	msgGetUnsentInstrumentIdsFailed                = "get unsent instrument IDs failed"
	msgUniqueViolationInstrumentControlAnalyte     = "Instrument Control Analyte already set to an Analyte Mapping for this instrument"
	msgUniqueViolationInstrumentAnalyte            = "Instrument Analyte already set to an Analyte Mapping for this instrument"
)

var (
	ErrInvalidConnectionMode                       = errors.New(msgInvalidConnectionMode)
	ErrInvalidResultMode                           = errors.New(msgInvalidResultMode)
	ErrInvalidInstrumentType                       = errors.New(msgInvalidInstrumentType)
	ErrCreateInstrumentFailed                      = errors.New(msgCreateInstrumentFailed)
	ErrGetInstrumentsFailed                        = errors.New(msgGetInstrumentsFailed)
	ErrGetInstrumentChangesFailed                  = errors.New(msgGetInstrumentChangesFailed)
	ErrGetInstrumentByIDFailed                     = errors.New(msgGetInstrumentByIDFailed)
	ErrGetInstrumentByIPFailed                     = errors.New(msgGetInstrumentByIPFailed)
	ErrInstrumentNotFound                          = errors.New(msgInstrumentNotFound)
	ErrUpdateInstrumentFailed                      = errors.New(msgUpdateInstrumentFailed)
	ErrDeleteInstrumentFailed                      = errors.New(msgDeleteInstrumentFailed)
	ErrCreateFtpConfigFailed                       = errors.New(msgCreateFtpConfigFailed)
	ErrDeleteFtpConfigFailed                       = errors.New(msgDeleteFtpConfigFailed)
	ErrGetFtpConfigFailed                          = errors.New(msgGetFtpConfigFailed)
	ErrFtpConfigNotFound                           = errors.New(msgFtpConfigNotFound)
	ErrGetProtocolByIDFailed                       = errors.New(msgGetProtocolByIDFailed)
	ErrUpsertSupportedProtocolFailed               = errors.New(msgUpsertSupportedProtocolFailed)
	ErrUpsertManufacturerTestsFailed               = errors.New(msgUpsertManufacturerTestsFailed)
	ErrGetManufacturerTestsFailed                  = errors.New(msgGetManufacturerTestsFailed)
	ErrUpsertProtocolAbilitiesFailed               = errors.New(msgUpsertProtocolAbilitiesFailed)
	ErrUpdateInstrumentStatusFailed                = errors.New(msgUpdateInstrumentStatusFailed)
	ErrUpsertAnalyteMappingsFailed                 = errors.New(msgUpsertAnalyteMappingsFailed)
	ErrGetAnalyteMappingsFailed                    = errors.New(msgGetAnalyteMappingsFailed)
	ErrUpdateAnalyteMappingFailed                  = errors.New(msgUpdateAnalyteMappingFailed)
	ErrDeleteAnalyteMappingFailed                  = errors.New(msgDeleteAnalyteMappingFailed)
	ErrCreateChannelMappingsFailed                 = errors.New(msgCreateChannelMappingsFailed)
	ErrGetChannelMappingsFailed                    = errors.New(msgGetChannelMappingsFailed)
	ErrUpdateChannelMappingFailed                  = errors.New(msgUpdateChannelMappingFailed)
	ErrDeleteChannelMappingFailed                  = errors.New(msgDeleteChannelMappingFailed)
	ErrGetResultMappingsFailed                     = errors.New(msgGetResultMappingsFailed)
	ErrDeleteResultMappingFailed                   = errors.New(msgDeleteResultMappingFailed)
	ErrUpdateResultMappingFailed                   = errors.New(msgUpdateResultMappingFailed)
	ErrCreateExpectedControlResultsFailed          = errors.New(msgCreateExpectedControlResultsFailed)
	ErrGetExpectedControlResultsFailed             = errors.New(msgGetExpectedControlResultsFailed)
	ErrGetNotSpecifiedExpectedControlResultsFailed = errors.New(msgGetNotSpecifiedExpectedControlResultsFailed)
	ErrDeleteExpectedControlResultFailed           = errors.New(msgDeleteExpectedControlResultFailed)
	ErrGetRequestMappingsFailed                    = errors.New(msgGetRequestMappingsFailed)
	ErrGetRequestMappingAnalytesFailed             = errors.New(msgGetRequestMappingAnalytesFailed)
	ErrDeleteRequestMappingsFailed                 = errors.New(msgDeleteRequestMappingsFailed)
	ErrUpdateRequestMappingFailed                  = errors.New(msgUpdateRequestMappingFailed)
	ErrDeleteRequestMappingAnalytesFailed          = errors.New(msgDeleteRequestMappingAnalytesFailed)
	ErrGetEncodingsFailed                          = errors.New(msgGetEncodingsFailed)
	ErrGetProtocolSettingsFailed                   = errors.New(msgGetProtocolSettingsFailed)
	ErrUpsertProtocolSettingsFailed                = errors.New(msgUpsertProtocolSettingsFailed)
	ErrDeleteProtocolMappingFailed                 = errors.New(msgDeleteProtocolMappingFailed)
	ErrGetInstrumentsSettingsFailed                = errors.New(msgGetInstrumentsSettingsFailed)
	ErrUpsertInstrumentSettingsFailed              = errors.New(msgUpsertInstrumentSettingsFailed)
	ErrDeleteInstrumentSettingsFailed              = errors.New(msgDeleteInstrumentSettingsFailed)
	ErrCheckAnalyteUsageFailed                     = errors.New(msgCheckAnalyteUsageFailed)
	ErrGetUnsentInstrumentIdsFailed                = errors.New(msgGetUnsentInstrumentIdsFailed)
	ErrUniqueViolationInstrumentControlAnalyte     = errors.New(msgUniqueViolationInstrumentControlAnalyte)
	ErrUniqueViolationInstrumentAnalyte            = errors.New(msgUniqueViolationInstrumentAnalyte)
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
	OrderPath        string       `db:"order_path"`
	OrderFileMask    string       `db:"order_file_mask"`
	OrderFileSuffix  string       `db:"order_file_suffix"`
	ResultPath       string       `db:"result_path"`
	ResultFileMask   string       `db:"result_file_mask"`
	ResultFileSuffix string       `db:"result_file_suffix"`
	FtpServerType    string       `db:"ftp_server_type"`
	CreatedAt        time.Time    `db:"created_at"`
	DeletedAt        sql.NullTime `db:"deleted_at"`
}

type analyteMappingDAO struct {
	ID                       uuid.UUID      `db:"id"`
	InstrumentID             uuid.UUID      `db:"instrument_id"`
	InstrumentAnalyte        string         `db:"instrument_analyte"`
	ControlInstrumentAnalyte sql.NullString `db:"control_instrument_analyte"`
	AnalyteID                uuid.UUID      `db:"analyte_id"`
	ResultType               ResultType     `db:"result_type"`
	ControlResultRequired    bool           `db:"control_result_required"`
	CreatedAt                time.Time      `db:"created_at"`
	ModifiedAt               sql.NullTime   `db:"modified_at"`
	DeletedAt                sql.NullTime   `db:"deleted_at"`
	ChannelMapping           []channelMappingDAO
	ResultMapping            []resultMappingDAO
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

type expectedControlResultDAO struct {
	ID               uuid.UUID         `db:"id"`
	AnalyteMappingID uuid.UUID         `db:"analyte_mapping_id"`
	SampleCode       string            `db:"sample_code"`
	Operator         ConditionOperator `db:"operator"`
	ExpectedValue    string            `db:"expected_value"`
	ExpectedValue2   sql.NullString    `db:"expected_value2"`
	CreatedAt        time.Time         `db:"created_at"`
	DeletedAt        sql.NullTime      `db:"deleted_at"`
	CreatedBy        uuid.UUID         `db:"created_by"`
	DeletedBy        uuid.NullUUID     `db:"deleted_by"`
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

type supportedManufacturerTestsDAO struct {
	ID                uuid.UUID    `db:"id"`
	TestName          string       `db:"test_name"`
	Channels          string       `db:"channels"`
	ValidResultValues string       `db:"valid_result_values"`
	CreatedAt         time.Time    `db:"created_at"`
	ModifiedAt        sql.NullTime `db:"modified_at"`
	DeletedAt         sql.NullTime `db:"deleted_at"`
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
	DeleteFtpConfig(ctx context.Context, instrumentId uuid.UUID) error
	GetProtocolByID(ctx context.Context, id uuid.UUID) (SupportedProtocol, error)
	GetSupportedProtocols(ctx context.Context) ([]SupportedProtocol, error)
	UpsertSupportedProtocol(ctx context.Context, id uuid.UUID, name string, description string) error
	GetProtocolAbilities(ctx context.Context, protocolID uuid.UUID) ([]ProtocolAbility, error)
	UpsertProtocolAbilities(ctx context.Context, protocolID uuid.UUID, protocolAbilities []ProtocolAbility) error
	GetProtocolSettings(ctx context.Context, protocolID uuid.UUID) ([]ProtocolSetting, error)
	UpsertProtocolSetting(ctx context.Context, protocolID uuid.UUID, protocolSetting ProtocolSetting) error
	DeleteProtocolSettings(ctx context.Context, protocolSettingIDs []uuid.UUID) error
	UpsertManufacturerTests(ctx context.Context, manufacturerTests []SupportedManufacturerTests) error
	GetManufacturerTests(ctx context.Context) ([]SupportedManufacturerTests, error)
	UpdateInstrumentStatus(ctx context.Context, id uuid.UUID, status InstrumentStatus) error
	UpsertAnalyteMappings(ctx context.Context, analyteMappings []AnalyteMapping, instrumentID uuid.UUID) ([]uuid.UUID, error)
	GetAnalyteMappings(ctx context.Context, instrumentIDs []uuid.UUID) (map[uuid.UUID][]AnalyteMapping, error)
	DeleteAnalyteMappings(ctx context.Context, ids []uuid.UUID) error
	UpsertChannelMappings(ctx context.Context, channelMappings []ChannelMapping, analyteMappingID uuid.UUID) ([]uuid.UUID, error)
	GetChannelMappings(ctx context.Context, analyteMappingIDs []uuid.UUID) (map[uuid.UUID][]ChannelMapping, error)
	DeleteChannelMappings(ctx context.Context, ids []uuid.UUID) error
	UpsertResultMappings(ctx context.Context, resultMappings []ResultMapping, analyteMappingID uuid.UUID) ([]uuid.UUID, error)
	GetResultMappings(ctx context.Context, analyteMappingIDs []uuid.UUID) (map[uuid.UUID][]ResultMapping, error)
	DeleteResultMappings(ctx context.Context, ids []uuid.UUID) error
	CreateExpectedControlResults(ctx context.Context, expectedControlResults []ExpectedControlResult) ([]uuid.UUID, error)
	GetExpectedControlResultsByInstrumentId(ctx context.Context, instrumentId uuid.UUID) ([]ExpectedControlResult, error)
	GetNotSpecifiedExpectedControlResultsByInstrumentId(ctx context.Context, instrumentId uuid.UUID) ([]NotSpecifiedExpectedControlResult, error)
	GetExpectedControlResultsByInstrumentIdAndSampleCodes(ctx context.Context, instrumentId uuid.UUID, sampleCodes []string) (map[uuid.UUID]ExpectedControlResult, error)
	GetExpectedControlResultsByAnalyteMappingIds(ctx context.Context, analyteMappingIds []uuid.UUID) (map[uuid.UUID][]ExpectedControlResult, error)
	UpdateExpectedControlResults(ctx context.Context, expectedControlResults []ExpectedControlResult) error
	DeleteExpectedControlResults(ctx context.Context, ids []uuid.UUID, deletedByUserId uuid.UUID) error
	DeleteExpectedControlResultsByAnalyteMappingIDs(ctx context.Context, ids []uuid.UUID, deletedByUserId uuid.UUID) error
	UpsertRequestMappingAnalytes(ctx context.Context, analyteIDsByRequestMappingID map[uuid.UUID][]uuid.UUID) error
	UpsertRequestMappings(ctx context.Context, requestMappings []RequestMapping, instrumentID uuid.UUID) error
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
	query := fmt.Sprintf(`INSERT INTO %s.sk_instruments(id, protocol_id, "type", "name", hostname, client_port, enabled, connection_mode, running_mode, captureresults, capturediagnostics, replytoquery, status, timezone, file_encoding)
		VALUES(:id, :protocol_id, :type, :name, :hostname, :client_port, :enabled, :connection_mode, :running_mode, :captureresults, :capturediagnostics, :replytoquery, :status, :timezone, :file_encoding);`, r.dbSchema)

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
            timezone = :timezone, file_encoding = :file_encoding, modified_at = timezone('utc',now()) 
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
	query := fmt.Sprintf(`INSERT INTO %s.sk_instrument_ftp_config(id, instrument_id, username, password,
        order_path, order_file_mask, order_file_suffix, result_path, result_file_mask, result_file_suffix, ftp_server_type)
		VALUES(:id, :instrument_id, :username, :password, :order_path, :order_file_mask, :order_file_suffix, :result_path,
		:result_file_mask, :result_file_suffix, :ftp_server_type)`, r.dbSchema)
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
			log.Warn().Interface("instrumentId", instrumentId).Msg("ftp config queried but not found")
			return ftpConfig, ErrFtpConfigNotFound
		}
		log.Error().Err(err).Msg(msgGetFtpConfigFailed)
		return ftpConfig, ErrGetFtpConfigFailed
	}

	return convertFtpConfigDaoToFtpConfig(dao), nil
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
		DO UPDATE SET abilities = excluded.abilities, request_mapping_available = excluded.request_mapping_available, modified_at = timezone('utc', now());`, r.dbSchema)
	protocolAbilityDAOs := convertProtocolAbilitiesToDAOs(protocolAbilities, protocolID)
	_, err := r.db.NamedExecContext(ctx, query, protocolAbilityDAOs)
	if err != nil {
		log.Error().Err(err).Msg(msgUpsertProtocolAbilitiesFailed)
		return ErrUpsertProtocolAbilitiesFailed
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

func (r *instrumentRepository) UpsertManufacturerTests(ctx context.Context, manufacturerTests []SupportedManufacturerTests) error {
	query := fmt.Sprintf(`INSERT INTO %s.sk_manufacturer_tests(test_name, channels, valid_result_values)
		VALUES(:test_name, :channels, :valid_result_values)
		ON CONFLICT (test_name) WHERE deleted_at IS NULL
		DO UPDATE SET channels = excluded.channels, valid_result_values = excluded.valid_result_values, modified_at = timezone('utc', now());`, r.dbSchema)
	manufacturerTestsDAOs := convertSupportedManufacturerTestsToDAOs(manufacturerTests)
	_, err := r.db.NamedExecContext(ctx, query, manufacturerTestsDAOs)
	if err != nil {
		log.Error().Err(err).Msg(msgUpsertManufacturerTestsFailed)
		return ErrUpsertManufacturerTestsFailed
	}
	return nil
}

func (r *instrumentRepository) GetManufacturerTests(ctx context.Context) ([]SupportedManufacturerTests, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_manufacturer_tests WHERE deleted_at IS NULL;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query)
	if err != nil {
		log.Error().Err(err).Msg(msgGetManufacturerTestsFailed)
		return nil, ErrGetManufacturerTestsFailed
	}
	defer rows.Close()
	manufacturerTests := make([]SupportedManufacturerTests, 0)
	for rows.Next() {
		var dao supportedManufacturerTestsDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetManufacturerTestsFailed)
			return nil, ErrGetManufacturerTestsFailed
		}
		manufacturerTest := convertDAOToSupportedManufacturerTest(dao)
		if err != nil {
			return nil, err
		}
		manufacturerTests = append(manufacturerTests, manufacturerTest)
	}
	return manufacturerTests, nil
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

func (r *instrumentRepository) UpsertAnalyteMappings(ctx context.Context, analyteMappings []AnalyteMapping, instrumentID uuid.UUID) ([]uuid.UUID, error) {
	if len(analyteMappings) == 0 {
		return []uuid.UUID{}, nil
	}
	ids := make([]uuid.UUID, len(analyteMappings))
	for i := range analyteMappings {
		ids[i] = analyteMappings[i].ID
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_analyte_mappings(id, instrument_id, instrument_analyte, analyte_id, result_type, control_instrument_analyte, control_result_required) 
		VALUES(:id, :instrument_id, :instrument_analyte, :analyte_id, :result_type, :control_instrument_analyte, :control_result_required)
		ON CONFLICT (id) WHERE deleted_at IS NULL DO UPDATE SET instrument_analyte = excluded.instrument_analyte, analyte_id = excluded.analyte_id,
		    result_type = excluded.result_type, control_instrument_analyte = excluded.control_instrument_analyte,
		    control_result_required = excluded.control_result_required, modified_at = timezone('utc', now());`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertAnalyteMappingsToDAOs(analyteMappings, instrumentID))
	if err != nil {
		log.Error().Err(err).Msg(msgUpsertAnalyteMappingsFailed)
		if IsErrorCode(err, UniqueViolationErrorCode) {
			if strings.Contains(err.Error(), "sk_un_analyte_mapping_instrument_id_control_instrument_analyte") {
				return []uuid.UUID{}, ErrUniqueViolationInstrumentControlAnalyte
			}
			return []uuid.UUID{}, ErrUniqueViolationInstrumentAnalyte
		}
		return []uuid.UUID{}, ErrUpsertAnalyteMappingsFailed
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

func (r *instrumentRepository) UpsertChannelMappings(ctx context.Context, channelMappings []ChannelMapping, analyteMappingID uuid.UUID) ([]uuid.UUID, error) {
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
		VALUES(:id, :instrument_channel, :channel_id, :analyte_mapping_id) ON CONFLICT (id)
		    DO UPDATE SET instrument_channel=excluded.instrument_channel, channel_id=excluded.channel_id, analyte_mapping_id=excluded.analyte_mapping_id, modified_at = timezone('utc', now());`, r.dbSchema)
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

func (r *instrumentRepository) UpsertResultMappings(ctx context.Context, resultMappings []ResultMapping, analyteMappingID uuid.UUID) ([]uuid.UUID, error) {
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
		VALUES(:id, :analyte_mapping_id, :key, :value, :index)
		ON CONFLICT(id) DO UPDATE SET analyte_mapping_id=excluded.analyte_mapping_id,key=excluded.key,value=excluded.value,index=excluded.index,modified_at=timezone('utc', now());`, r.dbSchema)
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

func (r *instrumentRepository) CreateExpectedControlResults(ctx context.Context, expectedControlResults []ExpectedControlResult) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, 0)
	for i := range expectedControlResults {
		if (expectedControlResults[i].ID == uuid.UUID{} || expectedControlResults[i].ID == uuid.Nil) {
			expectedControlResults[i].ID = uuid.New()
		}
		ids = append(ids, expectedControlResults[i].ID)
	}
	if len(ids) == 0 {
		return ids, nil
	}

	query := fmt.Sprintf(`INSERT INTO %s.sk_expected_control_result(id, analyte_mapping_id, sample_code, operator, expected_value, expected_value2, created_at, deleted_at, created_by, deleted_by) 
		VALUES(:id, :analyte_mapping_id, :sample_code, :operator, :expected_value, :expected_value2, timezone('utc', now()), :deleted_at, :created_by, :deleted_by);`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertExpectedControlResultsToDAOs(expectedControlResults))
	if err != nil {
		log.Error().Err(err).Msg(msgCreateExpectedControlResultsFailed)
		if IsErrorCode(err, ForeignKeyViolationErrorCode) {
			return ids, ErrAnalyteMappingNotFound
		}
		return ids, ErrCreateExpectedControlResultsFailed
	}
	return ids, nil
}

func (r *instrumentRepository) UpdateExpectedControlResults(ctx context.Context, expectedControlResults []ExpectedControlResult) error {
	if len(expectedControlResults) == 0 {
		return nil
	}
	expectedControlResultDAOs := convertExpectedControlResultsToDAOs(expectedControlResults)
	query := fmt.Sprintf(`UPDATE %s.sk_expected_control_result SET operator = :operator, expected_value = :expected_value, expected_value2 = :expected_value2 WHERE id = :id;`, r.dbSchema)
	for i := range expectedControlResultDAOs {
		_, err := r.db.NamedExecContext(ctx, query, expectedControlResultDAOs[i])
		if err != nil {
			log.Error().Err(err).Msg(msgCreateExpectedControlResultsFailed)
			if IsErrorCode(err, ForeignKeyViolationErrorCode) {
				return ErrAnalyteMappingNotFound
			}
			return ErrCreateExpectedControlResultsFailed
		}
	}
	return nil
}

func (r *instrumentRepository) DeleteExpectedControlResults(ctx context.Context, ids []uuid.UUID, deletedByUserId uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	query := fmt.Sprintf(`UPDATE %s.sk_expected_control_result SET deleted_at = timezone('utc', now()), deleted_by = ? WHERE id IN (?);`, r.dbSchema)
	query, args, _ := sqlx.In(query, deletedByUserId, ids)
	query = r.db.Rebind(query)
	_, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgDeleteExpectedControlResultFailed)
		return ErrDeleteExpectedControlResultFailed
	}
	return nil
}

func (r *instrumentRepository) DeleteExpectedControlResultsByAnalyteMappingIDs(ctx context.Context, analyteMappingIDs []uuid.UUID, deletedByUserId uuid.UUID) error {
	if len(analyteMappingIDs) == 0 {
		return nil
	}
	query := fmt.Sprintf(`UPDATE %s.sk_expected_control_result SET deleted_at = timezone('utc', now()), deleted_by = ? WHERE analyte_mapping_id IN (?);`, r.dbSchema)
	query, args, _ := sqlx.In(query, deletedByUserId, analyteMappingIDs)
	query = r.db.Rebind(query)
	_, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgDeleteExpectedControlResultFailed)
		return ErrDeleteExpectedControlResultFailed
	}
	return nil
}

func (r *instrumentRepository) GetExpectedControlResultsByInstrumentId(ctx context.Context, instrumentId uuid.UUID) ([]ExpectedControlResult, error) {
	expectedControlResults := make([]ExpectedControlResult, 0)
	if (instrumentId == uuid.UUID{}) || (instrumentId == uuid.Nil) {
		return expectedControlResults, nil
	}
	query := fmt.Sprintf(`WITH groupping AS (
	SELECT secr.sample_code, min(secr.created_at) as minCreatedAt
		FROM %s.sk_expected_control_result secr
			INNER JOIN %s.sk_analyte_mappings sam ON secr.analyte_mapping_id = sam.id WHERE sam.instrument_id = $1 AND secr.deleted_at IS NULL GROUP BY secr.sample_code
	)
	SELECT secr.id, secr.analyte_mapping_id, secr.sample_code, secr.operator, secr.expected_value, secr.expected_value2, secr.created_at, secr.deleted_at, secr.created_by, secr.deleted_by
		FROM %s.sk_expected_control_result secr
			INNER JOIN groupping ON secr.sample_code = groupping.sample_code
			INNER JOIN %s.sk_analyte_mappings sam ON secr.analyte_mapping_id = sam.id WHERE sam.instrument_id = $1 AND secr.deleted_at IS NULL ORDER BY groupping.minCreatedAt desc, secr.sample_code, sam.instrument_analyte;`, r.dbSchema, r.dbSchema, r.dbSchema, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query, instrumentId)
	if err != nil {
		log.Error().Err(err).Msg(msgGetExpectedControlResultsFailed)
		return nil, ErrGetExpectedControlResultsFailed
	}
	defer rows.Close()
	for rows.Next() {
		var dao expectedControlResultDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetExpectedControlResultsFailed)
			return nil, ErrGetExpectedControlResultsFailed
		}
		expectedControlResults = append(expectedControlResults, convertExpectedControlResultDaoToExpectedControlResult(dao))
	}
	return expectedControlResults, nil
}

func (r *instrumentRepository) GetNotSpecifiedExpectedControlResultsByInstrumentId(ctx context.Context, instrumentId uuid.UUID) ([]NotSpecifiedExpectedControlResult, error) {
	notSpecifiedExpectedControlResults := make([]NotSpecifiedExpectedControlResult, 0)
	if (instrumentId == uuid.UUID{}) || (instrumentId == uuid.Nil) {
		return notSpecifiedExpectedControlResults, nil
	}
	query := fmt.Sprintf(`SELECT scr.sample_code, scr.analyte_mapping_id
	FROM %s.sk_control_results scr
    	INNER JOIN %s.sk_analyte_mappings sam ON scr.instrument_id = sam.instrument_id AND scr.analyte_mapping_id = sam.id
    	LEFT JOIN %s.sk_expected_control_result secr ON sam.id = secr.analyte_mapping_id AND scr.sample_code = secr.sample_code
	WHERE scr.instrument_id = $1 and secr.id is null
	GROUP BY scr.sample_code, scr.analyte_mapping_id;`, r.dbSchema, r.dbSchema, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query, instrumentId)
	if err != nil {
		log.Error().Err(err).Msg(msgGetNotSpecifiedExpectedControlResultsFailed)
		return nil, ErrGetNotSpecifiedExpectedControlResultsFailed
	}
	defer rows.Close()
	for rows.Next() {
		var sampleCode string
		var analyteMappingId uuid.UUID
		err = rows.Scan(&sampleCode, &analyteMappingId)
		if err != nil {
			log.Error().Err(err).Msg(msgGetNotSpecifiedExpectedControlResultsFailed)
			return nil, ErrGetNotSpecifiedExpectedControlResultsFailed
		}
		notSpecifiedExpectedControlResults = append(notSpecifiedExpectedControlResults, NotSpecifiedExpectedControlResult{
			SampleCode:       sampleCode,
			AnalyteMappingId: analyteMappingId,
		})
	}
	return notSpecifiedExpectedControlResults, nil
}

func (r *instrumentRepository) GetExpectedControlResultsByInstrumentIdAndSampleCodes(ctx context.Context, instrumentId uuid.UUID, sampleCodes []string) (map[uuid.UUID]ExpectedControlResult, error) {
	expectedControlResultsMapById := make(map[uuid.UUID]ExpectedControlResult)
	if (instrumentId == uuid.UUID{}) || (instrumentId == uuid.Nil) {
		return expectedControlResultsMapById, nil
	}
	query := fmt.Sprintf(`SELECT secr.* FROM %s.sk_expected_control_result secr
         INNER JOIN %s.sk_analyte_mappings sam ON secr.analyte_mapping_id = sam.id WHERE sam.instrument_id = ? AND secr.sample_code IN (?) AND secr.deleted_at IS NULL ORDER BY secr.sample_code;`, r.dbSchema, r.dbSchema)
	query, args, _ := sqlx.In(query, instrumentId, sampleCodes)
	query = r.db.Rebind(query)
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgGetExpectedControlResultsFailed)
		return nil, ErrGetExpectedControlResultsFailed
	}
	defer rows.Close()
	for rows.Next() {
		var dao expectedControlResultDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetExpectedControlResultsFailed)
			return nil, ErrGetExpectedControlResultsFailed
		}
		expectedControlResultsMapById[dao.ID] = convertExpectedControlResultDaoToExpectedControlResult(dao)
	}
	return expectedControlResultsMapById, nil
}

func (r *instrumentRepository) GetExpectedControlResultsByAnalyteMappingIds(ctx context.Context, analyteMappingIds []uuid.UUID) (map[uuid.UUID][]ExpectedControlResult, error) {
	expectedControlResultMappingsByAnalyteMappingID := make(map[uuid.UUID][]ExpectedControlResult)
	if analyteMappingIds == nil || len(analyteMappingIds) == 0 {
		return expectedControlResultMappingsByAnalyteMappingID, nil
	}
	query := fmt.Sprintf(`SELECT secr.* FROM %s.sk_expected_control_result secr WHERE secr.analyte_mapping_id in (?) AND secr.deleted_at IS NULL;`, r.dbSchema)
	query, args, _ := sqlx.In(query, analyteMappingIds)
	query = r.db.Rebind(query)
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgGetExpectedControlResultsFailed)
		return nil, ErrGetExpectedControlResultsFailed
	}
	defer rows.Close()
	for rows.Next() {
		var dao expectedControlResultDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetExpectedControlResultsFailed)
			return nil, ErrGetExpectedControlResultsFailed
		}
		if _, ok := expectedControlResultMappingsByAnalyteMappingID[dao.AnalyteMappingID]; !ok {
			expectedControlResultMappingsByAnalyteMappingID[dao.AnalyteMappingID] = make([]ExpectedControlResult, 0)
		}
		expectedControlResultMappingsByAnalyteMappingID[dao.AnalyteMappingID] = append(expectedControlResultMappingsByAnalyteMappingID[dao.AnalyteMappingID], convertExpectedControlResultDaoToExpectedControlResult(dao))
	}
	return expectedControlResultMappingsByAnalyteMappingID, nil
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
		log.Error().Err(err).Msg(msgUpsertAnalyteMappingsFailed)
		return ErrUpsertAnalyteMappingsFailed
	}
	return nil
}

func (r *instrumentRepository) UpsertRequestMappings(ctx context.Context, requestMappings []RequestMapping, instrumentID uuid.UUID) error {
	if len(requestMappings) == 0 {
		return nil
	}
	daos := make([]requestMappingDAO, len(requestMappings))
	query := fmt.Sprintf(`INSERT INTO %s.sk_request_mappings (id, code, instrument_id, is_default)
								VALUES (:id, :code, :instrument_id, :is_default)
								ON CONFLICT (id) DO UPDATE SET code = excluded.code, is_default = excluded.is_default, modified_at = timezone('utc', now());`, r.dbSchema)
	for i := range requestMappings {
		daos[i] = convertRequestMappingToDAO(requestMappings[i], instrumentID)
	}
	_, err := r.db.NamedExecContext(ctx, query, daos)
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
	query := fmt.Sprintf(`SELECT request_mapping_id, analyte_id FROM %s.sk_request_mapping_analytes WHERE request_mapping_id IN (?) AND deleted_at IS NULL;`, r.dbSchema)
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
		OrderPath:        ftpConfig.OrderPath,
		OrderFileMask:    ftpConfig.OrderFileMask,
		OrderFileSuffix:  ftpConfig.OrderFileSuffix,
		ResultPath:       ftpConfig.ResultPath,
		ResultFileMask:   ftpConfig.ResultFileMask,
		ResultFileSuffix: ftpConfig.ResultFileSuffix,
		FtpServerType:    ftpConfig.FtpServerType,
		CreatedAt:        ftpConfig.CreatedAt,
	}
}

func convertFtpConfigDaoToFtpConfig(dao ftpConfigDAO) FTPConfig {
	return FTPConfig{
		ID:               dao.ID,
		InstrumentId:     dao.InstrumentId,
		Username:         dao.Username,
		Password:         dao.Password,
		OrderPath:        dao.OrderPath,
		OrderFileMask:    dao.OrderFileMask,
		OrderFileSuffix:  dao.OrderFileSuffix,
		ResultPath:       dao.ResultPath,
		ResultFileMask:   dao.ResultFileMask,
		ResultFileSuffix: dao.ResultFileSuffix,
		FtpServerType:    dao.FtpServerType,
		CreatedAt:        dao.CreatedAt,
	}
}

func convertAnalyteMappingToDAO(analyteMapping AnalyteMapping, instrumentID uuid.UUID) analyteMappingDAO {
	dao := analyteMappingDAO{
		ID:                    analyteMapping.ID,
		InstrumentID:          instrumentID,
		InstrumentAnalyte:     analyteMapping.InstrumentAnalyte,
		AnalyteID:             analyteMapping.AnalyteID,
		ResultType:            analyteMapping.ResultType,
		ControlResultRequired: analyteMapping.ControlResultRequired,
	}
	if analyteMapping.ControlInstrumentAnalyte != nil {
		dao.ControlInstrumentAnalyte = sql.NullString{
			String: *analyteMapping.ControlInstrumentAnalyte,
			Valid:  len(*analyteMapping.ControlInstrumentAnalyte) > 0,
		}
	}
	return dao
}

func convertAnalyteMappingsToDAOs(analyteMappings []AnalyteMapping, instrumentID uuid.UUID) []analyteMappingDAO {
	analyteMappingDAOs := make([]analyteMappingDAO, len(analyteMappings))
	for i := range analyteMappings {
		analyteMappingDAOs[i] = convertAnalyteMappingToDAO(analyteMappings[i], instrumentID)
	}
	return analyteMappingDAOs
}

func convertAnalyteMappingDaoToAnalyteMapping(dao analyteMappingDAO) AnalyteMapping {
	analyteMapping := AnalyteMapping{
		ID:                    dao.ID,
		InstrumentAnalyte:     dao.InstrumentAnalyte,
		AnalyteID:             dao.AnalyteID,
		ResultType:            dao.ResultType,
		ControlResultRequired: dao.ControlResultRequired,
	}
	if dao.ControlInstrumentAnalyte.Valid {
		analyteMapping.ControlInstrumentAnalyte = &dao.ControlInstrumentAnalyte.String
	}

	return analyteMapping
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

func convertExpectedControlResultToDAO(expectedControlResult ExpectedControlResult) expectedControlResultDAO {
	dao := expectedControlResultDAO{
		ID:               expectedControlResult.ID,
		AnalyteMappingID: expectedControlResult.AnalyteMappingId,
		SampleCode:       expectedControlResult.SampleCode,
		Operator:         expectedControlResult.Operator,
		ExpectedValue:    expectedControlResult.ExpectedValue,
		CreatedAt:        expectedControlResult.CreatedAt,
		CreatedBy:        expectedControlResult.CreatedBy,
		DeletedBy:        expectedControlResult.DeletedBy,
	}
	if expectedControlResult.ExpectedValue2 != nil {
		dao.ExpectedValue2 = sql.NullString{
			String: *expectedControlResult.ExpectedValue2,
			Valid:  true,
		}
	}
	if expectedControlResult.DeletedAt != nil {
		dao.DeletedAt = sql.NullTime{
			Time:  *expectedControlResult.DeletedAt,
			Valid: true,
		}
	}
	return dao
}

func convertExpectedControlResultsToDAOs(expectedControlResults []ExpectedControlResult) []expectedControlResultDAO {
	expectedControlResultsDAOs := make([]expectedControlResultDAO, 0)
	for _, expectedControlResult := range expectedControlResults {
		expectedControlResultsDAOs = append(expectedControlResultsDAOs, convertExpectedControlResultToDAO(expectedControlResult))
	}
	return expectedControlResultsDAOs
}

func convertExpectedControlResultDaoToExpectedControlResult(dao expectedControlResultDAO) ExpectedControlResult {
	expectedControlResult := ExpectedControlResult{
		ID:               dao.ID,
		AnalyteMappingId: dao.AnalyteMappingID,
		SampleCode:       dao.SampleCode,
		Operator:         dao.Operator,
		ExpectedValue:    dao.ExpectedValue,
		CreatedAt:        dao.CreatedAt,
		CreatedBy:        dao.CreatedBy,
		DeletedBy:        dao.DeletedBy,
	}

	if dao.ExpectedValue2.Valid {
		expectedControlResult.ExpectedValue2 = &dao.ExpectedValue2.String
	}
	if dao.DeletedAt.Valid {
		expectedControlResult.DeletedAt = &dao.DeletedAt.Time
	}

	return expectedControlResult
}

func convertRequestMappingToDAO(requestMapping RequestMapping, instrumentID uuid.UUID) requestMappingDAO {
	return requestMappingDAO{
		ID:           requestMapping.ID,
		InstrumentID: instrumentID,
		Code:         requestMapping.Code,
		IsDefault:    requestMapping.IsDefault,
	}
}

func convertSupportedProtocolDAOToSupportedProtocol(dao supportedProtocolDAO) SupportedProtocol {
	return SupportedProtocol{
		ID:          dao.ID,
		Name:        dao.Name,
		Description: util.SqlNullStringToStringPointer(dao.Description),
	}
}

func convertProtocolAbilityToDAO(protocolAbility ProtocolAbility, protocolID uuid.UUID) protocolAbilityDAO {
	return protocolAbilityDAO{
		ProtocolID:              protocolID,
		ConnectionMode:          string(protocolAbility.ConnectionMode),
		Abilities:               util.JoinEnumsAsString(protocolAbility.Abilities, ","),
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

func convertSupportedManufacturerTestToDAO(manufacturerTest SupportedManufacturerTests) supportedManufacturerTestsDAO {
	return supportedManufacturerTestsDAO{
		TestName:          manufacturerTest.TestName,
		Channels:          strings.Join(manufacturerTest.Channels, ","),
		ValidResultValues: strings.Join(manufacturerTest.ValidResultValues, ","),
	}
}

func convertSupportedManufacturerTestsToDAOs(manufacturerTests []SupportedManufacturerTests) []supportedManufacturerTestsDAO {
	manufacturerTestsDAOs := make([]supportedManufacturerTestsDAO, len(manufacturerTests))
	for i := range manufacturerTests {
		manufacturerTestsDAOs[i] = convertSupportedManufacturerTestToDAO(manufacturerTests[i])
	}
	return manufacturerTestsDAOs
}

func convertDAOToSupportedManufacturerTest(dao supportedManufacturerTestsDAO) SupportedManufacturerTests {
	return SupportedManufacturerTests{
		TestName:          dao.TestName,
		Channels:          strings.Split(dao.Channels, ","),
		ValidResultValues: strings.Split(dao.ValidResultValues, ","),
	}
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
