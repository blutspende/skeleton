package skeleton

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/blutspende/bloodlab-common/encoding"
	"github.com/blutspende/bloodlab-common/messagestatus"
	"github.com/blutspende/bloodlab-common/messagetype"
	"github.com/blutspende/bloodlab-common/utils"
	"github.com/blutspende/skeleton/db"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
	"time"
)

type MessageInRepository interface {
	Create(ctx context.Context, message MessageIn) (uuid.UUID, error)
	GetByDEAIDs(ctx context.Context, deaIDs []uuid.UUID) ([]MessageIn, error)
	GetByIDs(ctx context.Context, ids []uuid.UUID) ([]MessageIn, error)
	GetUnprocessed(ctx context.Context, limit, offset int, cutoffTime time.Time) ([]MessageIn, error)
	GetUnprocessedByInstrumentID(ctx context.Context, instrumentID uuid.UUID, limit, offset int) ([]MessageIn, error)
	GetUnsynced(ctx context.Context, limit, offset int, cutoffTime time.Time) ([]MessageIn, error)
	Update(ctx context.Context, message MessageIn) error
	UpdateDEAInfo(ctx context.Context, message MessageIn) error

	DeleteMessageInSampleCodesByIDs(ctx context.Context, ids []uuid.UUID) error
	GetMessageInIDsBySampleCode(ctx context.Context, sampleCode string) ([]uuid.UUID, error)
	GetMessageInSampleCodeIDsToDelete(ctx context.Context, limit int) ([]uuid.UUID, error)
	GetUnsentMessageInSampleCodes(ctx context.Context) (map[uuid.UUID]map[uuid.UUID]string, error)
	MarkSampleCodesAsUploadedByIDs(ctx context.Context, ids []uuid.UUID) error
	RegisterSampleCodes(ctx context.Context, id uuid.UUID, sampleCodes []string) ([]uuid.UUID, error)

	WithTransaction(tx db.DbConnection) MessageInRepository
	CreateTransaction() (db.DbConnection, error)
}

type messageInRepository struct {
	db           db.DbConnection
	dbSchema     string
	maxRetries   int
	sentDaysBack int
}

func (r *messageInRepository) Create(ctx context.Context, message MessageIn) (uuid.UUID, error) {
	if message.ID == uuid.Nil {
		message.ID = uuid.New()
	}
	if message.CreatedAt.IsZero() {
		message.CreatedAt = time.Now().UTC()
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_message_in (id, instrument_id, instrument_module_id, protocol_id, "type", encoding, raw, status, created_at)
									VALUES (:id, :instrument_id, :instrument_module_id, :protocol_id, :type, :encoding, :raw, :status, :created_at);`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertMessageInToDAO(message))
	if err != nil {
		log.Error().Err(err).Msg(msgCreateMessageInFailed)
		return uuid.Nil, ErrCreateMessageInFailed
	}

	return message.ID, nil
}

func (r *messageInRepository) GetByDEAIDs(ctx context.Context, deaIDs []uuid.UUID) ([]MessageIn, error) {
	if len(deaIDs) == 0 {
		return nil, nil
	}
	messages := make([]MessageIn, 0)
	err := utils.Partition(len(deaIDs), idPartitionSize, func(low int, high int) error {
		query := fmt.Sprintf(`SELECT * FROM %s.sk_message_in WHERE dea_raw_message_id IN (?) ORDER BY created_at DESC;`, r.dbSchema)
		query, args, _ := sqlx.In(query, deaIDs[low:high])
		query = r.db.Rebind(query)
		rows, err := r.db.QueryxContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg(msgGetMessageInsByDEAIDsFailed)
			return ErrGetMessageInsByDEAIDsFailed
		}
		defer rows.Close()

		for rows.Next() {
			var dao messageInDAO
			err = rows.StructScan(&dao)
			if err != nil {
				log.Error().Err(err).Msg(msgGetMessageInsByDEAIDsFailed)
				return ErrGetMessageInsByDEAIDsFailed
			}
			messages = append(messages, convertDAOToMessageIn(dao))
		}
		return nil
	})

	return messages, err
}

func (r *messageInRepository) GetByIDs(ctx context.Context, ids []uuid.UUID) ([]MessageIn, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	messages := make([]MessageIn, 0)
	err := utils.Partition(len(ids), idPartitionSize, func(low int, high int) error {
		query := fmt.Sprintf(`SELECT * FROM %s.sk_message_in WHERE ID IN (?) ORDER BY created_at;`, r.dbSchema)
		query, args, _ := sqlx.In(query, ids[low:high])
		query = r.db.Rebind(query)
		rows, err := r.db.QueryxContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg(msgGetMessageInsByIDsFailed)
			return ErrGetMessageInsByIDsFailed
		}
		defer rows.Close()

		for rows.Next() {
			var dao messageInDAO
			err = rows.StructScan(&dao)
			if err != nil {
				log.Error().Err(err).Msg(msgGetMessageInsByIDsFailed)
				return ErrGetMessageInsByIDsFailed
			}
			messages = append(messages, convertDAOToMessageIn(dao))
		}
		return nil
	})

	return messages, err
}

func (r *messageInRepository) GetUnprocessed(ctx context.Context, limit, offset int, cutoffTime time.Time) ([]MessageIn, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_message_in WHERE status <> $1 AND retry_count < $2 
										AND created_at >= (current_date - make_interval(days := $3))
         								AND created_at <= $4
         								ORDER BY created_at DESC LIMIT $5 OFFSET $6;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query, messagestatus.Processed, r.maxRetries, r.sentDaysBack, cutoffTime, limit, offset)
	if err != nil {
		log.Error().Err(err).Msg(msgGetUnprocessedMessageInsFailed)
		return nil, ErrGetUnprocessedMessageInsFailed
	}
	defer rows.Close()
	messages := make([]MessageIn, 0)
	for rows.Next() {
		var dao messageInDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetUnprocessedMessageInsFailed)
			return nil, ErrGetUnprocessedMessageInsFailed
		}
		messages = append(messages, convertDAOToMessageIn(dao))
	}

	return messages, nil
}

func (r *messageInRepository) GetUnprocessedByInstrumentID(ctx context.Context, instrumentID uuid.UUID, limit, offset int) ([]MessageIn, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_message_in 
         							WHERE instrument_id = $1 AND status <> $2 AND retry_count < $3 
         								AND created_at >= (current_date - make_interval(days := $4) 
         								ORDER BY created_at DESC LIMIT $5 OFFSET $6;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query, instrumentID, messagestatus.Processed, r.maxRetries, r.sentDaysBack, limit, offset)
	if err != nil {
		log.Error().Err(err).Msg(msgGetUnprocessedMessageInsFailed)
		return nil, ErrGetUnprocessedMessageInsFailed
	}
	defer rows.Close()
	messages := make([]MessageIn, 0)
	for rows.Next() {
		var dao messageInDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetUnprocessedMessageInsFailed)
			return nil, ErrGetUnprocessedMessageInsFailed
		}
		messages = append(messages, convertDAOToMessageIn(dao))
	}

	return messages, nil
}

func (r *messageInRepository) GetUnsynced(ctx context.Context, limit, offset int, cutoffTime time.Time) ([]MessageIn, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_message_in WHERE dea_raw_message_id IS NULL AND retry_count < $1 
									AND created_at >= (current_date - make_interval(days := $2))
                               		AND created_at <= $3
                               			ORDER BY created_at DESC LIMIT $4 OFFSET $5;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query, r.maxRetries, r.sentDaysBack, cutoffTime, limit, offset)
	if err != nil {
		log.Error().Err(err).Msg(msgGetUnsyncedMessageInsFailed)
		return nil, ErrGetUnsyncedMessageInsFailed
	}
	defer rows.Close()

	messages := make([]MessageIn, 0)
	for rows.Next() {
		var dao messageInDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetUnsyncedMessageInsFailed)
			return nil, ErrGetUnsyncedMessageInsFailed
		}

		messages = append(messages, convertDAOToMessageIn(dao))
	}

	return messages, nil
}

func (r *messageInRepository) Update(ctx context.Context, message MessageIn) error {
	query := fmt.Sprintf(`UPDATE %s.sk_message_in SET status = :status, 
									retry_count = :retry_count, modified_at = timezone('utc', now())`, r.dbSchema)
	if message.Error != nil {
		query += ", error = :error"
	}
	query += " WHERE id = :id;"
	_, err := r.db.NamedExecContext(ctx, query, convertMessageInToDAO(message))
	if err != nil {
		log.Error().Err(err).Msg(msgUpdateMessageInFailed)
		return ErrUpdateMessageInFailed
	}

	return nil
}

func (r *messageInRepository) UpdateDEAInfo(ctx context.Context, message MessageIn) error {
	query := fmt.Sprintf(`UPDATE %s.sk_message_in SET dea_raw_message_id = :dea_raw_message_id,
                            retry_count = :retry_count`, r.dbSchema)
	if message.Error != nil {
		query += ", error = :error"
	}
	query += " WHERE id = :id;"
	_, err := r.db.NamedExecContext(ctx, query, convertMessageInToDAO(message))
	if err != nil {
		log.Error().Err(err).Msg(msgUpdateMessageInDEAInfoFailed)
		return ErrUpdateMessageInDEAInfoFailed
	}

	return nil
}

func (r *messageInRepository) DeleteMessageInSampleCodesByIDs(ctx context.Context, ids []uuid.UUID) error {
	err := utils.Partition(len(ids), idPartitionSize, func(low int, high int) error {
		query := fmt.Sprintf(`DELETE FROM %s.sk_message_in_sample_codes WHERE id in (?)`, r.dbSchema)
		query, args, _ := sqlx.In(query, ids[low:high])
		query = r.db.Rebind(query)
		_, err := r.db.ExecContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg(msgDeleteMessageInSampleCodesByIDsFailed)
			return ErrDeleteMessageInSampleCodesByIDsFailed
		}
		return nil
	})

	return err
}

func (r *messageInRepository) GetMessageInIDsBySampleCode(ctx context.Context, sampleCode string) ([]uuid.UUID, error) {
	query := fmt.Sprintf(`SELECT message_in_id FROM %s.sk_message_in_sample_codes WHERE sample_code = $1;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query, sampleCode)
	if err != nil {
		log.Error().Err(err).Msg(msgGetMessageInIDsBySampleCodeFailed)
		return nil, ErrGetMessageInIDsBySampleCodeFailed
	}
	defer rows.Close()

	ids := make([]uuid.UUID, 0)
	for rows.Next() {
		var id uuid.UUID
		err = rows.Scan(&id)
		if err != nil {
			log.Error().Err(err).Msg(msgGetMessageInIDsBySampleCodeFailed)
			return nil, ErrGetMessageInIDsBySampleCodeFailed
		}
		ids = append(ids, id)
	}

	return ids, nil
}

func (r *messageInRepository) GetMessageInSampleCodeIDsToDelete(ctx context.Context, limit int) ([]uuid.UUID, error) {
	query := fmt.Sprintf(`SELECT id FROM %s.sk_message_in_sample_codes WHERE uploaded_to_dea_at IS NOT NULL AND created_at < (current_date - make_interval(days := $1)) ORDER BY created_at ASC LIMIT $2;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query, r.sentDaysBack, limit)
	if err != nil {
		log.Error().Err(err).Msg(msgGetMessageInSampleCodeIDsToDeleteFailed)
		return nil, ErrGetMessageInSampleCodeIDsToDeleteFailed
	}
	defer rows.Close()

	ids := make([]uuid.UUID, 0)
	for rows.Next() {
		var id uuid.UUID
		err = rows.Scan(&id)
		if err != nil {
			log.Error().Err(err).Msg(msgGetMessageInSampleCodeIDsToDeleteFailed)
			return nil, ErrGetMessageInSampleCodeIDsToDeleteFailed
		}
		ids = append(ids, id)
	}

	return ids, nil
}

func (r *messageInRepository) GetUnsentMessageInSampleCodes(ctx context.Context) (map[uuid.UUID]map[uuid.UUID]string, error) {
	sampleCodesByMessageIDsAndIDs := make(map[uuid.UUID]map[uuid.UUID]string)
	query := fmt.Sprintf(`SELECT id, message_in_id, sample_code FROM %s.sk_message_in_sample_codes WHERE uploaded_to_dea_at IS NULL;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query)
	if err != nil {
		log.Error().Err(err).Msg(msgGetUnsentMessageInSampleCodeIDsFailed)
		return nil, ErrGetUnsentMessageInSampleCodeIDsFailed
	}
	defer rows.Close()

	for rows.Next() {
		var id, messageID uuid.UUID
		var sampleCode string
		err = rows.Scan(&id, &messageID, &sampleCode)
		if err != nil {
			log.Error().Err(err).Msg(msgGetUnsentMessageInSampleCodeIDsFailed)
			return nil, ErrGetUnsentMessageInSampleCodeIDsFailed
		}
		if _, ok := sampleCodesByMessageIDsAndIDs[messageID]; !ok {
			sampleCodesByMessageIDsAndIDs[messageID] = make(map[uuid.UUID]string)
		}
		sampleCodesByMessageIDsAndIDs[messageID][id] = sampleCode
	}

	return sampleCodesByMessageIDsAndIDs, nil
}

func (r *messageInRepository) MarkSampleCodesAsUploadedByIDs(ctx context.Context, ids []uuid.UUID) error {
	err := utils.Partition(len(ids), idPartitionSize, func(low int, high int) error {
		query := fmt.Sprintf(`UPDATE %s.sk_message_in_sample_codes SET uploaded_to_dea_at = timezone('utc', now()) WHERE id IN (?);`, r.dbSchema)
		query, args, _ := sqlx.In(query, ids[low:high])
		query = r.db.Rebind(query)
		_, err := r.db.ExecContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg(msgMarkMessageInSampleCodesAsUploadedByIDsFailed)
			return ErrMarkMessageInSampleCodesAsUploadedByIDsFailed
		}
		return nil
	})

	return err
}

func (r *messageInRepository) RegisterSampleCodes(ctx context.Context, id uuid.UUID, sampleCodes []string) ([]uuid.UUID, error) {
	preparedValues := make([]map[string]interface{}, len(sampleCodes))
	for i := range sampleCodes {
		preparedValues[i] = map[string]interface{}{
			"message_in_id": id,
			"sample_code":   sampleCodes[i],
		}
	}
	ids := make([]uuid.UUID, 0)
	err := utils.Partition(len(preparedValues), maxParams/2, func(low int, high int) error {
		query := fmt.Sprintf(`INSERT INTO %s.sk_message_in_sample_codes (message_in_id, sample_code) VALUES (:message_in_id, :sample_code) ON CONFLICT (message_in_id, sample_code) DO NOTHING RETURNING id;`, r.dbSchema)
		rows, err := r.db.NamedQueryContext(ctx, query, preparedValues[low:high])
		if err != nil {
			log.Error().Err(err).Msg(msgRegisterSampleCodesToMessageInFailed)
			return ErrRegisterSampleCodesToMessageInFailed
		}
		defer rows.Close()

		for rows.Next() {
			var insertedID uuid.UUID
			err = rows.Scan(&insertedID)
			if err != nil {
				log.Error().Err(err).Msg(msgRegisterSampleCodesToMessageInFailed)
				return ErrRegisterSampleCodesToMessageInFailed
			}
			ids = append(ids, insertedID)
		}

		return nil
	})

	return ids, err
}

func (r *messageInRepository) WithTransaction(tx db.DbConnection) MessageInRepository {
	txRepo := *r
	txRepo.db = tx
	return &txRepo
}

func (r *messageInRepository) CreateTransaction() (db.DbConnection, error) {
	return r.db.CreateTransactionConnector()
}

func NewMessageInRepository(db db.DbConnection, dbSchema string, maxRetries, sentDaysBack int) MessageInRepository {
	return &messageInRepository{
		db:           db,
		dbSchema:     dbSchema,
		maxRetries:   maxRetries,
		sentDaysBack: sentDaysBack,
	}
}

type messageInDAO struct {
	ID                 uuid.UUID                   `db:"id"`
	InstrumentID       uuid.UUID                   `db:"instrument_id"`
	InstrumentModuleID uuid.NullUUID               `db:"instrument_module_id"`
	Status             messagestatus.MessageStatus `db:"status"`
	DEARawMessageID    uuid.NullUUID               `db:"dea_raw_message_id"`
	ProtocolID         uuid.UUID                   `db:"protocol_id"`
	Type               messagetype.MessageType     `db:"type"`
	Encoding           encoding.Encoding           `db:"encoding"`
	Raw                []byte                      `db:"raw"`
	Error              sql.NullString              `db:"error"`
	RetryCount         int                         `db:"retry_count"`
	CreatedAt          time.Time                   `db:"created_at"`
	ModifiedAt         sql.NullTime                `db:"modified_at"`
}

func convertMessageInToDAO(messageIn MessageIn) messageInDAO {
	dao := messageInDAO{
		ID:                 messageIn.ID,
		InstrumentID:       messageIn.InstrumentID,
		InstrumentModuleID: messageIn.InstrumentModuleID,
		Status:             messageIn.Status,
		DEARawMessageID:    messageIn.DEARawMessageID,
		ProtocolID:         messageIn.ProtocolID,
		Type:               messageIn.Type,
		Encoding:           messageIn.Encoding,
		Raw:                messageIn.Raw,
		RetryCount:         messageIn.RetryCount,
		CreatedAt:          messageIn.CreatedAt,
	}
	if messageIn.Error != nil {
		dao.Error = sql.NullString{
			String: *messageIn.Error,
			Valid:  len(*messageIn.Error) > 0,
		}
	}
	if messageIn.ModifiedAt != nil {
		dao.ModifiedAt = sql.NullTime{
			Time:  *messageIn.ModifiedAt,
			Valid: !messageIn.ModifiedAt.IsZero(),
		}
	}

	return dao
}

func convertDAOToMessageIn(dao messageInDAO) MessageIn {
	messageIn := MessageIn{
		ID:                 dao.ID,
		InstrumentID:       dao.InstrumentID,
		InstrumentModuleID: dao.InstrumentModuleID,
		Status:             dao.Status,
		DEARawMessageID:    dao.DEARawMessageID,
		ProtocolID:         dao.ProtocolID,
		Type:               dao.Type,
		Encoding:           dao.Encoding,
		Raw:                dao.Raw,
		RetryCount:         dao.RetryCount,
		CreatedAt:          dao.CreatedAt,
		ModifiedAt:         nil,
	}
	if dao.Error.Valid {
		messageIn.Error = &dao.Error.String
	}
	if dao.ModifiedAt.Valid {
		messageIn.ModifiedAt = &dao.ModifiedAt.Time
	}

	return messageIn
}

const (
	msgCreateMessageInFailed          = "create message in failed"
	msgGetMessageInsByDEAIDsFailed    = "get message ins by DEA IDs failed"
	msgGetMessageInsByIDsFailed       = "get message ins by IDs failed"
	msgGetUnprocessedMessageInsFailed = "get unprocessed message ins failed"
	msgGetUnsyncedMessageInsFailed    = "get unsynced message ins failed"
	msgUpdateMessageInDEAInfoFailed   = "update message in DEA info failed"
	msgUpdateMessageInFailed          = "update message in failed"

	msgDeleteMessageInSampleCodesByIDsFailed         = "delete message in sample codes by IDs failed"
	msgGetMessageInIDsBySampleCodeFailed             = "get message in IDs by sample code failed"
	msgGetMessageInSampleCodeIDsToDeleteFailed       = "get message in sample code IDs to delete failed"
	msgGetUnsentMessageInSampleCodeIDsFailed         = "get unsent message in sample codes failed"
	msgMarkMessageInSampleCodesAsUploadedByIDsFailed = "mark message in sample codes as uploaded to DEA by IDs failed"
	msgRegisterSampleCodesToMessageInFailed          = "register sample codes to message in failed"
)

var (
	ErrCreateMessageInFailed          = errors.New(msgCreateMessageInFailed)
	ErrGetMessageInsByDEAIDsFailed    = errors.New(msgGetMessageInsByDEAIDsFailed)
	ErrGetMessageInsByIDsFailed       = errors.New(msgGetMessageInsByIDsFailed)
	ErrGetUnprocessedMessageInsFailed = errors.New(msgGetUnprocessedMessageInsFailed)
	ErrGetUnsyncedMessageInsFailed    = errors.New(msgGetUnsyncedMessageInsFailed)
	ErrUpdateMessageInDEAInfoFailed   = errors.New(msgUpdateMessageInDEAInfoFailed)
	ErrUpdateMessageInFailed          = errors.New(msgUpdateMessageInFailed)

	ErrDeleteMessageInSampleCodesByIDsFailed         = errors.New(msgDeleteMessageInSampleCodesByIDsFailed)
	ErrGetMessageInIDsBySampleCodeFailed             = errors.New(msgGetMessageInIDsBySampleCodeFailed)
	ErrGetMessageInSampleCodeIDsToDeleteFailed       = errors.New(msgGetMessageInSampleCodeIDsToDeleteFailed)
	ErrGetUnsentMessageInSampleCodeIDsFailed         = errors.New(msgGetUnsentMessageInSampleCodeIDsFailed)
	ErrMarkMessageInSampleCodesAsUploadedByIDsFailed = errors.New(msgMarkMessageInSampleCodesAsUploadedByIDsFailed)
	ErrRegisterSampleCodesToMessageInFailed          = errors.New(msgRegisterSampleCodesToMessageInFailed)
)

const idPartitionSize = 2000
