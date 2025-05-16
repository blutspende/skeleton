package skeleton

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/blutspende/skeleton/db"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
	"time"
)

type MessageInRepository interface {
	Create(ctx context.Context, message MessageIn) (uuid.UUID, error)
	GetByID(ctx context.Context, id uuid.UUID) (MessageIn, error)
	GetByIDs(ctx context.Context, ids []uuid.UUID) ([]MessageIn, error)
	GetUnprocessedResultMessages(ctx context.Context, maxRetries, sentDaysBack, limit, offset int) ([]MessageIn, error)
	Update(ctx context.Context, message MessageIn) error
	UpdateDEAInfo(ctx context.Context, message MessageIn) error
	LinkSampleCodes(ctx context.Context, sampleCodes []string, id uuid.UUID) error

	WithTransaction(tx db.DbConnector) MessageInRepository
	CreateTransaction() (db.DbConnector, error)
}

type messageInRepository struct {
	db       db.DbConnector
	dbSchema string
}

func (r *messageInRepository) Create(ctx context.Context, message MessageIn) (uuid.UUID, error) {
	if message.ID == uuid.Nil {
		message.ID = uuid.New()
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_message_in (id, instrument_id, protocol_id, "type", encoding, raw)
									VALUES (:id, :instrument_id, :protocol_id, :type, :encoding, :raw);`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertMessageInToDAO(message))
	if err != nil {
		log.Error().Err(err).Msg(msgCreateMessageInFailed)
		return uuid.Nil, ErrCreateMessageInFailed
	}

	return message.ID, nil
}

func (r *messageInRepository) GetByID(ctx context.Context, id uuid.UUID) (MessageIn, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_message_in WHERE id $1;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query, id)
	if err != nil {
		log.Error().Err(err).Msg(msgGetMessageInByIDFailed)
		return MessageIn{}, ErrGetMessageInByIDFailed
	}
	defer rows.Close()

	if rows.Next() {
		var dao messageInDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetMessageInByIDFailed)
			return MessageIn{}, ErrGetMessageInByIDFailed
		}
		return convertDAOToMessageIn(dao), nil
	}

	return MessageIn{}, ErrMessageInNotFound
}

func (r *messageInRepository) GetByIDs(ctx context.Context, ids []uuid.UUID) ([]MessageIn, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	query := fmt.Sprintf(`SELECT * FROM %s.sk_message_in WHERE ID IN (?);`, r.dbSchema)
	query, args, _ := sqlx.In(query, ids)
	query = r.db.Rebind(query)
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgGetMessageInsByIDsFailed)
		return nil, ErrGetMessageInsByIDsFailed
	}
	defer rows.Close()

	messages := make([]MessageIn, 0)
	for rows.Next() {
		var dao messageInDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetMessageInsByIDsFailed)
			return nil, ErrGetMessageInsByIDsFailed
		}
		messages = append(messages, convertDAOToMessageIn(dao))
	}

	return messages, nil
}

func (r *messageInRepository) GetUnprocessedResultMessages(ctx context.Context, maxRetries, sentDaysBack, limit, offset int) ([]MessageIn, error) {
	// TODO - add indices to message_in-message_out tables
	query := fmt.Sprintf(`SELECT * FROM %s.sk_message_in 
         							WHERE "type" = $1 AND retry_count < $2 status IN ($3, $4)
         								AND created_at >= (current_date - make_interval(days := $5)) 
         								ORDER BY created_at DESC LIMIT $6 OFFSET $7;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query, MessageTypeResult, maxRetries, MessageStatusStored, MessageStatusError, sentDaysBack, limit, offset)
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

func (r *messageInRepository) Update(ctx context.Context, message MessageIn) error {
	query := fmt.Sprintf(`UPDATE %s.sk_message_in SET status = :status, 
									error = :error, retry_count = :retry_count, modified_at = timezone('utc', now())
									WHERE id = :id;`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertMessageInToDAO(message))
	if err != nil {
		log.Error().Err(err).Msg(msgUpdateMessageInFailed)
		return ErrUpdateMessageInFailed
	}

	return nil
}

func (r *messageInRepository) UpdateDEAInfo(ctx context.Context, message MessageIn) error {
	query := fmt.Sprintf(`UPDATE %s.sk_message_in SET dea_raw_message_id = :dea_raw_message_id, error = :error,
                            retry_count = :retry_count WHERE id = :id;`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertMessageInToDAO(message))
	if err != nil {
		log.Error().Err(err).Msg(msgUpdateMessageInDEAInfoFailed)
		return ErrUpdateMessageInDEAInfoFailed
	}

	return nil
}

func (r *messageInRepository) LinkSampleCodes(ctx context.Context, sampleCodes []string, id uuid.UUID) error {
	if len(sampleCodes) == 0 {
		return nil
	}
	preparedValues := make([]map[string]interface{}, len(sampleCodes))
	for i := range sampleCodes {
		preparedValues[i] = map[string]interface{}{
			"sample_code":   sampleCodes[i],
			"message_in_id": id,
		}
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_message_in_samplecodes (sample_code, message_in_id)
									VALUES (:sample_code, :message_in_id);`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, preparedValues)
	if err != nil {
		log.Error().Err(err).Msg(msgLinkSampleCodesToMessageInFailed)
		return ErrLinkSampleCodesToMessageInFailed
	}

	return nil
}

func (r *messageInRepository) WithTransaction(tx db.DbConnector) MessageInRepository {
	txRepo := *r
	txRepo.db = tx
	return &txRepo
}

func (r *messageInRepository) CreateTransaction() (db.DbConnector, error) {
	return r.db.CreateTransactionConnector()
}

func NewMessageInRepository(db db.DbConnector, dbSchema string) MessageInRepository {
	return &messageInRepository{
		db:       db,
		dbSchema: dbSchema,
	}
}

type messageInDAO struct {
	ID              uuid.UUID      `db:"id"`
	InstrumentID    uuid.UUID      `db:"instrument_id"`
	Status          MessageStatus  `db:"status"`
	DEARawMessageID uuid.NullUUID  `db:"dea_raw_message_id"`
	ProtocolID      uuid.UUID      `db:"protocol_id"`
	Type            MessageType    `db:"type"`
	Encoding        string         `db:"encoding"`
	Raw             []byte         `db:"raw"`
	Error           sql.NullString `db:"error"`
	RetryCount      int            `db:"retry_count"`
	CreatedAt       time.Time      `db:"created_at"`
	ModifiedAt      sql.NullTime   `db:"modified_at"`
}

func convertMessageInToDAO(messageIn MessageIn) messageInDAO {
	dao := messageInDAO{
		ID:              messageIn.ID,
		InstrumentID:    messageIn.InstrumentID,
		Status:          messageIn.Status,
		DEARawMessageID: messageIn.DEARawMessageID,
		ProtocolID:      messageIn.ProtocolID,
		Type:            messageIn.Type,
		Encoding:        messageIn.Encoding,
		Raw:             messageIn.Raw,
		RetryCount:      messageIn.RetryCount,
		CreatedAt:       messageIn.CreatedAt,
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
		ID:              dao.ID,
		InstrumentID:    dao.InstrumentID,
		Status:          dao.Status,
		DEARawMessageID: dao.DEARawMessageID,
		ProtocolID:      dao.ProtocolID,
		Type:            dao.Type,
		Encoding:        dao.Encoding,
		Raw:             dao.Raw,
		RetryCount:      dao.RetryCount,
		CreatedAt:       dao.CreatedAt,
		ModifiedAt:      nil,
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
	msgCreateMessageInFailed            = "create message in failed"
	msgGetMessageInByIDFailed           = "get message in by ID failed"
	msgGetMessageInsByIDsFailed         = "get message ins by IDs failed"
	msgGetUnprocessedMessageInsFailed   = "get unprocessed message ins failed"
	msgLinkSampleCodesToMessageInFailed = "link sample codes to message in failed"
	msgMessageInNotFound                = "message in not found"
	msgUpdateMessageInDEAInfoFailed     = "update message in DEA info failed"
	msgUpdateMessageInFailed            = "update message in failed"
)

var (
	ErrCreateMessageInFailed            = errors.New(msgCreateMessageInFailed)
	ErrGetMessageInByIDFailed           = errors.New(msgGetMessageInByIDFailed)
	ErrGetMessageInsByIDsFailed         = errors.New(msgGetMessageInsByIDsFailed)
	ErrGetUnprocessedMessageInsFailed   = errors.New(msgGetUnprocessedMessageInsFailed)
	ErrLinkSampleCodesToMessageInFailed = errors.New(msgLinkSampleCodesToMessageInFailed)
	ErrMessageInNotFound                = errors.New(msgMessageInNotFound)
	ErrUpdateMessageInDEAInfoFailed     = errors.New(msgUpdateMessageInDEAInfoFailed)
	ErrUpdateMessageInFailed            = errors.New(msgUpdateMessageInFailed)
)
