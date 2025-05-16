package skeleton

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/blutspende/skeleton/db"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"time"
)

type MessageOutRepository interface {
	Create(ctx context.Context, message MessageOut) (uuid.UUID, error)
	GetUnprocessed(ctx context.Context, maxRetries, sentDaysBack, limit, offset int) ([]MessageOut, error)
	Update(ctx context.Context, message MessageOut) error
	UpdateDEAInfo(ctx context.Context, message MessageOut) error
	LinkSampleCodes(ctx context.Context, sampleCodes []string, id uuid.UUID) error

	WithTransaction(tx db.DbConnector) MessageOutRepository
	CreateTransaction() (db.DbConnector, error)
}

type messageOutRepository struct {
	db       db.DbConnector
	dbSchema string
}

func (r *messageOutRepository) Create(ctx context.Context, message MessageOut) (uuid.UUID, error) {
	if message.ID == uuid.Nil {
		message.ID = uuid.New()
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_message_out (id, instrument_id, status, protocol_id, "type", encoding, raw, trigger_message_in_id)
									VALUES (:id, :instrument_id, :status, :protocol_id, :type, :encoding, :raw, :trigger_message_in_id);`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertMessageOutToDAO(message))
	if err != nil {
		log.Error().Err(err).Msg(msgCreateMessageOutFailed)
		return uuid.Nil, ErrCreateMessageOutFailed
	}

	return message.ID, nil
}

func (r *messageOutRepository) GetUnprocessed(ctx context.Context, maxRetries, sentDaysBack, limit, offset int) ([]MessageOut, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_message_in 
         							WHERE "type" <> $1 AND retry_count < $2 status IN ($3, $4)
         								AND created_at >= (current_date - make_interval(days := $5)) 
         								ORDER BY created_at DESC LIMIT $6 OFFSET $7;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query, MessageTypeAcknowledgement, maxRetries, MessageStatusStored, MessageStatusError, sentDaysBack, limit, offset)
	if err != nil {
		log.Error().Err(err).Msg(msgGetUnprocessedMessageOutsFailed)
		return nil, ErrGetUnprocessedMessageOutsFailed
	}
	defer rows.Close()
	messages := make([]MessageOut, 0)
	for rows.Next() {
		var dao messageOutDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetUnprocessedMessageOutsFailed)
			return nil, ErrGetUnprocessedMessageOutsFailed
		}
		messages = append(messages, convertDAOToMessageOut(dao))
	}

	return messages, nil
}

func (r *messageOutRepository) Update(ctx context.Context, message MessageOut) error {
	query := fmt.Sprintf(`UPDATE %s.sk_message_out SET status = :status,
									error = :error, retry_count = :retry_count, response_message_in_id = :response_message_in_id, 
									modified_at = timezone('utc', now())
									WHERE id = :id`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertMessageOutToDAO(message))
	if err != nil {
		log.Error().Err(err).Msg(msgUpdateMessageOutFailed)
		return ErrUpdateMessageOutFailed
	}

	return nil
}

func (r *messageOutRepository) UpdateDEAInfo(ctx context.Context, message MessageOut) error {
	//TODO implement me
	panic("implement me")
}

func (r *messageOutRepository) LinkSampleCodes(ctx context.Context, sampleCodes []string, id uuid.UUID) error {
	if len(sampleCodes) == 0 {
		return nil
	}
	preparedValues := make([]map[string]interface{}, len(sampleCodes))
	for i := range sampleCodes {
		preparedValues[i] = map[string]interface{}{
			"sample_code":    sampleCodes[i],
			"message_out_id": id,
		}
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_message_out_samplecodes (sample_code, message_out_id)
									VALUES (:sample_code, :message_out_id);`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, preparedValues)
	if err != nil {
		log.Error().Err(err).Msg(msgLinkSampleCodesToMessageOutFailed)
		return ErrLinkSampleCodesToMessageOutFailed
	}

	return nil
}

func (r *messageOutRepository) WithTransaction(tx db.DbConnector) MessageOutRepository {
	txRepo := *r
	txRepo.db = tx
	return &txRepo
}

func (r *messageOutRepository) CreateTransaction() (db.DbConnector, error) {
	return r.db.CreateTransactionConnector()
}

func NewMessageOutRepository(db db.DbConnector, dbSchema string) MessageOutRepository {
	return &messageOutRepository{
		db:       db,
		dbSchema: dbSchema,
	}
}

type messageOutDAO struct {
	ID                  uuid.UUID      `db:"id"`
	InstrumentID        uuid.UUID      `db:"instrument_id"`
	Status              MessageStatus  `db:"status"`
	DEARawMessageID     uuid.NullUUID  `db:"dea_raw_message_id"`
	ProtocolID          uuid.UUID      `db:"protocol_id"`
	Type                MessageType    `db:"type"`
	Encoding            string         `db:"encoding"`
	Raw                 []byte         `db:"raw"`
	Error               sql.NullString `db:"error"`
	RetryCount          int            `db:"retry_count"`
	TriggerMessageInID  uuid.NullUUID  `db:"trigger_message_in_id"`
	ResponseMessageInID uuid.NullUUID  `db:"response_message_in_id"`
	CreatedAt           time.Time      `db:"created_at"`
	ModifiedAt          sql.NullTime   `db:"modified_at"`
}

func convertMessageOutToDAO(messageOut MessageOut) messageOutDAO {
	dao := messageOutDAO{
		ID:                  messageOut.ID,
		InstrumentID:        messageOut.InstrumentID,
		Status:              messageOut.Status,
		DEARawMessageID:     messageOut.DEARawMessageID,
		ProtocolID:          messageOut.ProtocolID,
		Type:                messageOut.Type,
		Encoding:            messageOut.Encoding,
		Raw:                 messageOut.Raw,
		RetryCount:          messageOut.RetryCount,
		TriggerMessageInID:  messageOut.TriggerMessageInID,
		ResponseMessageInID: messageOut.ResponseMessageInID,
		CreatedAt:           messageOut.CreatedAt,
	}
	if messageOut.Error != nil {
		dao.Error = sql.NullString{
			String: *messageOut.Error,
			Valid:  len(*messageOut.Error) > 0,
		}
	}
	if messageOut.ModifiedAt != nil {
		dao.ModifiedAt = sql.NullTime{
			Time:  *messageOut.ModifiedAt,
			Valid: !messageOut.ModifiedAt.IsZero(),
		}
	}

	return dao
}

func convertDAOToMessageOut(dao messageOutDAO) MessageOut {
	messageOut := MessageOut{
		ID:                  dao.ID,
		InstrumentID:        dao.InstrumentID,
		Status:              dao.Status,
		DEARawMessageID:     dao.DEARawMessageID,
		ProtocolID:          dao.ProtocolID,
		Type:                dao.Type,
		Encoding:            dao.Encoding,
		Raw:                 dao.Raw,
		RetryCount:          dao.RetryCount,
		TriggerMessageInID:  dao.TriggerMessageInID,
		ResponseMessageInID: dao.ResponseMessageInID,
		CreatedAt:           dao.CreatedAt,
	}
	if dao.Error.Valid {
		messageOut.Error = &dao.Error.String
	}
	if dao.ModifiedAt.Valid {
		messageOut.ModifiedAt = &dao.ModifiedAt.Time
	}

	return messageOut
}

const (
	msgCreateMessageOutFailed            = "create message out failed"
	msgGetUnprocessedMessageOutsFailed   = "get unprocessed message outs failed"
	msgUpdateMessageOutFailed            = "update message out failed"
	msgLinkSampleCodesToMessageOutFailed = "link sample code to message out failed"
)

var (
	ErrCreateMessageOutFailed            = errors.New(msgCreateMessageOutFailed)
	ErrGetUnprocessedMessageOutsFailed   = errors.New(msgGetUnprocessedMessageOutsFailed)
	ErrUpdateMessageOutFailed            = errors.New(msgUpdateMessageOutFailed)
	ErrLinkSampleCodesToMessageOutFailed = errors.New(msgLinkSampleCodesToMessageOutFailed)
)
