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
	"strings"
	"time"
)

type MessageOutRepository interface {
	CreateBatch(ctx context.Context, messages []MessageOut) ([]uuid.UUID, error)
	DeleteByIDs(ctx context.Context, ids []uuid.UUID) error
	GetFullyRevokedUnsentMessageIDsByAnalysisRequestIDs(ctx context.Context, analysisRequestIDs []uuid.UUID) ([]uuid.UUID, error)
	GetUnprocessed(ctx context.Context, limit, offset int, cutoffTime time.Time) ([]MessageOut, error)
	GetUnprocessedByInstrumentID(ctx context.Context, instrumentID uuid.UUID, limit, offset int) ([]MessageOut, error)
	GetUnsynced(ctx context.Context, limit, offset int, cutoffTime time.Time) ([]MessageOut, error)
	Update(ctx context.Context, message MessageOut) error
	UpdateDEAInfo(ctx context.Context, message MessageOut) error

	WithTransaction(tx db.DbConnection) MessageOutRepository
	CreateTransaction() (db.DbConnection, error)
}

type messageOutRepository struct {
	db           db.DbConnection
	dbSchema     string
	maxRetries   int
	sentDaysBack int
}

func (r *messageOutRepository) CreateBatch(ctx context.Context, messages []MessageOut) ([]uuid.UUID, error) {
	if len(messages) == 0 {
		return nil, nil
	}
	messageIDs := make([]uuid.UUID, len(messages))
	for i := range messages {
		if messages[i].ID == uuid.Nil {
			messages[i].ID = uuid.New()
		}
		messageIDs[i] = messages[i].ID
	}
	err := utils.Partition(len(messages), maxParams/8, func(low int, high int) error {
		query := fmt.Sprintf(`INSERT INTO %s.sk_message_out (id, instrument_id, status, protocol_id, "type", encoding, raw, trigger_message_in_id, response_message_in_id)
									VALUES (:id, :instrument_id, :status, :protocol_id, :type, :encoding, :raw, :trigger_message_in_id, :response_message_in_id);`, r.dbSchema)
		_, err := r.db.NamedExecContext(ctx, query, convertMessageOutsToDAOs(messages[low:high]))
		if err != nil {
			log.Error().Err(err).Msg(msgCreateMessageOutBatchFailed)
			return ErrCreateMessageOutBatchFailed
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return messageIDs, nil
}

func (r *messageOutRepository) DeleteByIDs(ctx context.Context, ids []uuid.UUID) error {
	err := utils.Partition(len(ids), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`DELETE FROM %s.sk_message_out WHERE id IN (?);`, r.dbSchema)
		query, args, _ := sqlx.In(query, ids[low:high])
		query = r.db.Rebind(query)
		_, err := r.db.ExecContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg(msgDeleteMessageOutsByIDsFailed)
			return ErrDeleteMessageOutsByIDsFailed
		}

		return nil
	})

	return err
}

func (r *messageOutRepository) GetFullyRevokedUnsentMessageIDsByAnalysisRequestIDs(ctx context.Context, analysisRequestIDs []uuid.UUID) ([]uuid.UUID, error) {
	idsMap := make(map[uuid.UUID]any)
	err := utils.Partition(len(analysisRequestIDs), maxParams/2, func(low int, high int) error {
		query := strings.ReplaceAll(`SELECT smo.id FROM %schema_name%.sk_message_out smo 
    										INNER JOIN %schema_name%.sk_message_out_orders smoo ON smo.id = smoo.message_out_id
											INNER JOIN %schema_name%.sk_message_out_order_analysis_requests smooar on smoo.id = smooar.message_out_order_id
												WHERE smo."type" IN (?, ?) AND smo.status <> ? AND smooar.analysis_request_id IN (?) AND NOT EXISTS 
													( SELECT 1 FROM %schema_name%.sk_message_out_order_analysis_requests smoor LEFT JOIN %schema_name%.sk_analysis_requests sar on sar.id = smoor.analysis_request_id
														WHERE smoor.message_out_order_id = smoo.id AND sar.id IS NOT NULL);`, "%schema_name%", r.dbSchema)
		query, args, err := sqlx.In(query, messagetype.Order, messagetype.Reorder, messagestatus.Sent, analysisRequestIDs[low:high])
		query = r.db.Rebind(query)
		rows, err := r.db.QueryxContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg(msgGetFullyRevokedUnsentMessageOutIDsByAnalysisRequestIDsFailed)
			return ErrGetFullyRevokedUnsentMessageOutIDsByAnalysisRequestIDsFailed
		}
		defer rows.Close()

		for rows.Next() {
			var id uuid.UUID
			err = rows.Scan(&id)
			if err != nil {
				log.Error().Err(err).Msg(msgGetFullyRevokedUnsentMessageOutIDsByAnalysisRequestIDsFailed)
				return ErrGetFullyRevokedUnsentMessageOutIDsByAnalysisRequestIDsFailed
			}
			idsMap[id] = nil
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	ids := make([]uuid.UUID, len(idsMap))
	counter := 0
	for id := range idsMap {
		ids[counter] = id
		counter++
	}

	return ids, nil
}

func (r *messageOutRepository) GetUnprocessed(ctx context.Context, limit, offset int, cutoffTime time.Time) ([]MessageOut, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_message_in 
         							WHERE status IN ($1, $2) AND retry_count < $3 
         								AND created_at >= (current_date - make_interval(days := $4))
         								AND created_at <= $5
         								ORDER BY created_at DESC LIMIT $6 OFFSET $7;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query, messagestatus.Stored, messagestatus.Error, r.maxRetries, r.sentDaysBack, cutoffTime, limit, offset)
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

func (r *messageOutRepository) GetUnprocessedByInstrumentID(ctx context.Context, instrumentID uuid.UUID, limit, offset int) ([]MessageOut, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_message_out
         							WHERE instrument_id = $1 AND status IN ($2, $3) AND retry_count < $4 
         								AND created_at >= (current_date - make_interval(days := $5)) 
         								ORDER BY created_at DESC LIMIT $6 OFFSET $7;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query, instrumentID, messagestatus.Stored, messagestatus.Error, r.maxRetries, r.sentDaysBack, limit, offset)
	if err != nil {
		log.Error().Err(err).Msg(msgGetUnprocessedMessageOutsByInstrumentIDsFailed)
		return nil, ErrGetUnprocessedMessageOutsByInstrumentIDsFailed
	}
	defer rows.Close()
	messages := make([]MessageOut, 0)
	for rows.Next() {
		var dao messageOutDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetUnprocessedMessageOutsByInstrumentIDsFailed)
			return nil, ErrGetUnprocessedMessageOutsByInstrumentIDsFailed
		}
		messages = append(messages, convertDAOToMessageOut(dao))
	}

	return messages, nil
}

func (r *messageOutRepository) GetUnsynced(ctx context.Context, limit, offset int, cutoffTime time.Time) ([]MessageOut, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_message_out WHERE dea_raw_message_id IS NULL AND retry_count < $1 
									AND created_at >= (current_date - make_interval(days := $2))
                                	AND created_at <= $3
                               			ORDER BY created_at DESC LIMIT $4 OFFSET $5;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query, r.maxRetries, r.sentDaysBack, cutoffTime, limit, offset)
	if err != nil {
		log.Error().Err(err).Msg(msgGetUnsyncedMessageOutsFailed)
		return nil, ErrGetUnsyncedMessageOutsFailed
	}
	defer rows.Close()

	messages := make([]MessageOut, 0)
	for rows.Next() {
		var dao messageOutDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetUnsyncedMessageOutsFailed)
			return nil, ErrGetUnsyncedMessageOutsFailed
		}

		messages = append(messages, convertDAOToMessageOut(dao))
	}

	return messages, nil
}

func (r *messageOutRepository) Update(ctx context.Context, message MessageOut) error {
	query := fmt.Sprintf(`UPDATE %s.sk_message_out SET status = :status,
									retry_count = :retry_count, response_message_in_id = :response_message_in_id,
									modified_at = timezone('utc', now())`, r.dbSchema)
	if message.Error != nil {
		query += ", error = :error"
	}
	query += " WHERE id = :id;"
	_, err := r.db.NamedExecContext(ctx, query, convertMessageOutToDAO(message))
	if err != nil {
		log.Error().Err(err).Msg(msgUpdateMessageOutFailed)
		return ErrUpdateMessageOutFailed
	}

	return nil
}

func (r *messageOutRepository) UpdateDEAInfo(ctx context.Context, message MessageOut) error {
	query := fmt.Sprintf(`UPDATE %s.sk_message_out SET dea_raw_message_id = :dea_raw_message_id, retry_count = :retry_count`, r.dbSchema)
	if message.Error != nil {
		query += ", error = :error"
	}
	query += " WHERE id = :id;"
	_, err := r.db.NamedExecContext(ctx, query, convertMessageOutToDAO(message))
	if err != nil {
		log.Error().Err(err).Msg(msgUpdateMessageOutDEAInfoFailed)
		return ErrUpdateMessageOutDEAInfoFailed
	}

	return nil
}

func (r *messageOutRepository) WithTransaction(tx db.DbConnection) MessageOutRepository {
	txRepo := *r
	txRepo.db = tx
	return &txRepo
}

func (r *messageOutRepository) CreateTransaction() (db.DbConnection, error) {
	return r.db.CreateTransactionConnector()
}

func NewMessageOutRepository(db db.DbConnection, dbSchema string, maxRetries, sentDaysBack int) MessageOutRepository {
	return &messageOutRepository{
		db:           db,
		dbSchema:     dbSchema,
		sentDaysBack: sentDaysBack,
		maxRetries:   maxRetries,
	}
}

type messageOutDAO struct {
	ID                  uuid.UUID                   `db:"id"`
	InstrumentID        uuid.UUID                   `db:"instrument_id"`
	Status              messagestatus.MessageStatus `db:"status"`
	DEARawMessageID     uuid.NullUUID               `db:"dea_raw_message_id"`
	ProtocolID          uuid.UUID                   `db:"protocol_id"`
	Type                messagetype.MessageType     `db:"type"`
	Encoding            encoding.Encoding           `db:"encoding"`
	Raw                 []byte                      `db:"raw"`
	Error               sql.NullString              `db:"error"`
	RetryCount          int                         `db:"retry_count"`
	TriggerMessageInID  uuid.NullUUID               `db:"trigger_message_in_id"`
	ResponseMessageInID uuid.NullUUID               `db:"response_message_in_id"`
	CreatedAt           time.Time                   `db:"created_at"`
	ModifiedAt          sql.NullTime                `db:"modified_at"`
}

func convertMessageOutsToDAOs(messages []MessageOut) []messageOutDAO {
	daos := make([]messageOutDAO, len(messages))
	for i := range messages {
		daos[i] = convertMessageOutToDAO(messages[i])
	}

	return daos
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
	msgCreateMessageOutBatchFailed                                  = "create message out failed"
	msgDeleteMessageOutsByIDsFailed                                 = "delete message outs by IDs failed"
	msgGetUnprocessedMessageOutsFailed                              = "get unprocessed message outs failed"
	msgGetUnprocessedMessageOutsByInstrumentIDsFailed               = "get unprocessed message outs failed"
	msgGetUnsyncedMessageOutsFailed                                 = "get unsynced message outs failed"
	msgUpdateMessageOutFailed                                       = "update message out failed"
	msgUpdateMessageOutDEAInfoFailed                                = "update message out DEA info failed"
	msgGetFullyRevokedUnsentMessageOutIDsByAnalysisRequestIDsFailed = "get fully revoked unsent message out IDs by analysis requests failed"
)

var (
	ErrCreateMessageOutBatchFailed                                  = errors.New(msgCreateMessageOutBatchFailed)
	ErrDeleteMessageOutsByIDsFailed                                 = errors.New(msgDeleteMessageOutsByIDsFailed)
	ErrGetUnprocessedMessageOutsFailed                              = errors.New(msgGetUnprocessedMessageOutsFailed)
	ErrGetUnprocessedMessageOutsByInstrumentIDsFailed               = errors.New(msgGetUnprocessedMessageOutsByInstrumentIDsFailed)
	ErrGetUnsyncedMessageOutsFailed                                 = errors.New(msgGetUnsyncedMessageOutsFailed)
	ErrUpdateMessageOutFailed                                       = errors.New(msgUpdateMessageOutFailed)
	ErrUpdateMessageOutDEAInfoFailed                                = errors.New(msgUpdateMessageOutDEAInfoFailed)
	ErrGetFullyRevokedUnsentMessageOutIDsByAnalysisRequestIDsFailed = errors.New(msgGetFullyRevokedUnsentMessageOutIDsByAnalysisRequestIDsFailed)
)
