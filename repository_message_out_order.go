package skeleton

import (
	"context"
	"errors"
	"fmt"
	"github.com/blutspende/bloodlab-common/messagestatus"
	"github.com/blutspende/bloodlab-common/utils"
	"github.com/blutspende/skeleton/db"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
	"strings"
)

type MessageOutOrderRepository interface {
	CreateBatch(ctx context.Context, messageOutOrders []MessageOutOrder) ([]uuid.UUID, error)
	AddAnalysisRequestsToMessageOutOrder(ctx context.Context, messageOutOrderID uuid.UUID, analysisRequestIDs []uuid.UUID) error
	GetBySampleCodesAndRequestMappingIDs(ctx context.Context, sampleCodes []string, instrumentID uuid.UUID, includePending bool) (map[string]map[uuid.UUID][]MessageOutOrder, error)
	GetTestCodesToRevokeBySampleCodes(ctx context.Context, instrumentID uuid.UUID, analysisRequestIDs []uuid.UUID) (map[string][]string, error)
	GetTestCodesToCancelBySampleCodes(ctx context.Context, instrumentID uuid.UUID, analysisRequestIDs []uuid.UUID) (map[string][]string, error)
	CreateTransaction() (db.DbConnection, error)

	WithTransaction(tx db.DbConnection) MessageOutOrderRepository
}

type messageOutOrderRepository struct {
	db         db.DbConnection
	dbSchema   string
	maxRetries int
}

func (r *messageOutOrderRepository) CreateBatch(ctx context.Context, messageOutOrders []MessageOutOrder) ([]uuid.UUID, error) {
	if len(messageOutOrders) == 0 {
		return nil, nil
	}

	messageOutOrderIDs := make([]uuid.UUID, len(messageOutOrders))
	for i := range messageOutOrders {
		if messageOutOrders[i].ID == uuid.Nil {
			messageOutOrders[i].ID = uuid.New()
		}
		messageOutOrderIDs[i] = messageOutOrders[i].ID
	}
	err := utils.Partition(len(messageOutOrders), maxParams/4, func(low int, high int) error {
		query := fmt.Sprintf(`INSERT INTO %s.sk_message_out_orders (id, message_out_id, sample_code, request_mapping_id)
										VALUES (:id, :message_out_id, :sample_code, :request_mapping_id);`, r.dbSchema)
		_, err := r.db.NamedExecContext(ctx, query, convertMessageOutOrdersToDAOs(messageOutOrders[low:high]))
		if err != nil {
			log.Error().Err(err).Msg(msgCreateMessageOutOrderBatchFailed)
			return ErrCreateMessageOutOrderBatchFailed
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return messageOutOrderIDs, nil
}

func (r *messageOutOrderRepository) AddAnalysisRequestsToMessageOutOrder(ctx context.Context, messageOutOrderID uuid.UUID, analysisRequestIDs []uuid.UUID) error {
	if len(analysisRequestIDs) == 0 {
		return nil
	}

	preparedValues := make([]map[string]interface{}, len(analysisRequestIDs))
	for i := range analysisRequestIDs {
		preparedValues[i] = map[string]interface{}{
			"message_out_order_id": messageOutOrderID,
			"analysis_request_id":  analysisRequestIDs[i],
		}
	}
	err := utils.Partition(len(preparedValues), maxParams/2, func(low int, high int) error {
		query := fmt.Sprintf(`INSERT INTO %s.sk_message_out_order_analysis_requests (message_out_order_id, analysis_request_id)
					VALUES (:message_out_order_id, :analysis_request_id) ON CONFLICT (message_out_order_id, analysis_request_id) DO NOTHING;`, r.dbSchema)
		_, err := r.db.NamedExecContext(ctx, query, preparedValues[low:high])
		if err != nil {
			log.Error().Err(err).Msg(msgAddAnalysisRequestsToMessageOutOrderFailed)
			return ErrAddAnalysisRequestsToMessageOutOrderFailed
		}

		return nil
	})

	return err
}

func (r *messageOutOrderRepository) GetBySampleCodesAndRequestMappingIDs(ctx context.Context, sampleCodes []string, instrumentID uuid.UUID, includePending bool) (map[string]map[uuid.UUID][]MessageOutOrder, error) {
	queryArgs := []interface{}{sampleCodes, instrumentID, messagestatus.Sent}
	filterCondition := " AND (smo.status = ?"
	if includePending {
		filterCondition += " OR smo.retry_count < ?"
		queryArgs = append(queryArgs, r.maxRetries)
	}
	filterCondition += ")"

	query := fmt.Sprintf(`SELECT DISTINCT smoo.* FROM <schema_name>.sk_message_out smo
	INNER JOIN <schema_name>.sk_message_out_orders smoo ON smo.id = smoo.message_out_id
	INNER JOIN <schema_name>.sk_message_out_order_analysis_requests smooar ON smoo.id = smooar.message_out_order_id
	INNER JOIN <schema_name>.sk_analysis_requests sar ON sar.id = smooar.analysis_request_id
		WHERE smoo.sample_code IN (?) AND smo.instrument_id = ? AND sar.deleted_at IS NULL %s;`, filterCondition)
	query = strings.ReplaceAll(query, "<schema_name>", r.dbSchema)
	query, args, _ := sqlx.In(query, queryArgs...)
	query = r.db.Rebind(query)
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgGetMessageOutOrdersBySampleCodesAndRequestMappingIDsFailed)
		return nil, ErrGetMessageOutOrdersBySampleCodesAndRequestMappingIDsFailed
	}
	defer rows.Close()
	messageOutOrdersBySampleCodesMap := make(map[string]map[uuid.UUID][]MessageOutOrder)
	for rows.Next() {
		var dao messageOutOrderDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetMessageOutOrdersBySampleCodesAndRequestMappingIDsFailed)
			return nil, ErrGetMessageOutOrdersBySampleCodesAndRequestMappingIDsFailed
		}
		if _, ok := messageOutOrdersBySampleCodesMap[dao.SampleCode]; !ok {
			messageOutOrdersBySampleCodesMap[dao.SampleCode] = make(map[uuid.UUID][]MessageOutOrder)
		}
		messageOutOrdersBySampleCodesMap[dao.SampleCode][dao.RequestMappingID] = append(messageOutOrdersBySampleCodesMap[dao.SampleCode][dao.RequestMappingID], convertDAOToMessageOutOrder(dao))
	}

	return messageOutOrdersBySampleCodesMap, nil
}

func (r *messageOutOrderRepository) GetTestCodesToRevokeBySampleCodes(ctx context.Context, instrumentID uuid.UUID, analysisRequestIDs []uuid.UUID) (map[string][]string, error) {
	if len(analysisRequestIDs) == 0 {
		return nil, nil
	}
	query := `SELECT DISTINCT srm.code, smoo.sample_code FROM %schema_name%.sk_message_out smo
				INNER JOIN %schema_name%.sk_message_out_orders smoo ON smo.id = smoo.message_out_id
				INNER JOIN %schema_name%.sk_request_mappings srm ON srm.id = smoo.request_mapping_id
				INNER JOIN %schema_name%.sk_message_out_order_analysis_requests smooar on smoo.id = smooar.message_out_order_id
					WHERE smo.instrument_id = ? AND smo.status = ? 
						AND NOT EXISTS (SELECT 1 FROM %schema_name%.sk_message_out_order_analysis_requests smoor INNER JOIN %schema_name%.sk_analysis_requests sar on sar.id = smoor.analysis_request_id
											WHERE smoor.message_out_order_id = smoo.id AND sar.deleted_at IS NULL)
						AND smooar.analysis_request_id IN (?);`
	query = strings.ReplaceAll(query, "%schema_name%", r.dbSchema)
	query, args, _ := sqlx.In(query, instrumentID, messagestatus.Sent, analysisRequestIDs)
	query = r.db.Rebind(query)
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgGetTestCodesToRevokeBySampleCodesFailed)
		return nil, ErrGetTestCodesToRevokeBySampleCodesFailed
	}
	defer rows.Close()

	testCodesBySampleCodes := make(map[string][]string)
	for rows.Next() {
		var testCode, sampleCode string
		err = rows.Scan(&testCode, &sampleCode)
		if err != nil {
			log.Error().Err(err).Msg(msgGetTestCodesToRevokeBySampleCodesFailed)
			return nil, ErrGetTestCodesToRevokeBySampleCodesFailed
		}
		if _, ok := testCodesBySampleCodes[sampleCode]; !ok {
			testCodesBySampleCodes[sampleCode] = make([]string, 0)
		}
		testCodesBySampleCodes[sampleCode] = append(testCodesBySampleCodes[sampleCode], testCode)
	}

	return testCodesBySampleCodes, nil
}

func (r *messageOutOrderRepository) GetTestCodesToCancelBySampleCodes(ctx context.Context, instrumentID uuid.UUID, analysisRequestIDs []uuid.UUID) (map[string][]string, error) {
	if len(analysisRequestIDs) == 0 {
		return nil, nil
	}
	query := `SELECT DISTINCT srm.code, smoo.sample_code FROM %schema_name%.sk_message_out smo
				INNER JOIN %schema_name%.sk_message_out_orders smoo ON smo.id = smoo.message_out_id
				INNER JOIN %schema_name%.sk_request_mappings srm ON srm.id = smoo.request_mapping_id
				INNER JOIN %schema_name%.sk_message_out_order_analysis_requests smooar on smoo.id = smooar.message_out_order_id
					WHERE smo.instrument_id = ? AND smo.status = ? AND smooar.analysis_request_id IN (?);`
	query = strings.ReplaceAll(query, "%schema_name%", r.dbSchema)
	query, args, _ := sqlx.In(query, instrumentID, messagestatus.Sent, analysisRequestIDs)
	query = r.db.Rebind(query)
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgGetTestCodesToCancelBySampleCodesFailed)
		return nil, ErrGetTestCodesToCancelBySampleCodesFailed
	}
	defer rows.Close()

	testCodesBySampleCodes := make(map[string][]string)
	for rows.Next() {
		var testCode, sampleCode string
		err = rows.Scan(&testCode, &sampleCode)
		if err != nil {
			log.Error().Err(err).Msg(msgGetTestCodesToCancelBySampleCodesFailed)
			return nil, ErrGetTestCodesToCancelBySampleCodesFailed
		}
		if _, ok := testCodesBySampleCodes[sampleCode]; !ok {
			testCodesBySampleCodes[sampleCode] = make([]string, 0)
		}
		testCodesBySampleCodes[sampleCode] = append(testCodesBySampleCodes[sampleCode], testCode)
	}

	return testCodesBySampleCodes, nil
}

func (r *messageOutOrderRepository) CreateTransaction() (db.DbConnection, error) {
	return r.db.CreateTransactionConnector()
}

func (r *messageOutOrderRepository) WithTransaction(tx db.DbConnection) MessageOutOrderRepository {
	txRepo := *r
	txRepo.db = tx
	return &txRepo
}

func NewMessageOutOrderRepository(db db.DbConnection, dbSchema string, maxRetries int) MessageOutOrderRepository {
	return &messageOutOrderRepository{
		db:         db,
		dbSchema:   dbSchema,
		maxRetries: maxRetries,
	}
}

type messageOutOrderDAO struct {
	ID               uuid.UUID `db:"id"`
	MessageOutID     uuid.UUID `db:"message_out_id"`
	SampleCode       string    `db:"sample_code"`
	RequestMappingID uuid.UUID `db:"request_mapping_id"`
}

func convertMessageOutOrdersToDAOs(moos []MessageOutOrder) []messageOutOrderDAO {
	daos := make([]messageOutOrderDAO, len(moos))
	for i := range moos {
		daos[i] = convertMessageOutOrderToDAO(moos[i])
	}

	return daos
}

func convertMessageOutOrderToDAO(moo MessageOutOrder) messageOutOrderDAO {
	return messageOutOrderDAO{
		ID:               moo.ID,
		MessageOutID:     moo.MessageOutID,
		SampleCode:       moo.SampleCode,
		RequestMappingID: moo.RequestMappingID,
	}
}

func convertDAOToMessageOutOrder(dao messageOutOrderDAO) MessageOutOrder {
	return MessageOutOrder{
		ID:               dao.ID,
		MessageOutID:     dao.MessageOutID,
		SampleCode:       dao.SampleCode,
		RequestMappingID: dao.RequestMappingID,
	}
}

const (
	msgCreateMessageOutOrderBatchFailed                           = "create message out order failed"
	msgAddAnalysisRequestsToMessageOutOrderFailed                 = "add analysis requests to message out order failed"
	msgGetTestCodesToRevokeBySampleCodesFailed                    = "get test codes to revoke by sample codes failed"
	msgGetTestCodesToCancelBySampleCodesFailed                    = "get test codes to cancel by sample codes failed"
	msgGetMessageOutOrdersBySampleCodesAndRequestMappingIDsFailed = "get message out orders by sample codes and analyte IDs failed"
)

var (
	ErrCreateMessageOutOrderBatchFailed                           = errors.New(msgCreateMessageOutOrderBatchFailed)
	ErrAddAnalysisRequestsToMessageOutOrderFailed                 = errors.New(msgAddAnalysisRequestsToMessageOutOrderFailed)
	ErrGetTestCodesToRevokeBySampleCodesFailed                    = errors.New(msgGetTestCodesToRevokeBySampleCodesFailed)
	ErrGetTestCodesToCancelBySampleCodesFailed                    = errors.New(msgGetTestCodesToCancelBySampleCodesFailed)
	ErrGetMessageOutOrdersBySampleCodesAndRequestMappingIDsFailed = errors.New(msgGetMessageOutOrdersBySampleCodesAndRequestMappingIDsFailed)
)
