package skeleton

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/blutspende/skeleton/utils"
	"strconv"
	"strings"
	"time"

	"github.com/blutspende/skeleton/db"
	"github.com/pkg/errors"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
)

const (
	msgFailedToRevokeAnalysisRequests                      = "Failed to revoke analysis requests"
	msgSaveAnalysisRequestsInstrumentTransmissions         = "save analysis requests' instrument transmissions"
	msgIncreaseAnalysisRequestsSentToInstrumentCountFailed = "increase analysis requests sent to instrument count failed"
	msgInvalidResultStatus                                 = "invalid result status"
	msgConvertAnalysisResultsFailed                        = "convert analysis results"
	msgMarshalAnalysisResultsFailed                        = "marshal analysis results failed"
	msgInvalidReagentType                                  = "invalid reagent type"
	msgCreateAnalysisRequestsBatchFailed                   = "create analysis requests batch failed"
	msgGetSampleCodesByOrderIDFailed                       = "get sample codes by order ID failed"
	msgCreateSubjectsFailed                                = "create subject failed"
	msgCreateReagentInfoFailed                             = "create analysis result reagent infos failed"
	msgCreateWarningsFailed                                = "create warnings failed"
	msgFailedToSaveDEAImageID                              = "failed to save DEA image id"
	msgFailedToIncreaseImageUploadRetryCount               = "failed to increase image upload retry count"
	msgFailedToGetStuckImages                              = "failed to get stuck images"
	msgFailedToScanStuckImageId                            = "failed to scan stuck image id"
	msgFailedToScanStuckImageData                          = "failed to scan stuck image data"
	msgFailedToMarkImagesAsSyncedToCerberus                = "failed to mark images as synced to cerberus"
	msgFailedToMarkAnalysisRequestsAsProcessed             = "failed to mark analysis requests as processed"
	msgFailedToMarkAnalysisResultsAsProcessed              = "failed to mark analysis results as processed"
	msgGetAnalysisRequestExtraValuesFailed                 = "get analysis request extra values failed"
	msgDeleteAnalysisRequestExtraValuesFailed              = "delete analysis request extra values failed"
)

var (
	ErrFailedToRevokeAnalysisRequests                      = errors.New(msgFailedToRevokeAnalysisRequests)
	ErrSaveAnalysisRequestsInstrumentTransmissions         = errors.New(msgSaveAnalysisRequestsInstrumentTransmissions)
	ErrIncreaseAnalysisRequestsSentToInstrumentCountFailed = errors.New(msgIncreaseAnalysisRequestsSentToInstrumentCountFailed)
	ErrInvalidReagentType                                  = errors.New(msgInvalidReagentType)
	ErrInvalidResultStatus                                 = errors.New(msgInvalidResultStatus)
	ErrConvertAnalysisResultsFailed                        = errors.New(msgConvertAnalysisResultsFailed)
	ErrMarshalAnalysisResultsFailed                        = errors.New(msgMarshalAnalysisResultsFailed)
	ErrCreateAnalysisRequestsBatchFailed                   = errors.New(msgCreateAnalysisRequestsBatchFailed)
	ErrGetSampleCodesByOrderIDFailed                       = errors.New(msgGetSampleCodesByOrderIDFailed)
	ErrCreateSubjectsFailed                                = errors.New(msgCreateSubjectsFailed)
	ErrCreateReagentInfoFailed                             = errors.New(msgCreateReagentInfoFailed)
	ErrCreateWarningsFailed                                = errors.New(msgCreateWarningsFailed)
	ErrFailedToSaveDEAImageID                              = errors.New(msgFailedToSaveDEAImageID)
	ErrFailedToIncreaseImageUploadRetryCount               = errors.New(msgFailedToSaveDEAImageID)
	ErrFailedToGetStuckImages                              = errors.New(msgFailedToGetStuckImages)
	ErrFailedToScanStuckImageId                            = errors.New(msgFailedToScanStuckImageId)
	ErrFailedToScanStuckImageData                          = errors.New(msgFailedToScanStuckImageData)
	ErrFailedToMarkImagesAsSyncedToCerberus                = errors.New(msgFailedToMarkImagesAsSyncedToCerberus)
	ErrFailedToMarkAnalysisRequestsAsProcessed             = errors.New(msgFailedToMarkAnalysisRequestsAsProcessed)
	ErrFailedToMarkAnalysisResultsAsProcessed              = errors.New(msgFailedToMarkAnalysisResultsAsProcessed)
	ErrGetAnalysisRequestExtraValuesFailed                 = errors.New(msgGetAnalysisRequestExtraValuesFailed)
	ErrDeleteAnalysisRequestExtraValuesFailed              = errors.New(msgDeleteAnalysisRequestExtraValuesFailed)
)

type analysisRequestDAO struct {
	ID                          uuid.UUID    `db:"id"`
	WorkItemID                  uuid.UUID    `db:"work_item_id"`
	AnalyteID                   uuid.UUID    `db:"analyte_id"`
	SampleCode                  string       `db:"sample_code"`
	MaterialID                  uuid.UUID    `db:"material_id"`
	LaboratoryID                uuid.UUID    `db:"laboratory_id"`
	ValidUntilTime              time.Time    `db:"valid_until_time"`
	ReexaminationRequestedCount int          `db:"reexamination_requested_count"`
	SentToInstrumentCount       int          `db:"sent_to_instrument_count"`
	CreatedAt                   time.Time    `db:"created_at"`
	ModifiedAt                  sql.NullTime `db:"modified_at"`
	IsProcessed                 bool         `db:"is_processed"`
}

type subjectInfoDAO struct {
	ID                uuid.UUID      `db:"id"`
	AnalysisRequestID uuid.UUID      `db:"analysis_request_id"`
	Type              SubjectType    `db:"type"`
	DateOfBirth       sql.NullTime   `db:"date_of_birth"`
	FirstName         sql.NullString `db:"first_name"`
	LastName          sql.NullString `db:"last_name"`
	DonorID           sql.NullString `db:"donor_id"`
	DonationID        sql.NullString `db:"donation_id"`
	DonationType      sql.NullString `db:"donation_type"`
	Pseudonym         sql.NullString `db:"pseudonym"`
}

type analysisResultDAO struct {
	ID                       uuid.UUID         `db:"id"`
	AnalyteMappingID         uuid.UUID         `db:"analyte_mapping_id"`
	InstrumentID             uuid.UUID         `db:"instrument_id"`
	InstrumentRunID          uuid.UUID         `db:"instrument_run_id"`
	SampleCode               string            `db:"sample_code"`
	ResultRecordID           uuid.UUID         `db:"result_record_id"`
	BatchID                  uuid.UUID         `db:"batch_id"`
	Result                   string            `db:"result"`
	Status                   ResultStatus      `db:"status"`
	ResultMode               ResultMode        `db:"result_mode"`
	YieldedAt                sql.NullTime      `db:"yielded_at"`
	ValidUntil               time.Time         `db:"valid_until"`
	Operator                 string            `db:"operator"`
	TechnicalReleaseDateTime sql.NullTime      `db:"technical_release_datetime"`
	Edited                   bool              `db:"edited"`
	EditReason               sql.NullString    `db:"edit_reason"`
	IsInvalid                bool              `db:"is_invalid"`
	AnalyteMapping           analyteMappingDAO `db:"analyte_mapping"`
	ChannelResults           []channelResultDAO
	ExtraValues              []resultExtraValueDAO
	ReagentInfos             []reagentInfoDAO
	Images                   []imageDAO
	Warnings                 []warningDAO
}

type channelResultDAO struct {
	ID                    uuid.UUID `db:"id"`
	AnalysisResultID      uuid.UUID `db:"analysis_result_id"`
	ChannelID             uuid.UUID `db:"channel_id"`
	QualitativeResult     string    `db:"qualitative_result"`
	QualitativeResultEdit bool      `db:"qualitative_result_edited"`
	QuantitativeResults   []quantitativeChannelResultDAO
	Images                []imageDAO
}

type quantitativeChannelResultDAO struct {
	ID              uuid.UUID `db:"id"`
	ChannelResultID uuid.UUID `db:"channel_result_id"`
	Metric          string    `db:"metric"`
	Value           string    `db:"value"`
}

type resultExtraValueDAO struct {
	ID               uuid.UUID `db:"id"`
	AnalysisResultID uuid.UUID `db:"analysis_result_id"`
	Key              string    `db:"key"`
	Value            string    `db:"value"`
}

type requestExtraValueDAO struct {
	ID                uuid.UUID `db:"id"`
	AnalysisRequestID uuid.UUID `db:"analysis_request_id"`
	Key               string    `db:"key"`
	Value             string    `db:"value"`
}

type reagentInfoDAO struct {
	ID                      uuid.UUID   `db:"id"`
	AnalysisResultID        uuid.UUID   `db:"analysis_result_id"`
	SerialNumber            string      `db:"serial"`
	Name                    string      `db:"name"`
	Code                    string      `db:"code"`
	ShelfLife               time.Time   `db:"shelfLife"`
	LotNo                   string      `db:"lot_no"`
	ManufacturerName        string      `db:"manufacturer_name"`
	ReagentManufacturerDate time.Time   `db:"reagent_manufacturer_date"`
	ReagentType             ReagentType `db:"reagent_type"`
	UseUntil                time.Time   `db:"use_until"`
	DateCreated             time.Time   `db:"date_created"`
}

type imageDAO struct {
	ID               uuid.UUID      `db:"id"`
	AnalysisResultID uuid.UUID      `db:"analysis_result_id"`
	ChannelResultID  uuid.NullUUID  `db:"channel_result_id"`
	Name             string         `db:"name"`
	Description      sql.NullString `db:"description"`
	ImageBytes       []byte         `db:"image_bytes"`
	DeaImageID       uuid.NullUUID  `db:"dea_image_id"`
	UploadedToDeaAt  sql.NullTime   `db:"uploaded_to_dea_at"`
	UploadRetryCount int            `db:"upload_retry_count"`
	UploadError      sql.NullString `db:"upload_error"`
}

type cerberusImageDAO struct {
	ID          uuid.UUID      `db:"id"`
	DeaImageID  uuid.UUID      `db:"dea_image_id"`
	Name        string         `db:"name"`
	Description sql.NullString `db:"description"`
	YieldedAt   sql.NullTime   `db:"yielded_at"`
	WorkItemID  uuid.UUID      `db:"work_item_id"`
	ChannelID   uuid.NullUUID  `db:"channel_id"`
}

type warningDAO struct {
	ID               uuid.UUID `db:"id"`
	AnalysisResultID uuid.UUID `db:"analysis_result_id"`
	Warning          string    `db:"warning"`
}

type analysisRequestInfoDAO struct {
	RequestID         uuid.UUID      `db:"request_id"`
	SampleCode        string         `db:"sample_code"`
	WorkItemID        uuid.UUID      `db:"work_item_id"`
	AnalyteID         uuid.UUID      `db:"analyte_id"`
	RequestCreatedAt  time.Time      `db:"request_created_at"`
	ResultCreatedAt   sql.NullTime   `db:"result_created_at"`
	AnalyteMappingsID uuid.NullUUID  `db:"analyte_mapping_id"`
	ResultID          uuid.NullUUID  `db:"result_id"`
	TestName          sql.NullString `db:"test_name"`
	TestResult        sql.NullString `db:"test_result"`
	BatchCreatedAt    sql.NullTime   `db:"batch_created_at"`
	SourceIP          sql.NullString `db:"source_ip"`
	InstrumentID      uuid.NullUUID  `db:"instrument_id"`
}

type analysisResultInfoDAO struct {
	ID               uuid.UUID      `db:"result_id"`
	BatchID          uuid.NullUUID  `db:"batch_id"`
	RequestCreatedAt sql.NullTime   `db:"request_created_at"`
	WorkItemID       uuid.NullUUID  `db:"work_item_id"`
	SampleCode       string         `db:"sample_code"`
	AnalyteID        uuid.UUID      `db:"analyte_id"`
	ResultCreatedAt  time.Time      `db:"result_created_at"`
	TestName         sql.NullString `db:"test_name"`
	TestResult       sql.NullString `db:"test_result"`
	Status           string         `db:"status"`
}

type cerberusQueueItemDAO struct {
	ID                  uuid.UUID    `db:"queue_item_id"`
	JsonMessage         string       `db:"json_message"`
	LastHTTPStatus      int          `db:"last_http_status"`
	LastError           string       `db:"last_error"`
	LastErrorAt         sql.NullTime `db:"last_error_at"`
	TrialCount          int          `db:"trial_count"`
	RetryNotBefore      time.Time    `db:"retry_not_before"`
	RawResponse         string       `db:"raw_response"`
	ResponseJsonMessage string       `db:"response_json_message"`
	CreatedAt           time.Time    `db:"created_at"`
}

type AnalysisRepository interface {
	CreateAnalysisRequestsBatch(ctx context.Context, analysisRequests []AnalysisRequest) ([]uuid.UUID, []uuid.UUID, error)
	CreateAnalysisRequestExtraValues(ctx context.Context, extraValuesByAnalysisRequestIDs map[uuid.UUID][]ExtraValue) error
	GetAnalysisRequestsBySampleCodeAndAnalyteID(ctx context.Context, sampleCodes string, analyteID uuid.UUID) ([]AnalysisRequest, error)
	GetAnalysisRequestsBySampleCodes(ctx context.Context, sampleCodes []string, allowResending bool) (map[string][]AnalysisRequest, error)
	GetAnalysisRequestsInfo(ctx context.Context, instrumentID uuid.UUID, filter Filter) ([]AnalysisRequestInfo, int, error)
	GetAnalysisRequestExtraValuesByAnalysisRequestID(ctx context.Context, analysisRequestID uuid.UUID) (map[string]string, error)
	GetSampleCodesByOrderID(ctx context.Context, orderID uuid.UUID) ([]string, error)
	GetAnalysisResultsInfo(ctx context.Context, instrumentID uuid.UUID, filter Filter) ([]AnalysisResultInfo, int, error)
	GetAnalysisBatches(ctx context.Context, instrumentID uuid.UUID, filter Filter) ([]AnalysisBatch, int, error)
	//GetAnalysisRequestsForVisualization(ctx context.Context) (map[string][]AnalysisRequest, error)
	CreateSubjectsBatch(ctx context.Context, subjectInfosByAnalysisRequestID map[uuid.UUID]SubjectInfo) (map[uuid.UUID]uuid.UUID, error)
	GetSubjectsByAnalysisRequestIDs(ctx context.Context, analysisRequestIDs []uuid.UUID) (map[uuid.UUID]SubjectInfo, error)
	GetAnalysisRequestsByWorkItemIDs(ctx context.Context, workItemIds []uuid.UUID) ([]AnalysisRequest, error)
	RevokeAnalysisRequests(ctx context.Context, workItemIds []uuid.UUID) error
	DeleteAnalysisRequestExtraValues(ctx context.Context, workItemIDs []uuid.UUID) error
	IncreaseReexaminationRequestedCount(ctx context.Context, workItemIDs []uuid.UUID) error
	IncreaseSentToInstrumentCounter(ctx context.Context, analysisRequestIDs []uuid.UUID) error

	SaveAnalysisRequestsInstrumentTransmissions(ctx context.Context, analysisRequestIDs []uuid.UUID, instrumentID uuid.UUID) error

	CreateAnalysisResultsBatch(ctx context.Context, analysisResults []AnalysisResult) ([]AnalysisResult, error)
	GetAnalysisResultsBySampleCodeAndAnalyteID(ctx context.Context, sampleCode string, analyteID uuid.UUID) ([]AnalysisResult, error)
	GetAnalysisResultByID(ctx context.Context, id uuid.UUID, allowDeletedAnalyteMapping bool) (AnalysisResult, error)
	GetAnalysisResultsByIDs(ctx context.Context, ids []uuid.UUID) ([]AnalysisResult, error)
	GetAnalysisResultsByBatchIDs(ctx context.Context, batchIDs []uuid.UUID) ([]AnalysisResult, error)
	GetAnalysisResultsByBatchIDsMapped(ctx context.Context, batchIDs []uuid.UUID) (map[uuid.UUID][]AnalysisResultInfo, error)

	GetAnalysisResultQueueItems(ctx context.Context) ([]CerberusQueueItem, error)
	UpdateAnalysisResultQueueItemStatus(ctx context.Context, queueItem CerberusQueueItem) error
	CreateAnalysisResultQueueItem(ctx context.Context, analysisResults []AnalysisResult) (uuid.UUID, error)

	SaveImages(ctx context.Context, images []imageDAO) ([]uuid.UUID, error)
	GetStuckImageIDsForDEA(ctx context.Context) ([]uuid.UUID, error)
	GetStuckImageIDsForCerberus(ctx context.Context) ([]uuid.UUID, error)
	GetImagesForDEAUploadByIDs(ctx context.Context, ids []uuid.UUID) ([]imageDAO, error)
	GetImagesForCerberusSyncByIDs(ctx context.Context, ids []uuid.UUID) ([]cerberusImageDAO, error)
	SaveDEAImageID(ctx context.Context, imageID, deaImageID uuid.UUID) error
	IncreaseImageUploadRetryCount(ctx context.Context, imageID uuid.UUID, error string) error
	MarkImagesAsSyncedToCerberus(ctx context.Context, ids []uuid.UUID) error

	GetUnprocessedAnalysisRequests(ctx context.Context) ([]AnalysisRequest, error)
	GetUnprocessedAnalysisResultIDs(ctx context.Context) ([]uuid.UUID, error)
	MarkAnalysisRequestsAsProcessed(ctx context.Context, analysisRequestIDs []uuid.UUID) error
	MarkAnalysisResultsAsProcessed(ctx context.Context, analysisRequestIDs []uuid.UUID) error

	DeleteOldCerberusQueueItems(ctx context.Context, cleanupDays, limit int) (int64, error)
	DeleteOldAnalysisRequests(ctx context.Context, cleanupDays, limit int) (int64, error)
	DeleteOldAnalysisResults(ctx context.Context, cleanupDays, limit int) (int64, error)

	CreateTransaction() (db.DbConnector, error)
	WithTransaction(tx db.DbConnector) AnalysisRepository
}

type analysisRepository struct {
	db           db.DbConnector
	dbSchema     string
	isExternalTx bool
}

func NewAnalysisRepository(db db.DbConnector, dbSchema string) AnalysisRepository {
	return &analysisRepository{
		db:       db,
		dbSchema: dbSchema,
	}
}

const analysisRequestsBatchSize = 9000

// CreateAnalysisRequestsBatch
// Returns the ID and work item IDs of saved requests
func (r *analysisRepository) CreateAnalysisRequestsBatch(ctx context.Context, analysisRequests []AnalysisRequest) ([]uuid.UUID, []uuid.UUID, error) {
	ids := make([]uuid.UUID, 0, len(analysisRequests))
	workItemIDs := make([]uuid.UUID, 0, len(analysisRequests))
	var err error
	var idsPart, workItemIDsPart []uuid.UUID
	for i := 0; i < len(analysisRequests); i += analysisRequestsBatchSize {
		if len(analysisRequests) >= i+analysisRequestsBatchSize {
			idsPart, workItemIDsPart, err = r.createAnalysisRequestsBatch(ctx, analysisRequests[i:i+analysisRequestsBatchSize])
		} else {
			idsPart, workItemIDsPart, err = r.createAnalysisRequestsBatch(ctx, analysisRequests[i:])
		}
		if err != nil {
			return nil, nil, err
		}
		ids = append(ids, idsPart...)
		workItemIDs = append(workItemIDs, workItemIDsPart...)
	}
	return ids, workItemIDs, nil
}

func (r *analysisRepository) createAnalysisRequestsBatch(ctx context.Context, analysisRequests []AnalysisRequest) ([]uuid.UUID, []uuid.UUID, error) {
	if len(analysisRequests) == 0 {
		return []uuid.UUID{}, []uuid.UUID{}, nil
	}
	ids := make([]uuid.UUID, len(analysisRequests))
	for i := range analysisRequests {
		if (analysisRequests[i].ID == uuid.UUID{}) || (analysisRequests[i].ID == uuid.Nil) {
			analysisRequests[i].ID = uuid.New()
		}
		ids[i] = analysisRequests[i].ID
	}

	query := fmt.Sprintf(`INSERT INTO %s.sk_analysis_requests(id, work_item_id, analyte_id, sample_code, material_id, laboratory_id, valid_until_time, created_at)
				VALUES(:id, :work_item_id, :analyte_id, :sample_code, :material_id, :laboratory_id, :valid_until_time, :created_at)
				ON CONFLICT (work_item_id) DO NOTHING RETURNING work_item_id;`, r.dbSchema)
	rows, err := r.db.NamedQueryContext(ctx, query, convertAnalysisRequestsToDAOs(analysisRequests))
	if err != nil {
		log.Error().Err(err).Msg(msgCreateAnalysisRequestsBatchFailed)
		return []uuid.UUID{}, []uuid.UUID{}, ErrCreateAnalysisRequestsBatchFailed
	}
	defer rows.Close()

	savedWorkItemIDs := make([]uuid.UUID, len(analysisRequests))
	for rows.Next() {
		var workItemID uuid.UUID
		err = rows.Scan(&workItemID)
		if err != nil {
			log.Error().Err(err).Msg(msgCreateAnalysisRequestsBatchFailed)
			return []uuid.UUID{}, []uuid.UUID{}, ErrCreateAnalysisRequestsBatchFailed
		}
		savedWorkItemIDs = append(savedWorkItemIDs, workItemID)
	}

	return ids, savedWorkItemIDs, nil
}

const subjectBatchSize = 6000

func (r *analysisRepository) CreateSubjectsBatch(ctx context.Context, subjectInfosByAnalysisRequestID map[uuid.UUID]SubjectInfo) (map[uuid.UUID]uuid.UUID, error) {
	idsMap := make(map[uuid.UUID]uuid.UUID)
	if len(subjectInfosByAnalysisRequestID) == 0 {
		return idsMap, nil
	}
	subjectDAOs := make([]subjectInfoDAO, 0, len(subjectInfosByAnalysisRequestID))
	for analysisRequestID, subjectInfo := range subjectInfosByAnalysisRequestID {
		subjectDAO := convertSubjectToDAO(subjectInfo, analysisRequestID)
		subjectDAO.ID = uuid.New()
		idsMap[analysisRequestID] = subjectDAO.ID
		subjectDAOs = append(subjectDAOs, subjectDAO)
	}

	var err error
	for i := 0; i < len(subjectDAOs); i += subjectBatchSize {
		if len(subjectDAOs) >= i+subjectBatchSize {
			err = r.createSubjects(ctx, subjectDAOs[i:i+subjectBatchSize])
		} else {
			err = r.createSubjects(ctx, subjectDAOs[i:])
		}
		if err != nil {
			return nil, err
		}
	}
	return idsMap, nil
}

func (r *analysisRepository) createSubjects(ctx context.Context, subjectDAOs []subjectInfoDAO) error {
	query := fmt.Sprintf(`INSERT INTO %s.sk_subject_infos(id, analysis_request_id,"type",date_of_birth,first_name,last_name,donor_id,donation_id,donation_type,pseudonym)
		VALUES(id, :analysis_request_id,:type,:date_of_birth,:first_name,:last_name,:donor_id,:donation_id,:donation_type,:pseudonym);`, r.dbSchema)

	_, err := r.db.NamedExecContext(ctx, query, subjectDAOs)
	if err != nil {
		log.Error().Err(err).Msg("create subjects failed")
		return ErrCreateSubjectsFailed
	}
	return nil
}

func (r *analysisRepository) GetAnalysisRequestsBySampleCodeAndAnalyteID(ctx context.Context, sampleCode string, analyteID uuid.UUID) ([]AnalysisRequest, error) {
	query := fmt.Sprintf(`SELECT *
					FROM %s.sk_analysis_requests sar
					WHERE sar.sample_code = $1
					AND sar.analyte_id = $2
					AND sar.valid_until_time >= timezone('utc',now());`, r.dbSchema)

	rows, err := r.db.QueryxContext(ctx, query, sampleCode, analyteID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Trace().Msg("No analysis requests")
			return []AnalysisRequest{}, nil
		}
		log.Error().Err(err).Msg("Can not search for AnalysisRequests")
		return []AnalysisRequest{}, err
	}
	defer rows.Close()

	analysisRequests := make([]AnalysisRequest, 0)
	for rows.Next() {
		request := analysisRequestDAO{}
		err := rows.StructScan(&request)
		if err != nil {
			log.Error().Err(err).Msg("Can not scan data")
			return []AnalysisRequest{}, err
		}

		analysisRequests = append(analysisRequests, convertAnalysisRequestDAOToAnalysisRequest(request))
	}

	return analysisRequests, err
}

func (r *analysisRepository) GetAnalysisRequestsBySampleCodes(ctx context.Context, sampleCodes []string, allowResending bool) (map[string][]AnalysisRequest, error) {
	analysisRequestsBySampleCodes := make(map[string][]AnalysisRequest)
	if len(sampleCodes) == 0 {
		return analysisRequestsBySampleCodes, nil
	}
	err := utils.Partition(len(sampleCodes), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`SELECT * FROM %s.sk_analysis_requests 
					WHERE sample_code in (?)
					AND valid_until_time >= timezone('utc',now())`, r.dbSchema)
		if !allowResending {
			query += " AND reexamination_requested_count >= sent_to_instrument_count"
		}
		query += ";"

		query, args, _ := sqlx.In(query, sampleCodes[low:high])
		query = r.db.Rebind(query)

		rows, err := r.db.QueryxContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg("Can not search for AnalysisRequests")
			return err
		}

		defer rows.Close()

		for rows.Next() {
			request := analysisRequestDAO{}
			err := rows.StructScan(&request)
			if err != nil {
				log.Error().Err(err).Msg("Can not scan data")
				return err
			}
			if _, ok := analysisRequestsBySampleCodes[request.SampleCode]; !ok {
				analysisRequestsBySampleCodes[request.SampleCode] = make([]AnalysisRequest, 0)
			}
			analysisRequestsBySampleCodes[request.SampleCode] = append(analysisRequestsBySampleCodes[request.SampleCode], convertAnalysisRequestDAOToAnalysisRequest(request))
		}

		return nil
	})

	return analysisRequestsBySampleCodes, err
}

func (r *analysisRepository) GetAnalysisRequestsInfo(ctx context.Context, instrumentID uuid.UUID, filter Filter) ([]AnalysisRequestInfo, int, error) {
	preparedValues := map[string]interface{}{
		"instrument_id": instrumentID,
	}

	query := `select * from (
        SELECT distinct on (req.created_at, req.id)
       req.id AS request_id,
	   req.sample_code AS sample_code,
	   req.work_item_id AS work_item_id,
	   req.analyte_id AS analyte_id,
	   req.created_at as request_created_at,
	   am.analyte_id as analyte_mapping_id,
       am.instrument_analyte as test_name,
	   res.id AS result_id,
	   res."result" AS test_result,
       COALESCE(res.yielded_at, res.created_at) AS result_created_at,
	   i.hostname as source_ip,
	   i.id as instrument_id
FROM %schema_name%.sk_analysis_requests req
LEFT JOIN %schema_name%.sk_analysis_results res ON (res.sample_code = req.sample_code and res.instrument_id = :instrument_id)
LEFT JOIN %schema_name%.sk_analysis_results res2 ON (res2.sample_code = req.sample_code AND res2.instrument_id = :instrument_id AND res.created_at < res2.created_at)
LEFT JOIN %schema_name%.sk_instruments i ON i.id = res.instrument_id
LEFT JOIN %schema_name%.sk_analyte_mappings am ON am.instrument_id = i.id AND req.analyte_id = am.analyte_id
WHERE res2 IS NULL`

	countQuery := `select count(request_id) from (
    SELECT distinct on (req.created_at, req.id) req.id as request_id
FROM %schema_name%.sk_analysis_requests req
LEFT JOIN %schema_name%.sk_analysis_results res ON (res.sample_code = req.sample_code and res.instrument_id = :instrument_id)
LEFT JOIN %schema_name%.sk_analysis_results res2 ON (res2.sample_code = req.sample_code AND res2.instrument_id = :instrument_id AND res.created_at < res2.created_at)
LEFT JOIN %schema_name%.sk_instruments i ON i.id = res.instrument_id
LEFT JOIN %schema_name%.sk_analyte_mappings am ON am.instrument_id = i.id AND req.analyte_id = am.analyte_id
WHERE res2 IS NULL`

	if filter.TimeFrom != nil {
		preparedValues["time_from"] = filter.TimeFrom.UTC()

		query += ` AND req.created_at >= :time_from`
		countQuery += ` AND req.created_at >= :time_from`
	}

	if filter.Filter != nil {
		preparedValues["filter"] = "%" + strings.ToLower(*filter.Filter) + "%"

		query += ` AND (LOWER(req.sample_code) LIKE :filter OR LOWER(am.instrument_analyte) LIKE :filter)`
		countQuery += ` AND (LOWER(req.sample_code) LIKE :filter OR LOWER(am.instrument_analyte) LIKE :filter)`
	}

	query = strings.ReplaceAll(query, "%schema_name%", r.dbSchema)

	query += ` order by req.created_at desc, req.id) as sub `
	countQuery += `) as sub `

	query += applyPagination(filter.Pageable, "", "") + `;`

	countQuery = strings.ReplaceAll(countQuery, "%schema_name%", r.dbSchema)

	log.Trace().Str("query", query).Interface("args", preparedValues).Msg("GetAnalysisRequestsInfo")
	rows, err := r.db.NamedQueryContext(ctx, query, preparedValues)
	if err != nil {
		log.Error().Err(err).Msg("Can not get analysis request list")
		return nil, 0, err
	}
	defer rows.Close()

	requestInfoList := make([]analysisRequestInfoDAO, 0)
	for rows.Next() {
		request := analysisRequestInfoDAO{}
		err = rows.StructScan(&request)
		if err != nil {
			log.Error().Err(err).Msg("Failed to struct scan request")
			return nil, 0, err
		}
		requestInfoList = append(requestInfoList, request)
	}

	stmt, err := r.db.PrepareNamed(countQuery)
	if err != nil {
		log.Error().Err(err).Msg("Failed to prepare named count query")
		return nil, 0, err
	}
	var count int
	if err = stmt.QueryRowx(preparedValues).Scan(&count); err != nil {
		log.Error().Err(err).Msg("Failed to execute named count query")
		return nil, 0, err
	}

	return convertRequestInfoDAOsToRequestInfos(requestInfoList), count, nil
}

func (r *analysisRepository) GetAnalysisRequestExtraValuesByAnalysisRequestID(ctx context.Context, analysisRequestID uuid.UUID) (map[string]string, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_analysis_request_extra_values
					WHERE analysis_request_id = $1;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query, analysisRequestID)
	if err != nil {
		log.Error().Err(err).Msg(msgGetAnalysisRequestExtraValuesFailed)
		return nil, ErrGetAnalysisRequestExtraValuesFailed
	}

	defer rows.Close()

	extraValueMap := make(map[string]string)
	for rows.Next() {
		var extraValue requestExtraValueDAO
		err := rows.StructScan(&extraValue)
		if err != nil {
			log.Error().Err(err).Msg(msgGetAnalysisRequestExtraValuesFailed)
			return nil, ErrGetAnalysisRequestExtraValuesFailed
		}
		extraValueMap[extraValue.Key] = extraValue.Value
	}

	return extraValueMap, nil
}

func (r *analysisRepository) GetSampleCodesByOrderID(ctx context.Context, orderID uuid.UUID) ([]string, error) {
	query := fmt.Sprintf(`SELECT DISTINCT sample_code from %s.sk_analysis_requests sar
									INNER JOIN %s.sk_analysis_request_extra_values sarev ON sar.id = sarev.analysis_request_id
										WHERE valid_until_time >= timezone('utc', now()) AND sarev.key = 'OrderID' and sarev.value = $1;`, r.dbSchema, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query, orderID)
	if err != nil {
		log.Error().Err(err).Msg(msgGetSampleCodesByOrderIDFailed)
		return nil, ErrGetSampleCodesByOrderIDFailed
	}
	defer rows.Close()

	sampleCodes := make([]string, 0)
	for rows.Next() {
		var sampleCode string
		err := rows.Scan(&sampleCode)
		if err != nil {
			log.Error().Err(err).Msg(msgGetSampleCodesByOrderIDFailed)
			return nil, ErrGetSampleCodesByOrderIDFailed
		}

		sampleCodes = append(sampleCodes, sampleCode)
	}

	return sampleCodes, nil
}

func (r *analysisRepository) GetAnalysisResultsInfo(ctx context.Context, instrumentID uuid.UUID, filter Filter) ([]AnalysisResultInfo, int, error) {
	preparedValues := map[string]interface{}{
		"instrument_id": instrumentID,
	}

	query := `SELECT res.id AS result_id,
	   res.sample_code AS sample_code,
	   am.analyte_id AS analyte_id,
	   COALESCE(res.yielded_at, res.created_at) AS result_created_at,
       am.instrument_analyte as test_name,
	   res.result AS test_result,
       res.status AS status,
       req.created_at AS request_created_at,
       req.work_item_id as work_item_id
FROM %schema_name%.sk_analysis_results res
LEFT JOIN %schema_name%.sk_analyte_mappings am ON am.instrument_id = res.instrument_id AND res.analyte_mapping_id = am.id
LEFT JOIN %schema_name%.sk_analysis_requests req ON req.sample_code = res.sample_code AND req.analyte_id = am.analyte_id
WHERE res.instrument_id = :instrument_id`

	countQuery := `SELECT count(res.id)
FROM %schema_name%.sk_analysis_results res
LEFT JOIN %schema_name%.sk_analyte_mappings am ON am.instrument_id = res.instrument_id AND res.analyte_mapping_id = am.id
LEFT JOIN %schema_name%.sk_analysis_requests req ON req.sample_code = res.sample_code AND req.analyte_id = am.analyte_id
WHERE res.instrument_id = :instrument_id`

	if filter.TimeFrom != nil {
		preparedValues["time_from"] = filter.TimeFrom.UTC()

		query += ` AND res.created_at >= :time_from`
		countQuery += ` AND res.created_at >= :time_from`
	}

	if filter.Filter != nil {
		preparedValues["filter"] = "%" + strings.ToLower(*filter.Filter) + "%"

		query += ` AND (LOWER(res.sample_code) LIKE :filter OR LOWER(am.instrument_analyte) LIKE :filter)`
		countQuery += ` AND (LOWER(res.sample_code) LIKE :filter OR LOWER(am.instrument_analyte) LIKE :filter)`
	}

	query = strings.ReplaceAll(query, "%schema_name%", r.dbSchema)
	query += applyPagination(filter.Pageable, "res", "req.created_at DESC, res.id") + `;`

	countQuery = strings.ReplaceAll(countQuery, "%schema_name%", r.dbSchema)
	log.Trace().Str("query", query).Interface("args", preparedValues).Msg("GetAnalysisResultsInfo")

	rows, err := r.db.NamedQueryContext(ctx, query, preparedValues)
	if err != nil {
		log.Error().Err(err).Msg("Can not get analysis request list")
		return nil, 0, err
	}
	defer rows.Close()

	resultInfoList := make([]analysisResultInfoDAO, 0)
	for rows.Next() {
		result := analysisResultInfoDAO{}
		err = rows.StructScan(&result)
		if err != nil {
			log.Error().Err(err).Msg("Failed to struct scan request")
			return nil, 0, err
		}
		resultInfoList = append(resultInfoList, result)
	}

	stmt, err := r.db.PrepareNamed(countQuery)
	if err != nil {
		log.Error().Err(err).Msg("Failed to prepare named count query")
		return nil, 0, err
	}
	var count int
	if err = stmt.QueryRowx(preparedValues).Scan(&count); err != nil {
		log.Error().Err(err).Msg("Failed to execute named count query")
		return nil, 0, err
	}

	return convertResultInfoDAOsToResultInfos(resultInfoList), count, nil
}

func (r *analysisRepository) GetAnalysisBatches(ctx context.Context, instrumentID uuid.UUID, filter Filter) ([]AnalysisBatch, int, error) {
	preparedValues := map[string]interface{}{
		"instrument_id": instrumentID,
	}

	query := `SELECT distinct res.batch_id
FROM %schema_name%.sk_analysis_results res
LEFT JOIN %schema_name%.sk_analyte_mappings am ON am.instrument_id = res.instrument_id AND res.analyte_mapping_id = am.id
LEFT JOIN %schema_name%.sk_analysis_requests req ON req.sample_code = res.sample_code AND req.analyte_id = am.analyte_id
WHERE res.instrument_id = :instrument_id`

	countQuery := `SELECT count(res.id)
FROM %schema_name%.sk_analysis_results res
LEFT JOIN %schema_name%.sk_analyte_mappings am ON am.instrument_id = res.instrument_id AND res.analyte_mapping_id = am.id
LEFT JOIN %schema_name%.sk_analysis_requests req ON req.sample_code = res.sample_code AND req.analyte_id = am.analyte_id
WHERE res.instrument_id = :instrument_id`

	if filter.TimeFrom != nil {
		preparedValues["time_from"] = filter.TimeFrom.UTC()

		query += ` AND req.created_at >= :time_from`
		countQuery += ` AND req.created_at >= :time_from`
	}

	if filter.Filter != nil {
		preparedValues["filter"] = "%" + strings.ToLower(*filter.Filter) + "%"

		query += ` AND (LOWER(res.sample_code) LIKE :filter OR LOWER(am.instrument_analyte) LIKE :filter)`
		countQuery += ` AND (LOWER(res.sample_code) LIKE :filter OR LOWER(am.instrument_analyte) LIKE :filter)`
	}

	query = strings.ReplaceAll(query, "%schema_name%", r.dbSchema)
	query += applyPagination(filter.Pageable, "", "") + `;`

	countQuery = strings.ReplaceAll(countQuery, "%schema_name%", r.dbSchema)
	log.Trace().Str("query", query).Interface("args", preparedValues).Msg("GetAnalysisResultsInfo")
	rows, err := r.db.NamedQueryContext(ctx, query, preparedValues)
	if err != nil {
		log.Error().Err(err).Msg("Can not get analysis request list")
		return nil, 0, err
	}
	defer rows.Close()

	batchIDs := make([]uuid.UUID, 0)
	for rows.Next() {
		var batchID uuid.UUID
		err = rows.Scan(&batchID)
		if err != nil {
			log.Error().Err(err).Msg("Failed to struct scan request")
			return nil, 0, err
		}
		batchIDs = append(batchIDs, batchID)
	}

	batches, err := r.GetAnalysisResultsByBatchIDsMapped(ctx, batchIDs)

	analysisBatchList := make([]AnalysisBatch, 0)

	for batchID, results := range batches {
		analysisBatchList = append(analysisBatchList, AnalysisBatch{
			ID:      batchID,
			Results: results,
		})
	}

	return analysisBatchList, len(batchIDs), err
}

func (r *analysisRepository) GetSubjectsByAnalysisRequestIDs(ctx context.Context, analysisRequestIDs []uuid.UUID) (map[uuid.UUID]SubjectInfo, error) {
	subjectsMap := make(map[uuid.UUID]SubjectInfo)
	if len(analysisRequestIDs) == 0 {
		return subjectsMap, nil
	}
	err := utils.Partition(len(analysisRequestIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`SELECT * FROM %s.sk_subject_infos WHERE analysis_request_id IN (?);`, r.dbSchema)
		query, args, _ := sqlx.In(query, analysisRequestIDs[low:high])
		query = r.db.Rebind(query)
		rows, err := r.db.QueryxContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg("get subjects by analysis request ids failed")
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var subjectDao subjectInfoDAO
			err = rows.StructScan(&subjectDao)
			if err != nil {
				log.Error().Err(err).Msg("scan subject info failed")
				return err
			}
			subjectsMap[subjectDao.AnalysisRequestID] = convertSubjectDAOToSubjectInfo(subjectDao)
		}
		return nil
	})

	return subjectsMap, err
}

func (r *analysisRepository) GetAnalysisRequestsByWorkItemIDs(ctx context.Context, workItemIds []uuid.UUID) ([]AnalysisRequest, error) {
	analysisRequests := make([]AnalysisRequest, 0)
	if len(workItemIds) == 0 {
		return analysisRequests, nil
	}
	err := utils.Partition(len(workItemIds), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`SELECT * FROM %s.sk_analysis_requests WHERE work_item_id in (?);`, r.dbSchema)
		query, args, _ := sqlx.In(query, workItemIds[low:high])
		query = r.db.Rebind(query)

		rows, err := r.db.QueryxContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg("Can not search for AnalysisRequests")
			return err
		}

		defer rows.Close()

		for rows.Next() {
			request := analysisRequestDAO{}
			err := rows.StructScan(&request)
			if err != nil {
				log.Error().Err(err).Msg("Can not scan data")
				return err
			}
			analysisRequests = append(analysisRequests, convertAnalysisRequestDAOToAnalysisRequest(request))
		}

		return nil
	})

	return analysisRequests, err
}

func (r *analysisRepository) RevokeAnalysisRequests(ctx context.Context, workItemIds []uuid.UUID) error {
	if len(workItemIds) == 0 {
		return nil
	}
	err := utils.Partition(len(workItemIds), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`DELETE FROM %s.sk_analysis_requests WHERE work_item_id IN (?);`, r.dbSchema)

		query = strings.ReplaceAll(query, "%schema_name%", r.dbSchema)
		query, args, _ := sqlx.In(query, workItemIds[low:high])
		query = r.db.Rebind(query)
		_, err := r.db.ExecContext(ctx, query, args...)
		return err
	})
	if err != nil {
		log.Error().Err(err).Msg(msgFailedToRevokeAnalysisRequests)
		return ErrFailedToRevokeAnalysisRequests
	}

	return nil
}

func (r *analysisRepository) DeleteAnalysisRequestExtraValues(ctx context.Context, workItemIDs []uuid.UUID) error {
	if len(workItemIDs) == 0 {
		return nil
	}
	err := utils.Partition(len(workItemIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`DELETE FROM %s.sk_analysis_request_extra_values WHERE analysis_request_id IN
 			(SELECT id FROM %s.sk_analysis_requests WHERE work_item_id IN (?));`, r.dbSchema, r.dbSchema)

		query, args, _ := sqlx.In(query, workItemIDs[low:high])
		query = r.db.Rebind(query)
		_, err := r.db.ExecContext(ctx, query, args...)
		return err
	})
	if err != nil {
		log.Error().Err(err).Msg(msgDeleteAnalysisRequestExtraValuesFailed)
		return ErrDeleteAnalysisRequestExtraValuesFailed
	}

	return nil
}

func (r *analysisRepository) IncreaseReexaminationRequestedCount(ctx context.Context, workItemIDs []uuid.UUID) error {
	if len(workItemIDs) == 0 {
		return nil
	}
	err := utils.Partition(len(workItemIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`UPDATE %s.sk_analysis_requests SET reexamination_requested_count = reexamination_requested_count + 1, modified_at = timezone('utc', now()) WHERE work_item_id IN (?);`, r.dbSchema)

		query = strings.ReplaceAll(query, "%schema_name%", r.dbSchema)
		query, args, _ := sqlx.In(query, workItemIDs[low:high])
		query = r.db.Rebind(query)

		_, err := r.db.ExecContext(ctx, query, args...)
		return err
	})
	if err != nil {
		log.Error().Err(err).Msg(msgFailedToRevokeAnalysisRequests)
		return ErrFailedToRevokeAnalysisRequests
	}

	return nil
}

func (r *analysisRepository) IncreaseSentToInstrumentCounter(ctx context.Context, analysisRequestIDs []uuid.UUID) error {
	if len(analysisRequestIDs) == 0 {
		return nil
	}
	err := utils.Partition(len(analysisRequestIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`UPDATE %s.sk_analysis_requests SET sent_to_instrument_count = sent_to_instrument_count + 1 WHERE id IN (?);`, r.dbSchema)

		query = strings.ReplaceAll(query, "%schema_name%", r.dbSchema)
		query, args, _ := sqlx.In(query, analysisRequestIDs[low:high])
		query = r.db.Rebind(query)

		_, err := r.db.ExecContext(ctx, query, args...)
		return err
	})
	if err != nil {
		log.Error().Err(err).Msg(msgIncreaseAnalysisRequestsSentToInstrumentCountFailed)
		return ErrIncreaseAnalysisRequestsSentToInstrumentCountFailed
	}

	return nil
}

func (r *analysisRepository) SaveAnalysisRequestsInstrumentTransmissions(ctx context.Context, analysisRequestIDs []uuid.UUID, instrumentID uuid.UUID) error {
	args := make([]map[string]interface{}, len(analysisRequestIDs))
	for i := range analysisRequestIDs {
		args[i] = map[string]interface{}{
			"analysis_request_id": analysisRequestIDs[i],
			"instrument_id":       instrumentID,
		}
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_analysis_request_instrument_transmissions(analysis_request_id, instrument_id) VALUES (:analysis_request_id, :instrument_id);`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, args)
	if err != nil {
		log.Error().Err(err).Msg(msgSaveAnalysisRequestsInstrumentTransmissions)
		return ErrSaveAnalysisRequestsInstrumentTransmissions
	}
	return nil
}

const analysisResultBatchSize = 3500

func (r *analysisRepository) CreateAnalysisResultsBatch(ctx context.Context, analysisResults []AnalysisResult) ([]AnalysisResult, error) {
	savedAnalysisResults := make([]AnalysisResult, 0, len(analysisResults))
	var err error
	var savedResults []AnalysisResult
	for i := 0; i < len(analysisResults); i += analysisResultBatchSize {
		if len(analysisResults) >= i+analysisResultBatchSize {
			savedResults, err = r.createAnalysisResultsBatch(ctx, analysisResults[i:i+analysisResultBatchSize])
		} else {
			savedResults, err = r.createAnalysisResultsBatch(ctx, analysisResults[i:])
		}
		if err != nil {
			return nil, err
		}
		savedAnalysisResults = append(savedAnalysisResults, savedResults...)
	}
	return savedAnalysisResults, nil
}

func (r *analysisRepository) createAnalysisResultsBatch(ctx context.Context, analysisResults []AnalysisResult) ([]AnalysisResult, error) {
	if len(analysisResults) == 0 {
		return analysisResults, nil
	}
	for i := range analysisResults {
		if (analysisResults[i].ID == uuid.UUID{}) || (analysisResults[i].ID == uuid.Nil) {
			analysisResults[i].ID = uuid.New()
		}
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_analysis_results(id, analyte_mapping_id, instrument_id, sample_code, instrument_run_id, result_record_id, batch_id, "result", status, result_mode, yielded_at, valid_until, operator, technical_release_datetime, edited, edit_reason, is_invalid)
		VALUES(:id, :analyte_mapping_id, :instrument_id, :sample_code, :instrument_run_id, :result_record_id, :batch_id, :result, :status, :result_mode, :yielded_at, :valid_until, :operator, :technical_release_datetime, :edited, :edit_reason, :is_invalid);`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertAnalysisResultsToDAOs(analysisResults))
	if err != nil {
		log.Error().Err(err).Msg("create analysis result batch failed")
		return analysisResults, err
	}

	extraValuesMap := make(map[uuid.UUID][]ExtraValue)
	warningsMap := make(map[uuid.UUID][]string)
	reagentInfosMap := make(map[uuid.UUID][]ReagentInfo)

	for i := range analysisResults {
		extraValuesMap[analysisResults[i].ID] = analysisResults[i].ExtraValues
		warningsMap[analysisResults[i].ID] = analysisResults[i].Warnings
		reagentInfosMap[analysisResults[i].ID] = analysisResults[i].ReagentInfos
	}

	err = r.CreateAnalysisResultExtraValues(ctx, extraValuesMap)
	if err != nil {
		return analysisResults, err
	}

	for i := range analysisResults {
		quantitativeChannelResultsMap := make(map[uuid.UUID]map[string]string)
		channelImagesMap := make(map[uuid.NullUUID][]Image)
		channelResultIDs, err := r.createChannelResults(ctx, analysisResults[i].ChannelResults, analysisResults[i].ID)
		if err != nil {
			return analysisResults, err
		}
		for j := range analysisResults[i].ChannelResults {
			if len(channelResultIDs) > j {
				analysisResults[i].ChannelResults[j].ID = channelResultIDs[j]
			}
			quantitativeChannelResultsMap[channelResultIDs[j]] = analysisResults[i].ChannelResults[j].QuantitativeResults
			channelImagesMap[uuid.NullUUID{UUID: channelResultIDs[j], Valid: true}] = analysisResults[i].ChannelResults[j].Images
		}
		err = r.CreateChannelResultQuantitativeValues(ctx, quantitativeChannelResultsMap)
		if err != nil {
			return analysisResults, err
		}
	}

	err = r.CreateWarnings(ctx, warningsMap)
	if err != nil {
		return analysisResults, err
	}

	return analysisResults, nil
}

func (r *analysisRepository) GetAnalysisResultsBySampleCodeAndAnalyteID(ctx context.Context, sampleCode string, analyteID uuid.UUID) ([]AnalysisResult, error) {
	query := `SELECT sar.id, sar.analyte_mapping_id, sar.instrument_id, sar.sample_code, sar.instrument_run_id, sar.is_invalid,
       				sar.result_record_id, sar.batch_id, sar."result", sar.status, sar.result_mode, sar.yielded_at,
       				sar.valid_until, sar.operator, sar.edited, sar.edit_reason, sam.id AS "analyte_mapping.id",
       				sam.instrument_id AS "analyte_mapping.instrument_id", sam.instrument_analyte AS "analyte_mapping.instrument_analyte",
					sam.analyte_id AS "analyte_mapping.analyte_id", sam.result_type AS "analyte_mapping.result_type",
					sam.created_at AS "analyte_mapping.created_at", sam.modified_at AS "analyte_mapping.modified_at"
			FROM %schema_name%.sk_analysis_results sar
			INNER JOIN %schema_name%.sk_analyte_mappings sam ON sar.analyte_mapping_id = sam.id AND sam.deleted_at IS NULL
			WHERE sar.sample_code = $1
			AND sam.analyte_id = $2;`

	query = strings.ReplaceAll(query, "%schema_name%", r.dbSchema)

	rows, err := r.db.QueryxContext(ctx, query, sampleCode, analyteID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Trace().Msg("No analysis results")
			return []AnalysisResult{}, nil
		}
		log.Error().Err(err).Msg("Can not search for AnalysisResults")
		return []AnalysisResult{}, err
	}
	defer rows.Close()

	analysisResultDAOs := make([]analysisResultDAO, 0)
	analysisResultIDs := make([]uuid.UUID, 0)
	analyteMappingIDsMap := make(map[uuid.UUID]any)
	for rows.Next() {
		result := analysisResultDAO{}
		err := rows.StructScan(&result)
		if err != nil {
			log.Error().Err(err).Msg("Can not scan data")
			return []AnalysisResult{}, err
		}

		analysisResultDAOs = append(analysisResultDAOs, result)
		analysisResultIDs = append(analysisResultIDs, result.ID)
		analyteMappingIDsMap[result.AnalyteMappingID] = nil
	}
	analyteMappingIDs := make([]uuid.UUID, len(analyteMappingIDsMap))
	for analyteMappingID := range analyteMappingIDsMap {
		analyteMappingIDs = append(analyteMappingIDs, analyteMappingID)
	}

	extraValuesMap, err := r.getAnalysisResultExtraValues(ctx, analysisResultIDs)
	if err != nil {
		return nil, err
	}

	reagentInfoMap, err := r.getReagentInfoMap(ctx, analysisResultIDs)
	if err != nil {
		return nil, err
	}

	imagesMap, err := r.getImages(ctx, analysisResultIDs)
	if err != nil {
		return nil, err
	}

	warningsMap, err := r.getWarnings(ctx, analysisResultIDs)
	if err != nil {
		return nil, err
	}

	channelResultsMap, err := r.getChannelResults(ctx, analysisResultIDs)
	if err != nil {
		return nil, err
	}

	channelResultIDs := make([]uuid.UUID, 0)
	for _, channelResults := range channelResultsMap {
		for _, channelResult := range channelResults {
			channelResultIDs = append(channelResultIDs, channelResult.ID)
		}
	}

	channelResultQuantitativeValuesMap, err := r.getChannelResultQuantitativeValues(ctx, channelResultIDs)
	if err != nil {
		return nil, err
	}

	channelMappingsMap, err := r.getChannelMappings(ctx, analyteMappingIDs)
	if err != nil {
		return nil, err
	}
	resultMappingsMap, err := r.getResultMappings(ctx, analyteMappingIDs)
	if err != nil {
		return nil, err
	}

	analysisResults := make([]AnalysisResult, len(analysisResultDAOs))
	for analysisResultIndex := range analysisResultDAOs {
		analysisResultDAOs[analysisResultIndex].ExtraValues = extraValuesMap[analysisResultDAOs[analysisResultIndex].ID]
		analysisResultDAOs[analysisResultIndex].ReagentInfos = reagentInfoMap[analysisResultDAOs[analysisResultIndex].ID]

		for _, image := range imagesMap[analysisResultDAOs[analysisResultIndex].ID] {
			if image.ChannelResultID.Valid {
				continue
			}
			analysisResultDAOs[analysisResultIndex].Images = append(analysisResultDAOs[analysisResultIndex].Images, image)
		}

		analysisResultDAOs[analysisResultIndex].Warnings = warningsMap[analysisResultDAOs[analysisResultIndex].ID]

		for _, channelResult := range channelResultsMap[analysisResultDAOs[analysisResultIndex].ID] {
			for _, image := range imagesMap[analysisResultDAOs[analysisResultIndex].ID] {
				if !image.ChannelResultID.Valid || (image.ChannelResultID.Valid && image.ChannelResultID.UUID != channelResult.ID) {
					continue
				}
				channelResult.Images = append(channelResult.Images, image)
			}

			channelResult.QuantitativeResults = channelResultQuantitativeValuesMap[channelResult.ID]
			analysisResultDAOs[analysisResultIndex].ChannelResults = append(analysisResultDAOs[analysisResultIndex].ChannelResults, channelResult)
		}

		analysisResult := convertAnalysisResultDAOToAnalysisResult(analysisResultDAOs[analysisResultIndex])
		analysisResult.AnalyteMapping.ChannelMappings = channelMappingsMap[analysisResultDAOs[analysisResultIndex].AnalyteMappingID]
		analysisResult.AnalyteMapping.ResultMappings = resultMappingsMap[analysisResultDAOs[analysisResultIndex].AnalyteMappingID]
		analysisResults[analysisResultIndex] = analysisResult
	}

	return analysisResults, err
}

func (r *analysisRepository) GetAnalysisResultByID(ctx context.Context, id uuid.UUID, allowDeletedAnalyteMapping bool) (AnalysisResult, error) {
	query := `SELECT sar.id, sar.analyte_mapping_id, sar.instrument_id, sar.sample_code, sar.instrument_run_id, sar.result_record_id, sar.batch_id, sar."result", sar.status, sar.result_mode, sar.yielded_at, sar.valid_until, sar.operator, sar.edited, sar.edit_reason, sar.is_invalid,
					sam.id AS "analyte_mapping.id", sam.instrument_id AS "analyte_mapping.instrument_id", sam.instrument_analyte AS "analyte_mapping.instrument_analyte", sam.analyte_id AS "analyte_mapping.analyte_id", sam.result_type AS "analyte_mapping.result_type", sam.created_at AS "analyte_mapping.created_at", sam.modified_at AS "analyte_mapping.modified_at"
			FROM %schema_name%.sk_analysis_results sar
			INNER JOIN %schema_name%.sk_analyte_mappings sam ON sar.analyte_mapping_id = sam.id`

	if !allowDeletedAnalyteMapping {
		query += ` AND sam.deleted_at IS NULL`
	}

	query += ` WHERE sar.id = $1;`

	query = strings.ReplaceAll(query, "%schema_name%", r.dbSchema)

	row := r.db.QueryRowxContext(ctx, query, id)
	result := analysisResultDAO{}
	err := row.StructScan(&result)
	if err != nil {
		log.Error().Err(err).Msg("Can not scan data")
		return AnalysisResult{}, err
	}

	if err != nil {
		if err == sql.ErrNoRows {
			log.Trace().Msg("No analysis results")
			return AnalysisResult{}, nil
		}
		log.Error().Err(err).Msg("Can not search for AnalysisResults")
		return AnalysisResult{}, err
	}

	extraValues, err := r.getAnalysisResultExtraValues(ctx, []uuid.UUID{result.ID})
	if err != nil {
		return AnalysisResult{}, err
	}
	result.ExtraValues = extraValues[result.ID]

	reagentInfoMap, err := r.getReagentInfoMap(ctx, []uuid.UUID{result.ID})
	if err != nil {
		return AnalysisResult{}, err
	}
	result.ReagentInfos = reagentInfoMap[result.ID]

	imagesMap, err := r.getImages(ctx, []uuid.UUID{result.ID})
	if err != nil {
		return AnalysisResult{}, err
	}
	for _, image := range imagesMap[result.ID] {
		if image.ChannelResultID.Valid {
			continue
		}
		result.Images = append(result.Images, image)
	}

	warningsMap, err := r.getWarnings(ctx, []uuid.UUID{result.ID})
	if err != nil {
		return AnalysisResult{}, err
	}
	result.Warnings = warningsMap[result.ID]

	channelResultsMap, err := r.getChannelResults(ctx, []uuid.UUID{result.ID})
	if err != nil {
		return AnalysisResult{}, err
	}

	channelResultIDs := make([]uuid.UUID, len(channelResultsMap[result.ID]))
	for i := range channelResultsMap[result.ID] {
		channelResultIDs[i] = channelResultsMap[result.ID][i].ID
	}

	quantitativeValuesByChannelResultID, err := r.getChannelResultQuantitativeValues(ctx, channelResultIDs)
	if err != nil {
		return AnalysisResult{}, err
	}

	channelMappingsMap, err := r.getChannelMappings(ctx, []uuid.UUID{result.AnalyteMappingID})
	if err != nil {
		return AnalysisResult{}, err
	}
	resultMappingsMap, err := r.getResultMappings(ctx, []uuid.UUID{result.AnalyteMappingID})
	if err != nil {
		return AnalysisResult{}, err
	}

	for _, channelResult := range channelResultsMap[result.ID] {
		channelResult.QuantitativeResults = quantitativeValuesByChannelResultID[channelResult.ID]

		for _, image := range imagesMap[result.ID] {
			if !image.ChannelResultID.Valid || (image.ChannelResultID.Valid && image.ChannelResultID.UUID != channelResult.ID) {
				continue
			}
			channelResult.Images = append(channelResult.Images, image)
		}

		result.ChannelResults = append(result.ChannelResults, channelResult)
	}

	analysisResult := convertAnalysisResultDAOToAnalysisResult(result)

	analysisResult.AnalyteMapping.ChannelMappings = channelMappingsMap[result.AnalyteMappingID]
	analysisResult.AnalyteMapping.ResultMappings = resultMappingsMap[result.AnalyteMappingID]

	return analysisResult, err
}

func (r *analysisRepository) GetAnalysisResultsByIDs(ctx context.Context, ids []uuid.UUID) ([]AnalysisResult, error) {
	if len(ids) == 0 {
		return []AnalysisResult{}, nil
	}

	query := `SELECT sar.id, sar.analyte_mapping_id, sar.instrument_id, sar.sample_code, sar.instrument_run_id, sar.result_record_id, sar.batch_id, sar."result", sar.status, sar.result_mode, sar.yielded_at, sar.valid_until, sar.operator, sar.edited, sar.edit_reason, sar.is_invalid,
					sam.id AS "analyte_mapping.id", sam.instrument_id AS "analyte_mapping.instrument_id", sam.instrument_analyte AS "analyte_mapping.instrument_analyte", sam.analyte_id AS "analyte_mapping.analyte_id", sam.result_type AS "analyte_mapping.result_type", sam.created_at AS "analyte_mapping.created_at", sam.modified_at AS "analyte_mapping.modified_at"
			FROM %schema_name%.sk_analysis_results sar
			INNER JOIN %schema_name%.sk_analyte_mappings sam ON sar.analyte_mapping_id = sam.id
			WHERE sar.id IN (?);`

	query = strings.ReplaceAll(query, "%schema_name%", r.dbSchema)

	query, args, _ := sqlx.In(query, ids)
	query = r.db.Rebind(query)

	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Trace().Msg("No analysis requests")
			return []AnalysisResult{}, nil
		}
		log.Error().Err(err).Msg("Can not search for AnalysisRequests")
		return []AnalysisResult{}, err
	}
	defer rows.Close()

	analysisResultDAOs := make([]analysisResultDAO, 0)
	for rows.Next() {
		result := analysisResultDAO{}
		err := rows.StructScan(&result)
		if err != nil {
			log.Error().Err(err).Msg("Can not scan data")
			return []AnalysisResult{}, err
		}

		analysisResultDAOs = append(analysisResultDAOs, result)
	}

	extraValues, err := r.getAnalysisResultExtraValues(ctx, ids)
	if err != nil {
		return []AnalysisResult{}, err
	}

	reagentInfoMap, err := r.getReagentInfoMap(ctx, ids)
	if err != nil {
		return []AnalysisResult{}, err
	}

	imagesMap, err := r.getImages(ctx, ids)
	if err != nil {
		return []AnalysisResult{}, err
	}

	warningsMap, err := r.getWarnings(ctx, ids)
	if err != nil {
		return []AnalysisResult{}, err
	}

	channelResultsMap, err := r.getChannelResults(ctx, ids)
	if err != nil {
		return []AnalysisResult{}, err
	}

	channelResultIDs := make([]uuid.UUID, 0)
	for _, channelResults := range channelResultsMap {
		for _, channelResult := range channelResults {
			channelResultIDs = append(channelResultIDs, channelResult.ID)
		}
	}

	quantitativeValuesByChannelResultID, err := r.getChannelResultQuantitativeValues(ctx, channelResultIDs)
	if err != nil {
		return []AnalysisResult{}, err
	}

	analyteMappingIDs := make([]uuid.UUID, 0)

	for _, analysisResultDAO := range analysisResultDAOs {
		analyteMappingIDs = append(analyteMappingIDs, analysisResultDAO.AnalyteMappingID)
	}

	channelMappingsMap, err := r.getChannelMappings(ctx, analyteMappingIDs)
	if err != nil {
		return []AnalysisResult{}, err
	}
	resultMappingsMap, err := r.getResultMappings(ctx, analyteMappingIDs)
	if err != nil {
		return []AnalysisResult{}, err
	}

	analysisResults := make([]AnalysisResult, 0)

	for i := range analysisResultDAOs {
		analysisResultDAOs[i].ExtraValues = extraValues[analysisResultDAOs[i].ID]
		analysisResultDAOs[i].ReagentInfos = reagentInfoMap[analysisResultDAOs[i].ID]

		for _, image := range imagesMap[analysisResultDAOs[i].ID] {
			if image.ChannelResultID.Valid {
				continue
			}
			analysisResultDAOs[i].Images = append(analysisResultDAOs[i].Images, image)
		}

		analysisResultDAOs[i].Warnings = warningsMap[analysisResultDAOs[i].ID]

		for _, channelResult := range channelResultsMap[analysisResultDAOs[i].ID] {
			channelResult.QuantitativeResults = quantitativeValuesByChannelResultID[channelResult.ID]

			for _, image := range imagesMap[analysisResultDAOs[i].ID] {
				if !image.ChannelResultID.Valid || (image.ChannelResultID.Valid && image.ChannelResultID.UUID != channelResult.ID) {
					continue
				}
				channelResult.Images = append(channelResult.Images, image)
			}

			analysisResultDAOs[i].ChannelResults = append(analysisResultDAOs[i].ChannelResults, channelResult)
		}

		analysisResult := convertAnalysisResultDAOToAnalysisResult(analysisResultDAOs[i])

		analysisResult.AnalyteMapping.ChannelMappings = channelMappingsMap[analysisResultDAOs[i].AnalyteMappingID]
		analysisResult.AnalyteMapping.ResultMappings = resultMappingsMap[analysisResultDAOs[i].AnalyteMappingID]

		analysisResults = append(analysisResults, analysisResult)
	}

	return analysisResults, err
}

func (r *analysisRepository) GetAnalysisResultsByBatchIDs(ctx context.Context, batchIDs []uuid.UUID) ([]AnalysisResult, error) {
	if len(batchIDs) == 0 {
		return []AnalysisResult{}, nil
	}
	analysisResultDAOs := make([]analysisResultDAO, 0)
	analysisResultIDs := make([]uuid.UUID, 0)
	analyteMappingIDsMap := make(map[uuid.UUID]any)

	err := utils.Partition(len(batchIDs), maxParams, func(low int, high int) error {
		query := `SELECT sar.id, sar.analyte_mapping_id, sar.instrument_id, sar.sample_code, sar.instrument_run_id, sar.result_record_id, sar.batch_id, sar."result", sar.status, sar.result_mode, sar.yielded_at, sar.valid_until, sar.operator, sar.edited, sar.edit_reason, sar.is_invalid,
					sam.id AS "analyte_mapping.id", sam.instrument_id AS "analyte_mapping.instrument_id", sam.instrument_analyte AS "analyte_mapping.instrument_analyte", sam.analyte_id AS "analyte_mapping.analyte_id", sam.result_type AS "analyte_mapping.result_type", sam.created_at AS "analyte_mapping.created_at", sam.modified_at AS "analyte_mapping.modified_at"
			FROM %schema_name%.sk_analysis_results sar
			INNER JOIN %schema_name%.sk_analyte_mappings sam ON sar.analyte_mapping_id = sam.id AND sam.deleted_at IS NULL
			WHERE sar.batch_id IN (?);`

		query = strings.ReplaceAll(query, "%schema_name%", r.dbSchema)
		query, args, _ := sqlx.In(query, batchIDs[low:high])
		query = r.db.Rebind(query)

		rows, err := r.db.QueryxContext(ctx, query, args...)

		if err != nil {
			if err == sql.ErrNoRows {
				log.Trace().Msg("No analysis results")
				return nil
			}
			log.Error().Err(err).Msg("Can not search for AnalysisResults")
			return err
		}
		defer rows.Close()

		for rows.Next() {
			result := analysisResultDAO{}
			err := rows.StructScan(&result)
			if err != nil {
				log.Error().Err(err).Msg("Can not scan data")
				return err
			}

			analysisResultDAOs = append(analysisResultDAOs, result)
			analysisResultIDs = append(analysisResultIDs, result.ID)
			analyteMappingIDsMap[result.AnalyteMappingID] = nil
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	analyteMappingIDs := make([]uuid.UUID, len(analyteMappingIDsMap))
	for analyteMappingID := range analyteMappingIDsMap {
		analyteMappingIDs = append(analyteMappingIDs, analyteMappingID)
	}

	extraValuesMap, err := r.getAnalysisResultExtraValues(ctx, analysisResultIDs)
	if err != nil {
		return nil, err
	}

	reagentInfoMap, err := r.getReagentInfoMap(ctx, analysisResultIDs)
	if err != nil {
		return nil, err
	}

	imagesMap, err := r.getImages(ctx, analysisResultIDs)
	if err != nil {
		return nil, err
	}

	warningsMap, err := r.getWarnings(ctx, analysisResultIDs)
	if err != nil {
		return nil, err
	}

	channelResultsMap, err := r.getChannelResults(ctx, analysisResultIDs)
	if err != nil {
		return nil, err
	}

	channelResultIDs := make([]uuid.UUID, 0)
	for _, channelResults := range channelResultsMap {
		for _, channelResult := range channelResults {
			channelResultIDs = append(channelResultIDs, channelResult.ID)
		}
	}

	channelResultQuantitativeValuesMap, err := r.getChannelResultQuantitativeValues(ctx, channelResultIDs)
	if err != nil {
		return nil, err
	}

	channelMappingsMap, err := r.getChannelMappings(ctx, analyteMappingIDs)
	if err != nil {
		return nil, err
	}

	resultMappingsMap, err := r.getResultMappings(ctx, analyteMappingIDs)
	if err != nil {
		return nil, err
	}

	analysisResults := make([]AnalysisResult, len(analysisResultDAOs))
	for analysisResultIndex := range analysisResultDAOs {
		analysisResultDAOs[analysisResultIndex].ExtraValues = extraValuesMap[analysisResultDAOs[analysisResultIndex].ID]
		analysisResultDAOs[analysisResultIndex].ReagentInfos = reagentInfoMap[analysisResultDAOs[analysisResultIndex].ID]

		for _, image := range imagesMap[analysisResultDAOs[analysisResultIndex].ID] {
			if image.ChannelResultID.Valid {
				continue
			}
			analysisResultDAOs[analysisResultIndex].Images = append(analysisResultDAOs[analysisResultIndex].Images, image)
		}

		analysisResultDAOs[analysisResultIndex].Warnings = warningsMap[analysisResultDAOs[analysisResultIndex].ID]

		for _, channelResult := range channelResultsMap[analysisResultDAOs[analysisResultIndex].ID] {
			for _, image := range imagesMap[analysisResultDAOs[analysisResultIndex].ID] {
				if !image.ChannelResultID.Valid || (image.ChannelResultID.Valid && image.ChannelResultID.UUID != channelResult.ID) {
					continue
				}
				channelResult.Images = append(channelResult.Images, image)
			}

			channelResult.QuantitativeResults = channelResultQuantitativeValuesMap[channelResult.ID]
			analysisResultDAOs[analysisResultIndex].ChannelResults = append(analysisResultDAOs[analysisResultIndex].ChannelResults, channelResult)
		}

		analysisResult := convertAnalysisResultDAOToAnalysisResult(analysisResultDAOs[analysisResultIndex])

		analysisResult.AnalyteMapping.ChannelMappings = channelMappingsMap[analysisResultDAOs[analysisResultIndex].AnalyteMappingID]
		analysisResult.AnalyteMapping.ResultMappings = resultMappingsMap[analysisResultDAOs[analysisResultIndex].AnalyteMappingID]

		analysisResults[analysisResultIndex] = analysisResult
	}

	return analysisResults, err
}

func (r *analysisRepository) GetAnalysisResultsByBatchIDsMapped(ctx context.Context, batchIDs []uuid.UUID) (map[uuid.UUID][]AnalysisResultInfo, error) {
	resultMap := make(map[uuid.UUID][]AnalysisResultInfo)

	if len(batchIDs) < 1 {
		return resultMap, nil
	}

	err := utils.Partition(len(batchIDs), maxParams, func(low int, high int) error {
		query := `SELECT res.id AS result_id,
       res.batch_id AS batch_id,
	   res.sample_code AS sample_code,
	   am.analyte_id AS analyte_id,
	   COALESCE(res.yielded_at, res.created_at) AS result_created_at,
       am.instrument_analyte as test_name,
	   res.result AS test_result,
       res.status AS status,
       req.created_at AS request_created_at,
       req.work_item_id as work_item_id
FROM %schema_name%.sk_analysis_results res
LEFT JOIN %schema_name%.sk_analyte_mappings am ON am.instrument_id = res.instrument_id AND res.analyte_mapping_id = am.id
LEFT JOIN %schema_name%.sk_analysis_requests req ON req.sample_code = res.sample_code AND req.analyte_id = am.analyte_id
WHERE res.batch_id IN (?)`

		query = strings.ReplaceAll(query, "%schema_name%", r.dbSchema)
		query, args, _ := sqlx.In(query, batchIDs[low:high])
		query = r.db.Rebind(query)

		rows, err := r.db.QueryxContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg("Can not get analysis request list")
			return err
		}
		defer rows.Close()

		for rows.Next() {
			result := analysisResultInfoDAO{}
			err = rows.StructScan(&result)
			if err != nil {
				log.Error().Err(err).Msg("Failed to struct scan request")
				return err
			}

			if result.BatchID.Valid {
				resultMap[result.BatchID.UUID] = append(resultMap[result.BatchID.UUID], convertResultInfoDAOToResultInfo(result))
			}
		}
		return nil
	})

	return resultMap, err
}

func (r *analysisRepository) getChannelMappings(ctx context.Context, analyteMappingIDs []uuid.UUID) (map[uuid.UUID][]ChannelMapping, error) {
	channelMappingsMap := make(map[uuid.UUID][]ChannelMapping)
	err := utils.Partition(len(analyteMappingIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`SELECT * FROM %s.sk_channel_mappings WHERE analyte_mapping_id IN (?) AND deleted_at IS NULL;`, r.dbSchema)
		query, args, _ := sqlx.In(query, analyteMappingIDs[low:high])
		query = r.db.Rebind(query)
		rows, err := r.db.QueryxContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg(msgGetChannelMappingsFailed)
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var dao channelMappingDAO
			err = rows.StructScan(&dao)
			if err != nil {
				log.Error().Err(err).Msg(msgGetChannelMappingsFailed)
				return err
			}
			channelMappingsMap[dao.AnalyteMappingID] = append(channelMappingsMap[dao.AnalyteMappingID], convertChannelMappingDaoToChannelMapping(dao))
		}
		return nil
	})

	return channelMappingsMap, err
}

func (r *analysisRepository) getResultMappings(ctx context.Context, analyteMappingIDs []uuid.UUID) (map[uuid.UUID][]ResultMapping, error) {
	resultMappingsMap := make(map[uuid.UUID][]ResultMapping)
	err := utils.Partition(len(analyteMappingIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`SELECT * FROM %s.sk_result_mappings WHERE analyte_mapping_id IN (?) AND deleted_at IS NULL;`, r.dbSchema)
		query, args, _ := sqlx.In(query, analyteMappingIDs[low:high])
		query = r.db.Rebind(query)
		rows, err := r.db.QueryxContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg(msgGetResultMappingsFailed)
			return ErrGetResultMappingsFailed
		}
		defer rows.Close()
		for rows.Next() {
			var dao resultMappingDAO
			err = rows.StructScan(&dao)
			if err != nil {
				log.Error().Err(err).Msg(msgGetResultMappingsFailed)
				return ErrGetResultMappingsFailed
			}
			resultMappingsMap[dao.AnalyteMappingID] = append(resultMappingsMap[dao.AnalyteMappingID], convertResultMappingDaoToChannelMapping(dao))
		}
		return nil
	})

	return resultMappingsMap, err
}

const maxParams = 65000

func (r *analysisRepository) getAnalysisResultExtraValues(ctx context.Context, analysisResultIDs []uuid.UUID) (map[uuid.UUID][]resultExtraValueDAO, error) {
	extraValuesMap := make(map[uuid.UUID][]resultExtraValueDAO)
	err := utils.Partition(len(analysisResultIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`SELECT sare.id, sare.analysis_result_id, sare."key", sare."value"
		FROM %s.sk_analysis_result_extravalues sare WHERE sare.analysis_result_id IN (?);`, r.dbSchema)
		query, args, _ := sqlx.In(query, analysisResultIDs[low:high])
		query = r.db.Rebind(query)
		rows, err := r.db.QueryxContext(ctx, query, args...)
		if err != nil {
			if err == sql.ErrNoRows {
				log.Trace().Msg("No extra value")
				return nil
			}
			log.Error().Err(err).Msg("Can not search for extra values")
			return err
		}
		defer rows.Close()

		for rows.Next() {
			extraValue := resultExtraValueDAO{}
			err = rows.StructScan(&extraValue)
			if err != nil {
				log.Error().Err(err).Msg("Can not scan data")
				return err
			}

			extraValuesMap[extraValue.AnalysisResultID] = append(extraValuesMap[extraValue.AnalysisResultID], extraValue)
		}
		return nil
	})

	return extraValuesMap, err
}

const extraValueBatchSize = 15000

func (r *analysisRepository) CreateAnalysisResultExtraValues(ctx context.Context, extraValuesByAnalysisRequestIDs map[uuid.UUID][]ExtraValue) error {
	extraValuesDAOs := make([]resultExtraValueDAO, 0)
	for analysisResultID, extraValues := range extraValuesByAnalysisRequestIDs {
		evs := convertExtraValuesToResultExtraValueDAOs(extraValues, analysisResultID)
		for i := range evs {
			evs[i].ID = uuid.New()
		}
		extraValuesDAOs = append(extraValuesDAOs, evs...)
	}
	if len(extraValuesDAOs) == 0 {
		return nil
	}
	var err error
	for i := 0; i < len(extraValuesDAOs); i += extraValueBatchSize {
		if len(extraValuesDAOs) >= i+extraValueBatchSize {
			err = r.createAnalysisResultExtraValues(ctx, extraValuesDAOs[i:i+extraValueBatchSize])
		} else {
			err = r.createAnalysisResultExtraValues(ctx, extraValuesDAOs[i:])
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *analysisRepository) CreateAnalysisRequestExtraValues(ctx context.Context, extraValuesByAnalysisRequestIDs map[uuid.UUID][]ExtraValue) error {
	extraValuesDAOs := make([]requestExtraValueDAO, 0)
	for analysisRequestID, extraValues := range extraValuesByAnalysisRequestIDs {
		evs := convertExtraValuesToRequestExtraValueDAOs(extraValues, analysisRequestID)
		for i := range evs {
			evs[i].ID = uuid.New()
		}
		extraValuesDAOs = append(extraValuesDAOs, evs...)
	}
	if len(extraValuesDAOs) == 0 {
		return nil
	}
	var err error
	for i := 0; i < len(extraValuesDAOs); i += extraValueBatchSize {
		if len(extraValuesDAOs) >= i+extraValueBatchSize {
			err = r.createAnalysisRequestExtraValues(ctx, extraValuesDAOs[i:i+extraValueBatchSize])
		} else {
			err = r.createAnalysisRequestExtraValues(ctx, extraValuesDAOs[i:])
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *analysisRepository) createAnalysisResultExtraValues(ctx context.Context, extraValues []resultExtraValueDAO) error {
	query := fmt.Sprintf(`INSERT INTO %s.sk_analysis_result_extravalues(id, analysis_result_id, "key", "value")
		VALUES(:id, :analysis_result_id, :key, :value)`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, extraValues)
	if err != nil {
		log.Error().Err(err).Msg("create analysis result batch failed")
		return err
	}
	return nil
}

func (r *analysisRepository) createAnalysisRequestExtraValues(ctx context.Context, extraValues []requestExtraValueDAO) error {
	query := fmt.Sprintf(`INSERT INTO %s.sk_analysis_request_extra_values(id, analysis_request_id, "key", "value")
		VALUES(:id, :analysis_request_id, :key, :value)`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, extraValues)
	if err != nil {
		log.Error().Err(err).Msg("create analysis request extra values failed")
		return err
	}
	return nil
}

func (r *analysisRepository) getChannelResults(ctx context.Context, analysisResultIDs []uuid.UUID) (map[uuid.UUID][]channelResultDAO, error) {
	channelResultsMap := make(map[uuid.UUID][]channelResultDAO)
	err := utils.Partition(len(analysisResultIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`SELECT scr.id, scr.analysis_result_id, scr.channel_id, scr.qualitative_result, scr.qualitative_result_edited
		FROM %s.sk_channel_results scr WHERE scr.analysis_result_id IN (?);`, r.dbSchema)
		query, args, _ := sqlx.In(query, analysisResultIDs[low:high])
		query = r.db.Rebind(query)
		rows, err := r.db.QueryxContext(ctx, query, args...)
		if err != nil {
			if err == sql.ErrNoRows {
				log.Trace().Msg("No channel result")
				return nil
			}
			log.Error().Err(err).Msg("Can not search for channel results")
			return err
		}

		defer rows.Close()
		for rows.Next() {
			channelResult := channelResultDAO{}
			err = rows.StructScan(&channelResult)
			if err != nil {
				log.Error().Err(err).Msg("Can not scan data")
				return err
			}

			channelResultsMap[channelResult.AnalysisResultID] = append(channelResultsMap[channelResult.AnalysisResultID], channelResult)
		}
		return nil
	})

	return channelResultsMap, err
}

const channelResultBatchSize = 12000

func (r *analysisRepository) CreateChannelResults(ctx context.Context, channelResults []ChannelResult, analysisResultID uuid.UUID) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, 0, len(channelResults))
	var err error
	var idsPart []uuid.UUID
	for i := 0; i < len(channelResults); i += channelResultBatchSize {
		if len(channelResults) >= i+channelResultBatchSize {
			idsPart, err = r.createChannelResults(ctx, channelResults[i:i+channelResultBatchSize], analysisResultID)
		} else {
			idsPart, err = r.createChannelResults(ctx, channelResults[i:], analysisResultID)
		}
		if err != nil {
			return nil, err
		}
		ids = append(ids, idsPart...)
	}
	return ids, err
}

func (r *analysisRepository) createChannelResults(ctx context.Context, channelResults []ChannelResult, analysisResultID uuid.UUID) ([]uuid.UUID, error) {
	if len(channelResults) == 0 {
		return []uuid.UUID{}, nil
	}
	ids := make([]uuid.UUID, len(channelResults))
	for i := range channelResults {
		if (channelResults[i].ID == uuid.UUID{}) || (channelResults[i].ID == uuid.Nil) {
			channelResults[i].ID = uuid.New()
		}
		ids[i] = channelResults[i].ID
	}

	query := fmt.Sprintf(`INSERT INTO %s.sk_channel_results(id, analysis_result_id, channel_id, qualitative_result, qualitative_result_edited)
		VALUES(:id, :analysis_result_id, :channel_id, :qualitative_result, :qualitative_result_edited);`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertChannelResultsToDAOs(channelResults, analysisResultID))
	if err != nil {
		log.Error().Err(err).Msg("create channel result batch failed")
		return []uuid.UUID{}, err
	}
	return ids, nil
}

func (r *analysisRepository) getChannelResultQuantitativeValues(ctx context.Context, channelResultIDs []uuid.UUID) (map[uuid.UUID][]quantitativeChannelResultDAO, error) {
	valuesByChannelResultID := make(map[uuid.UUID][]quantitativeChannelResultDAO)

	err := utils.Partition(len(channelResultIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`SELECT scrqv.id, scrqv.channel_result_id, scrqv.metric, scrqv."value"
		FROM %s.sk_channel_result_quantitative_values scrqv WHERE scrqv.channel_result_id IN (?);`, r.dbSchema)
		query, args, _ := sqlx.In(query, channelResultIDs[low:high])
		query = r.db.Rebind(query)

		rows, err := r.db.QueryxContext(ctx, query, args...)
		if err != nil {
			if err == sql.ErrNoRows {
				log.Trace().Msg("No quantitative channel result")
				return nil
			}
			log.Error().Err(err).Msg("Can not search for quantitative channel results")
			return err
		}
		defer rows.Close()
		for rows.Next() {
			quantitativeChannelResult := quantitativeChannelResultDAO{}
			err = rows.StructScan(&quantitativeChannelResult)
			if err != nil {
				log.Error().Err(err).Msg("Can not scan data")
				return err
			}

			valuesByChannelResultID[quantitativeChannelResult.ChannelResultID] = append(valuesByChannelResultID[quantitativeChannelResult.ChannelResultID], quantitativeChannelResult)
		}

		return nil
	})

	return valuesByChannelResultID, err
}

const channelResultQVBatchSize = 15000

func (r *analysisRepository) CreateChannelResultQuantitativeValues(ctx context.Context, quantitativeValuesByChannelResultIDs map[uuid.UUID]map[string]string) error {
	quantitativeChannelResultDAOs := make([]quantitativeChannelResultDAO, 0)
	for channelResultID, quantitativeValues := range quantitativeValuesByChannelResultIDs {
		qrDAOs := convertQuantitativeResultsToDAOs(quantitativeValues, channelResultID)
		for i := range qrDAOs {
			qrDAOs[i].ID = uuid.New()
		}
		quantitativeChannelResultDAOs = append(quantitativeChannelResultDAOs, qrDAOs...)
	}
	var err error
	for i := 0; i < len(quantitativeChannelResultDAOs); i += channelResultQVBatchSize {
		if len(quantitativeChannelResultDAOs) >= i+channelResultQVBatchSize {
			err = r.createChannelResultQuantitativeValues(ctx, quantitativeChannelResultDAOs[i:i+channelResultQVBatchSize])
		} else {
			err = r.createChannelResultQuantitativeValues(ctx, quantitativeChannelResultDAOs[i:])
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *analysisRepository) createChannelResultQuantitativeValues(ctx context.Context, quantitativeChannelResults []quantitativeChannelResultDAO) error {
	if len(quantitativeChannelResults) == 0 {
		return nil
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_channel_result_quantitative_values(id, channel_result_id, metric, "value")
		VALUES(:id, :channel_result_id, :metric, :value);`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, quantitativeChannelResults)
	if err != nil {
		log.Error().Err(err).Msg("create channel result quantitative values batch failed")
		return err
	}
	return nil
}

func (r *analysisRepository) getReagentInfoMap(ctx context.Context, analysisResultIDs []uuid.UUID) (map[uuid.UUID][]reagentInfoDAO, error) {
	reagentInfosMap := make(map[uuid.UUID][]reagentInfoDAO)
	err := utils.Partition(len(analysisResultIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`SELECT sarri.id, sarri.analysis_result_id, sarri.serial, sarri."name", sarri.code, sarri.shelf_life, sarri.lot_no, sarri.manufacturer_name, sarri.reagent_manufacturer_date,
        sarri.reagent_type, sarri.use_until, sarri.date_created
		FROM %s.sk_analysis_result_reagent_infos sarri WHERE sarri.analysis_result_id IN (?);`, r.dbSchema)
		query, args, _ := sqlx.In(query, analysisResultIDs[low:high])
		query = r.db.Rebind(query)
		rows, err := r.db.QueryxContext(ctx, query, args...)
		if err != nil {
			if err == sql.ErrNoRows {
				log.Trace().Msg("No reagent info")
				return nil
			}
			log.Error().Err(err).Msg("Can not search for reagent info")
			return err
		}
		defer rows.Close()

		for rows.Next() {
			reagentInfo := reagentInfoDAO{}
			err = rows.StructScan(&reagentInfo)
			if err != nil {
				log.Error().Err(err).Msg("Can not scan data")
				return err
			}
			reagentInfosMap[reagentInfo.AnalysisResultID] = append(reagentInfosMap[reagentInfo.AnalysisResultID], reagentInfo)
		}
		return nil
	})

	return reagentInfosMap, err
}

const reagentInfoBatchSize = 5000

func (r *analysisRepository) CreateReagentInfos(ctx context.Context, reagentInfosByAnalysisResultID map[uuid.UUID][]ReagentInfo) error {
	reagentInfoDAOs := make([]reagentInfoDAO, 0)
	for analysisResultID, reagentInfos := range reagentInfosByAnalysisResultID {
		ridaos := convertReagentInfosToDAOs(reagentInfos, analysisResultID)
		for i := range ridaos {
			ridaos[i].ID = uuid.New()
		}
		reagentInfoDAOs = append(reagentInfoDAOs, ridaos...)
	}
	var err error
	for i := 0; i < len(reagentInfoDAOs); i += reagentInfoBatchSize {
		if len(reagentInfoDAOs) >= i+reagentInfoBatchSize {
			err = r.createReagentInfos(ctx, reagentInfoDAOs[i:i+reagentInfoBatchSize])
		} else {
			err = r.createReagentInfos(ctx, reagentInfoDAOs[i:])
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *analysisRepository) createReagentInfos(ctx context.Context, reagentInfoDAOs []reagentInfoDAO) error {
	if len(reagentInfoDAOs) == 0 {
		return nil
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_analysis_result_reagent_info(id, analysis_result_id, serial, name, code, shelfLife, lot_no, manufacturer_name, reagent_manufacturer_date, reagent_type, use_until, date_created)
		VALUES(:id, :analysis_result_id, :serial, :name, :code, :shelfLife, :lot_no, :manufacturer_name, :reagent_manufacturer_date, :reagent_type, :use_until, :date_created)`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, reagentInfoDAOs)
	if err != nil {
		log.Error().Err(err).Msg(msgCreateReagentInfoFailed)
		return ErrCreateReagentInfoFailed
	}
	return nil
}

func (r *analysisRepository) getImages(ctx context.Context, analysisResultIDs []uuid.UUID) (map[uuid.UUID][]imageDAO, error) {
	imagesMap := make(map[uuid.UUID][]imageDAO)
	err := utils.Partition(len(analysisResultIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`SELECT sari.id, sari.analysis_result_id, sari.channel_result_id, sari.name, sari.description, sari.dea_image_id FROM %s.sk_analysis_result_images sari WHERE sari.analysis_result_id IN (?);`, r.dbSchema)
		query, args, _ := sqlx.In(query, analysisResultIDs[low:high])
		query = r.db.Rebind(query)
		rows, err := r.db.QueryxContext(ctx, query, args...)
		if err != nil {
			if err == sql.ErrNoRows {
				log.Trace().Msg("No images")
				return nil
			}
			log.Error().Err(err).Msg("Can not search for Images")
			return err
		}

		defer rows.Close()
		for rows.Next() {
			image := imageDAO{}
			err := rows.StructScan(&image)
			if err != nil {
				log.Error().Err(err).Msg("Can not scan data")
				return err
			}

			imagesMap[image.AnalysisResultID] = append(imagesMap[image.AnalysisResultID], image)
		}

		return nil
	})

	return imagesMap, err
}

const imageBatchSize = 8000

func (r *analysisRepository) SaveImages(ctx context.Context, images []imageDAO) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, 0, len(images))
	var err error
	var idsPart []uuid.UUID
	for i := 0; i < len(images); i += imageBatchSize {
		if len(images) >= i+imageBatchSize {
			idsPart, err = r.saveImages(ctx, images[i:i+imageBatchSize])
		} else {
			idsPart, err = r.saveImages(ctx, images[i:])
		}
		if err != nil {
			return nil, err
		}
		ids = append(ids, idsPart...)
	}
	return ids, nil
}

func (r *analysisRepository) saveImages(ctx context.Context, images []imageDAO) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, len(images))
	if len(images) == 0 {
		return ids, nil
	}
	for i := range images {
		images[i].ID = uuid.New()
		ids[i] = images[i].ID
	}

	query := fmt.Sprintf(`INSERT INTO %s.sk_analysis_result_images(id, analysis_result_id, channel_result_id, "name", description, image_bytes, dea_image_id, uploaded_to_dea_at)
		VALUES(:id, :analysis_result_id, :channel_result_id, :name, :description, :image_bytes, :dea_image_id, :uploaded_to_dea_at);`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, images)
	if err != nil {
		log.Error().Err(err).Msg("create analysis result images failed")
		return nil, err
	}
	return ids, nil
}

func (r *analysisRepository) getWarnings(ctx context.Context, analysisResultIDs []uuid.UUID) (map[uuid.UUID][]warningDAO, error) {
	warningsMap := make(map[uuid.UUID][]warningDAO)
	err := utils.Partition(len(analysisResultIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`SELECT sarw.id, sarw.analysis_result_id, sarw.warning FROM %s.sk_analysis_result_warnings sarw WHERE sarw.analysis_result_id IN (?);`, r.dbSchema)
		query, args, _ := sqlx.In(query, analysisResultIDs[low:high])
		query = r.db.Rebind(query)
		rows, err := r.db.QueryxContext(ctx, query, args...)
		if err != nil {
			if err == sql.ErrNoRows {
				log.Trace().Msg("No analysis result warnings")
				return nil
			}
			log.Error().Err(err).Msg("Can not search for AnalysisResultWarnings")
			return err
		}
		defer rows.Close()

		for rows.Next() {
			warning := warningDAO{}
			err := rows.StructScan(&warning)
			if err != nil {
				log.Error().Err(err).Msg("Can not scan data")
				return err
			}

			warningsMap[warning.AnalysisResultID] = append(warningsMap[warning.AnalysisResultID], warning)
		}
		return nil
	})

	return warningsMap, err
}

const warningBatchCount = 20000

func (r *analysisRepository) CreateWarnings(ctx context.Context, warningsByAnalysisResultID map[uuid.UUID][]string) error {
	warningDAOs := make([]warningDAO, 0)
	for analysisResultID, warnings := range warningsByAnalysisResultID {
		wdaos := convertWarningsToDAOs(warnings, analysisResultID)
		for i := range wdaos {
			wdaos[i].ID = uuid.New()
		}
		warningDAOs = append(warningDAOs, wdaos...)
	}
	var err error
	for i := 0; i < len(warningDAOs); i += warningBatchCount {
		if len(warningDAOs) >= i+warningBatchCount {
			err = r.createWarnings(ctx, warningDAOs[i:i+warningBatchCount])
		} else {
			err = r.createWarnings(ctx, warningDAOs[i:])
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *analysisRepository) createWarnings(ctx context.Context, warningDAOs []warningDAO) error {
	if len(warningDAOs) == 0 {
		return nil
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_analysis_result_warnings(id, analysis_result_id, warning)
		VALUES(:id, :analysis_result_id, :warning)`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, warningDAOs)
	if err != nil {
		log.Error().Err(err).Msg(msgCreateWarningsFailed)
		return ErrCreateWarningsFailed
	}
	return nil
}

func (r *analysisRepository) CreateAnalysisResultQueueItem(ctx context.Context, analysisResults []AnalysisResult) (uuid.UUID, error) {
	log.Trace().Int("analysisResultCount", len(analysisResults)).Msg("Creating analysis result queue item")

	if len(analysisResults) < 1 {
		return uuid.Nil, nil
	}
	analysisResultTOs, err := convertAnalysisResultsToTOs(analysisResults)
	if err != nil {
		log.Error().Err(err).Msg(msgConvertAnalysisResultsFailed)
		return uuid.Nil, ErrConvertAnalysisResultsFailed
	}
	jsonData, err := json.Marshal(analysisResultTOs)
	if err != nil {
		log.Error().Err(err).
			Interface("analysisResults", analysisResults).
			Msg("Failed to marshal analysis results, skipping further processing until manual intervention")
		return uuid.Nil, ErrMarshalAnalysisResultsFailed
	}

	cerberusQueueItem := cerberusQueueItemDAO{
		ID:          uuid.New(),
		JsonMessage: string(jsonData),
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_cerberus_queue_items(queue_item_id, json_message) VALUES (:queue_item_id, :json_message);`, r.dbSchema)
	_, err = r.db.NamedExecContext(ctx, query, cerberusQueueItem)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create cerberus queue item")
		return uuid.Nil, err
	}

	return cerberusQueueItem.ID, nil
}

func (r *analysisRepository) DeleteOldCerberusQueueItems(ctx context.Context, cleanupDays, limit int) (int64, error) {
	query := fmt.Sprintf(`DELETE FROM %s.sk_cerberus_queue_items WHERE queue_item_id IN
				(SELECT queue_item_id FROM %s.sk_cerberus_queue_items scqi
					WHERE last_http_status >= 200
						AND last_http_status <= 299
						AND created_at <= current_date - ($1 ||' DAY')::INTERVAL LIMIT $2);`, r.dbSchema, r.dbSchema)
	result, err := r.db.ExecContext(ctx, query, strconv.Itoa(cleanupDays), limit)
	if err != nil {
		log.Error().Err(err).Msg("delete old cerberus queue items failed")
		return 0, err
	}

	return result.RowsAffected()
}

func (r *analysisRepository) DeleteOldAnalysisRequests(ctx context.Context, cleanupDays, limit int) (int64, error) {
	tx, err := r.getTransaction()
	if err != nil {
		log.Error().Err(err).Msg("delete old analysis requests failed")
		return 0, err
	}

	txR := *r
	txR.db = tx
	query := fmt.Sprintf(`SELECT id, sample_code FROM %s.sk_analysis_requests WHERE valid_until_time < (current_date - ($1 ||' DAY')::INTERVAL) AND is_processed LIMIT $2;`, r.dbSchema)
	rows, err := txR.db.QueryxContext(ctx, query, cleanupDays, limit)
	if err != nil {
		if !r.isExternalTx {
			_ = tx.Rollback()
		}
		log.Error().Err(err).Msg("delete old analysis requests failed")
		return 0, err
	}
	defer rows.Close()
	sampleCodes := make([]string, 0)
	analysisRequestIDs := make([]uuid.UUID, 0)
	for rows.Next() {
		var id uuid.UUID
		var sampleCode string
		err = rows.Scan(&id, &sampleCode)
		if err != nil {
			if !r.isExternalTx {
				_ = tx.Rollback()
			}
			log.Error().Err(err).Msg("delete old analysis requests failed")
			return 0, err
		}
		analysisRequestIDs = append(analysisRequestIDs, id)
		sampleCodes = append(sampleCodes, sampleCode)
	}
	if len(analysisRequestIDs) == 0 {
		return 0, err
	}

	err = txR.deleteAnalysisRequestExtraValuesByAnalysisRequestIDs(ctx, analysisRequestIDs)
	if err != nil {
		if !r.isExternalTx {
			_ = tx.Rollback()
		}
		return 0, err
	}
	err = txR.deleteSubjectInfosByAnalysisRequestIDs(ctx, analysisRequestIDs)
	if err != nil {
		if !r.isExternalTx {
			_ = tx.Rollback()
		}
		return 0, err
	}
	err = txR.deleteAnalysisRequestInstrumentTransmissionsByAnalysisRequestIDs(ctx, analysisRequestIDs)
	if err != nil {
		if !r.isExternalTx {
			_ = tx.Rollback()
		}
		return 0, err
	}

	err = txR.deleteAppliedSortingRuleTargetsBySampleCodes(ctx, sampleCodes)
	if err != nil {
		if !r.isExternalTx {
			_ = tx.Rollback()
		}
		return 0, err
	}

	query = fmt.Sprintf("DELETE FROM %s.sk_analysis_requests WHERE id IN (?);", r.dbSchema)
	query, args, _ := sqlx.In(query, analysisRequestIDs)
	query = txR.db.Rebind(query)
	result, err := txR.db.ExecContext(ctx, query, args...)
	if err != nil {
		if !r.isExternalTx {
			_ = tx.Rollback()
		}
		log.Error().Err(err).Msg("delete old analysis requests failed")
		return 0, err
	}
	if !r.isExternalTx {
		err = tx.Commit()
		if err != nil {
			_ = tx.Rollback()
			return 0, err
		}
	}

	return result.RowsAffected()
}

func (r *analysisRepository) deleteAnalysisRequestExtraValuesByAnalysisRequestIDs(ctx context.Context, analysisRequestIDs []uuid.UUID) error {
	if len(analysisRequestIDs) == 0 {
		return nil
	}
	err := utils.Partition(len(analysisRequestIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf("DELETE FROM %s.sk_analysis_request_extra_values WHERE analysis_request_id IN (?);", r.dbSchema)
		query, args, _ := sqlx.In(query, analysisRequestIDs[low:high])
		query = r.db.Rebind(query)
		_, err := r.db.ExecContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg("delete analysis request extra values by analysis request IDs failed")
			return err
		}

		return nil
	})

	return err
}

func (r *analysisRepository) deleteSubjectInfosByAnalysisRequestIDs(ctx context.Context, analysisRequestIDs []uuid.UUID) error {
	if len(analysisRequestIDs) == 0 {
		return nil
	}
	err := utils.Partition(len(analysisRequestIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf("DELETE FROM %s.sk_subject_infos WHERE analysis_request_id IN (?);", r.dbSchema)
		query, args, _ := sqlx.In(query, analysisRequestIDs[low:high])
		query = r.db.Rebind(query)
		_, err := r.db.ExecContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg("delete subject infos by analysis request IDs failed")
			return err
		}

		return nil
	})

	return err
}

func (r *analysisRepository) deleteAnalysisRequestInstrumentTransmissionsByAnalysisRequestIDs(ctx context.Context, analysisRequestIDs []uuid.UUID) error {
	if len(analysisRequestIDs) == 0 {
		return nil
	}
	err := utils.Partition(len(analysisRequestIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf("DELETE FROM %s.sk_analysis_request_instrument_transmissions WHERE analysis_request_id IN (?);", r.dbSchema)
		query, args, _ := sqlx.In(query, analysisRequestIDs[low:high])
		query = r.db.Rebind(query)
		_, err := r.db.ExecContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg("delete analysis request instrument transmissions by analysis request IDs failed")
			return err
		}

		return nil
	})

	return err
}

func (r *analysisRepository) deleteAppliedSortingRuleTargetsBySampleCodes(ctx context.Context, sampleCodes []string) error {
	if len(sampleCodes) == 0 {
		return nil
	}
	err := utils.Partition(len(sampleCodes), maxParams, func(low int, high int) error {
		query := fmt.Sprintf("DELETE FROM %s.sk_applied_sorting_rule_targets WHERE sample_code IN (?);", r.dbSchema)
		query, args, _ := sqlx.In(query, sampleCodes[low:high])
		query = r.db.Rebind(query)
		_, err := r.db.ExecContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg("delete applied sorting rule targets by sample codes failed")
			return err
		}

		return nil
	})

	return err
}

func (r *analysisRepository) DeleteOldAnalysisResults(ctx context.Context, cleanupDays, limit int) (int64, error) {
	tx, err := r.getTransaction()
	if err != nil {
		log.Error().Err(err).Msg("delete old analysis results failed")
		return 0, err
	}
	txR := *r
	txR.db = tx

	query := fmt.Sprintf(`SELECT id FROM %s.sk_analysis_results WHERE GREATEST(valid_until, created_at + INTERVAL '14 DAYS') < (current_date - ($1 ||' DAY')::INTERVAL) AND is_processed LIMIT $2;`, r.dbSchema)
	rows, err := txR.db.QueryxContext(ctx, query, cleanupDays, limit)
	if err != nil {
		if !r.isExternalTx {
			_ = tx.Rollback()
		}
		log.Error().Err(err).Msg("delete old analysis results failed")
		return 0, err
	}
	defer rows.Close()

	analysisResultIDs := make([]uuid.UUID, 0)
	for rows.Next() {
		var id uuid.UUID
		err = rows.Scan(&id)
		if err != nil {
			if !r.isExternalTx {
				_ = tx.Rollback()
			}
			log.Error().Err(err).Msg("delete old analysis results failed")
			return 0, err
		}
		analysisResultIDs = append(analysisResultIDs, id)
	}
	if len(analysisResultIDs) == 0 {
		_ = tx.Rollback()
		return 0, nil
	}

	err = txR.deleteImagesByAnalysisResultIDs(ctx, analysisResultIDs)
	if err != nil {
		if !r.isExternalTx {
			_ = tx.Rollback()
		}
		return 0, err
	}
	err = txR.deleteChannelResultsByAnalysisResultIDs(ctx, analysisResultIDs)
	if err != nil {
		if !r.isExternalTx {
			_ = tx.Rollback()
		}
		return 0, err
	}
	err = txR.deleteAnalysisResultWarningsByAnalysisResultIDs(ctx, analysisResultIDs)
	if err != nil {
		if !r.isExternalTx {
			_ = tx.Rollback()
		}
		return 0, err
	}
	err = txR.deleteAnalysisResultExtraValuesByAnalysisResultIDs(ctx, analysisResultIDs)
	if err != nil {
		if !r.isExternalTx {
			_ = tx.Rollback()
		}
		return 0, err
	}
	err = txR.deleteReagentInfosByAnalysisResultIDs(ctx, analysisResultIDs)
	if err != nil {
		if !r.isExternalTx {
			_ = tx.Rollback()
		}
		return 0, err
	}
	query = fmt.Sprintf(`DELETE FROM %s.sk_analysis_results WHERE id IN (?);`, r.dbSchema)
	query, args, _ := sqlx.In(query, analysisResultIDs)
	query = txR.db.Rebind(query)
	result, err := txR.db.ExecContext(ctx, query, args...)
	if err != nil {
		if !r.isExternalTx {
			_ = tx.Rollback()
		}
		log.Error().Err(err).Msg("delete old analysis results failed")
		return 0, err
	}

	if !r.isExternalTx {
		err = tx.Commit()
		if err != nil {
			_ = tx.Rollback()
			return 0, err
		}
	}

	return result.RowsAffected()
}

func (r *analysisRepository) deleteChannelResultsByAnalysisResultIDs(ctx context.Context, analysisResultIDs []uuid.UUID) error {
	if len(analysisResultIDs) == 0 {
		return nil
	}

	channelResultIDs := make([]uuid.UUID, 0)
	err := utils.Partition(len(analysisResultIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`SELECT id from %s.sk_channel_results WHERE analysis_result_id IN (?);`, r.dbSchema)
		query, args, _ := sqlx.In(query, analysisResultIDs[low:high])
		query = r.db.Rebind(query)

		rows, err := r.db.QueryxContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg("delete channel results by analysis result IDs failed")
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var id uuid.UUID
			err = rows.Scan(&id)
			if err != nil {
				log.Error().Err(err).Msg("delete channel results by analysis result IDs failed")
				return err
			}
			channelResultIDs = append(channelResultIDs, id)
		}

		return nil
	})
	if err != nil {
		return err
	}

	err = utils.Partition(len(channelResultIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`DELETE FROM %s.sk_channel_result_quantitative_values WHERE channel_result_id IN (?);`, r.dbSchema)
		query, args, _ := sqlx.In(query, channelResultIDs[low:high])
		query = r.db.Rebind(query)
		_, err := r.db.ExecContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg("delete channel results by analysis result IDs failed")
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	err = utils.Partition(len(channelResultIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`DELETE FROM %s.sk_channel_results WHERE id IN (?);`, r.dbSchema)
		query, args, _ := sqlx.In(query, channelResultIDs[low:high])
		query = r.db.Rebind(query)
		_, err := r.db.ExecContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg("delete channel results by analysis result IDs failed")
			return err
		}

		return nil
	})

	return err
}

func (r *analysisRepository) deleteAnalysisResultWarningsByAnalysisResultIDs(ctx context.Context, analysisResultIDs []uuid.UUID) error {
	if len(analysisResultIDs) == 0 {
		return nil
	}

	err := utils.Partition(len(analysisResultIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf("DELETE FROM %s.sk_analysis_result_warnings WHERE analysis_result_id IN (?);", r.dbSchema)
		query, args, _ := sqlx.In(query, analysisResultIDs[low:high])
		query = r.db.Rebind(query)
		_, err := r.db.ExecContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg("delete analysis result warnings failed")
			return err
		}

		return nil
	})

	return err
}

func (r *analysisRepository) deleteImagesByAnalysisResultIDs(ctx context.Context, analysisResultIDs []uuid.UUID) error {
	if len(analysisResultIDs) == 0 {
		return nil
	}

	err := utils.Partition(len(analysisResultIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf("DELETE FROM %s.sk_analysis_result_images WHERE analysis_result_id IN (?);", r.dbSchema)
		query, args, _ := sqlx.In(query, analysisResultIDs[low:high])
		query = r.db.Rebind(query)
		_, err := r.db.ExecContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg("delete images by analysis result IDs failed")
			return err
		}

		return nil
	})

	return err
}

func (r *analysisRepository) deleteAnalysisResultExtraValuesByAnalysisResultIDs(ctx context.Context, analysisResultIDs []uuid.UUID) error {
	if len(analysisResultIDs) == 0 {
		return nil
	}

	err := utils.Partition(len(analysisResultIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf("DELETE FROM %s.sk_analysis_result_extravalues WHERE analysis_result_id IN (?);", r.dbSchema)
		query, args, _ := sqlx.In(query, analysisResultIDs[low:high])
		query = r.db.Rebind(query)
		_, err := r.db.ExecContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg("delete analysis result extra values by analysis result IDs failed")
			return err
		}

		return nil
	})
	return err
}

func (r *analysisRepository) deleteReagentInfosByAnalysisResultIDs(ctx context.Context, analysisResultIDs []uuid.UUID) error {
	if len(analysisResultIDs) == 0 {
		return nil
	}

	err := utils.Partition(len(analysisResultIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf("DELETE FROM %s.sk_analysis_result_reagent_infos WHERE analysis_result_id IN (?);", r.dbSchema)
		query, args, _ := sqlx.In(query, analysisResultIDs[low:high])
		query = r.db.Rebind(query)
		_, err := r.db.ExecContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg("delete reagent infos by analysis result IDs failed")
			return err
		}

		return nil
	})

	return err
}

func convertAnalysisResultsToTOs(analysisResults []AnalysisResult) ([]AnalysisResultTO, error) {
	analysisResultTOs := make([]AnalysisResultTO, len(analysisResults))
	var err error
	for i := range analysisResults {
		analysisResultTOs[i], err = convertAnalysisResultToTO(analysisResults[i])
		if err != nil {
			return nil, err
		}
	}
	return analysisResultTOs, nil
}

func convertAnalysisResultToTO(ar AnalysisResult) (AnalysisResultTO, error) {
	analysisResultTO := AnalysisResultTO{
		WorkingItemID:            ar.AnalysisRequest.WorkItemID,
		ValidUntil:               ar.ValidUntil,
		ResultYieldDateTime:      ar.ResultYieldDateTime,
		ExaminedMaterial:         ar.AnalysisRequest.MaterialID,
		Result:                   ar.Result,
		Mode:                     string(ar.ResultMode),
		Status:                   string(ar.Status),
		Operator:                 ar.Operator,
		TechnicalReleaseDateTime: ar.TechnicalReleaseDateTime,
		InstrumentID:             ar.Instrument.ID,
		InstrumentRunID:          ar.InstrumentRunID,
		Edited:                   ar.Edited,
		EditReason:               ar.EditReason,
		IsInvalid:                ar.IsInvalid,
		ChannelResults:           make([]ChannelResultTO, 0),
		ExtraValues:              make([]ExtraValueTO, 0),
		ReagentInfos:             make([]ReagentInfoTO, 0),
		Images:                   make([]ImageTO, 0),
		WarnFlag:                 ar.WarnFlag,
		Warnings:                 ar.Warnings,
	}

	switch ar.Status {
	case Preliminary:
		analysisResultTO.Status = "PRE"
	case Final:
		analysisResultTO.Status = "FIN"
	default:
		return analysisResultTO, ErrInvalidResultStatus
	}

	for _, ev := range ar.ExtraValues {
		extraValueTO := ExtraValueTO{
			Key:   ev.Key,
			Value: ev.Value,
		}
		analysisResultTO.ExtraValues = append(analysisResultTO.ExtraValues, extraValueTO)
	}

	for _, img := range ar.Images {
		if !img.DeaImageID.Valid {
			continue
		}
		imageTO := ImageTO{
			ID:          img.DeaImageID.UUID,
			Name:        img.Name,
			Description: img.Description,
		}
		analysisResultTO.Images = append(analysisResultTO.Images, imageTO)
	}

	for _, cr := range ar.ChannelResults {
		channelResultTO := ChannelResultTO{
			ChannelID:             cr.ChannelID,
			QualitativeResult:     cr.QualitativeResult,
			QualitativeResultEdit: cr.QualitativeResultEdit,
			QuantitativeResults:   cr.QuantitativeResults,
			Images:                make([]ImageTO, 0),
		}
		for _, img := range cr.Images {
			if !img.DeaImageID.Valid {
				continue
			}
			imageTO := ImageTO{
				ID:          img.DeaImageID.UUID,
				Name:        img.Name,
				Description: img.Description,
			}
			channelResultTO.Images = append(channelResultTO.Images, imageTO)
		}
		analysisResultTO.ChannelResults = append(analysisResultTO.ChannelResults, channelResultTO)
	}

	for _, ri := range ar.ReagentInfos {
		reagentInfoTO := ReagentInfoTO{
			SerialNumber:            ri.SerialNumber,
			Name:                    ri.Name,
			Code:                    ri.Code,
			LotNo:                   ri.LotNo,
			ReagentManufacturerDate: ri.ReagentManufacturerDate,
			UseUntil:                ri.UseUntil,
			ShelfLife:               ri.ShelfLife,
			//TODO Add an expiry : ExpiryDateTime: (this has to be added to database as well)
			ManufacturerName: ri.ManufacturerName,
			DateCreated:      ri.DateCreated,
		}

		switch ri.ReagentType {
		case Reagent:
			reagentInfoTO.ReagentType = "Reagent"
		case Diluent:
			reagentInfoTO.ReagentType = "Diluent"
		default:
			return analysisResultTO, ErrInvalidReagentType
		}
		analysisResultTO.ReagentInfos = append(analysisResultTO.ReagentInfos, reagentInfoTO)
	}
	return analysisResultTO, nil
}

func (r *analysisRepository) GetAnalysisResultQueueItems(ctx context.Context) ([]CerberusQueueItem, error) {
	query := fmt.Sprintf(`SELECT queue_item_id, json_message, last_http_status, last_error, last_error_at, trial_count, retry_not_before, created_at FROM %s.sk_cerberus_queue_items 
			WHERE trial_count < 5760 /* 4 days á 2 minutes */ AND last_http_status NOT BETWEEN 200 AND 299 AND created_at > timezone('utc', now()-interval '14 days') AND retry_not_before < timezone('utc', now())
			ORDER BY created_at LIMIT 10;`, r.dbSchema)

	rows, err := r.db.QueryxContext(ctx, query)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Trace().Msg("No analysis results")
			return []CerberusQueueItem{}, nil
		}
		log.Error().Err(err).Msg("Can not search for AnalysisResults")
		return []CerberusQueueItem{}, err
	}
	defer rows.Close()

	cerberusQueueItems := make([]CerberusQueueItem, 0)
	for rows.Next() {
		queueItemDAO := cerberusQueueItemDAO{}
		err = rows.StructScan(&queueItemDAO)
		if err != nil {
			log.Error().Err(err).Msg("Can not scan data")
			return []CerberusQueueItem{}, err
		}

		cerberusQueueItems = append(cerberusQueueItems, convertCerberusQueueItemDAOToCerberusQueueItem(queueItemDAO))
	}

	return cerberusQueueItems, err
}

func (r *analysisRepository) UpdateAnalysisResultQueueItemStatus(ctx context.Context, queueItem CerberusQueueItem) error {
	query := fmt.Sprintf(`UPDATE %s.sk_cerberus_queue_items
			SET last_http_status = :last_http_status, last_error = :last_error, last_error_at = :last_error_at, trial_count = trial_count + 1, retry_not_before = :retry_not_before, raw_response = :raw_response, response_json_message = :response_json_message
			WHERE queue_item_id = :queue_item_id;`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertCerberusQueueItemToCerberusQueueItemDAO(queueItem))
	if err != nil {
		log.Error().Err(err).Msg("Update result transmission status failed")
		return err
	}
	return nil
}

func (r *analysisRepository) GetStuckImageIDsForDEA(ctx context.Context) ([]uuid.UUID, error) {
	query := fmt.Sprintf(`SELECT sari.id FROM %s.sk_analysis_result_images sari WHERE sari.dea_image_id IS NULL AND sari.image_bytes IS NOT NULL;`, r.dbSchema)

	imageIDs := make([]uuid.UUID, 0)

	rows, err := r.db.QueryxContext(ctx, query)
	if err != nil {
		log.Error().Err(err).Msg(msgFailedToGetStuckImages)
		return imageIDs, ErrFailedToGetStuckImages
	}
	defer rows.Close()

	for rows.Next() {
		var id uuid.UUID
		err := rows.Scan(&id)
		if err != nil {
			log.Error().Err(err).Msg(msgFailedToScanStuckImageId)
			return imageIDs, ErrFailedToScanStuckImageId
		}

		imageIDs = append(imageIDs, id)
	}

	return imageIDs, err
}

func (r *analysisRepository) GetStuckImageIDsForCerberus(ctx context.Context) ([]uuid.UUID, error) {
	query := fmt.Sprintf(`SELECT sari.id FROM %s.sk_analysis_result_images sari WHERE sari.sync_to_cerberus_needed IS TRUE;`, r.dbSchema)

	imageIDs := make([]uuid.UUID, 0)

	rows, err := r.db.QueryxContext(ctx, query)
	if err != nil {
		log.Error().Err(err).Msg(msgFailedToGetStuckImages)
		return imageIDs, ErrFailedToGetStuckImages
	}
	defer rows.Close()

	for rows.Next() {
		var id uuid.UUID
		err := rows.Scan(&id)
		if err != nil {
			log.Error().Err(err).Msg(msgFailedToScanStuckImageId)
			return imageIDs, ErrFailedToScanStuckImageId
		}

		imageIDs = append(imageIDs, id)
	}

	return imageIDs, err
}

func (r *analysisRepository) GetImagesForDEAUploadByIDs(ctx context.Context, ids []uuid.UUID) ([]imageDAO, error) {
	if len(ids) == 0 {
		return []imageDAO{}, nil
	}

	query := fmt.Sprintf(`SELECT sari.id, sari.image_bytes, sari.name FROM %s.sk_analysis_result_images sari WHERE sari.id IN (?);`, r.dbSchema)

	query, args, _ := sqlx.In(query, ids)
	query = r.db.Rebind(query)

	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgFailedToGetStuckImages)
		return []imageDAO{}, ErrFailedToGetStuckImages
	}
	defer rows.Close()

	images := make([]imageDAO, 0)
	for rows.Next() {
		image := imageDAO{}
		err := rows.StructScan(&image)
		if err != nil {
			log.Error().Err(err).Msg(msgFailedToScanStuckImageData)
			return []imageDAO{}, ErrFailedToScanStuckImageData
		}

		images = append(images, image)
	}

	return images, err
}

func (r *analysisRepository) GetImagesForCerberusSyncByIDs(ctx context.Context, ids []uuid.UUID) ([]cerberusImageDAO, error) {
	if len(ids) == 0 {
		return []cerberusImageDAO{}, nil
	}

	query := `SELECT sari.id, sari.dea_image_id, sari."name", sari.description, sar.yielded_at, sar2.work_item_id, scr.channel_id
				FROM %schema_name%.sk_analysis_result_images sari
				INNER JOIN %schema_name%.sk_analysis_results sar on sar.id = sari.analysis_result_id
				INNER JOIN %schema_name%.sk_channel_results scr on scr.id = sari.channel_result_id
				INNER JOIN %schema_name%.sk_analyte_mappings sam on sam.id = sar.analyte_mapping_id 
				LEFT JOIN %schema_name%.sk_analysis_requests sar2 on (sar2.sample_code = sar.sample_code and sar2.analyte_id = sam.analyte_id)
				WHERE sari.id IN (?);`

	query = strings.ReplaceAll(query, "%schema_name%", r.dbSchema)

	query, args, _ := sqlx.In(query, ids)
	query = r.db.Rebind(query)

	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgFailedToGetStuckImages)
		return []cerberusImageDAO{}, ErrFailedToGetStuckImages
	}
	defer rows.Close()

	images := make([]cerberusImageDAO, 0)
	for rows.Next() {
		image := cerberusImageDAO{}
		err := rows.StructScan(&image)
		if err != nil {
			log.Error().Err(err).Msg(msgFailedToScanStuckImageData)
			return []cerberusImageDAO{}, ErrFailedToScanStuckImageData
		}

		images = append(images, image)
	}

	return images, err
}

func (r *analysisRepository) SaveDEAImageID(ctx context.Context, imageID, deaImageID uuid.UUID) error {
	query := fmt.Sprintf(`
		UPDATE %s.sk_analysis_result_images
		SET dea_image_id = $2, image_bytes = NULL,
		    uploaded_to_dea_at = timezone('utc', now()),
		    upload_error = NULL, 
		    sync_to_cerberus_needed = true 
		WHERE id = $1;`, r.dbSchema)

	_, err := r.db.ExecContext(ctx, query, imageID, deaImageID)
	if err != nil {
		log.Error().Err(err).Msg(msgFailedToSaveDEAImageID)
		return ErrFailedToSaveDEAImageID
	}

	return nil
}

func (r *analysisRepository) IncreaseImageUploadRetryCount(ctx context.Context, imageID uuid.UUID, error string) error {
	query := fmt.Sprintf(`UPDATE %s.sk_analysis_result_images SET upload_retry_count = upload_retry_count + 1, upload_error = $2 WHERE id = $1;`, r.dbSchema)

	_, err := r.db.ExecContext(ctx, query, imageID, error)
	if err != nil {
		log.Error().Err(err).Msg(msgFailedToIncreaseImageUploadRetryCount)
		return ErrFailedToIncreaseImageUploadRetryCount
	}

	return nil
}

func (r *analysisRepository) MarkImagesAsSyncedToCerberus(ctx context.Context, ids []uuid.UUID) error {
	query := fmt.Sprintf(`UPDATE %s.sk_analysis_result_images SET sync_to_cerberus_needed = false WHERE id IN (?);`, r.dbSchema)

	query, args, _ := sqlx.In(query, ids)
	query = r.db.Rebind(query)

	_, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgFailedToMarkImagesAsSyncedToCerberus)
		return ErrFailedToMarkImagesAsSyncedToCerberus
	}

	return nil
}

func (r *analysisRepository) GetUnprocessedAnalysisRequests(ctx context.Context) ([]AnalysisRequest, error) {
	query := fmt.Sprintf(`SELECT *
					FROM %s.sk_analysis_requests sar
					WHERE sar.is_processed IS FALSE;`, r.dbSchema)

	rows, err := r.db.QueryxContext(ctx, query)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Trace().Msg("No analysis requests")
			return []AnalysisRequest{}, nil
		}
		log.Error().Err(err).Msg("Can not search for AnalysisRequests")
		return []AnalysisRequest{}, err
	}
	defer rows.Close()

	analysisRequests := make([]AnalysisRequest, 0)
	for rows.Next() {
		request := analysisRequestDAO{}
		err := rows.StructScan(&request)
		if err != nil {
			log.Error().Err(err).Msg("Can not scan data")
			return []AnalysisRequest{}, err
		}

		analysisRequests = append(analysisRequests, convertAnalysisRequestDAOToAnalysisRequest(request))
	}

	return analysisRequests, err
}

func (r *analysisRepository) GetUnprocessedAnalysisResultIDs(ctx context.Context) ([]uuid.UUID, error) {
	query := fmt.Sprintf(`SELECT sar.id FROM %s.sk_analysis_results sar WHERE sar.is_processed IS FALSE;`, r.dbSchema)

	rows, err := r.db.QueryxContext(ctx, query)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Trace().Msg("Unprocessed analysis results not found")
			return []uuid.UUID{}, nil
		}
		log.Error().Err(err).Msg("Failed to get unprocessed analysis result ids")
		return []uuid.UUID{}, err
	}
	defer rows.Close()

	analysisResultIDs := make([]uuid.UUID, 0)
	for rows.Next() {
		var id uuid.UUID
		err := rows.Scan(&id)
		if err != nil {
			log.Error().Err(err).Msg("Can not scan unprocessed analysis result id")
			return []uuid.UUID{}, err
		}

		analysisResultIDs = append(analysisResultIDs, id)
	}

	return analysisResultIDs, err
}

func (r *analysisRepository) MarkAnalysisRequestsAsProcessed(ctx context.Context, analysisRequestIDs []uuid.UUID) error {
	err := utils.Partition(len(analysisRequestIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`UPDATE %s.sk_analysis_requests SET is_processed = true WHERE id IN (?);`, r.dbSchema)

		query, args, _ := sqlx.In(query, analysisRequestIDs)
		query = r.db.Rebind(query)

		_, err := r.db.ExecContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg(msgFailedToMarkAnalysisRequestsAsProcessed)
			return ErrFailedToMarkAnalysisRequestsAsProcessed
		}

		return nil
	})

	return err
}

func (r *analysisRepository) MarkAnalysisResultsAsProcessed(ctx context.Context, analysisRequestIDs []uuid.UUID) error {
	err := utils.Partition(len(analysisRequestIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`UPDATE %s.sk_analysis_results SET is_processed = true WHERE id IN (?);`, r.dbSchema)

		query, args, _ := sqlx.In(query, analysisRequestIDs)
		query = r.db.Rebind(query)

		_, err := r.db.ExecContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg(msgFailedToMarkAnalysisResultsAsProcessed)
			return ErrFailedToMarkAnalysisResultsAsProcessed
		}
		return nil
	})

	return err
}

func (r *analysisRepository) CreateTransaction() (db.DbConnector, error) {
	return r.db.CreateTransactionConnector()
}

func (r *analysisRepository) WithTransaction(tx db.DbConnector) AnalysisRepository {
	txRepo := *r
	txRepo.db = tx
	txRepo.isExternalTx = true
	return &txRepo
}

func (r *analysisRepository) getTransaction() (db.DbConnector, error) {
	if r.isExternalTx {
		return r.db, nil
	}

	return r.CreateTransaction()
}

func convertAnalysisRequestsToDAOs(analysisRequests []AnalysisRequest) []analysisRequestDAO {
	analysisRequestDAOs := make([]analysisRequestDAO, len(analysisRequests))
	for i := range analysisRequestDAOs {
		analysisRequestDAOs[i] = convertAnalysisRequestToDAO(analysisRequests[i])
	}
	return analysisRequestDAOs
}

func convertAnalysisRequestToDAO(analysisRequest AnalysisRequest) analysisRequestDAO {
	dao := analysisRequestDAO{
		ID:                          analysisRequest.ID,
		WorkItemID:                  analysisRequest.WorkItemID,
		AnalyteID:                   analysisRequest.AnalyteID,
		SampleCode:                  analysisRequest.SampleCode,
		MaterialID:                  analysisRequest.MaterialID,
		LaboratoryID:                analysisRequest.LaboratoryID,
		ValidUntilTime:              analysisRequest.ValidUntilTime,
		CreatedAt:                   analysisRequest.CreatedAt,
		ReexaminationRequestedCount: analysisRequest.ReexaminationRequestedCount,
	}

	if analysisRequest.ModifiedAt != nil {
		dao.ModifiedAt = sql.NullTime{
			Time:  *analysisRequest.ModifiedAt,
			Valid: true,
		}
	}

	return dao
}

func convertAnalysisRequestDAOsToAnalysisRequests(analysisRequestDAOs []analysisRequestDAO) []AnalysisRequest {
	analysisRequests := make([]AnalysisRequest, len(analysisRequestDAOs))
	for i := range analysisRequestDAOs {
		analysisRequests[i] = convertAnalysisRequestDAOToAnalysisRequest(analysisRequestDAOs[i])
	}
	return analysisRequests
}

func convertAnalysisResultToDAO(analysisResult AnalysisResult) analysisResultDAO {
	analysisResultDAO := analysisResultDAO{
		ID:               analysisResult.ID,
		AnalyteMappingID: analysisResult.AnalyteMapping.ID,
		InstrumentID:     analysisResult.Instrument.ID,
		ResultMode:       analysisResult.ResultMode,
		SampleCode:       analysisResult.SampleCode,
		InstrumentRunID:  analysisResult.InstrumentRunID,
		ResultRecordID:   analysisResult.ResultRecordID,
		BatchID:          analysisResult.BatchID,
		Result:           analysisResult.Result,
		Status:           analysisResult.Status,
		ValidUntil:       analysisResult.ValidUntil,
		Operator:         analysisResult.Operator,
		Edited:           analysisResult.Edited,
		EditReason: sql.NullString{
			String: analysisResult.EditReason,
			Valid:  true,
		},
		IsInvalid: analysisResult.IsInvalid,
	}

	if analysisResult.ResultYieldDateTime != nil {
		analysisResultDAO.YieldedAt = sql.NullTime{
			Time:  *analysisResult.ResultYieldDateTime,
			Valid: true,
		}
	}

	if analysisResult.TechnicalReleaseDateTime != nil {
		analysisResultDAO.TechnicalReleaseDateTime = sql.NullTime{
			Time:  *analysisResult.TechnicalReleaseDateTime,
			Valid: true,
		}
	}

	return analysisResultDAO
}

func convertAnalysisResultsToDAOs(analysisResults []AnalysisResult) []analysisResultDAO {
	DAOs := make([]analysisResultDAO, len(analysisResults))
	for i := range analysisResults {
		DAOs[i] = convertAnalysisResultToDAO(analysisResults[i])
	}
	return DAOs
}

func convertAnalysisResultDAOToAnalysisResult(analysisResultDAO analysisResultDAO) AnalysisResult {
	analysisResult := AnalysisResult{
		ID:             analysisResultDAO.ID,
		AnalyteMapping: convertAnalyteMappingDaoToAnalyteMapping(analysisResultDAO.AnalyteMapping),
		Instrument: Instrument{
			ID: analysisResultDAO.InstrumentID,
		},
		SampleCode:      analysisResultDAO.SampleCode,
		ResultRecordID:  analysisResultDAO.ResultRecordID,
		BatchID:         analysisResultDAO.BatchID,
		Result:          analysisResultDAO.Result,
		ResultMode:      analysisResultDAO.ResultMode,
		Status:          analysisResultDAO.Status,
		ValidUntil:      analysisResultDAO.ValidUntil,
		Operator:        analysisResultDAO.Operator,
		InstrumentRunID: analysisResultDAO.InstrumentRunID,
		Edited:          analysisResultDAO.Edited,
		EditReason:      nullStringToString(analysisResultDAO.EditReason),
		IsInvalid:       analysisResultDAO.IsInvalid,
		Warnings:        convertAnalysisWarningDAOsToWarnings(analysisResultDAO.Warnings),
		ChannelResults:  convertChannelResultDAOsToChannelResults(analysisResultDAO.ChannelResults),
		ExtraValues:     convertExtraValueDAOsToExtraValues(analysisResultDAO.ExtraValues),
		ReagentInfos:    convertReagentInfoDAOsToReagentInfoList(analysisResultDAO.ReagentInfos),
		Images:          convertImageDAOsToImages(analysisResultDAO.Images),
	}

	if analysisResultDAO.YieldedAt.Valid {
		analysisResult.ResultYieldDateTime = &analysisResultDAO.YieldedAt.Time
	}

	if analysisResultDAO.TechnicalReleaseDateTime.Valid {
		analysisResult.TechnicalReleaseDateTime = &analysisResultDAO.TechnicalReleaseDateTime.Time
	}

	return analysisResult
}

func convertAnalysisResultDAOsToAnalysisResults(analysisResults []analysisResultDAO) []AnalysisResult {
	results := make([]AnalysisResult, len(analysisResults))
	for i := range analysisResults {
		results[i] = convertAnalysisResultDAOToAnalysisResult(analysisResults[i])
	}
	return results
}

func convertChannelResultToDAO(channelResult ChannelResult, analysisResultID uuid.UUID) channelResultDAO {
	return channelResultDAO{
		ID:                    channelResult.ID,
		AnalysisResultID:      analysisResultID,
		ChannelID:             channelResult.ChannelID,
		QualitativeResult:     channelResult.QualitativeResult,
		QualitativeResultEdit: channelResult.QualitativeResultEdit,
	}
}

func convertChannelResultsToDAOs(channelResults []ChannelResult, analysisResultID uuid.UUID) []channelResultDAO {
	channelResultDAOs := make([]channelResultDAO, len(channelResults))
	for i := range channelResults {
		channelResultDAOs[i] = convertChannelResultToDAO(channelResults[i], analysisResultID)
	}
	return channelResultDAOs
}

func convertExtraValueToResultExtraValueDAO(extraValue ExtraValue, analysisResultID uuid.UUID) resultExtraValueDAO {
	return resultExtraValueDAO{
		AnalysisResultID: analysisResultID,
		Key:              extraValue.Key,
		Value:            extraValue.Value,
	}
}

func convertExtraValuesToResultExtraValueDAOs(extraValues []ExtraValue, analysisResultID uuid.UUID) []resultExtraValueDAO {
	extraValueDAOs := make([]resultExtraValueDAO, len(extraValues))
	for i := range extraValues {
		extraValueDAOs[i] = convertExtraValueToResultExtraValueDAO(extraValues[i], analysisResultID)
	}
	return extraValueDAOs
}

func convertExtraValueToRequestExtraValueDAO(extraValue ExtraValue, analysisRequestID uuid.UUID) requestExtraValueDAO {
	return requestExtraValueDAO{
		AnalysisRequestID: analysisRequestID,
		Key:               extraValue.Key,
		Value:             extraValue.Value,
	}
}

func convertExtraValuesToRequestExtraValueDAOs(extraValues []ExtraValue, analysisRequestID uuid.UUID) []requestExtraValueDAO {
	extraValueDAOs := make([]requestExtraValueDAO, len(extraValues))
	for i := range extraValues {
		extraValueDAOs[i] = convertExtraValueToRequestExtraValueDAO(extraValues[i], analysisRequestID)
	}
	return extraValueDAOs
}

func convertQuantitativeResultsToDAOs(quantitativeResults map[string]string, channelResultID uuid.UUID) []quantitativeChannelResultDAO {
	DAOs := make([]quantitativeChannelResultDAO, 0, len(quantitativeResults))
	for metric, value := range quantitativeResults {
		DAOs = append(DAOs, quantitativeChannelResultDAO{
			ChannelResultID: channelResultID,
			Metric:          metric,
			Value:           value,
		})
	}
	return DAOs
}

func convertReagentInfoToDAO(reagentInfo ReagentInfo, analysisResultID uuid.UUID) reagentInfoDAO {
	return reagentInfoDAO{
		AnalysisResultID:        analysisResultID,
		SerialNumber:            reagentInfo.SerialNumber,
		Name:                    reagentInfo.Name,
		Code:                    reagentInfo.Code,
		ShelfLife:               reagentInfo.ShelfLife,
		LotNo:                   reagentInfo.LotNo,
		ManufacturerName:        reagentInfo.ManufacturerName,
		ReagentManufacturerDate: reagentInfo.ReagentManufacturerDate,
		ReagentType:             reagentInfo.ReagentType,
		UseUntil:                reagentInfo.UseUntil,
	}
}

func convertReagentInfosToDAOs(reagentInfos []ReagentInfo, analysisResultID uuid.UUID) []reagentInfoDAO {
	reagentInfoDAOs := make([]reagentInfoDAO, len(reagentInfos))
	for i := range reagentInfos {
		reagentInfoDAOs[i] = convertReagentInfoToDAO(reagentInfos[i], analysisResultID)
	}
	return reagentInfoDAOs
}

func convertWarningsToDAOs(warnings []string, analysisResultID uuid.UUID) []warningDAO {
	warningDAOs := make([]warningDAO, len(warnings))
	for i := range warnings {
		warningDAOs[i] = warningDAO{
			AnalysisResultID: analysisResultID,
			Warning:          warnings[i],
		}
	}
	return warningDAOs
}

func convertImageToDAO(image Image, analysisResultID uuid.UUID, channelResultID uuid.NullUUID) imageDAO {
	dao := imageDAO{
		ID:               image.ID,
		AnalysisResultID: analysisResultID,
		ChannelResultID:  channelResultID,
		Name:             image.Name,
	}
	if image.Description != nil {
		dao.Description = sql.NullString{
			String: *image.Description,
			Valid:  len(*image.Description) > 0,
		}
	}
	return dao
}

func convertImagesToDAOs(images []Image, analysisResultID uuid.UUID, channelResultID uuid.NullUUID) []imageDAO {
	imageDAOs := make([]imageDAO, len(images))
	for i := range images {
		imageDAOs[i] = convertImageToDAO(images[i], analysisResultID, channelResultID)
	}
	return imageDAOs
}

func convertAnalysisRequestDAOToAnalysisRequest(analysisRequest analysisRequestDAO) AnalysisRequest {
	req := AnalysisRequest{
		ID:                          analysisRequest.ID,
		WorkItemID:                  analysisRequest.WorkItemID,
		AnalyteID:                   analysisRequest.AnalyteID,
		SampleCode:                  analysisRequest.SampleCode,
		MaterialID:                  analysisRequest.MaterialID,
		LaboratoryID:                analysisRequest.LaboratoryID,
		ValidUntilTime:              analysisRequest.ValidUntilTime,
		CreatedAt:                   analysisRequest.CreatedAt,
		ReexaminationRequestedCount: analysisRequest.ReexaminationRequestedCount,
	}

	if analysisRequest.ModifiedAt.Valid {
		req.ModifiedAt = &analysisRequest.ModifiedAt.Time
	}

	return req
}

func convertSubjectToDAO(subject SubjectInfo, analysisRequestID uuid.UUID) subjectInfoDAO {
	dao := subjectInfoDAO{
		AnalysisRequestID: analysisRequestID,
		Type:              subject.Type,
	}
	if subject.DateOfBirth != nil {
		dao.DateOfBirth = sql.NullTime{Time: *subject.DateOfBirth, Valid: true}
	}
	if subject.FirstName != nil {
		dao.FirstName = sql.NullString{String: *subject.FirstName, Valid: true}
	}
	if subject.LastName != nil {
		dao.LastName = sql.NullString{String: *subject.LastName, Valid: true}
	}
	if subject.DonorID != nil {
		dao.DonorID = sql.NullString{String: *subject.DonorID, Valid: true}
	}
	if subject.DonationID != nil {
		dao.DonationID = sql.NullString{String: *subject.DonationID, Valid: true}
	}
	if subject.DonationType != nil {
		dao.DonationType = sql.NullString{String: *subject.DonationType, Valid: true}
	}
	if subject.Pseudonym != nil {
		dao.Pseudonym = sql.NullString{String: *subject.Pseudonym, Valid: true}
	}
	return dao
}

func convertSubjectDAOToSubjectInfo(subject subjectInfoDAO) SubjectInfo {
	subjectInfo := SubjectInfo{
		Type: subject.Type,
	}
	if subject.DateOfBirth.Valid {
		subjectInfo.DateOfBirth = &subject.DateOfBirth.Time
	}
	if subject.FirstName.Valid {
		subjectInfo.FirstName = &subject.FirstName.String
	}
	if subject.LastName.Valid {
		subjectInfo.LastName = &subject.LastName.String
	}
	if subject.DonorID.Valid {
		subjectInfo.DonorID = &subject.DonorID.String
	}
	if subject.DonationID.Valid {
		subjectInfo.DonationID = &subject.DonationID.String
	}
	if subject.DonationType.Valid {
		subjectInfo.DonationType = &subject.DonationType.String
	}
	if subject.Pseudonym.Valid {
		subjectInfo.Pseudonym = &subject.Pseudonym.String
	}
	return subjectInfo
}

func convertRequestInfoDAOToRequestInfo(analysisRequestInfoDAO analysisRequestInfoDAO) AnalysisRequestInfo {
	analysisRequestInfo := AnalysisRequestInfo{
		ID:                analysisRequestInfoDAO.RequestID,
		WorkItemID:        analysisRequestInfoDAO.WorkItemID,
		AnalyteID:         analysisRequestInfoDAO.AnalyteID,
		SampleCode:        analysisRequestInfoDAO.SampleCode,
		RequestCreatedAt:  analysisRequestInfoDAO.RequestCreatedAt,
		ResultID:          nullUUIDToUUIDPointer(analysisRequestInfoDAO.ResultID),
		AnalyteMappingsID: nullUUIDToUUIDPointer(analysisRequestInfoDAO.AnalyteMappingsID),
		TestName:          nullStringToStringPointer(analysisRequestInfoDAO.TestName),
		TestResult:        nullStringToStringPointer(analysisRequestInfoDAO.TestResult),
		BatchCreatedAt:    nullTimeToTimePointer(analysisRequestInfoDAO.BatchCreatedAt),
		Status:            AnalysisRequestStatusOpen,
		SourceIP:          nullStringToString(analysisRequestInfoDAO.SourceIP),
		InstrumentID:      nullUUIDToUUIDPointer(analysisRequestInfoDAO.InstrumentID),
	}

	// Todo - Check conditions
	if analysisRequestInfo.ResultID != nil && *analysisRequestInfo.ResultID != uuid.Nil {
		analysisRequestInfo.Status = AnalysisRequestStatusProcessed
	}

	if !analysisRequestInfoDAO.AnalyteMappingsID.Valid {
		analysisRequestInfo.MappingError = true
	}

	if analysisRequestInfoDAO.ResultCreatedAt.Valid {
		analysisRequestInfo.ResultCreatedAt = &analysisRequestInfoDAO.ResultCreatedAt.Time
	}

	return analysisRequestInfo
}

func convertResultInfoDAOToResultInfo(analysisResultInfoDAO analysisResultInfoDAO) AnalysisResultInfo {
	analysisResultInfo := AnalysisResultInfo{
		ID:               analysisResultInfoDAO.ID,
		BatchID:          nullUUIDToUUIDPointer(analysisResultInfoDAO.BatchID),
		RequestCreatedAt: nullTimeToTimePointer(analysisResultInfoDAO.RequestCreatedAt),
		WorkItemID:       nullUUIDToUUIDPointer(analysisResultInfoDAO.WorkItemID),
		AnalyteID:        analysisResultInfoDAO.AnalyteID,
		SampleCode:       analysisResultInfoDAO.SampleCode,
		ResultCreatedAt:  analysisResultInfoDAO.ResultCreatedAt,
		TestName:         nullStringToStringPointer(analysisResultInfoDAO.TestName),
		TestResult:       nullStringToStringPointer(analysisResultInfoDAO.TestResult),
		Status:           analysisResultInfoDAO.Status,
	}

	return analysisResultInfo
}

func convertRequestInfoDAOsToRequestInfos(analysisRequestInfoDAOs []analysisRequestInfoDAO) []AnalysisRequestInfo {
	analysisRequestInfo := make([]AnalysisRequestInfo, len(analysisRequestInfoDAOs))
	for i := range analysisRequestInfoDAOs {
		analysisRequestInfo[i] = convertRequestInfoDAOToRequestInfo(analysisRequestInfoDAOs[i])
	}
	return analysisRequestInfo
}

func convertResultInfoDAOsToResultInfos(analysisResultInfoDAOs []analysisResultInfoDAO) []AnalysisResultInfo {
	analysisResultInfo := make([]AnalysisResultInfo, len(analysisResultInfoDAOs))
	for i := range analysisResultInfoDAOs {
		analysisResultInfo[i] = convertResultInfoDAOToResultInfo(analysisResultInfoDAOs[i])
	}
	return analysisResultInfo
}

func convertCerberusQueueItemToCerberusQueueItemDAO(cerberusQueueItem CerberusQueueItem) cerberusQueueItemDAO {
	return cerberusQueueItemDAO{
		ID:                  cerberusQueueItem.ID,
		JsonMessage:         cerberusQueueItem.JsonMessage,
		LastHTTPStatus:      cerberusQueueItem.LastHTTPStatus,
		LastError:           cerberusQueueItem.LastError,
		LastErrorAt:         timePointerToNullTime(cerberusQueueItem.LastErrorAt),
		TrialCount:          cerberusQueueItem.TrialCount,
		RetryNotBefore:      cerberusQueueItem.RetryNotBefore,
		RawResponse:         cerberusQueueItem.RawResponse,
		ResponseJsonMessage: cerberusQueueItem.ResponseJsonMessage,
	}
}

func convertCerberusQueueItemDAOToCerberusQueueItem(cerberusQueueItemDAO cerberusQueueItemDAO) CerberusQueueItem {
	return CerberusQueueItem{
		ID:                  cerberusQueueItemDAO.ID,
		JsonMessage:         cerberusQueueItemDAO.JsonMessage,
		LastHTTPStatus:      cerberusQueueItemDAO.LastHTTPStatus,
		LastError:           cerberusQueueItemDAO.LastError,
		LastErrorAt:         nullTimeToTimePointer(cerberusQueueItemDAO.LastErrorAt),
		TrialCount:          cerberusQueueItemDAO.TrialCount,
		RetryNotBefore:      cerberusQueueItemDAO.RetryNotBefore,
		RawResponse:         cerberusQueueItemDAO.RawResponse,
		ResponseJsonMessage: cerberusQueueItemDAO.ResponseJsonMessage,
	}
}

func convertReagentInfoDAOsToReagentInfoList(reagentInfoDAOs []reagentInfoDAO) []ReagentInfo {
	reagentInfoList := make([]ReagentInfo, len(reagentInfoDAOs))
	for i, reagentInfoDAO := range reagentInfoDAOs {
		reagentInfoList[i] = ReagentInfo{
			SerialNumber:            reagentInfoDAO.SerialNumber,
			Name:                    reagentInfoDAO.Name,
			Code:                    reagentInfoDAO.Code,
			ShelfLife:               reagentInfoDAO.ShelfLife,
			LotNo:                   reagentInfoDAO.LotNo,
			ManufacturerName:        reagentInfoDAO.ManufacturerName,
			ReagentManufacturerDate: reagentInfoDAO.ReagentManufacturerDate,
			ReagentType:             reagentInfoDAO.ReagentType,
			UseUntil:                reagentInfoDAO.UseUntil,
			DateCreated:             reagentInfoDAO.DateCreated,
		}
	}
	return reagentInfoList
}

func convertAnalysisWarningDAOsToWarnings(warningDAOs []warningDAO) []string {
	warnings := make([]string, len(warningDAOs))
	for i := range warningDAOs {
		warnings[i] = warningDAOs[i].Warning
	}
	return warnings
}

func convertExtraValueDAOsToExtraValues(extraValueDAOs []resultExtraValueDAO) []ExtraValue {
	extraValues := make([]ExtraValue, len(extraValueDAOs))
	for i, extraValueDAO := range extraValueDAOs {
		extraValues[i] = ExtraValue{
			Key:   extraValueDAO.Key,
			Value: extraValueDAO.Value,
		}
	}
	return extraValues
}

func convertChannelResultDAOsToChannelResults(channelResultDAOs []channelResultDAO) []ChannelResult {
	channelResults := make([]ChannelResult, len(channelResultDAOs))
	for i, channelResultDAO := range channelResultDAOs {
		channelResults[i] = ChannelResult{
			ID:                    channelResultDAO.ID,
			ChannelID:             channelResultDAO.ChannelID,
			QualitativeResult:     channelResultDAO.QualitativeResult,
			QualitativeResultEdit: channelResultDAO.QualitativeResultEdit,
			QuantitativeResults:   convertQuantitativeResultDAOsToQuantitativeResults(channelResultDAO.QuantitativeResults),
			Images:                convertImageDAOsToImages(channelResultDAO.Images),
		}
	}
	return channelResults
}

func convertQuantitativeResultDAOsToQuantitativeResults(quantitativeChannelResultDAOs []quantitativeChannelResultDAO) map[string]string {
	quantitativeChannelResults := make(map[string]string, len(quantitativeChannelResultDAOs))
	for _, quantitativeChannelResultDAO := range quantitativeChannelResultDAOs {
		quantitativeChannelResults[quantitativeChannelResultDAO.Metric] = quantitativeChannelResultDAO.Value
	}
	return quantitativeChannelResults
}

func convertImageDAOsToImages(imageDAOs []imageDAO) []Image {
	images := make([]Image, len(imageDAOs))
	for i, imageDAO := range imageDAOs {
		images[i] = Image{
			ID:          imageDAO.ID,
			Name:        imageDAO.Name,
			Description: nullStringToStringPointer(imageDAO.Description),
			DeaImageID:  imageDAO.DeaImageID,
		}
	}
	return images
}
