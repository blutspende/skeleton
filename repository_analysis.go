package skeleton

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/db"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
)

type analysisRequestDAO struct {
	ID             uuid.UUID `db:"id"`
	WorkItemID     uuid.UUID `db:"work_item_id"`
	AnalyteID      uuid.UUID `db:"analyte_id"`
	SampleCode     string    `db:"sample_code"`
	MaterialID     uuid.UUID `db:"material_id"`
	LaboratoryID   uuid.UUID `db:"laboratory_id"`
	ValidUntilTime time.Time `db:"valid_until_time"`
	CreatedAt      time.Time `db:"created_at"`
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
	YieldedAt                time.Time         `db:"yielded_at"`
	ValidUntil               time.Time         `db:"valid_until"`
	Operator                 string            `db:"operator"`
	TechnicalReleaseDateTime time.Time         `db:"technical_release_datetime"`
	RunCounter               int               `db:"run_counter"`
	Edited                   bool              `db:"edited"`
	EditReason               sql.NullString    `db:"edit_reason"`
	AnalyteMapping           analyteMappingDAO `db:"analyte_mapping"`
	ChannelResults           []channelResultDAO
	ExtraValues              []extraValueDAO
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

type extraValueDAO struct {
	ID               uuid.UUID `db:"id"`
	AnalysisResultID uuid.UUID `db:"analysis_result_id"`
	Key              string    `db:"key"`
	Value            string    `db:"value"`
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
	RequestCreatedAt  time.Time      `db:"request_date"`
	AnalyteMappingsID uuid.NullUUID  `db:"analyte_mapping_id"`
	ResultID          uuid.NullUUID  `db:"result_id"`
	TestName          sql.NullString `db:"test_name"`
	TestResult        sql.NullString `db:"test_result"`
	BatchCreatedAt    sql.NullTime   `db:"batch_created_at"`
	SentToCerberusAt  sql.NullTime   `db:"sent_to_cerberus_at"`
	SourceIP          sql.NullString `db:"source_ip"`
	InstrumentID      uuid.NullUUID  `db:"instrument_id"`
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
	GetAnalysisRequestsBySampleCodeAndAnalyteID(ctx context.Context, sampleCodes string, analyteID uuid.UUID) ([]AnalysisRequest, error)
	GetAnalysisRequestsBySampleCodes(ctx context.Context, sampleCodes []string) (map[string][]AnalysisRequest, error)
	GetAnalysisRequestsInfo(ctx context.Context, instrumentID uuid.UUID, pageable Pageable) ([]AnalysisRequestInfo, int, error)
	//GetAnalysisRequestsForVisualization(ctx context.Context) (map[string][]AnalysisRequest, error)
	CreateSubjectsBatch(ctx context.Context, subjectInfosByAnalysisRequestID map[uuid.UUID]SubjectInfo) (map[uuid.UUID]uuid.UUID, error)
	GetSubjectsByAnalysisRequestIDs(ctx context.Context, analysisRequestIDs []uuid.UUID) (map[uuid.UUID]SubjectInfo, error)

	CreateAnalysisResultsBatch(ctx context.Context, analysisResults []AnalysisResult) ([]AnalysisResult, error)
	GetAnalysisResultsBySampleCodeAndAnalyteID(ctx context.Context, sampleCode string, analyteID uuid.UUID) ([]AnalysisResult, error)

	GetAnalysisResultQueueItems(ctx context.Context) ([]CerberusQueueItem, error)
	UpdateAnalysisResultQueueItemStatus(ctx context.Context, queueItem CerberusQueueItem) error
	CreateAnalysisResultQueueItem(ctx context.Context, analysisResults []AnalysisResult) (uuid.UUID, error)

	CreateTransaction() (db.DbConnector, error)
	WithTransaction(tx db.DbConnector) AnalysisRepository
}

type analysisRepository struct {
	db       db.DbConnector
	dbSchema string
}

func NewAnalysisRepository(db db.DbConnector, dbSchema string) AnalysisRepository {
	return &analysisRepository{
		db:       db,
		dbSchema: dbSchema,
	}
}

// CreateAnalysisRequestsBatch
// Returns the ID and work item IDs of saved requests
func (r *analysisRepository) CreateAnalysisRequestsBatch(ctx context.Context, analysisRequests []AnalysisRequest) ([]uuid.UUID, []uuid.UUID, error) {
	if len(analysisRequests) == 0 {
		return []uuid.UUID{}, []uuid.UUID{}, nil
	}
	ids := make([]uuid.UUID, len(analysisRequests))
	workItemIDs := make([]uuid.UUID, len(analysisRequests))
	for i := range analysisRequests {
		if (analysisRequests[i].ID == uuid.UUID{}) || (analysisRequests[i].ID == uuid.Nil) {
			analysisRequests[i].ID = uuid.New()
		}
		ids[i] = analysisRequests[i].ID
		workItemIDs[i] = analysisRequests[i].WorkItemID
	}

	query := fmt.Sprintf(`INSERT INTO %s.sk_analysis_requests(id, work_item_id, analyte_id, sample_code, material_id, laboratory_id, valid_until_time)
				VALUES(:id, :work_item_id, :analyte_id, :sample_code, :material_id, :laboratory_id, :valid_until_time) 
				ON CONFLICT (work_item_id) DO 
				UPDATE SET analyte_id = excluded.analyte_id, sample_code = excluded.sample_code, material_id = excluded.material_id, laboratory_id = excluded.laboratory_id, valid_until_time = excluded.valid_until_time;`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertAnalysisRequestsToDAOs(analysisRequests))
	if err != nil {
		log.Error().Err(err).Msg("Can not create RequestData")
		return []uuid.UUID{}, []uuid.UUID{}, nil
	}

	return ids, workItemIDs, nil
}

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
	query := fmt.Sprintf(`INSERT INTO %s.sk_subject_infos(id, analysis_request_id,"type",date_of_birth,first_name,last_name,donor_id,donation_id,donation_type,pseudonym)
		VALUES(id, :analysis_request_id,:type,:date_of_birth,:first_name,:last_name,:donor_id,:donation_id,:donation_type,:pseudonym);`, r.dbSchema)

	_, err := r.db.NamedExecContext(ctx, query, subjectDAOs)
	if err != nil {
		log.Error().Err(err).Msg("create subjects failed")
		return map[uuid.UUID]uuid.UUID{}, err
	}
	return idsMap, nil
}

func (r *analysisRepository) GetAnalysisRequestsBySampleCodeAndAnalyteID(ctx context.Context, sampleCode string, analyteID uuid.UUID) ([]AnalysisRequest, error) {
	query := fmt.Sprintf(`SELECT sar.id, sar.work_item_id, sar.analyte_id, sar.sample_code, sar.material_id, sar.laboratory_id, sar.valid_until_time, sar.created_at
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

func (r *analysisRepository) GetAnalysisRequestsBySampleCodes(ctx context.Context, sampleCodes []string) (map[string][]AnalysisRequest, error) {
	analysisRequestsBySampleCodes := make(map[string][]AnalysisRequest)
	if len(sampleCodes) == 0 {
		return analysisRequestsBySampleCodes, nil
	}
	query := fmt.Sprintf(`SELECT * FROM %s.sk_analysis_requests 
					WHERE sample_code in (?)
					AND valid_until_time >= timezone('utc',now());`, r.dbSchema)

	query, args, _ := sqlx.In(query, sampleCodes)
	query = r.db.Rebind(query)

	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg("Can not search for AnalysisRequests")
		return analysisRequestsBySampleCodes, err
	}

	defer rows.Close()

	for rows.Next() {
		request := analysisRequestDAO{}
		err := rows.StructScan(&request)
		if err != nil {
			log.Error().Err(err).Msg("Can not scan data")
			return analysisRequestsBySampleCodes, err
		}
		if _, ok := analysisRequestsBySampleCodes[request.SampleCode]; !ok {
			analysisRequestsBySampleCodes[request.SampleCode] = make([]AnalysisRequest, 0)
		}
		analysisRequestsBySampleCodes[request.SampleCode] = append(analysisRequestsBySampleCodes[request.SampleCode], convertAnalysisRequestDAOToAnalysisRequest(request))
	}

	return analysisRequestsBySampleCodes, err
}

func (r *analysisRepository) GetAnalysisRequestsInfo(ctx context.Context, instrumentID uuid.UUID, pageable Pageable) ([]AnalysisRequestInfo, int, error) {
	query := `SELECT req.id AS request_id,
	   req.sample_code AS sample_code,
	   req.work_item_id AS work_item_id,
	   req.analyte_id AS analyte_id,
	   req.created_at as request_date,
	   am.analyte_id as analyte_mapping_id,
	   res.id AS result_id,
	   res."result" AS test_result,
	   i.hostname as source_ip,
	   i.id as instrument_id
FROM %schema_name%.sk_analysis_requests req
LEFT JOIN %schema_name%.sk_analysis_results res ON res.sample_code = req.sample_code
LEFT JOIN %schema_name%.sk_instruments i ON i.id = res.instrument_id
LEFT JOIN %schema_name%.sk_analyte_mappings am ON am.instrument_id = i.id AND req.analyte_id = am.analyte_id
WHERE res.instrument_id = :instrument_id`

	query = strings.ReplaceAll(query, "%schema_name%", r.dbSchema)
	countQuery := query

	query += applyPagination(pageable, "req", "req.created_at DESC, req.id") + `;`

	preparedValues := map[string]interface{}{
		"instrument_id": instrumentID,
		//"time_from":     filter.TimeFrom.UTC(),
		//"limit":         filter.PageSize,
		//"offset":        filter.PageNumber * filter.PageSize,
	}

	//filterQuery := ``
	//
	//if filter.Filter != "" {
	//	namedValues["filter"] = filter.Filter + "%"
	//	filterQuery += " AND req.sample_code like :filter "
	//}
	//
	//if !filter.TimeFrom.IsZero() {
	//	filterQuery += " AND req.created_at >= :time_from "
	//}
	//
	//query += filterQuery
	//
	//if filter.Sort != "" {
	//	splitSort := strings.Split(filter.Sort, " ")
	//	sort := ""
	//	direction := ""
	//
	//	if strings.ToLower(splitSort[1]) == "desc" {
	//		direction = " desc"
	//	} else {
	//		direction = " asc"
	//	}
	//
	//	switch splitSort[0] {
	//	case "createdAt":
	//		sort = "req.created_at"
	//	case "sample_code":
	//		sort = "req.sample_code"
	//	case "testName":
	//		sort = "test_name"
	//	case "transmissionDate":
	//		sort = "res.batch_created_at"
	//	default:
	//		sort = "req.created_at"
	//	}
	//
	//	query += ` order by ` + sort + direction
	//} else {
	//	query += ` order by res.batch_created_at DESC `
	//}
	//
	//query += ` Limit :limit Offset :offset`

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
		log.Error().Msg("Failed to prepare named count query")
		return nil, 0, err
	}
	var count int
	if err = stmt.QueryRowx(preparedValues).Scan(&count); err != nil {
		log.Error().Msg("Failed to execute named count query")
		return nil, 0, err
	}

	return convertRequestInfoDAOsToRequestInfos(requestInfoList), count, nil
}

func (r *analysisRepository) GetSubjectsByAnalysisRequestIDs(ctx context.Context, analysisRequestIDs []uuid.UUID) (map[uuid.UUID]SubjectInfo, error) {
	subjectsMap := make(map[uuid.UUID]SubjectInfo)
	if len(analysisRequestIDs) == 0 {
		return subjectsMap, nil
	}
	query := fmt.Sprintf(`SELECT * FROM %s.sk_subject_infos WHERE analysis_request_id IN (?);`, r.dbSchema)
	query, args, _ := sqlx.In(query, analysisRequestIDs)
	query = r.db.Rebind(query)
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg("get subjects by analysis request ids failed")
		return subjectsMap, err
	}
	defer rows.Close()
	for rows.Next() {
		var subjectDao subjectInfoDAO
		err = rows.StructScan(&subjectDao)
		if err != nil {
			log.Error().Err(err).Msg("scan subject info failed")
			return map[uuid.UUID]SubjectInfo{}, err
		}
		subjectsMap[subjectDao.AnalysisRequestID] = convertSubjectDAOToSubjectInfo(subjectDao)
	}
	return subjectsMap, nil

}

func (r *analysisRepository) CreateAnalysisResultsBatch(ctx context.Context, analysisResults []AnalysisResult) ([]AnalysisResult, error) {
	if len(analysisResults) == 0 {
		return analysisResults, nil
	}
	for i := range analysisResults {
		if (analysisResults[i].ID == uuid.UUID{}) || (analysisResults[i].ID == uuid.Nil) {
			analysisResults[i].ID = uuid.New()
		}
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_analysis_results(id, analyte_mapping_id, instrument_id, sample_code, instrument_run_id, result_record_id, batch_id, "result", status, result_mode, yielded_at, valid_until, operator, technical_release_datetime, run_counter, edited, edit_reason)
		VALUES(:id, :analyte_mapping_id, :instrument_id, :sample_code, :instrument_run_id, :result_record_id, :batch_id, :result, :status, :result_mode, :yielded_at, :valid_until, :operator, :technical_release_datetime, :run_counter, :edited, :edit_reason);`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertAnalysisResultsToDAOs(analysisResults))
	if err != nil {
		log.Error().Err(err).Msg("create analysis result batch failed")
		return analysisResults, err
	}

	extraValuesMap := make(map[uuid.UUID][]ExtraValue)
	warningsMap := make(map[uuid.UUID][]string)
	reagentInfosMap := make(map[uuid.UUID][]ReagentInfo)
	imagesMap := make(map[uuid.UUID]map[uuid.NullUUID][]Image)

	for i := range analysisResults {
		extraValuesMap[analysisResults[i].ID] = analysisResults[i].ExtraValues
		warningsMap[analysisResults[i].ID] = analysisResults[i].Warnings
		reagentInfosMap[analysisResults[i].ID] = analysisResults[i].ReagentInfos
		imagesMap[analysisResults[i].ID] = make(map[uuid.NullUUID][]Image)
		imagesMap[analysisResults[i].ID][uuid.NullUUID{Valid: false}] = analysisResults[i].Images
	}

	err = r.createExtraValues(ctx, extraValuesMap)
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
			quantitativeChannelResultsMap[channelResultIDs[j]] = analysisResults[i].ChannelResults[j].QuantitativeResults
			channelImagesMap[uuid.NullUUID{UUID: channelResultIDs[j], Valid: true}] = analysisResults[i].ChannelResults[j].Images
		}
		imagesMap[analysisResults[i].ID] = channelImagesMap
		err = r.createChannelResultQuantitativeValues(ctx, quantitativeChannelResultsMap)
	}

	err = r.createWarnings(ctx, warningsMap)
	if err != nil {
		return analysisResults, err
	}

	err = r.createImages(ctx, imagesMap)
	if err != nil {
		return analysisResults, err
	}

	return analysisResults, nil
}

func (r *analysisRepository) GetAnalysisResultsBySampleCodeAndAnalyteID(ctx context.Context, sampleCode string, analyteID uuid.UUID) ([]AnalysisResult, error) {
	query := `SELECT sar.id, sar.analyte_mapping_id, sar.instrument_id, sar.sample_code, sar.instrument_run_id, sar.result_record_id, sar.batch_id, sar."result", sar.status, sar.result_mode, sar.yielded_at, sar.valid_until, sar.operator, sar.edited, sar.edit_reason,
					sam.id AS "analyte_mapping.id", sam.instrument_id AS "analyte_mapping.instrument_id", sam.instrument_analyte AS "analyte_mapping.instrument_analyte", sam.analyte_id AS "analyte_mapping.analyte_id", sam.result_type AS "analyte_mapping.result_type", sam.created_at AS "analyte_mapping.created_at", sam.modified_at AS "analyte_mapping.modified_at"
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
	for rows.Next() {
		result := analysisResultDAO{}
		err := rows.StructScan(&result)
		if err != nil {
			log.Error().Err(err).Msg("Can not scan data")
			return []AnalysisResult{}, err
		}

		analysisResultDAOs = append(analysisResultDAOs, result)
	}

	analysisResults := make([]AnalysisResult, len(analysisResultDAOs))
	for analysisResultIndex, dao := range analysisResultDAOs {
		extraValues, err := r.getExtraValues(ctx, dao.ID)
		if err != nil {
			return nil, err
		}
		dao.ExtraValues = extraValues

		reagentInfoList, err := r.getReagentInfoList(ctx, dao.ID)
		if err != nil {
			return nil, err
		}
		dao.ReagentInfos = reagentInfoList

		images, err := r.getImages(ctx, dao.ID)
		if err != nil {
			return nil, err
		}
		dao.Images = images

		warnings, err := r.getWarnings(ctx, dao.ID)
		if err != nil {
			return nil, err
		}
		dao.Warnings = warnings

		channelResults, err := r.getChannelResults(ctx, dao.ID)
		if err != nil {
			return nil, err
		}
		for i := range channelResults {
			channelResultImages, err := r.getChannelResultImages(ctx, dao.ID, channelResults[i].ID)
			if err != nil {
				return nil, err
			}
			channelResults[i].Images = channelResultImages

			quantitativeValues, err := r.getChannelResultQuantitativeValues(ctx, channelResults[i].ID)
			if err != nil {
				return nil, err
			}
			channelResults[i].QuantitativeResults = quantitativeValues
		}
		dao.ChannelResults = channelResults

		analysisResult := convertAnalysisResultDAOToAnalysisResult(dao)

		channelMappings, err := r.getChannelMappings(ctx, dao.AnalyteMappingID)
		if err != nil {
			return nil, err
		}
		analysisResult.AnalyteMapping.ChannelMappings = channelMappings

		resultMappings, err := r.getResultMappings(ctx, dao.AnalyteMappingID)
		if err != nil {
			return nil, err
		}
		analysisResult.AnalyteMapping.ResultMappings = resultMappings

		analysisResults[analysisResultIndex] = analysisResult
	}

	return analysisResults, err
}

func (r *analysisRepository) getChannelMappings(ctx context.Context, analyteMappingID uuid.UUID) ([]ChannelMapping, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_channel_mappings WHERE analyte_mapping_id = $1 AND deleted_at IS NULL;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query, analyteMappingID)
	if err != nil {
		log.Error().Err(err).Msg(msgGetChannelMappingsFailed)
		return nil, ErrGetChannelMappingsFailed
	}
	defer rows.Close()
	channelMappings := make([]ChannelMapping, 0)
	for rows.Next() {
		var dao channelMappingDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetChannelMappingsFailed)
			return nil, ErrGetChannelMappingsFailed
		}
		channelMappings = append(channelMappings, convertChannelMappingDaoToChannelMapping(dao))
	}
	return channelMappings, nil
}

func (r *analysisRepository) getResultMappings(ctx context.Context, analyteMappingID uuid.UUID) ([]ResultMapping, error) {
	query := fmt.Sprintf(`SELECT * FROM %s.sk_result_mappings WHERE analyte_mapping_id = $1 AND deleted_at IS NULL;`, r.dbSchema)
	rows, err := r.db.QueryxContext(ctx, query, analyteMappingID)
	if err != nil {
		log.Error().Err(err).Msg(msgGetResultMappingsFailed)
		return nil, ErrGetResultMappingsFailed
	}
	defer rows.Close()
	resultMappings := make([]ResultMapping, 0)
	for rows.Next() {
		var dao resultMappingDAO
		err = rows.StructScan(&dao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetResultMappingsFailed)
			return nil, ErrGetResultMappingsFailed
		}
		resultMappings = append(resultMappings, convertResultMappingDaoToChannelMapping(dao))
	}
	return resultMappings, nil
}

func (r *analysisRepository) getExtraValues(ctx context.Context, analysisResultID uuid.UUID) ([]extraValueDAO, error) {
	query := fmt.Sprintf(`SELECT sare.id, sare.analysis_result_id, sare."key", sare."value"
		FROM %s.sk_analysis_result_extravalues sare WHERE sare.analysis_result_id = $1;`, r.dbSchema)

	rows, err := r.db.QueryxContext(ctx, query, analysisResultID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Trace().Msg("No extra value")
			return []extraValueDAO{}, nil
		}
		log.Error().Err(err).Msg("Can not search for extra values")
		return []extraValueDAO{}, err
	}
	defer rows.Close()

	extraValues := make([]extraValueDAO, 0)
	for rows.Next() {
		extraValue := extraValueDAO{}
		err := rows.StructScan(&extraValue)
		if err != nil {
			log.Error().Err(err).Msg("Can not scan data")
			return []extraValueDAO{}, err
		}

		extraValues = append(extraValues, extraValue)
	}

	return extraValues, err
}

func (r *analysisRepository) createExtraValues(ctx context.Context, extraValuesByAnalysisRequestIDs map[uuid.UUID][]ExtraValue) error {
	extraValuesDAOs := make([]extraValueDAO, 0)
	for analysisResultID, extraValues := range extraValuesByAnalysisRequestIDs {
		evs := convertExtraValuesToDAOs(extraValues, analysisResultID)
		for i := range evs {
			evs[i].ID = uuid.New()
		}
		extraValuesDAOs = append(extraValuesDAOs, evs...)
	}
	if len(extraValuesDAOs) == 0 {
		return nil
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_analysis_result_extravalues(id, analysis_result_id, "key", "value")
		VALUES(:id, :analysis_result_id, :key, :value)`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, extraValuesDAOs)
	if err != nil {
		log.Error().Err(err).Msg("create analysis result batch failed")
		return err
	}
	return nil
}

func (r *analysisRepository) getChannelResults(ctx context.Context, analysisResultID uuid.UUID) ([]channelResultDAO, error) {
	query := fmt.Sprintf(`SELECT scr.id, scr.analysis_result_id, scr.channel_id, scr.qualitative_result, scr.qualitative_result_edited
		FROM %s.sk_channel_results scr WHERE scr.analysis_result_id = $1;`, r.dbSchema)

	rows, err := r.db.QueryxContext(ctx, query, analysisResultID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Trace().Msg("No channel result")
			return []channelResultDAO{}, nil
		}
		log.Error().Err(err).Msg("Can not search for channel results")
		return []channelResultDAO{}, err
	}
	defer rows.Close()

	channelResults := make([]channelResultDAO, 0)
	for rows.Next() {
		channelResult := channelResultDAO{}
		err := rows.StructScan(&channelResult)
		if err != nil {
			log.Error().Err(err).Msg("Can not scan data")
			return []channelResultDAO{}, err
		}

		channelResults = append(channelResults, channelResult)
	}

	return channelResults, err
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

func (r *analysisRepository) getChannelResultQuantitativeValues(ctx context.Context, channelResultID uuid.UUID) ([]quantitativeChannelResultDAO, error) {
	query := fmt.Sprintf(`SELECT scrqv.id, scrqv.channel_result_id, scrqv.metric, scrqv."value"
		FROM %s.sk_channel_result_quantitative_values scrqv WHERE scrqv.channel_result_id = $1;`, r.dbSchema)

	rows, err := r.db.QueryxContext(ctx, query, channelResultID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Trace().Msg("No quantitative channel result")
			return []quantitativeChannelResultDAO{}, nil
		}
		log.Error().Err(err).Msg("Can not search for quantitative channel results")
		return []quantitativeChannelResultDAO{}, err
	}
	defer rows.Close()

	quantitativeChannelResults := make([]quantitativeChannelResultDAO, 0)
	for rows.Next() {
		quantitativeChannelResult := quantitativeChannelResultDAO{}
		err := rows.StructScan(&quantitativeChannelResult)
		if err != nil {
			log.Error().Err(err).Msg("Can not scan data")
			return []quantitativeChannelResultDAO{}, err
		}

		quantitativeChannelResults = append(quantitativeChannelResults, quantitativeChannelResult)
	}

	return quantitativeChannelResults, err
}

func (r *analysisRepository) createChannelResultQuantitativeValues(ctx context.Context, quantitativeValuesByChannelResultIDs map[uuid.UUID]map[string]string) error {
	quantitativeChannelResultDAOs := make([]quantitativeChannelResultDAO, 0)
	for channelResultID, quantitativeValues := range quantitativeValuesByChannelResultIDs {
		qrDAOs := convertQuantitativeResultsToDAOs(quantitativeValues, channelResultID)
		for i := range qrDAOs {
			qrDAOs[i].ID = uuid.New()
		}
		quantitativeChannelResultDAOs = append(quantitativeChannelResultDAOs, qrDAOs...)
	}
	if len(quantitativeChannelResultDAOs) == 0 {
		return nil
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_channel_result_quantitative_results(id, channel_result_id, metric, "value")
		VALUES(:id, :channel_result_id, :metric, :value);`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, quantitativeChannelResultDAOs)
	if err != nil {
		log.Error().Err(err).Msg("create channel result quantitative values batch failed")
		return err
	}
	return nil
}

func (r *analysisRepository) getReagentInfoList(ctx context.Context, analysisResultID uuid.UUID) ([]reagentInfoDAO, error) {
	query := fmt.Sprintf(`SELECT sarri.id, sarri.analysis_result_id, sarri.serial, sarri."name", sarri.code, sarri.shelf_life, sarri.lot_no, sarri.manufacturer_name, sarri.reagent_manufacturer_date,
        sarri.reagent_type, sarri.use_until, sarri.date_created
		FROM %s.sk_analysis_result_reagent_infos sarri WHERE sarri.analysis_result_id = $1;`, r.dbSchema)

	rows, err := r.db.QueryxContext(ctx, query, analysisResultID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Trace().Msg("No reagent info")
			return []reagentInfoDAO{}, nil
		}
		log.Error().Err(err).Msg("Can not search for reagent info")
		return []reagentInfoDAO{}, err
	}
	defer rows.Close()

	reagentInfoList := make([]reagentInfoDAO, 0)
	for rows.Next() {
		reagentInfo := reagentInfoDAO{}
		err := rows.StructScan(&reagentInfo)
		if err != nil {
			log.Error().Err(err).Msg("Can not scan data")
			return []reagentInfoDAO{}, err
		}

		reagentInfoList = append(reagentInfoList, reagentInfo)
	}

	return reagentInfoList, err
}

func (r *analysisRepository) createReagentInfos(ctx context.Context, reagentInfosByAnalysisResultID map[uuid.UUID][]ReagentInfo) error {
	reagentInfoDAOs := make([]reagentInfoDAO, 0)
	for analysisResultID, reagentInfos := range reagentInfosByAnalysisResultID {
		ridaos := convertReagentInfosToDAOs(reagentInfos, analysisResultID)
		for i := range ridaos {
			ridaos[i].ID = uuid.New()
		}
		reagentInfoDAOs = append(reagentInfoDAOs, ridaos...)
	}
	if len(reagentInfoDAOs) == 0 {
		return nil
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_analysis_result_reagent_info(id, analysis_result_id, serial, name, code, shelfLife, lot_no, manufacturer_name, reagent_manufacturer_date, reagent_type, use_until, date_created)
		VALUES(:id, :analysis_result_id, :serial, :name, :code, :shelfLife, :lot_no, :manufacturer_name, :reagent_manufacturer_date, :reagent_type, :use_until, :date_created)`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, reagentInfoDAOs)
	if err != nil {
		log.Error().Err(err).Msg("create analysis result reagent infos failed")
		return err
	}
	return nil
}

func (r *analysisRepository) getImages(ctx context.Context, analysisResultID uuid.UUID) ([]imageDAO, error) {
	query := fmt.Sprintf(`SELECT sari.id, sari.analysis_result_id, sari.channel_result_id, sari.name, sari.description FROM %s.sk_analysis_result_images sari WHERE sari.analysis_result_id = $1;`, r.dbSchema)

	rows, err := r.db.QueryxContext(ctx, query, analysisResultID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Trace().Msg("No images")
			return []imageDAO{}, nil
		}
		log.Error().Err(err).Msg("Can not search for Images")
		return []imageDAO{}, err
	}
	defer rows.Close()

	images := make([]imageDAO, 0)
	for rows.Next() {
		image := imageDAO{}
		err := rows.StructScan(&image)
		if err != nil {
			log.Error().Err(err).Msg("Can not scan data")
			return []imageDAO{}, err
		}

		images = append(images, image)
	}

	return images, err
}

func (r *analysisRepository) getChannelResultImages(ctx context.Context, analysisResultID uuid.UUID, channelResultID uuid.UUID) ([]imageDAO, error) {
	query := fmt.Sprintf(`SELECT sari.id, sari.analysis_result_id, sari.channel_result_id, sari.name, sari.description
		FROM %s.sk_analysis_result_images sari WHERE sari.analysis_result_id = $1 AND sari.channel_result_id = $2;`, r.dbSchema)

	rows, err := r.db.QueryxContext(ctx, query, analysisResultID, channelResultID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Trace().Msg("No images")
			return []imageDAO{}, nil
		}
		log.Error().Err(err).Msg("Can not search for Images")
		return []imageDAO{}, err
	}
	defer rows.Close()

	images := make([]imageDAO, 0)
	for rows.Next() {
		image := imageDAO{}
		err := rows.StructScan(&image)
		if err != nil {
			log.Error().Err(err).Msg("Can not scan data")
			return []imageDAO{}, err
		}

		images = append(images, image)
	}

	return images, err
}

func (r *analysisRepository) createImages(ctx context.Context, images map[uuid.UUID]map[uuid.NullUUID][]Image) error {
	imageDAOs := make([]imageDAO, 0)
	for analysisResultID, imagesMap := range images {
		for channelResultID, images := range imagesMap {
			idaos := convertImagesToDAOs(images, analysisResultID, channelResultID)
			for i := range idaos {
				idaos[i].ID = uuid.New()
			}
			imageDAOs = append(imageDAOs, idaos...)
		}
	}
	if len(imageDAOs) == 0 {
		return nil
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_analysis_result_extravalues(id, analysis_result_id, "key", "value")
		VALUES(:id, :analysis_result_id, :key, :value)`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, imageDAOs)
	if err != nil {
		log.Error().Err(err).Msg("create analysis result images failed")
		return err
	}
	return nil
}

func (r *analysisRepository) getWarnings(ctx context.Context, analysisResultID uuid.UUID) ([]warningDAO, error) {
	query := fmt.Sprintf(`SELECT sarw.id, sarw.analysis_result_id, sarw.warning FROM %s.sk_analysis_result_warnings sarw WHERE sarw.analysis_result_id = $1;`, r.dbSchema)

	rows, err := r.db.QueryxContext(ctx, query, analysisResultID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Trace().Msg("No analysis result warnings")
			return []warningDAO{}, nil
		}
		log.Error().Err(err).Msg("Can not search for AnalysisResultWarnings")
		return []warningDAO{}, err
	}
	defer rows.Close()

	warnings := make([]warningDAO, 0)
	for rows.Next() {
		warning := warningDAO{}
		err := rows.StructScan(&warning)
		if err != nil {
			log.Error().Err(err).Msg("Can not scan data")
			return []warningDAO{}, err
		}

		warnings = append(warnings, warning)
	}

	return warnings, err
}

func (r *analysisRepository) createWarnings(ctx context.Context, warningsByAnalysisResultID map[uuid.UUID][]string) error {
	warningDAOs := make([]warningDAO, 0)
	for analysisResultID, warnings := range warningsByAnalysisResultID {
		wdaos := convertWarningsToDAOs(warnings, analysisResultID)
		for i := range wdaos {
			wdaos[i].ID = uuid.New()
		}
		warningDAOs = append(warningDAOs, wdaos...)
	}
	if len(warningDAOs) == 0 {
		return nil
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_analysis_result_warnings(id, analysis_result_id, warning)
		VALUES(:id, :analysis_result_id, :warning)`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, warningDAOs)
	if err != nil {
		log.Error().Err(err).Msg("create analysis result warnings failed")
		return err
	}
	return nil
}

func (r *analysisRepository) CreateAnalysisResultQueueItem(ctx context.Context, analysisResults []AnalysisResult) (uuid.UUID, error) {
	if len(analysisResults) < 1 {
		return uuid.Nil, nil
	}

	jsonData, err := json.Marshal(analysisResults)
	if err != nil {
		return uuid.Nil, nil
	}

	cerberusQueueItem := cerberusQueueItemDAO{
		ID:          uuid.New(),
		JsonMessage: string(jsonData),
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_cerberus_queue_items(queue_item_id, json_message) VALUES (:queue_item_id, :json_message);`, r.dbSchema)
	_, err = r.db.NamedExecContext(ctx, query, cerberusQueueItem)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create cerberus queue item")
		return uuid.Nil, nil
	}

	return cerberusQueueItem.ID, nil
}

func (r *analysisRepository) GetAnalysisResultQueueItems(ctx context.Context) ([]CerberusQueueItem, error) {
	query := fmt.Sprintf(`SELECT queue_item_id, json_message, last_http_status, last_error, last_error_at, trial_count, retry_not_before, created_at FROM %s.sk_cerberus_queue_items 
			WHERE trial_count < 5760 /* 4 days á 2 minutes */ AND last_http_status NOT BETWEEN 200 AND 299 AND created_at > timezone('utc', now()-interval '14 days') AND retry_not_before < timezone('utc', now())
			ORDER BY created_at LIMIT 1;`, r.dbSchema)

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

func (r *analysisRepository) CreateTransaction() (db.DbConnector, error) {
	return r.db.CreateTransactionConnector()
}

func (r *analysisRepository) WithTransaction(tx db.DbConnector) AnalysisRepository {
	txRepo := *r
	txRepo.db = tx
	return &txRepo
}

func convertAnalysisRequestsToDAOs(analysisRequests []AnalysisRequest) []analysisRequestDAO {
	analysisRequestDAOs := make([]analysisRequestDAO, len(analysisRequests))
	for i := range analysisRequestDAOs {
		analysisRequestDAOs[i] = convertAnalysisRequestToDAO(analysisRequests[i])
	}
	return analysisRequestDAOs
}

func convertAnalysisRequestToDAO(analysisRequest AnalysisRequest) analysisRequestDAO {
	return analysisRequestDAO{
		ID:             analysisRequest.ID,
		WorkItemID:     analysisRequest.WorkItemID,
		AnalyteID:      analysisRequest.AnalyteID,
		SampleCode:     analysisRequest.SampleCode,
		MaterialID:     analysisRequest.MaterialID,
		LaboratoryID:   analysisRequest.LaboratoryID,
		ValidUntilTime: analysisRequest.ValidUntilTime,
		CreatedAt:      analysisRequest.CreatedAt,
	}
}

func convertAnalysisRequestDAOsToAnalysisRequests(analysisRequestDAOs []analysisRequestDAO) []AnalysisRequest {
	analysisRequests := make([]AnalysisRequest, len(analysisRequestDAOs))
	for i := range analysisRequestDAOs {
		analysisRequests[i] = convertAnalysisRequestDAOToAnalysisRequest(analysisRequestDAOs[i])
	}
	return analysisRequests
}

func convertAnalysisResultToDAO(analysisResult AnalysisResult) analysisResultDAO {
	return analysisResultDAO{
		ID:                       analysisResult.ID,
		AnalyteMappingID:         analysisResult.AnalyteMapping.ID,
		InstrumentID:             analysisResult.Instrument.ID,
		ResultMode:               analysisResult.ResultMode,
		SampleCode:               analysisResult.SampleCode,
		InstrumentRunID:          analysisResult.InstrumentRunID,
		ResultRecordID:           analysisResult.ResultRecordID,
		BatchID:                  analysisResult.BatchID,
		Result:                   analysisResult.Result,
		Status:                   analysisResult.Status,
		YieldedAt:                analysisResult.ResultYieldDateTime,
		ValidUntil:               analysisResult.ValidUntil,
		Operator:                 analysisResult.Operator,
		TechnicalReleaseDateTime: analysisResult.TechnicalReleaseDateTime,
		Edited:                   analysisResult.Edited,
		EditReason: sql.NullString{
			String: analysisResult.EditReason,
			Valid:  true,
		},
	}
}

func convertAnalysisResultsToDAOs(analysisResults []AnalysisResult) []analysisResultDAO {
	DAOs := make([]analysisResultDAO, len(analysisResults))
	for i := range analysisResults {
		DAOs[i] = convertAnalysisResultToDAO(analysisResults[i])
	}
	return DAOs
}

func convertAnalysisResultDAOToAnalysisResult(analysisResult analysisResultDAO) AnalysisResult {
	return AnalysisResult{
		ID:             analysisResult.ID,
		AnalyteMapping: convertAnalyteMappingDaoToAnalyteMapping(analysisResult.AnalyteMapping),
		Instrument: Instrument{
			ID: analysisResult.InstrumentID,
		},
		SampleCode:               analysisResult.SampleCode,
		ResultRecordID:           analysisResult.ResultRecordID,
		BatchID:                  analysisResult.BatchID,
		Result:                   analysisResult.Result,
		ResultMode:               analysisResult.ResultMode,
		Status:                   analysisResult.Status,
		ResultYieldDateTime:      analysisResult.YieldedAt,
		ValidUntil:               analysisResult.ValidUntil,
		Operator:                 analysisResult.Operator,
		TechnicalReleaseDateTime: analysisResult.TechnicalReleaseDateTime,
		InstrumentRunID:          analysisResult.InstrumentRunID,
		RunCounter:               analysisResult.RunCounter,
		Edited:                   analysisResult.Edited,
		EditReason:               nullStringToString(analysisResult.EditReason),
		Warnings:                 convertAnalysisWarningDAOsToWarnings(analysisResult.Warnings),
		ChannelResults:           convertChannelResultDAOsToChannelResults(analysisResult.ChannelResults),
		ExtraValues:              convertExtraValueDAOsToExtraValues(analysisResult.ExtraValues),
		ReagentInfos:             convertReagentInfoDAOsToReagentInfoList(analysisResult.ReagentInfos),
		Images:                   convertImageDAOsToImages(analysisResult.Images),
	}
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

func convertExtraValueToDAO(extraValue ExtraValue, analysisResultID uuid.UUID) extraValueDAO {
	return extraValueDAO{
		AnalysisResultID: analysisResultID,
		Key:              extraValue.Key,
		Value:            extraValue.Value,
	}
}

func convertExtraValuesToDAOs(extraValues []ExtraValue, analysisResultID uuid.UUID) []extraValueDAO {
	extraValueDAOs := make([]extraValueDAO, len(extraValues))
	for i := range extraValues {
		extraValueDAOs[i] = convertExtraValueToDAO(extraValues[i], analysisResultID)
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
	return AnalysisRequest{
		ID:             analysisRequest.ID,
		WorkItemID:     analysisRequest.WorkItemID,
		AnalyteID:      analysisRequest.AnalyteID,
		SampleCode:     analysisRequest.SampleCode,
		MaterialID:     analysisRequest.MaterialID,
		LaboratoryID:   analysisRequest.LaboratoryID,
		ValidUntilTime: analysisRequest.ValidUntilTime,
		CreatedAt:      analysisRequest.CreatedAt,
	}
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
		TestResult:        nullStringToStringPointer(analysisRequestInfoDAO.TestName),
		BatchCreatedAt:    nullTimeToTimePointer(analysisRequestInfoDAO.BatchCreatedAt),
		Status:            AnalysisRequestStatusOpen,
		SentToCerberusAt:  nullTimeToTimePointer(analysisRequestInfoDAO.SentToCerberusAt),
		SourceIP:          nullStringToString(analysisRequestInfoDAO.SourceIP),
		InstrumentID:      nullUUIDToUUIDPointer(analysisRequestInfoDAO.InstrumentID),
	}

	// Todo - Check conditions
	if analysisRequestInfoDAO.SentToCerberusAt.Valid && !analysisRequestInfoDAO.SentToCerberusAt.Time.IsZero() {
		analysisRequestInfo.Status = AnalysisRequestStatusSent
	}

	if !analysisRequestInfoDAO.AnalyteMappingsID.Valid {
		analysisRequestInfo.MappingError = true
	}

	return analysisRequestInfo
}

func convertRequestInfoDAOsToRequestInfos(analysisRequestInfoDAOs []analysisRequestInfoDAO) []AnalysisRequestInfo {
	analysisRequestInfo := make([]AnalysisRequestInfo, len(analysisRequestInfoDAOs))
	for i := range analysisRequestInfoDAOs {
		analysisRequestInfo[i] = convertRequestInfoDAOToRequestInfo(analysisRequestInfoDAOs[i])
	}
	return analysisRequestInfo
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

func convertExtraValueDAOsToExtraValues(extraValueDAOs []extraValueDAO) []ExtraValue {
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
		}
	}
	return images
}
