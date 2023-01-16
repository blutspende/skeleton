package skeleton

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/db"
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
	ID                uuid.UUID      `db:"id"`
	AnalysisRequestID uuid.UUID      `db:"analysis_request_id"`
	AnalyteMappingID  uuid.UUID      `db:"analyte_mapping_id"`
	InstrumentID      uuid.UUID      `db:"instrument_id"`
	InstrumentRunID   uuid.UUID      `db:"instrument_run_id"`
	ResultRecordID    uuid.UUID      `db:"result_record_id"`
	BatchID           uuid.UUID      `db:"batch_id"`
	Result            string         `db:"result"`
	Status            ResultStatus   `db:"status"`
	ResultMode        ResultMode     `db:"mode"`
	YieldedAt         time.Time      `db:"yielded_at"`
	ValidUntil        time.Time      `db:"valid_until"`
	Operator          string         `db:"operator"`
	Edited            bool           `db:"edited"`
	EditReason        sql.NullString `db:"edit_reason"`
	Error             sql.NullString `db:"error"`
	ErrorTimestamp    sql.NullTime   `db:"error_timestamp"`
	RetryCount        int            `db:"retry_count"`
	SentToCerberusAt  sql.NullTime   `db:"sent_to_cerberus_at"`
	ChannelResults    []channelResultDAO
	ExtraValues       []extraValueDAO
	ReagentInfos      []reagentInfoDAO
	Images            []imageDAO
	Warnings          []warningDAO
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

type AnalysisRepository interface {
	CreateAnalysisRequestsBatch(ctx context.Context, analysisRequests []AnalysisRequest) ([]uuid.UUID, error)
	GetAnalysisRequestsBySampleCodes(ctx context.Context, sampleCodes []string) (map[string][]AnalysisRequest, error)
	CreateSubjectsBatch(ctx context.Context, subjectInfosByAnalysisRequestID map[uuid.UUID]SubjectInfo) (map[uuid.UUID]uuid.UUID, error)
	GetSubjectsByAnalysisRequestIDs(ctx context.Context, analysisRequestIDs []uuid.UUID) (map[uuid.UUID]SubjectInfo, error)

	CreateAnalysisResultsBatch(ctx context.Context, analysisResults []AnalysisResult) ([]uuid.UUID, error)
	UpdateResultTransmissionData(ctx context.Context, analysisResultID uuid.UUID, success bool, errorMessage string) error

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

func (r *analysisRepository) CreateAnalysisRequestsBatch(ctx context.Context, analysisRequests []AnalysisRequest) ([]uuid.UUID, error) {
	if len(analysisRequests) == 0 {
		return []uuid.UUID{}, nil
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_analysis_requests(id, work_item_id, analyte_id, sample_code, material_id, laboratory_id, valid_until_time)
				VALUES(:id, :work_item_id, :analyte_id, :sample_code, :material_id, :laboratory_id, :valid_until_time) 
				ON CONFLICT (work_item_id) DO 
				UPDATE SET analyte_id = :analyte_id, sample_code = :sample_code, material_id = :material_id, laboratory_id = :laboratory_id, valid_until_time = :valid_until_time;`, r.dbSchema)
	ids := make([]uuid.UUID, len(analysisRequests))
	_, err := r.db.NamedExecContext(ctx, query, convertAnalysisRequestsToDAOs(analysisRequests))
	if err != nil {
		log.Error().Err(err).Msg("Can not create RequestData")
		return []uuid.UUID{}, err
	}

	return ids, nil
}

func (r *analysisRepository) CreateSubjectsBatch(ctx context.Context, subjectInfosByAnalysisRequestID map[uuid.UUID]SubjectInfo) (map[uuid.UUID]uuid.UUID, error) {
	idsMap := make(map[uuid.UUID]uuid.UUID)
	if len(subjectInfosByAnalysisRequestID) == 0 {
		return idsMap, nil
	}
	subjectDAOs := make([]subjectInfoDAO, len(subjectInfosByAnalysisRequestID))
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

func (r *analysisRepository) GetAnalysisRequestsBySampleCodes(ctx context.Context, sampleCodes []string) (map[string][]AnalysisRequest, error) {
	analysisRequestsBySampleCodes := make(map[string][]AnalysisRequest)
	if len(sampleCodes) == 0 {
		return analysisRequestsBySampleCodes, nil
	}
	query := fmt.Sprintf(`SELECT * FROM %s.sk_analysis_requests 
					WHERE sample_code in (?)
					AND valid_until_time <= timezone('utc',now());`, r.dbSchema)

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

func (r *analysisRepository) CreateAnalysisResultsBatch(ctx context.Context, analysisResults []AnalysisResult) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, len(analysisResults))
	if len(analysisResults) == 0 {
		return ids, nil
	}
	for i := range analysisResults {
		if (analysisResults[i].ID == uuid.UUID{}) || (analysisResults[i].ID == uuid.Nil) {
			analysisResults[i].ID = uuid.New()
		}
	}
	query := fmt.Sprintf(`INSERT INTO %s.sk_analysis_results(id, analysis_request_id, analyte_mapping_id, instrument_id, instrument_run_id, result_record_id, batch_id, "result", status, mode, yielded_at, valid_until, operator, edited, edit_reason)
		VALUES(:id, :analysis_request_id, :analyte_mapping_id, :instrument_id, :instrument_run_id, :result_record_id, :batch_id, :result, :status, :mode, :yielded_at, :valid_until, :operator, :edited, :edit_reason)`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertAnalysisResultsToDAOs(analysisResults))
	if err != nil {
		log.Error().Err(err).Msg("create analysis result batch failed")
		return []uuid.UUID{}, err
	}

	extraValuesMap := make(map[uuid.UUID][]ExtraValue)
	warningsMap := make(map[uuid.UUID][]string)
	reagentInfosMap := make(map[uuid.UUID][]ReagentInfo)
	imagesMap := make(map[uuid.UUID]map[uuid.NullUUID][]Image)

	for i := range analysisResults {
		extraValuesMap[ids[i]] = analysisResults[i].ExtraValues
		warningsMap[ids[i]] = analysisResults[i].Warnings
		reagentInfosMap[ids[i]] = analysisResults[i].ReagentInfos
		imagesMap[ids[i]] = make(map[uuid.NullUUID][]Image)
		imagesMap[ids[i]][uuid.NullUUID{Valid: false}] = analysisResults[i].Images
	}

	err = r.createExtraValues(ctx, extraValuesMap)
	if err != nil {
		return []uuid.UUID{}, err
	}

	for i := range analysisResults {
		quantitativeChannelResultsMap := make(map[uuid.UUID]map[string]string)
		channelImagesMap := make(map[uuid.NullUUID][]Image)
		channelResultIDs, err := r.createChannelResults(ctx, analysisResults[i].ChannelResults, ids[i])
		if err != nil {
			return []uuid.UUID{}, err
		}
		for j := range analysisResults[i].ChannelResults {
			quantitativeChannelResultsMap[channelResultIDs[j]] = analysisResults[i].ChannelResults[j].QuantitativeResults
			channelImagesMap[uuid.NullUUID{UUID: channelResultIDs[j], Valid: true}] = analysisResults[i].ChannelResults[j].Images
		}
		imagesMap[ids[i]] = channelImagesMap
		err = r.createChannelResultQuantitativeValues(ctx, quantitativeChannelResultsMap)
	}

	err = r.createWarnings(ctx, warningsMap)
	if err != nil {
		return []uuid.UUID{}, err
	}

	err = r.createImages(ctx, imagesMap)
	if err != nil {
		return []uuid.UUID{}, err
	}

	return ids, nil
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

func (r *analysisRepository) createChannelResults(ctx context.Context, channelResults []ChannelResult, analysisResultID uuid.UUID) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, len(channelResults))
	if len(channelResults) == 0 {
		return ids, nil
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

func (r *analysisRepository) createChannelResultQuantitativeValues(ctx context.Context, quantitativeValuesByChannelResultIDs map[uuid.UUID]map[string]string) error {
	quantitativeChannelResultDAOs := make([]quantitativeChannelResultDAO, 0)
	for channelResultID, quantitativeValues := range quantitativeValuesByChannelResultIDs {
		qrDAOs := convertQuantiativeResultsToDAOs(quantitativeValues, channelResultID)
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

func (r *analysisRepository) UpdateResultTransmissionData(ctx context.Context, analysisResultID uuid.UUID, success bool, errorMessage string) error {
	var query string
	var err error
	if success {
		query = fmt.Sprintf(`UPDATE %s.sk_analysis_results SET sent_to_cerberus_at = timezone('utc',now()) WHERE id = $1;`, r.dbSchema)
		_, err = r.db.ExecContext(ctx, query, analysisResultID)
	} else {
		query = fmt.Sprintf(`UPDATE %s.sk_analysis_results SET retry_count = retry_count + 1, error_message = $2 WHERE id = $1;`, r.dbSchema)
		_, err = r.db.ExecContext(ctx, query, analysisResultID, errorMessage)
	}
	if err != nil {
		log.Error().
			Interface("analysisResultID", analysisResultID).
			Bool("success", success).
			Str("errorMessage", errorMessage).
			Err(err).
			Msg("update result transmission status failed")
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
		ID:                analysisResult.ID,
		AnalysisRequestID: analysisResult.AnalysisRequest.ID,
		AnalyteMappingID:  analysisResult.AnalyteMapping.ID,
		InstrumentID:      analysisResult.Instrument.ID,
		ResultMode:        analysisResult.Instrument.ResultMode,
		InstrumentRunID:   analysisResult.InstrumentRunID,
		ResultRecordID:    analysisResult.ResultRecordID,
		BatchID:           analysisResult.BatchID,
		Result:            analysisResult.Result,
		Status:            analysisResult.Status,
		YieldedAt:         analysisResult.ResultYieldDateTime,
		ValidUntil:        analysisResult.ValidUntil,
		Operator:          analysisResult.Operator,
		Edited:            analysisResult.Edited,
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

func convertQuantiativeResultsToDAOs(quantitativeResults map[string]string, channelResultID uuid.UUID) []quantitativeChannelResultDAO {
	DAOs := make([]quantitativeChannelResultDAO, len(quantitativeResults))
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
		warningDAOs = append(warningDAOs, warningDAO{
			AnalysisResultID: analysisResultID,
			Warning:          warnings[i],
		})
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
