package skeleton

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/blutspende/bloodlab-common/utils"

	"errors"

	"github.com/blutspende/skeleton/db"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
)

const (
	msgRevokeAnalysisRequestsFailed                                  = "Revoke analysis requests failed"
	msgSaveAnalysisRequestsInstrumentTransmissions                   = "Save analysis requests' instrument transmissions"
	msgIncreaseAnalysisRequestsSentToInstrumentCountFailed           = "Increase analysis requests sent to instrument count failed"
	msgInvalidResultStatus                                           = "Invalid result status"
	msgConvertAnalysisResultsFailed                                  = "Convert analysis results"
	msgMarshalAnalysisResultsFailed                                  = "Marshal analysis results failed"
	msgCreateAnalysisRequestsBatchFailed                             = "Create analysis requests batch failed"
	msgGetSampleCodesByOrderIDFailed                                 = "Get sample codes by order ID failed"
	msgCreateSubjectsFailed                                          = "Create subject failed"
	msgCreateReagentFailed                                           = "Create analysis result reagent infos failed"
	msgCreateWarningsFailed                                          = "Create warnings failed"
	msgCreateControlResultWarningsFailed                             = "Create control result warnings failed"
	msgSaveDEAImageIDFailed                                          = "Save DEA image id failed"
	msgIncreaseImageUploadRetryCountFailed                           = "Increase image upload retry count failed"
	msgGetStuckImagesFailed                                          = "Get stuck images failed"
	msgScanStuckImageIdFailed                                        = "Scan stuck image id failed"
	msgScanStuckImageDataFailed                                      = "Scan stuck image data failed"
	msgMarkImagesAsSyncedToCerberusFailed                            = "Mark images as synced to cerberus failed"
	msgMarkAnalysisRequestsAsProcessedFailed                         = "Mark analysis requests as processed failed"
	msgMarkAnalysisResultsAsProcessedFailed                          = "Mark analysis results as processed failed"
	msgGetAnalysisRequestExtraValuesFailed                           = "Get analysis request extra values failed"
	msgDeleteAnalysisRequestExtraValuesFailed                        = "Delete analysis request extra values failed"
	msgCreateControlResultFailed                                     = "Create control result failed"
	msgUpdateControlResultFailed                                     = "Update control result failed"
	msgCreateAnalysisResultReagentRelationsFailed                    = "Create analysis result reagent relations failed"
	msgCreateReagentControlResultRelationsFailed                     = "Create reagent control result relations failed"
	msgCreateAnalysisResultControlResultRelationsFailed              = "Create analysis result control result relations failed"
	msgMarkAnalysisResultControlResultRelationAsProcessedFailed      = "Mark analysis result control result relation as processed failed"
	msgSaveCerberusIdForAnalysisResultFailed                         = "Save cerberus id for analysis result failed"
	msgGetControlResultsByIDsFailed                                  = "Get control results by ids failed"
	msgGetReagentsByIDsFailed                                        = "Get reagents by ids failed"
	msgLoadAnalysisResultIdsWithoutControlByReagentFailed            = "Load analysis result ids without control result by reagent failed"
	msgLoadAnalysisResultIdsWhereLatestControlResultIsInValidFailed  = "Load analysis result ids where latest control result is invalid failed"
	msgLoadLatestControlResultIdFailed                               = "Load latest control result id by reagent failed"
	msgLoadNotValidatedControlResultsFailed                          = "Load not validated control results failed"
	msgGetAnalysisResultIdsForStatusRecalculationByControlIdsFailed  = "Get analysis result ids for status recalculation by control ids failed"
	msgTooManyAnalysisResultsFromGatheringConnectedData              = "Too many analysis results from gathering connected data"
	msgGetAnalysisResultsFailed                                      = "Get analysis results failed"
	msgGetAnalysisRequestsFailed                                     = "Get analysis requests failed"
	msgGetUnprocessedAnalysisRequestsFailed                          = "Get unprocessed analysis requests failed"
	msgGetSubjectInfosFailed                                         = "Get subject infos failed"
	msgCreateAnalysisResultBatchFailed                               = "Create analysis result batch failed"
	msgUpdateAnalysisResultStatusBatchFailed                         = "Update analysis result status batch failed"
	msgGetAnalysisResultExtraValuesFailed                            = "Get analysis result extra values failed"
	msgGetAnalysisResultReagentRelationsFailed                       = "Get analysis result reagent relations failed"
	msgGetAnalysisResultControlResultRelationsFailed                 = "Get analysis result control result relations failed"
	msgCreateControlResultChannelResultQuantitativeValuesBatchFailed = "Create control result channel result quantitative values batch failed"
	msgGetReagentControlResultRelationsFailed                        = "Get reagent control result relations failed"
	msgGetImagesFailed                                               = "Get images failed"
	msgGetAnalysisResultWarningsFailed                               = "Get analysis result warnings failed"
	msgGetAnalysisResultChannelResultsFailed                         = "Get analysis result channel results failed"
	msgGetChannelResultQuantitativeValuesFailed                      = "Get channel result quantitative values failed"
	msgCreateAnalysisResultExtraValuesFailed                         = "Create analysis result extra values failed"
	msgCreateAnalysisRequestExtraValuesFailed                        = "Create analysis request extra values failed"
	msgCreateControlResultExtraValuesFailed                          = "Create control result extra values failed"
	msgCreateChannelResultBatchFailed                                = "Create channel result batch failed"
	msgCreateControlResultChannelResultBatchFailed                   = "Create control result channel result batch failed"
	msgCreateControlResultQuantitativeValuesBatchFailed              = "Create channel result quantitative values batch failed"
	msgCreateAnalysisResultImagesFailed                              = "Create analysis result images failed"
	msgCreateControlResultImagesFailed                               = "Create control result images failed"
	msgCreateCerberusQueueItemFailed                                 = "Create cerberus queue item failed"
	msgDeleteOldCerberusQueueItemsFailed                             = "Delete old cerberus queue items failed"
	msgDeleteOldAnalysisRequestsFailed                               = "Delete old analysis requests failed"
	msgDeleteAnalysisRequestExtraValuesByAnalysisRequestIdFailed     = "Delete analysis request extra values by analysis request IDs failed"
	msgDeleteSubjectInfosByAnalysisRequestIdFailed                   = "Delete subject infos by analysis request IDs failed"
	msgDeleteAnalysisRequestInstrumentTransmissionsFailed            = "Delete analysis request instrument transmissions by analysis request IDs failed"
	msgDeleteAppliedSortingRuleTargetsBySampleCodesFailed            = "Delete applied sorting rule targets by sample codes failed"
	msgDeleteOldAnalysisResultsFailed                                = "Delete old analysis results failed"
	msgDeleteChannelResultsByAnalysisResultIdsFailed                 = "Delete channel results by analysis result IDs failed"
	msgDeleteAnalysisResultWarningsFailed                            = "Delete analysis result warnings failed"
	msgDeleteImagesByAnalysisResultIdsFailed                         = "Delete images by analysis result IDs failed"
	msgDeleteAnalysisResultExtraValuesByAnalysisResultIdsFailed      = "Delete analysis result extra values by analysis result IDs failed"
	msgDeleteReagentInfosByAnalysisResultIdsFailed                   = "Delete reagent infos by analysis result IDs failed"
	msgGetCerberusQueueItemsFailed                                   = "Get cerberus queue items failed"
	msgUpdateCerberusQueueItemStatusFailed                           = "Update cerberus queue item status failed"
	msgUnprocessedAnalysisResultIdsFailed                            = "Get unprocessed analysis result ids failed"
	msgGetControlResultImagesFailed                                  = "Get control result images failed"
	msgGetControlResultQuantitativeChannelResultsFailed              = "Get control result quantitative channel results failed"
	msgGetControlResultChannelResultsFailed                          = "Get control result channel results failed"
	msgGetControlResultWarningsFailed                                = "Get control result warnings failed"
	msgGetControlResultExtraValuesFailed                             = "Get control result extra values failed"
	msgAnalyteMappingNotFound                                        = "Analyte Mapping not found"
	msgUpdateAnalysisResultDEARawMessageIDFailed                     = "update analysis result DEARawMessageID failed"
	msgMissingDEARawMessageID                                        = "missing DEA raw message ID"
)

var (
	ErrRevokeAnalysisRequestsFailed                                  = errors.New(msgRevokeAnalysisRequestsFailed)
	ErrSaveAnalysisRequestsInstrumentTransmissions                   = errors.New(msgSaveAnalysisRequestsInstrumentTransmissions)
	ErrIncreaseAnalysisRequestsSentToInstrumentCountFailed           = errors.New(msgIncreaseAnalysisRequestsSentToInstrumentCountFailed)
	ErrInvalidResultStatus                                           = errors.New(msgInvalidResultStatus)
	ErrConvertAnalysisResultsFailed                                  = errors.New(msgConvertAnalysisResultsFailed)
	ErrMarshalAnalysisResultsFailed                                  = errors.New(msgMarshalAnalysisResultsFailed)
	ErrCreateAnalysisRequestsBatchFailed                             = errors.New(msgCreateAnalysisRequestsBatchFailed)
	ErrGetSampleCodesByOrderIDFailed                                 = errors.New(msgGetSampleCodesByOrderIDFailed)
	ErrCreateSubjectsFailed                                          = errors.New(msgCreateSubjectsFailed)
	ErrCreateReagentFailed                                           = errors.New(msgCreateReagentFailed)
	ErrCreateWarningsFailed                                          = errors.New(msgCreateWarningsFailed)
	ErrCreateControlResultWarningsFailed                             = errors.New(msgCreateControlResultWarningsFailed)
	ErrSaveDEAImageIDFailed                                          = errors.New(msgSaveDEAImageIDFailed)
	ErrIncreaseImageUploadRetryCountFailed                           = errors.New(msgSaveDEAImageIDFailed)
	ErrGetStuckImagesFailed                                          = errors.New(msgGetStuckImagesFailed)
	ErrScanStuckImageIdFailed                                        = errors.New(msgScanStuckImageIdFailed)
	ErrScanStuckImageDataFailed                                      = errors.New(msgScanStuckImageDataFailed)
	ErrMarkImagesAsSyncedToCerberusFailed                            = errors.New(msgMarkImagesAsSyncedToCerberusFailed)
	ErrMarkAnalysisRequestsAsProcessedFailed                         = errors.New(msgMarkAnalysisRequestsAsProcessedFailed)
	ErrMarkAnalysisResultsAsProcessedFailed                          = errors.New(msgMarkAnalysisResultsAsProcessedFailed)
	ErrGetAnalysisRequestExtraValuesFailed                           = errors.New(msgGetAnalysisRequestExtraValuesFailed)
	ErrDeleteAnalysisRequestExtraValuesFailed                        = errors.New(msgDeleteAnalysisRequestExtraValuesFailed)
	ErrCreateControlResultFailed                                     = errors.New(msgCreateControlResultFailed)
	ErrUpdateControlResultFailed                                     = errors.New(msgUpdateControlResultFailed)
	ErrCreateAnalysisResultReagentRelationsFailed                    = errors.New(msgCreateAnalysisResultReagentRelationsFailed)
	ErrCreateReagentControlResultRelationsFailed                     = errors.New(msgCreateReagentControlResultRelationsFailed)
	ErrCreateAnalysisResultControlResultRelationsFailed              = errors.New(msgCreateAnalysisResultControlResultRelationsFailed)
	ErrMarkAnalysisResultControlResultRelationAsProcessedFailed      = errors.New(msgMarkAnalysisResultControlResultRelationAsProcessedFailed)
	ErrSaveCerberusIdForAnalysisResultFailed                         = errors.New(msgSaveCerberusIdForAnalysisResultFailed)
	ErrGetControlResultsByIDsFailed                                  = errors.New(msgGetControlResultsByIDsFailed)
	ErrGetReagentsByIDsFailed                                        = errors.New(msgGetReagentsByIDsFailed)
	ErrLoadAnalysisResultIdsWithoutControlByReagentFailed            = errors.New(msgLoadAnalysisResultIdsWithoutControlByReagentFailed)
	ErrLoadAnalysisResultIdsWhereLatestControlResultIsInValidFailed  = errors.New(msgLoadAnalysisResultIdsWhereLatestControlResultIsInValidFailed)
	ErrLoadLatestControlResultIdFailed                               = errors.New(msgLoadLatestControlResultIdFailed)
	ErrLoadNotValidatedControlResultsFailed                          = errors.New(msgLoadNotValidatedControlResultsFailed)
	ErrGetAnalysisResultIdsForStatusRecalculationByControlIdsFailed  = errors.New(msgGetAnalysisResultIdsForStatusRecalculationByControlIdsFailed)
	ErrTooManyAnalysisResultsFromGatheringConnectedData              = errors.New(msgTooManyAnalysisResultsFromGatheringConnectedData)
	ErrGetAnalysisResultsFailed                                      = errors.New(msgGetAnalysisResultsFailed)
	ErrGetAnalysisRequestsFailed                                     = errors.New(msgGetAnalysisRequestsFailed)
	ErrGetUnprocessedAnalysisRequestsFailed                          = errors.New(msgGetUnprocessedAnalysisRequestsFailed)
	ErrGetSubjectInfosFailed                                         = errors.New(msgGetSubjectInfosFailed)
	ErrCreateAnalysisResultBatchFailed                               = errors.New(msgCreateAnalysisResultBatchFailed)
	ErrUpdateAnalysisResultStatusBatchFailed                         = errors.New(msgUpdateAnalysisResultStatusBatchFailed)
	ErrGetAnalysisResultExtraValuesFailed                            = errors.New(msgGetAnalysisResultExtraValuesFailed)
	ErrGetAnalysisResultReagentRelationsFailed                       = errors.New(msgGetAnalysisResultReagentRelationsFailed)
	ErrGetAnalysisResultControlResultRelationsFailed                 = errors.New(msgGetAnalysisResultControlResultRelationsFailed)
	ErrCreateControlResultChannelResultQuantitativeValuesBatchFailed = errors.New(msgCreateControlResultChannelResultQuantitativeValuesBatchFailed)
	ErrGetReagentControlResultRelationsFailed                        = errors.New(msgGetReagentControlResultRelationsFailed)
	ErrGetImagesFailed                                               = errors.New(msgGetImagesFailed)
	ErrGetAnalysisResultWarningsFailed                               = errors.New(msgGetAnalysisResultWarningsFailed)
	ErrGetAnalysisResultChannelResultsFailed                         = errors.New(msgGetAnalysisResultChannelResultsFailed)
	ErrGetChannelResultQuantitativeValuesFailed                      = errors.New(msgGetChannelResultQuantitativeValuesFailed)
	ErrCreateAnalysisResultExtraValuesFailed                         = errors.New(msgCreateAnalysisResultExtraValuesFailed)
	ErrCreateAnalysisRequestExtraValuesFailed                        = errors.New(msgCreateAnalysisRequestExtraValuesFailed)
	ErrCreateControlResultExtraValuesFailed                          = errors.New(msgCreateControlResultExtraValuesFailed)
	ErrCreateChannelResultBatchFailed                                = errors.New(msgCreateChannelResultBatchFailed)
	ErrCreateControlResultChannelResultBatchFailed                   = errors.New(msgCreateControlResultChannelResultBatchFailed)
	ErrCreateControlResultQuantitativeValuesBatchFailed              = errors.New(msgCreateControlResultQuantitativeValuesBatchFailed)
	ErrCreateAnalysisResultImagesFailed                              = errors.New(msgCreateAnalysisResultImagesFailed)
	ErrCreateControlResultImagesFailed                               = errors.New(msgCreateControlResultImagesFailed)
	ErrCreateCerberusQueueItemFailed                                 = errors.New(msgCreateCerberusQueueItemFailed)
	ErrDeleteOldCerberusQueueItemsFailed                             = errors.New(msgDeleteOldCerberusQueueItemsFailed)
	ErrDeleteOldAnalysisRequestsFailed                               = errors.New(msgDeleteOldAnalysisRequestsFailed)
	ErrDeleteAnalysisRequestExtraValuesByAnalysisRequestIdFailed     = errors.New(msgDeleteAnalysisRequestExtraValuesByAnalysisRequestIdFailed)
	ErrDeleteSubjectInfosByAnalysisRequestIdFailed                   = errors.New(msgDeleteSubjectInfosByAnalysisRequestIdFailed)
	ErrDeleteAnalysisRequestInstrumentTransmissionsFailed            = errors.New(msgDeleteAnalysisRequestInstrumentTransmissionsFailed)
	ErrDeleteAppliedSortingRuleTargetsBySampleCodesFailed            = errors.New(msgDeleteAppliedSortingRuleTargetsBySampleCodesFailed)
	ErrDeleteOldAnalysisResultsFailed                                = errors.New(msgDeleteOldAnalysisResultsFailed)
	ErrDeleteChannelResultsByAnalysisResultIdsFailed                 = errors.New(msgDeleteChannelResultsByAnalysisResultIdsFailed)
	ErrDeleteAnalysisResultWarningsFailed                            = errors.New(msgDeleteAnalysisResultWarningsFailed)
	ErrDeleteImagesByAnalysisResultIdsFailed                         = errors.New(msgDeleteImagesByAnalysisResultIdsFailed)
	ErrDeleteAnalysisResultExtraValuesByAnalysisResultIdsFailed      = errors.New(msgDeleteAnalysisResultExtraValuesByAnalysisResultIdsFailed)
	ErrDeleteReagentInfosByAnalysisResultIdsFailed                   = errors.New(msgDeleteReagentInfosByAnalysisResultIdsFailed)
	ErrGetCerberusQueueItemsFailed                                   = errors.New(msgGetCerberusQueueItemsFailed)
	ErrUpdateCerberusQueueItemStatusFailed                           = errors.New(msgUpdateCerberusQueueItemStatusFailed)
	ErrUnprocessedAnalysisResultIdsFailed                            = errors.New(msgUnprocessedAnalysisResultIdsFailed)
	ErrGetControlResultImagesFailed                                  = errors.New(msgGetControlResultImagesFailed)
	ErrGetControlResultQuantitativeChannelResultsFailed              = errors.New(msgGetControlResultQuantitativeChannelResultsFailed)
	ErrGetControlResultChannelResultsFailed                          = errors.New(msgGetControlResultChannelResultsFailed)
	ErrGetControlResultWarningsFailed                                = errors.New(msgGetControlResultWarningsFailed)
	ErrGetControlResultExtraValuesFailed                             = errors.New(msgGetControlResultExtraValuesFailed)
	ErrAnalyteMappingNotFound                                        = errors.New(msgAnalyteMappingNotFound)
	ErrUpdateAnalysisResultDEARawMessageIDFailed                     = errors.New(msgUpdateAnalysisResultDEARawMessageIDFailed)
	ErrMissingDEARawMessageID                                        = errors.New(msgMissingDEARawMessageID)
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
	DEARawMessageID          uuid.NullUUID     `db:"dea_raw_message_id"`
	MessageInID              uuid.UUID         `db:"message_in_id"`
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
	Reagents                 []reagentDAO
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

type reagentDAO struct {
	ID             uuid.UUID    `db:"id"`
	Manufacturer   string       `db:"manufacturer"`
	SerialNumber   string       `db:"serial"`
	LotNo          string       `db:"lot_no"`
	Name           string       `db:"name"`
	Type           ReagentType  `db:"type"`
	ExpirationDate sql.NullTime `db:"expiration_date"`
	CreatedAt      time.Time    `db:"created_at"`
}

type controlResultDAO struct {
	ID                         uuid.UUID         `db:"id"`
	SampleCode                 string            `db:"sample_code"`
	AnalyteMappingID           uuid.UUID         `db:"analyte_mapping_id"`
	InstrumentID               uuid.UUID         `db:"instrument_id"`
	ExpectedControlResultId    uuid.NullUUID     `db:"expected_control_result_id"`
	IsValid                    bool              `db:"is_valid"`
	IsComparedToExpectedResult bool              `db:"is_compared_to_expected_result"`
	Result                     string            `db:"result"`
	ExaminedAt                 time.Time         `db:"examined_at"`
	CreatedAt                  time.Time         `db:"created_at"`
	AnalyteMapping             analyteMappingDAO `db:"analyte_mapping"`
	ChannelResults             []controlResultChannelResultDAO
	ExtraValues                []controlResultExtraValueDAO
	Warnings                   []controlResultWarningDAO
}

type controlResultChannelResultDAO struct {
	ID                    uuid.UUID `db:"id"`
	ControlResultId       uuid.UUID `db:"control_result_id"`
	ChannelID             uuid.UUID `db:"channel_id"`
	QualitativeResult     string    `db:"qualitative_result"`
	QualitativeResultEdit bool      `db:"qualitative_result_edited"`
	QuantitativeResults   []quantitativeChannelResultDAO
	Images                []controlResultImageDAO
}

type controlResultWarningDAO struct {
	ID              uuid.UUID `db:"id"`
	ControlResultId uuid.UUID `db:"control_result_id"`
	Warning         string    `db:"warning"`
}

type controlResultExtraValueDAO struct {
	ID              uuid.UUID `db:"id"`
	ControlResultId uuid.UUID `db:"control_result_id"`
	Key             string    `db:"key"`
	Value           string    `db:"value"`
}

type controlResultImageDAO struct {
	ID               uuid.UUID      `db:"id"`
	ControlResultId  uuid.UUID      `db:"control_result_id"`
	ChannelResultID  uuid.NullUUID  `db:"channel_result_id"`
	Name             string         `db:"name"`
	Description      sql.NullString `db:"description"`
	ImageBytes       []byte         `db:"image_bytes"`
	DeaImageID       uuid.NullUUID  `db:"dea_image_id"`
	UploadedToDeaAt  sql.NullTime   `db:"uploaded_to_dea_at"`
	UploadRetryCount int            `db:"upload_retry_count"`
	UploadError      sql.NullString `db:"upload_error"`
}

type analysisResultReagentRelationDAO struct {
	AnalysisResultID uuid.UUID `db:"analysis_result_id"`
	ReagentID        uuid.UUID `db:"reagent_id"`
}

type reagentControlResultRelationDAO struct {
	ReagentID       uuid.UUID `db:"reagent_id"`
	ControlResultID uuid.UUID `db:"control_result_id"`
	IsProcessed     bool      `db:"is_processed"`
}

type analysisResultControlResultRelationDAO struct {
	AnalysisResultID uuid.UUID `db:"analysis_result_id"`
	ControlResultID  uuid.UUID `db:"control_result_id"`
	IsProcessed      bool      `db:"is_processed"`
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
	CreateAnalysisRequestsBatch(ctx context.Context, analysisRequests []AnalysisRequest) ([]uuid.UUID, error)
	CreateAnalysisRequestExtraValues(ctx context.Context, extraValuesByAnalysisRequestIDs map[uuid.UUID][]ExtraValue) error
	GetAnalysisRequestsBySampleCodeAndAnalyteID(ctx context.Context, sampleCodes string, analyteID uuid.UUID) ([]AnalysisRequest, error)
	GetAnalysisRequestsBySampleCodes(ctx context.Context, sampleCodes []string, allowResending bool) (map[string][]AnalysisRequest, error)
	GetAnalysisRequestExtraValuesByAnalysisRequestID(ctx context.Context, analysisRequestID uuid.UUID) (map[string]string, error)
	GetSampleCodesByOrderID(ctx context.Context, orderID uuid.UUID) ([]string, error)
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
	UpdateStatusAnalysisResultsBatch(ctx context.Context, analysisResultsToUpdate []AnalysisResult) error
	UpdateAnalysisResultDEARawMessageID(ctx context.Context, analysisResultID uuid.UUID, deaRawMessageID uuid.NullUUID) error
	CreateAnalysisResultReagentRelations(ctx context.Context, relationDAOs []analysisResultReagentRelationDAO) error
	GetAnalysisResultsBySampleCodeAndAnalyteID(ctx context.Context, sampleCode string, analyteID uuid.UUID) ([]AnalysisResult, error)
	GetAnalysisResultByCerberusID(ctx context.Context, id uuid.UUID, allowDeletedAnalyteMapping bool) (AnalysisResult, error)
	GetAnalysisResultsByIDs(ctx context.Context, ids []uuid.UUID) ([]AnalysisResult, error)
	GetAnalysisResultIdsForStatusRecalculationByControlIds(ctx context.Context, controlResultIds []uuid.UUID) ([]uuid.UUID, error)
	CreateAnalysisResultExtraValues(ctx context.Context, extraValuesByAnalysisRequestIDs map[uuid.UUID][]ExtraValue) error
	CreateChannelResults(ctx context.Context, channelResults []ChannelResult, analysisResultID uuid.UUID) ([]uuid.UUID, error)
	CreateChannelResultQuantitativeValues(ctx context.Context, quantitativeValuesByChannelResultIDs map[uuid.UUID]map[string]string) error
	CreateReagentBatch(ctx context.Context, reagents []Reagent) ([]Reagent, error)
	CreateControlResults(ctx context.Context, controlResultsMap map[uuid.UUID]map[uuid.UUID][]ControlResult) (map[uuid.UUID]map[uuid.UUID][]uuid.UUID, error)
	CreateWarnings(ctx context.Context, warningsByAnalysisResultID map[uuid.UUID][]string) error

	UpdateCerberusQueueItemStatus(ctx context.Context, queueItem CerberusQueueItem) error
	GetAnalysisResultQueueItems(ctx context.Context) ([]CerberusQueueItem, error)
	CreateAnalysisResultQueueItem(ctx context.Context, analysisResults []AnalysisResult) (uuid.UUID, error)

	SaveImages(ctx context.Context, images []imageDAO) ([]uuid.UUID, error)
	SaveControlResultImages(ctx context.Context, images []controlResultImageDAO) ([]uuid.UUID, error)
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
	DeleteOldAnalysisRequestsWithTx(ctx context.Context, cleanupDays, limit int, tx db.DbConnection) (int64, error)
	DeleteOldAnalysisResultsWithTx(ctx context.Context, cleanupDays, limit int, tx db.DbConnection) (int64, error)

	CreateReagents(ctx context.Context, reagents []Reagent) ([]uuid.UUID, error)
	GetReagentsByIDs(ctx context.Context, reagentIDs []uuid.UUID) (map[uuid.UUID]Reagent, error)
	CreateControlResultBatch(ctx context.Context, controlResults []ControlResult) ([]ControlResult, error)
	UpdateControlResultBatch(ctx context.Context, controlResults []ControlResult) error
	GetControlResultsByIDs(ctx context.Context, controlResultIDs []uuid.UUID) (map[uuid.UUID]ControlResult, error)
	CreateReagentControlResultRelations(ctx context.Context, relationDAOs []reagentControlResultRelationDAO) error
	CreateAnalysisResultControlResultRelations(ctx context.Context, relationDAOs []analysisResultControlResultRelationDAO) error

	SaveCerberusIDForAnalysisResult(ctx context.Context, analysisResultID uuid.UUID, cerberusID uuid.UUID) error

	GetAnalysisResultIdsWithoutControlByReagent(ctx context.Context, controlResult ControlResult, reagent Reagent, analysisResultWithoutControlSearchDays int) ([]uuid.UUID, error)
	GetAnalysisResultIdsWhereLastestControlIsInvalid(ctx context.Context, controlResult ControlResult, reagent Reagent, analysisResultWithInvalidControlSearchDays int) ([]uuid.UUID, error)
	GetLatestControlResultsByReagent(ctx context.Context, reagent Reagent, resultYieldTime *time.Time, analyteMapping AnalyteMapping, instrumentId uuid.UUID, ControlResultSearchDays int) ([]ControlResult, error)
	GetControlResultsToValidate(ctx context.Context, analyteMappingIds []uuid.UUID) ([]ControlResult, error)

	MarkReagentControlResultRelationsAsProcessed(ctx context.Context, controlResultID uuid.UUID, reagentIDs []uuid.UUID) error
	MarkAnalysisResultControlResultRelationsAsProcessed(ctx context.Context, controlResultID uuid.UUID, analysisResultIDs []uuid.UUID) error

	CreateTransaction() (db.DbConnection, error)
	WithTransaction(tx db.DbConnection) AnalysisRepository
}

type analysisRepository struct {
	db       db.DbConnection
	dbSchema string
}

func NewAnalysisRepository(db db.DbConnection, dbSchema string) AnalysisRepository {
	return &analysisRepository{
		db:       db,
		dbSchema: dbSchema,
	}
}

const analysisRequestsBatchSize = 9000

// CreateAnalysisRequestsBatch
// Returns the CerberusID and work item IDs of saved requests
func (r *analysisRepository) CreateAnalysisRequestsBatch(ctx context.Context, analysisRequests []AnalysisRequest) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, 0, len(analysisRequests))
	err := utils.Partition(len(analysisRequests), analysisRequestsBatchSize, func(low int, high int) error {
		idsPart, err := r.createAnalysisRequestsBatch(ctx, analysisRequests[low:high])
		if err != nil {
			return err
		}
		ids = append(ids, idsPart...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func (r *analysisRepository) createAnalysisRequestsBatch(ctx context.Context, analysisRequests []AnalysisRequest) ([]uuid.UUID, error) {
	if len(analysisRequests) == 0 {
		return nil, nil
	}
	ids := make([]uuid.UUID, 0)
	err := utils.Partition(len(analysisRequests), maxParams/8, func(low int, high int) error {
		query := fmt.Sprintf(`INSERT INTO %s.sk_analysis_requests(id, work_item_id, analyte_id, sample_code, material_id, laboratory_id, valid_until_time, created_at)
				VALUES(:id, :work_item_id, :analyte_id, :sample_code, :material_id, :laboratory_id, :valid_until_time, :created_at)
				ON CONFLICT (work_item_id) DO NOTHING RETURNING id;`, r.dbSchema)
		rows, err := r.db.NamedQueryContext(ctx, query, convertAnalysisRequestsToDAOs(analysisRequests[low:high]))
		if err != nil {
			log.Error().Err(err).Msg(msgCreateAnalysisRequestsBatchFailed)
			return ErrCreateAnalysisRequestsBatchFailed
		}
		defer rows.Close()

		for rows.Next() {
			var analysisRequestID uuid.UUID
			err = rows.Scan(&analysisRequestID)
			if err != nil {
				log.Error().Err(err).Msg(msgCreateAnalysisRequestsBatchFailed)
				return ErrCreateAnalysisRequestsBatchFailed
			}
			ids = append(ids, analysisRequestID)
		}

		return nil
	})

	return ids, err
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

	err := utils.Partition(len(subjectDAOs), subjectBatchSize, func(low int, high int) error {
		err := r.createSubjects(ctx, subjectDAOs[low:high])
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return idsMap, nil
}

func (r *analysisRepository) createSubjects(ctx context.Context, subjectDAOs []subjectInfoDAO) error {
	query := fmt.Sprintf(`INSERT INTO %s.sk_subject_infos(id, analysis_request_id,"type",date_of_birth,first_name,last_name,donor_id,donation_id,donation_type,pseudonym)
		VALUES(id, :analysis_request_id,:type,:date_of_birth,:first_name,:last_name,:donor_id,:donation_id,:donation_type,:pseudonym);`, r.dbSchema)

	_, err := r.db.NamedExecContext(ctx, query, subjectDAOs)
	if err != nil {
		log.Error().Err(err).Msg(msgCreateSubjectsFailed)
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
		log.Error().Err(err).Msg(msgGetAnalysisRequestsFailed)
		return []AnalysisRequest{}, ErrGetAnalysisRequestsFailed
	}
	defer rows.Close()

	analysisRequests := make([]AnalysisRequest, 0)
	for rows.Next() {
		request := analysisRequestDAO{}
		err := rows.StructScan(&request)
		if err != nil {
			log.Error().Err(err).Msg(msgGetAnalysisRequestsFailed)
			return []AnalysisRequest{}, ErrGetAnalysisRequestsFailed
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
			log.Error().Err(err).Msg(msgGetAnalysisRequestsFailed)
			return ErrGetAnalysisRequestsFailed
		}

		defer rows.Close()

		for rows.Next() {
			request := analysisRequestDAO{}
			err := rows.StructScan(&request)
			if err != nil {
				log.Error().Err(err).Msg(msgGetAnalysisRequestsFailed)
				return ErrGetAnalysisRequestsFailed
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
			log.Error().Err(err).Msg(msgGetSubjectInfosFailed)
			return ErrGetSubjectInfosFailed
		}
		defer rows.Close()
		for rows.Next() {
			var subjectDao subjectInfoDAO
			err = rows.StructScan(&subjectDao)
			if err != nil {
				log.Error().Err(err).Msg(msgGetSubjectInfosFailed)
				return ErrGetSubjectInfosFailed
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
			log.Error().Err(err).Msg(msgGetAnalysisRequestsFailed)
			return ErrGetAnalysisRequestsFailed
		}

		defer rows.Close()

		for rows.Next() {
			request := analysisRequestDAO{}
			err := rows.StructScan(&request)
			if err != nil {
				log.Error().Err(err).Msg(msgGetAnalysisRequestsFailed)
				return ErrGetAnalysisRequestsFailed
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
		log.Error().Err(err).Msg(msgRevokeAnalysisRequestsFailed)
		return ErrRevokeAnalysisRequestsFailed
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
		log.Error().Err(err).Msg(msgRevokeAnalysisRequestsFailed)
		return ErrRevokeAnalysisRequestsFailed
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
	savedAnalysisResults, err := r.createAnalysisResultsBatch(ctx, analysisResults)
	if err != nil {
		return nil, err
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

	err := utils.Partition(len(analysisResults), analysisResultBatchSize, func(low int, high int) error {
		query := fmt.Sprintf(`INSERT INTO %s.sk_analysis_results(id, analyte_mapping_id, instrument_id, sample_code, instrument_run_id, dea_raw_message_id, message_in_id, "result", status, result_mode, yielded_at, valid_until, operator, technical_release_datetime, edited, edit_reason, is_invalid)
			VALUES(:id, :analyte_mapping_id, :instrument_id, :sample_code, :instrument_run_id, :dea_raw_message_id, :message_in_id, :result, :status, :result_mode, :yielded_at, :valid_until, :operator, :technical_release_datetime, :edited, :edit_reason, :is_invalid);`, r.dbSchema)
		_, err := r.db.NamedExecContext(ctx, query, convertAnalysisResultsToDAOs(analysisResults[low:high]))
		if err != nil {
			log.Error().Err(err).Msg(msgCreateAnalysisResultBatchFailed)
			return ErrCreateAnalysisResultBatchFailed
		}
		return nil
	})
	if err != nil {
		return analysisResults, err
	}
	return analysisResults, err
}

func (r *analysisRepository) UpdateStatusAnalysisResultsBatch(ctx context.Context, analysisResultsToUpdate []AnalysisResult) error {
	if len(analysisResultsToUpdate) == 0 {
		return nil
	}
	statusMap := make(map[ResultStatus][]uuid.UUID)
	for i := range analysisResultsToUpdate {
		if _, ok := statusMap[analysisResultsToUpdate[i].Status]; !ok {
			statusMap[analysisResultsToUpdate[i].Status] = make([]uuid.UUID, 0)
		}
		statusMap[analysisResultsToUpdate[i].Status] = append(statusMap[analysisResultsToUpdate[i].Status], analysisResultsToUpdate[i].ID)
	}

	var err error
	for status := range statusMap {
		err = utils.Partition(len(statusMap[status]), analysisResultBatchSize, func(low int, high int) error {
			query := fmt.Sprintf(`UPDATE %s.sk_analysis_results SET status = ? WHERE id in (?);`, r.dbSchema)
			query, ars, _ := sqlx.In(query, status, statusMap[status][low:high])
			query = r.db.Rebind(query)
			_, dbError := r.db.ExecContext(ctx, query, ars...)
			if dbError != nil {
				log.Error().Err(dbError).Msg(msgUpdateAnalysisResultStatusBatchFailed)
				return ErrUpdateAnalysisResultStatusBatchFailed
			}
			return nil
		})
	}
	return err
}

const analysisResultRelationBatch = 15000

func (r *analysisRepository) CreateAnalysisResultReagentRelations(ctx context.Context, relationDAOs []analysisResultReagentRelationDAO) error {
	if len(relationDAOs) == 0 {
		return nil
	}

	err := utils.Partition(len(relationDAOs), analysisResultRelationBatch, func(low int, high int) error {
		query := fmt.Sprintf(`INSERT INTO %s.sk_analysis_result_reagent_relations(analysis_result_id, reagent_id)
		VALUES(:analysis_result_id, :reagent_id) ON CONFLICT DO NOTHING`, r.dbSchema)

		_, err := r.db.NamedExecContext(ctx, query, relationDAOs[low:high])

		if err != nil {
			log.Error().Err(err).Msg(msgCreateAnalysisResultReagentRelationsFailed)
			return ErrCreateAnalysisResultReagentRelationsFailed
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (r *analysisRepository) CreateReagentControlResultRelations(ctx context.Context, relationDAOs []reagentControlResultRelationDAO) error {
	if len(relationDAOs) == 0 {
		return nil
	}

	err := utils.Partition(len(relationDAOs), analysisResultRelationBatch, func(low int, high int) error {
		query := fmt.Sprintf(`INSERT INTO %s.sk_reagent_control_result_relations(reagent_id, control_result_id, is_processed)
		VALUES(:reagent_id, :control_result_id, :is_processed) ON CONFLICT DO NOTHING`, r.dbSchema)

		_, err := r.db.NamedExecContext(ctx, query, relationDAOs[low:high])

		if err != nil {
			log.Error().Err(err).Msg(msgCreateReagentControlResultRelationsFailed)
			return ErrCreateReagentControlResultRelationsFailed
		}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (r *analysisRepository) CreateAnalysisResultControlResultRelations(ctx context.Context, relationDAOs []analysisResultControlResultRelationDAO) error {
	if len(relationDAOs) == 0 {
		return nil
	}

	err := utils.Partition(len(relationDAOs), analysisResultRelationBatch, func(low int, high int) error {
		query := fmt.Sprintf(`INSERT INTO %s.sk_analysis_result_control_result_relations(analysis_result_id, control_result_id, is_processed)
		VALUES(:analysis_result_id, :control_result_id, :is_processed) ON CONFLICT DO NOTHING`, r.dbSchema)

		_, err := r.db.NamedExecContext(ctx, query, relationDAOs[low:high])

		if err != nil {
			log.Error().Err(err).Msg(msgCreateAnalysisResultControlResultRelationsFailed)
			return ErrCreateAnalysisResultControlResultRelationsFailed
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (r *analysisRepository) GetAnalysisResultsBySampleCodeAndAnalyteID(ctx context.Context, sampleCode string, analyteID uuid.UUID) ([]AnalysisResult, error) {
	query := `SELECT sar.id, sar.analyte_mapping_id, sar.instrument_id, sar.sample_code, sar.instrument_run_id, sar.is_invalid,
					sar.message_in_id, sar.dea_raw_message_id, sar."result", sar.status, sar.result_mode, sar.yielded_at,
       				sar.valid_until, sar.operator, sar.edited, sar.edit_reason, sam.id AS "analyte_mapping.id",
       				sam.instrument_id AS "analyte_mapping.instrument_id", sam.instrument_analyte AS "analyte_mapping.instrument_analyte",
					sam.analyte_id AS "analyte_mapping.analyte_id", sam.result_type AS "analyte_mapping.result_type",
					sam.control_result_required AS "analyte_mapping.control_result_required", sam.created_at AS "analyte_mapping.created_at", 
					sam.modified_at AS "analyte_mapping.modified_at"
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
		log.Error().Err(err).Msg(msgGetAnalysisResultsFailed)
		return []AnalysisResult{}, ErrGetAnalysisResultsFailed
	}
	defer rows.Close()

	analysisResultDAOs := make([]analysisResultDAO, 0)
	for rows.Next() {
		result := analysisResultDAO{}
		err := rows.StructScan(&result)
		if err != nil {
			log.Error().Err(err).Msg(msgGetAnalysisResultsFailed)
			return []AnalysisResult{}, ErrGetAnalysisResultsFailed
		}

		analysisResultDAOs = append(analysisResultDAOs, result)
	}

	return r.gatherAndAttachAllConnectedDataToAnalysisResults(ctx, analysisResultDAOs)
}

func (r *analysisRepository) GetAnalysisResultByCerberusID(ctx context.Context, id uuid.UUID, allowDeletedAnalyteMapping bool) (AnalysisResult, error) {
	query := `SELECT sar.id, sar.analyte_mapping_id, sar.instrument_id, sar.sample_code, sar.instrument_run_id, sar.dea_raw_message_id, sar."result", sar.status, sar.result_mode, sar.yielded_at, sar.valid_until, sar.operator, sar.edited, sar.edit_reason, sar.is_invalid, sar.message_in_id,
					sam.id AS "analyte_mapping.id", sam.instrument_id AS "analyte_mapping.instrument_id", sam.instrument_analyte AS "analyte_mapping.instrument_analyte", sam.analyte_id AS "analyte_mapping.analyte_id", sam.result_type AS "analyte_mapping.result_type", 
					sam.control_result_required AS "analyte_mapping.control_result_required", sam.created_at AS "analyte_mapping.created_at", sam.modified_at AS "analyte_mapping.modified_at"
			FROM %schema_name%.sk_analysis_results sar
			INNER JOIN %schema_name%.sk_analyte_mappings sam ON sar.analyte_mapping_id = sam.id`

	if !allowDeletedAnalyteMapping {
		query += ` AND sam.deleted_at IS NULL`
	}

	query += ` WHERE sar.cerberus_id = $1;`

	query = strings.ReplaceAll(query, "%schema_name%", r.dbSchema)

	row := r.db.QueryRowxContext(ctx, query, id)
	result := analysisResultDAO{}
	err := row.StructScan(&result)
	if err != nil {
		log.Error().Err(err).Msg(msgGetAnalysisResultsFailed)
		return AnalysisResult{}, ErrGetAnalysisResultsFailed
	}

	if err != nil {
		if err == sql.ErrNoRows {
			log.Trace().Msg("No analysis results")
			return AnalysisResult{}, nil
		}
		log.Error().Err(err).Msg(msgGetAnalysisResultsFailed)
		return AnalysisResult{}, ErrGetAnalysisResultsFailed
	}

	analysisResults, err := r.gatherAndAttachAllConnectedDataToAnalysisResults(ctx, []analysisResultDAO{result})
	if err != nil {
		return AnalysisResult{}, err
	}
	if len(analysisResults) != 1 {
		log.Error().Msg(msgTooManyAnalysisResultsFromGatheringConnectedData)
		return AnalysisResult{}, ErrTooManyAnalysisResultsFromGatheringConnectedData
	}
	return analysisResults[0], err
}

func (r *analysisRepository) GetAnalysisResultsByIDs(ctx context.Context, ids []uuid.UUID) ([]AnalysisResult, error) {
	if len(ids) == 0 {
		return []AnalysisResult{}, nil
	}

	query := `SELECT sar.id, sar.analyte_mapping_id, sar.instrument_id, sar.message_in_id, sar.sample_code, sar.instrument_run_id, sar.dea_raw_message_id, sar."result", sar.status, sar.result_mode, sar.yielded_at, sar.valid_until, sar.operator, sar.edited, sar.edit_reason, sar.is_invalid,
					sam.id AS "analyte_mapping.id", sam.instrument_id AS "analyte_mapping.instrument_id", sam.instrument_analyte AS "analyte_mapping.instrument_analyte", sam.analyte_id AS "analyte_mapping.analyte_id", sam.result_type AS "analyte_mapping.result_type", 
					sam.control_result_required AS "analyte_mapping.control_result_required", sam.created_at AS "analyte_mapping.created_at", sam.modified_at AS "analyte_mapping.modified_at"
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
		log.Error().Err(err).Msg(msgGetAnalysisResultsFailed)
		return []AnalysisResult{}, ErrGetAnalysisResultsFailed
	}
	defer rows.Close()

	analysisResultDAOs := make([]analysisResultDAO, 0)
	for rows.Next() {
		result := analysisResultDAO{}
		err := rows.StructScan(&result)
		if err != nil {
			log.Error().Err(err).Msg(msgGetAnalysisResultsFailed)
			return []AnalysisResult{}, ErrGetAnalysisResultsFailed
		}

		analysisResultDAOs = append(analysisResultDAOs, result)
	}

	return r.gatherAndAttachAllConnectedDataToAnalysisResults(ctx, analysisResultDAOs)
}

func (r *analysisRepository) gatherAndAttachAllConnectedDataToAnalysisResults(ctx context.Context, analysisResultDAOs []analysisResultDAO) ([]AnalysisResult, error) {
	analysisResultIDs := make([]uuid.UUID, 0)
	analyteMappingIDs := make([]uuid.UUID, 0)
	for i := range analysisResultDAOs {
		analysisResultIDs = append(analysisResultIDs, analysisResultDAOs[i].ID)
		analyteMappingIDs = append(analyteMappingIDs, analysisResultDAOs[i].AnalyteMappingID)
	}

	extraValuesMap, err := r.getAnalysisResultExtraValues(ctx, analysisResultIDs)
	if err != nil {
		return nil, err
	}

	reagentIDsMap, err := r.getAnalysisResultReagentRelations(ctx, analysisResultIDs)
	if err != nil {
		return nil, err
	}

	reagentIDs := make([]uuid.UUID, 0)

	for _, reagents := range reagentIDsMap {
		reagentIDs = append(reagentIDs, reagents...)
	}

	reagentsByIDs, err := r.GetReagentsByIDs(ctx, reagentIDs)
	if err != nil {
		return nil, err
	}

	controlResultIDsByReagentIDMap, err := r.getReagentControlResultRelations(ctx, reagentIDs)
	if err != nil {
		return nil, err
	}

	controlResultIDs := make([]uuid.UUID, 0)

	for _, controlResults := range controlResultIDsByReagentIDMap {
		controlResultIDs = append(controlResultIDs, controlResults...)
	}

	controlResultIDsByAnalysisResultIDMap, err := r.getAnalysisResultControlResultRelations(ctx, analysisResultIDs)
	if err != nil {
		return nil, err
	}

	for _, controlResults := range controlResultIDsByAnalysisResultIDMap {
		controlResultIDs = append(controlResultIDs, controlResults...)
	}

	controlResultsByIDs, err := r.GetControlResultsByIDs(ctx, controlResultIDs)
	if err != nil {
		return nil, err
	}

	controlResultMap := make(map[uuid.UUID][]ControlResult)

	for reagentID, reagent := range reagentsByIDs {
		for _, controlResultID := range controlResultIDsByReagentIDMap[reagentID] {
			reagent.ControlResults = append(reagentsByIDs[reagentID].ControlResults, controlResultsByIDs[controlResultID])
		}

		reagentsByIDs[reagentID] = reagent
	}

	reagentMap := make(map[uuid.UUID][]Reagent)

	for analysisResultID, reagentIds := range reagentIDsMap {
		analysisControlIdMap := make(map[uuid.UUID]bool)
		for i := range controlResultIDsByAnalysisResultIDMap[analysisResultID] {
			analysisControlIdMap[controlResultIDsByAnalysisResultIDMap[analysisResultID][i]] = false
		}
		for _, reagentID := range reagentIds {
			reagentControlIds := controlResultIDsByReagentIDMap[reagentID]
			reagent := reagentsByIDs[reagentID]
			reagent.ControlResults = make([]ControlResult, 0)
			for _, controlId := range reagentControlIds {
				if _, ok := analysisControlIdMap[controlId]; ok {
					reagent.ControlResults = append(reagent.ControlResults, controlResultsByIDs[controlId])
					analysisControlIdMap[controlId] = true
				}
			}
			reagentMap[analysisResultID] = append(reagentMap[analysisResultID], reagent)
		}

		for controlId, addedToReagent := range analysisControlIdMap {
			if !addedToReagent {
				controlResultMap[analysisResultID] = append(controlResultMap[analysisResultID], controlResultsByIDs[controlId])
			}
		}
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
	expectedControlResultMap, err := r.getExpectedControlResultsByAnalyteMappingIds(ctx, analyteMappingIDs)
	if err != nil {
		return nil, err
	}

	analysisResults := make([]AnalysisResult, len(analysisResultDAOs))
	for analysisResultIndex := range analysisResultDAOs {
		analysisResultDAOs[analysisResultIndex].ExtraValues = extraValuesMap[analysisResultDAOs[analysisResultIndex].ID]

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
		analysisResult.Reagents = reagentMap[analysisResult.ID]
		analysisResult.ControlResults = controlResultMap[analysisResult.ID]
		analysisResult.AnalyteMapping.ChannelMappings = channelMappingsMap[analysisResultDAOs[analysisResultIndex].AnalyteMappingID]
		analysisResult.AnalyteMapping.ResultMappings = resultMappingsMap[analysisResultDAOs[analysisResultIndex].AnalyteMappingID]
		analysisResult.AnalyteMapping.ExpectedControlResults = expectedControlResultMap[analysisResultDAOs[analysisResultIndex].AnalyteMappingID]

		analysisResults[analysisResultIndex] = analysisResult
	}

	return analysisResults, err
}

func (r *analysisRepository) GetAnalysisResultIdsForStatusRecalculationByControlIds(ctx context.Context, controlResultIds []uuid.UUID) ([]uuid.UUID, error) {
	analysisResultIds := make([]uuid.UUID, 0)
	query := `SELECT sar.id
		FROM %schema_name%.sk_analysis_results sar
			INNER JOIN %schema_name%.sk_analysis_result_control_result_relations sarcrr ON sar.id = sarcrr.analysis_result_id
			INNER JOIN %schema_name%.sk_control_results scr ON sarcrr.control_result_id = scr.id
			INNER JOIN %schema_name%.sk_analyte_mappings sam ON scr.instrument_id = sam.instrument_id AND scr.analyte_mapping_id = sam.id
			LEFT JOIN %schema_name%.sk_expected_control_result secr ON sam.id = secr.analyte_mapping_id AND scr.sample_code = secr.sample_code
		WHERE sar.status != 'FIN' AND sar.is_processed = TRUE AND CASE WHEN sam.control_result_required = TRUE AND (secr.id IS NULL OR scr.is_compared_to_expected_result = FALSE) THEN FALSE ELSE TRUE END`

	if len(controlResultIds) > 0 {
		query += ` AND scr.id IN (?) `
	}

	query += ` GROUP BY sar.id;`
	query = strings.ReplaceAll(query, "%schema_name%", r.dbSchema)

	var rows *sqlx.Rows
	var err error
	if len(controlResultIds) > 0 {
		var args []interface{}
		query, args, _ = sqlx.In(query, controlResultIds)
		query = r.db.Rebind(query)
		rows, err = r.db.QueryxContext(ctx, query, args...)
	} else {
		rows, err = r.db.QueryxContext(ctx, query)
	}

	if err != nil {
		log.Error().Err(err).Msg(msgGetAnalysisResultIdsForStatusRecalculationByControlIdsFailed)
		return analysisResultIds, ErrGetAnalysisResultIdsForStatusRecalculationByControlIdsFailed
	}
	defer rows.Close()
	for rows.Next() {
		var analysisResultId uuid.UUID
		err = rows.Scan(&analysisResultId)
		if err != nil {
			log.Error().Err(err).Msg(msgGetAnalysisResultIdsForStatusRecalculationByControlIdsFailed)
			return analysisResultIds, ErrGetAnalysisResultIdsForStatusRecalculationByControlIdsFailed
		}
		analysisResultIds = append(analysisResultIds, analysisResultId)
	}
	return analysisResultIds, nil
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
			return ErrGetChannelMappingsFailed
		}
		defer rows.Close()
		for rows.Next() {
			var dao channelMappingDAO
			err = rows.StructScan(&dao)
			if err != nil {
				log.Error().Err(err).Msg(msgGetChannelMappingsFailed)
				return ErrGetChannelMappingsFailed
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
			log.Error().Err(err).Msg(msgGetAnalysisResultExtraValuesFailed)
			return ErrGetAnalysisResultExtraValuesFailed
		}
		defer rows.Close()

		for rows.Next() {
			extraValue := resultExtraValueDAO{}
			err = rows.StructScan(&extraValue)
			if err != nil {
				log.Error().Err(err).Msg(msgGetAnalysisResultExtraValuesFailed)
				return ErrGetAnalysisResultExtraValuesFailed
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

	err := r.createAnalysisResultExtraValues(ctx, extraValuesDAOs)
	if err != nil {
		return err
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

	err := r.createAnalysisRequestExtraValues(ctx, extraValuesDAOs)
	if err != nil {
		return err
	}

	return nil
}

func (r *analysisRepository) createAnalysisResultExtraValues(ctx context.Context, extraValues []resultExtraValueDAO) error {
	err := utils.Partition(len(extraValues), extraValueBatchSize, func(low int, high int) error {
		query := fmt.Sprintf(`INSERT INTO %s.sk_analysis_result_extravalues(id, analysis_result_id, "key", "value")
		VALUES(:id, :analysis_result_id, :key, :value)`, r.dbSchema)
		_, err := r.db.NamedExecContext(ctx, query, extraValues[low:high])
		if err != nil {
			log.Error().Err(err).Msg(msgCreateAnalysisResultExtraValuesFailed)
			return ErrCreateAnalysisResultExtraValuesFailed
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (r *analysisRepository) createAnalysisRequestExtraValues(ctx context.Context, extraValues []requestExtraValueDAO) error {
	err := utils.Partition(len(extraValues), extraValueBatchSize, func(low int, high int) error {
		query := fmt.Sprintf(`INSERT INTO %s.sk_analysis_request_extra_values(id, analysis_request_id, "key", "value")
		VALUES(:id, :analysis_request_id, :key, :value)`, r.dbSchema)
		_, err := r.db.NamedExecContext(ctx, query, extraValues[low:high])
		if err != nil {
			log.Error().Err(err).Msg(msgCreateAnalysisRequestExtraValuesFailed)
			return ErrCreateAnalysisRequestExtraValuesFailed
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (r *analysisRepository) createControlResultExtraValues(ctx context.Context, extraValues []controlResultExtraValueDAO) error {
	err := utils.Partition(len(extraValues), extraValueBatchSize, func(low int, high int) error {
		query := fmt.Sprintf(`INSERT INTO %s.sk_control_result_extravalues(id, control_result_id, "key", "value")
		VALUES(:id, :control_result_id, :key, :value)`, r.dbSchema)
		_, err := r.db.NamedExecContext(ctx, query, extraValues[low:high])
		if err != nil {
			log.Error().Err(err).Msg(msgCreateControlResultExtraValuesFailed)
			return ErrCreateControlResultExtraValuesFailed
		}
		return nil
	})
	if err != nil {
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
			log.Error().Err(err).Msg(msgGetAnalysisResultChannelResultsFailed)
			return ErrGetAnalysisResultChannelResultsFailed
		}

		defer rows.Close()
		for rows.Next() {
			channelResult := channelResultDAO{}
			err = rows.StructScan(&channelResult)
			if err != nil {
				log.Error().Err(err).Msg(msgGetAnalysisResultChannelResultsFailed)
				return ErrGetAnalysisResultChannelResultsFailed
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

	ids, err = r.createChannelResults(ctx, channelResults, analysisResultID)
	if err != nil {
		return nil, err
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

	err := utils.Partition(len(channelResults), channelResultBatchSize, func(low int, high int) error {
		query := fmt.Sprintf(`INSERT INTO %s.sk_channel_results(id, analysis_result_id, channel_id, qualitative_result, qualitative_result_edited)
		VALUES(:id, :analysis_result_id, :channel_id, :qualitative_result, :qualitative_result_edited);`, r.dbSchema)
		_, err := r.db.NamedExecContext(ctx, query, convertChannelResultsToDAOs(channelResults[low:high], analysisResultID))
		if err != nil {
			log.Error().Err(err).Msg(msgCreateChannelResultBatchFailed)
			return ErrCreateChannelResultBatchFailed
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return ids, nil
}

func (r *analysisRepository) createControlResultChannelResults(ctx context.Context, channelResults []ChannelResult, controlResultID uuid.UUID) ([]uuid.UUID, error) {
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

	err := utils.Partition(len(channelResults), channelResultBatchSize, func(low int, high int) error {
		query := fmt.Sprintf(`INSERT INTO %s.sk_control_result_channel_results(id, control_result_id, channel_id, qualitative_result, qualitative_result_edited)
		VALUES(:id, :control_result_id, :channel_id, :qualitative_result, :qualitative_result_edited);`, r.dbSchema)
		_, err := r.db.NamedExecContext(ctx, query, convertChannelResultsToControlResultChannelResultDAOs(channelResults[low:high], controlResultID))
		if err != nil {
			log.Error().Err(err).Msg(msgCreateControlResultChannelResultBatchFailed)
			return ErrCreateControlResultChannelResultBatchFailed
		}
		return nil
	})
	if err != nil {
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
			log.Error().Err(err).Msg(msgGetChannelResultQuantitativeValuesFailed)
			return ErrGetChannelResultQuantitativeValuesFailed
		}
		defer rows.Close()
		for rows.Next() {
			quantitativeChannelResult := quantitativeChannelResultDAO{}
			err = rows.StructScan(&quantitativeChannelResult)
			if err != nil {
				log.Error().Err(err).Msg(msgGetChannelResultQuantitativeValuesFailed)
				return ErrGetChannelResultQuantitativeValuesFailed
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

	err := r.createChannelResultQuantitativeValues(ctx, quantitativeChannelResultDAOs)
	if err != nil {
		return err
	}

	return nil
}

func (r *analysisRepository) createChannelResultQuantitativeValues(ctx context.Context, quantitativeChannelResults []quantitativeChannelResultDAO) error {
	if len(quantitativeChannelResults) == 0 {
		return nil
	}
	err := utils.Partition(len(quantitativeChannelResults), channelResultQVBatchSize, func(low int, high int) error {
		query := fmt.Sprintf(`INSERT INTO %s.sk_channel_result_quantitative_values(id, channel_result_id, metric, "value")
		VALUES(:id, :channel_result_id, :metric, :value);`, r.dbSchema)
		_, err := r.db.NamedExecContext(ctx, query, quantitativeChannelResults[low:high])
		if err != nil {
			log.Error().Err(err).Msg(msgCreateControlResultQuantitativeValuesBatchFailed)
			return ErrCreateControlResultQuantitativeValuesBatchFailed
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (r *analysisRepository) CreateControlResultChannelResultQuantitativeValues(ctx context.Context, quantitativeValuesByChannelResultIDs map[uuid.UUID]map[string]string) error {
	quantitativeChannelResultDAOs := make([]quantitativeChannelResultDAO, 0)
	for channelResultID, quantitativeValues := range quantitativeValuesByChannelResultIDs {
		qrDAOs := convertQuantitativeResultsToDAOs(quantitativeValues, channelResultID)
		for i := range qrDAOs {
			qrDAOs[i].ID = uuid.New()
		}
		quantitativeChannelResultDAOs = append(quantitativeChannelResultDAOs, qrDAOs...)
	}
	err := r.createControlResultChannelResultQuantitativeValues(ctx, quantitativeChannelResultDAOs)
	if err != nil {
		return err
	}
	return nil
}

func (r *analysisRepository) createControlResultChannelResultQuantitativeValues(ctx context.Context, quantitativeChannelResults []quantitativeChannelResultDAO) error {
	if len(quantitativeChannelResults) == 0 {
		return nil
	}

	err := utils.Partition(len(quantitativeChannelResults), channelResultQVBatchSize, func(low int, high int) error {
		query := fmt.Sprintf(`INSERT INTO %s.sk_control_result_channel_result_quantitative_values(id, channel_result_id, metric, "value")
		VALUES(:id, :channel_result_id, :metric, :value);`, r.dbSchema)
		_, err := r.db.NamedExecContext(ctx, query, quantitativeChannelResults[low:high])
		if err != nil {
			log.Error().Err(err).Msg(msgCreateControlResultChannelResultQuantitativeValuesBatchFailed)
			return ErrCreateControlResultChannelResultQuantitativeValuesBatchFailed
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (r *analysisRepository) getAnalysisResultReagentRelations(ctx context.Context, analysisResultIDs []uuid.UUID) (map[uuid.UUID][]uuid.UUID, error) {
	reagentIDsMap := make(map[uuid.UUID][]uuid.UUID)
	err := utils.Partition(len(analysisResultIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`SELECT analysis_result_id, reagent_id FROM %s.sk_analysis_result_reagent_relations sarrr WHERE sarrr.analysis_result_id IN (?);`, r.dbSchema)
		query, args, _ := sqlx.In(query, analysisResultIDs[low:high])
		query = r.db.Rebind(query)
		rows, err := r.db.QueryxContext(ctx, query, args...)
		if err != nil {
			if err == sql.ErrNoRows {
				log.Trace().Msg("No analysis result reagent relations")
				return nil
			}
			log.Error().Err(err).Msg(msgGetAnalysisResultReagentRelationsFailed)
			return ErrGetAnalysisResultReagentRelationsFailed
		}
		defer rows.Close()

		for rows.Next() {
			var analysisResultID, reagentID uuid.UUID
			err = rows.Scan(&analysisResultID, &reagentID)
			if err != nil {
				log.Error().Err(err).Msg(msgGetAnalysisResultReagentRelationsFailed)
				return ErrGetAnalysisResultReagentRelationsFailed
			}

			reagentIDsMap[analysisResultID] = append(reagentIDsMap[analysisResultID], reagentID)
		}
		return nil
	})

	return reagentIDsMap, err
}

func (r *analysisRepository) getReagentControlResultRelations(ctx context.Context, reagentIDs []uuid.UUID) (map[uuid.UUID][]uuid.UUID, error) {
	controlResultIDsMap := make(map[uuid.UUID][]uuid.UUID)
	err := utils.Partition(len(reagentIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`SELECT reagent_id, control_result_id FROM %s.sk_reagent_control_result_relations srcrr WHERE srcrr.reagent_id IN (?);`, r.dbSchema)
		query, args, _ := sqlx.In(query, reagentIDs[low:high])
		query = r.db.Rebind(query)
		rows, err := r.db.QueryxContext(ctx, query, args...)
		if err != nil {
			if err == sql.ErrNoRows {
				log.Trace().Msg("No reagent control result relations")
				return nil
			}
			log.Error().Err(err).Msg(msgGetReagentControlResultRelationsFailed)
			return ErrGetReagentControlResultRelationsFailed
		}
		defer rows.Close()

		for rows.Next() {
			var reagentID, controlResultID uuid.UUID
			err = rows.Scan(&reagentID, &controlResultID)
			if err != nil {
				log.Error().Err(err).Msg(msgGetReagentControlResultRelationsFailed)
				return ErrGetReagentControlResultRelationsFailed
			}

			controlResultIDsMap[reagentID] = append(controlResultIDsMap[reagentID], controlResultID)
		}
		return nil
	})

	return controlResultIDsMap, err
}

func (r *analysisRepository) getAnalysisResultControlResultRelations(ctx context.Context, analysisResultIDs []uuid.UUID) (map[uuid.UUID][]uuid.UUID, error) {
	controlResultIDsMap := make(map[uuid.UUID][]uuid.UUID)
	err := utils.Partition(len(analysisResultIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`SELECT analysis_result_id, control_result_id FROM %s.sk_analysis_result_control_result_relations sarcrr WHERE sarcrr.analysis_result_id IN (?);`, r.dbSchema)
		query, args, _ := sqlx.In(query, analysisResultIDs[low:high])
		query = r.db.Rebind(query)
		rows, err := r.db.QueryxContext(ctx, query, args...)
		if err != nil {
			if err == sql.ErrNoRows {
				log.Trace().Msg("No analysis result control result relations")
				return nil
			}
			log.Error().Err(err).Msg(msgGetAnalysisResultControlResultRelationsFailed)
			return ErrGetAnalysisResultControlResultRelationsFailed
		}
		defer rows.Close()

		for rows.Next() {
			var analysisResultID, controlResultID uuid.UUID
			err = rows.Scan(&analysisResultID, &controlResultID)
			if err != nil {
				log.Error().Err(err).Msg(msgGetAnalysisResultControlResultRelationsFailed)
				return ErrGetAnalysisResultControlResultRelationsFailed
			}

			controlResultIDsMap[analysisResultID] = append(controlResultIDsMap[analysisResultID], controlResultID)
		}
		return nil
	})

	return controlResultIDsMap, err
}

const reagentBatchSize = 5000

func (r *analysisRepository) CreateReagentBatch(ctx context.Context, reagents []Reagent) ([]Reagent, error) {
	reagentDAOs := make([]reagentDAO, 0)
	rDAOs := make([]reagentDAO, len(reagents))
	for i := range reagents {
		rDAOs[i] = convertReagentToDAO(reagents[i])
	}

	reagentDAOs = append(reagentDAOs, rDAOs...)

	reagentIDs, err := r.createReagents(ctx, reagentDAOs)
	if err != nil {
		return nil, err
	}

	for i := range reagents {
		reagents[i].ID = reagentIDs[i]
	}

	return reagents, nil
}

func (r *analysisRepository) createReagents(ctx context.Context, reagentDAOs []reagentDAO) ([]uuid.UUID, error) {
	if len(reagentDAOs) == 0 {
		return nil, nil
	}
	reagentsToSave := make([]reagentDAO, 0)
	uniqueReagentToIncomingIndexMap := make(map[string]int)
	reagentsToInsertCounter := 0
	for i := range reagentDAOs {
		if reagentDAOs[i].ID == uuid.Nil {
			reagentDAOs[i].ID = uuid.New()
		}
		if _, ok := uniqueReagentToIncomingIndexMap[getUniqueReagentString(reagentDAOs[i])]; !ok {
			uniqueReagentToIncomingIndexMap[getUniqueReagentString(reagentDAOs[i])] = reagentsToInsertCounter
			reagentsToSave = append(reagentsToSave, reagentDAOs[i])
			reagentsToInsertCounter++
		}
	}
	insertedIds := make([]uuid.UUID, 0)

	err := utils.Partition(len(reagentsToSave), reagentBatchSize, func(low int, high int) error {
		query := fmt.Sprintf(`INSERT INTO %s.sk_reagents(id, manufacturer, serial, lot_no, type, name)
		VALUES(:id, :manufacturer, :serial, :lot_no, :type, :name)
		ON CONFLICT (manufacturer, serial, lot_no, name) DO UPDATE SET manufacturer = excluded.manufacturer RETURNING id;`, r.dbSchema)

		rows, err := r.db.NamedQueryContext(ctx, query, reagentsToSave[low:high])
		if err != nil {
			log.Error().Err(err).Msg(msgCreateReagentFailed)
			return ErrCreateReagentFailed
		}
		defer rows.Close()

		for rows.Next() {
			var id uuid.UUID
			err = rows.Scan(&id)
			if err != nil {
				log.Error().Err(err).Msg(msgCreateReagentFailed)
				return ErrCreateReagentFailed
			}
			insertedIds = append(insertedIds, id)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	returnIds := make([]uuid.UUID, 0)
	for _, reagent := range reagentDAOs {
		if _, ok := uniqueReagentToIncomingIndexMap[getUniqueReagentString(reagent)]; !ok {
			log.Error().
				Err(err).
				Interface("reagents", reagentDAOs).
				Msg(msgCreateReagentFailed)
			return nil, ErrCreateReagentFailed
		}
		returnIds = append(returnIds, insertedIds[uniqueReagentToIncomingIndexMap[getUniqueReagentString(reagent)]])
	}

	return returnIds, nil
}

func getUniqueReagentString(reagent reagentDAO) string {
	return fmt.Sprintf("%s%s%s", reagent.Manufacturer, reagent.LotNo, reagent.SerialNumber)
}

const controlResultBatchSize = 5000

func (r *analysisRepository) CreateControlResults(ctx context.Context, controlResultsMap map[uuid.UUID]map[uuid.UUID][]ControlResult) (map[uuid.UUID]map[uuid.UUID][]uuid.UUID, error) {
	controlResults := make([]ControlResult, 0)
	idMap := make(map[uuid.UUID]map[uuid.UUID][]uuid.UUID)
	for analysisResultID, reagents := range controlResultsMap {
		idMap[analysisResultID] = make(map[uuid.UUID][]uuid.UUID)

		for reagentID, reagentControlResults := range reagents {
			idMap[analysisResultID][reagentID] = make([]uuid.UUID, 0)

			crs := make([]ControlResult, 0)
			for i := range reagentControlResults {
				var crID uuid.UUID

				if reagentControlResults[i].ID == uuid.Nil {
					crID = uuid.New()
					reagentControlResults[i].ID = crID

					crs = append(crs, reagentControlResults[i])
				} else {
					crID = reagentControlResults[i].ID
				}

				idMap[analysisResultID][reagentID] = append(idMap[analysisResultID][reagentID], crID)
			}

			controlResults = append(controlResults, crs...)
		}
	}

	_, err := r.createControlResults(ctx, controlResults)
	if err != nil {
		return nil, err
	}
	return idMap, nil
}

func (r *analysisRepository) createControlResults(ctx context.Context, controlResults []ControlResult) ([]uuid.UUID, error) {
	if len(controlResults) == 0 {
		return []uuid.UUID{}, nil
	}

	controlResultIds := make([]uuid.UUID, 0)

	for i := range controlResults {
		if (controlResults[i].ID == uuid.UUID{}) || (controlResults[i].ID == uuid.Nil) {
			controlResults[i].ID = uuid.New()
		}
		controlResultIds = append(controlResultIds, controlResults[i].ID)
	}

	err := utils.Partition(len(controlResults), controlResultBatchSize, func(low int, high int) error {
		query := fmt.Sprintf(`INSERT INTO %s.sk_control_results(id, sample_code, analyte_mapping_id, instrument_id, expected_control_result_id, is_valid, is_compared_to_expected_result, result, examined_at)
		VALUES(:id, :sample_code, :analyte_mapping_id, :instrument_id, :expected_control_result_id, :is_valid, :is_compared_to_expected_result, :result, :examined_at)`, r.dbSchema)

		_, err := r.db.NamedExecContext(ctx, query, convertControlResultsToDAO(controlResults[low:high]))
		if err != nil {
			log.Error().Err(err).Msg(msgCreateControlResultFailed)
			if IsErrorCode(err, ForeignKeyViolationErrorCode) {
				return ErrAnalyteMappingNotFound
			}
			return ErrCreateControlResultFailed
		}
		return nil
	})

	extraValueDAOs := make([]controlResultExtraValueDAO, 0)
	warningDAOs := make([]controlResultWarningDAO, 0)

	for i := range controlResults {
		for j := range controlResults[i].ExtraValues {
			extraValueDAOs = append(extraValueDAOs, controlResultExtraValueDAO{
				ID:              uuid.New(),
				ControlResultId: controlResults[i].ID,
				Key:             controlResults[i].ExtraValues[j].Key,
				Value:           controlResults[i].ExtraValues[j].Value,
			})
		}
		for k := range controlResults[i].Warnings {
			warningDAOs = append(warningDAOs, controlResultWarningDAO{
				ID:              uuid.New(),
				ControlResultId: controlResults[i].ID,
				Warning:         controlResults[i].Warnings[k],
			})
		}
	}

	err = r.createControlResultExtraValues(ctx, extraValueDAOs)
	if err != nil {
		return []uuid.UUID{}, err
	}

	for i := range controlResults {
		quantitativeChannelResultsMap := make(map[uuid.UUID]map[string]string)
		channelImagesMap := make(map[uuid.NullUUID][]Image)
		channelResultIDs := make([]uuid.UUID, 0)
		channelResultIDs, err = r.createControlResultChannelResults(ctx, controlResults[i].ChannelResults, controlResults[i].ID)
		if err != nil {
			return []uuid.UUID{}, err
		}
		for j := range controlResults[i].ChannelResults {
			if len(channelResultIDs) > j {
				controlResults[i].ChannelResults[j].ID = channelResultIDs[j]
			}
			quantitativeChannelResultsMap[channelResultIDs[j]] = controlResults[i].ChannelResults[j].QuantitativeResults
			channelImagesMap[uuid.NullUUID{UUID: channelResultIDs[j], Valid: true}] = controlResults[i].ChannelResults[j].Images
		}

		err = r.CreateControlResultChannelResultQuantitativeValues(ctx, quantitativeChannelResultsMap)
		if err != nil {
			return []uuid.UUID{}, err
		}
	}

	err = r.createControlResultWarnings(ctx, warningDAOs)
	if err != nil {
		return []uuid.UUID{}, err
	}

	return controlResultIds, nil
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
			log.Error().Err(err).Msg(msgGetImagesFailed)
			return ErrGetImagesFailed
		}

		defer rows.Close()
		for rows.Next() {
			image := imageDAO{}
			err := rows.StructScan(&image)
			if err != nil {
				log.Error().Err(err).Msg(msgGetImagesFailed)
				return ErrGetImagesFailed
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
	err := utils.Partition(len(images), imageBatchSize, func(low int, high int) error {
		idsPart, err := r.saveImages(ctx, images[low:high])
		if err != nil {
			return err
		}
		ids = append(ids, idsPart...)
		return nil
	})
	if err != nil {
		return nil, err
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
		log.Error().Err(err).Msg(msgCreateAnalysisResultImagesFailed)
		return nil, ErrCreateAnalysisResultImagesFailed
	}
	return ids, nil
}

func (r *analysisRepository) SaveControlResultImages(ctx context.Context, images []controlResultImageDAO) ([]uuid.UUID, error) {
	ids, err := r.saveControlResultImages(ctx, images)
	if err != nil {
		return nil, err
	}

	return ids, nil
}

func (r *analysisRepository) saveControlResultImages(ctx context.Context, controlResultImages []controlResultImageDAO) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, len(controlResultImages))
	if len(controlResultImages) == 0 {
		return ids, nil
	}
	for i := range controlResultImages {
		controlResultImages[i].ID = uuid.New()
		ids[i] = controlResultImages[i].ID
	}

	err := utils.Partition(len(controlResultImages), imageBatchSize, func(low int, high int) error {
		query := fmt.Sprintf(`INSERT INTO %s.sk_analysis_result_images(id, analysis_result_id, channel_result_id, "name", description, image_bytes, dea_image_id, uploaded_to_dea_at)
		VALUES(:id, :analysis_result_id, :channel_result_id, :name, :description, :image_bytes, :dea_image_id, :uploaded_to_dea_at);`, r.dbSchema)
		_, err := r.db.NamedExecContext(ctx, query, controlResultImages[low:high])
		if err != nil {
			log.Error().Err(err).Msg(msgCreateControlResultImagesFailed)
			return ErrCreateControlResultImagesFailed
		}
		return nil
	})
	if err != nil {
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
			log.Error().Err(err).Msg(msgGetAnalysisResultWarningsFailed)
			return ErrGetAnalysisResultWarningsFailed
		}
		defer rows.Close()

		for rows.Next() {
			warning := warningDAO{}
			err := rows.StructScan(&warning)
			if err != nil {
				log.Error().Err(err).Msg(msgGetAnalysisResultWarningsFailed)
				return ErrGetAnalysisResultWarningsFailed
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

	err := r.createWarnings(ctx, warningDAOs)
	if err != nil {
		return err
	}

	return nil
}

func (r *analysisRepository) createWarnings(ctx context.Context, warningDAOs []warningDAO) error {
	if len(warningDAOs) == 0 {
		return nil
	}

	err := utils.Partition(len(warningDAOs), warningBatchCount, func(low int, high int) error {
		query := fmt.Sprintf(`INSERT INTO %s.sk_analysis_result_warnings(id, analysis_result_id, warning)
		VALUES(:id, :analysis_result_id, :warning)`, r.dbSchema)
		_, err := r.db.NamedExecContext(ctx, query, warningDAOs[low:high])
		if err != nil {
			log.Error().Err(err).Msg(msgCreateWarningsFailed)
			return ErrCreateWarningsFailed
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (r *analysisRepository) createControlResultWarnings(ctx context.Context, warnings []controlResultWarningDAO) error {
	err := utils.Partition(len(warnings), warningBatchCount, func(low int, high int) error {
		query := fmt.Sprintf(`INSERT INTO %s.sk_control_result_warnings(id, control_result_id, warning)
		VALUES(:id, :analysis_result_id, :warning)`, r.dbSchema)
		_, err := r.db.NamedExecContext(ctx, query, warnings[low:high])
		if err != nil {
			log.Error().Err(err).Msg(msgCreateControlResultWarningsFailed)
			return ErrCreateControlResultWarningsFailed
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (r *analysisRepository) CreateAnalysisResultQueueItem(ctx context.Context, analysisResults []AnalysisResult) (uuid.UUID, error) {
	log.Trace().Int("analysisResultCount", len(analysisResults)).Msg("Creating analysis result queue item")

	if len(analysisResults) < 1 {
		return uuid.Nil, nil
	}
	analysisResultSetTOs, err := convertAnalysisResultsToTOs(analysisResults)
	if err != nil {
		log.Error().Err(err).Msg(msgConvertAnalysisResultsFailed)
		return uuid.Nil, ErrConvertAnalysisResultsFailed
	}
	jsonData, err := json.Marshal(analysisResultSetTOs)
	if err != nil {
		log.Error().Err(err).
			Interface("analysisResultSets", analysisResultSetTOs).
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
		log.Error().Err(err).Msg(msgCreateCerberusQueueItemFailed)
		return uuid.Nil, ErrCreateCerberusQueueItemFailed
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
		log.Error().Err(err).Msg(msgDeleteOldCerberusQueueItemsFailed)
		return 0, ErrDeleteOldCerberusQueueItemsFailed
	}

	return result.RowsAffected()
}

func (r *analysisRepository) DeleteOldAnalysisRequestsWithTx(ctx context.Context, cleanupDays, limit int, tx db.DbConnection) (int64, error) {
	txR := *r
	txR.db = tx
	query := fmt.Sprintf(`SELECT id, sample_code FROM %s.sk_analysis_requests WHERE valid_until_time < (current_date - make_interval(days := $1)) AND is_processed LIMIT $2;`, r.dbSchema)
	rows, err := txR.db.QueryxContext(ctx, query, cleanupDays, limit)
	if err != nil {
		log.Error().Err(err).Msg(msgDeleteOldAnalysisRequestsFailed)
		return 0, ErrDeleteOldAnalysisRequestsFailed
	}
	defer rows.Close()
	sampleCodes := make([]string, 0)
	analysisRequestIDs := make([]uuid.UUID, 0)
	for rows.Next() {
		var id uuid.UUID
		var sampleCode string
		err = rows.Scan(&id, &sampleCode)
		if err != nil {
			log.Error().Err(err).Msg(msgDeleteOldAnalysisRequestsFailed)
			return 0, ErrDeleteOldAnalysisRequestsFailed
		}
		analysisRequestIDs = append(analysisRequestIDs, id)
		sampleCodes = append(sampleCodes, sampleCode)
	}
	if len(analysisRequestIDs) == 0 {
		return 0, err
	}

	err = txR.deleteAnalysisRequestExtraValuesByAnalysisRequestIDs(ctx, analysisRequestIDs)
	if err != nil {
		return 0, err
	}
	err = txR.deleteSubjectInfosByAnalysisRequestIDs(ctx, analysisRequestIDs)
	if err != nil {
		return 0, err
	}
	err = txR.deleteAnalysisRequestInstrumentTransmissionsByAnalysisRequestIDs(ctx, analysisRequestIDs)
	if err != nil {
		return 0, err
	}

	err = txR.deleteAppliedSortingRuleTargetsBySampleCodes(ctx, sampleCodes)
	if err != nil {
		return 0, err
	}

	query = fmt.Sprintf("DELETE FROM %s.sk_analysis_requests WHERE id IN (?);", r.dbSchema)
	query, args, _ := sqlx.In(query, analysisRequestIDs)
	query = txR.db.Rebind(query)
	result, err := txR.db.ExecContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgDeleteOldAnalysisRequestsFailed)
		return 0, ErrDeleteOldAnalysisRequestsFailed
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
			log.Error().Err(err).Msg(msgDeleteAnalysisRequestExtraValuesByAnalysisRequestIdFailed)
			return ErrDeleteAnalysisRequestExtraValuesByAnalysisRequestIdFailed
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
			log.Error().Err(err).Msg(msgDeleteSubjectInfosByAnalysisRequestIdFailed)
			return ErrDeleteSubjectInfosByAnalysisRequestIdFailed
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
			log.Error().Err(err).Msg(msgDeleteAnalysisRequestInstrumentTransmissionsFailed)
			return ErrDeleteAnalysisRequestInstrumentTransmissionsFailed
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
			log.Error().Err(err).Msg(msgDeleteAppliedSortingRuleTargetsBySampleCodesFailed)
			return ErrDeleteAppliedSortingRuleTargetsBySampleCodesFailed
		}

		return nil
	})

	return err
}

func (r *analysisRepository) DeleteOldAnalysisResultsWithTx(ctx context.Context, cleanupDays, limit int, tx db.DbConnection) (int64, error) {
	txR := *r
	txR.db = tx

	query := fmt.Sprintf(`SELECT id FROM %s.sk_analysis_results WHERE GREATEST(valid_until, created_at + INTERVAL '14 DAYS') < (current_date - make_interval(days := $1))  AND is_processed LIMIT $2;`, r.dbSchema)
	rows, err := txR.db.QueryxContext(ctx, query, cleanupDays, limit)
	if err != nil {
		log.Error().Err(err).Msg(msgDeleteOldAnalysisResultsFailed)
		return 0, ErrDeleteOldAnalysisResultsFailed
	}
	defer rows.Close()

	analysisResultIDs := make([]uuid.UUID, 0)
	for rows.Next() {
		var id uuid.UUID
		err = rows.Scan(&id)
		if err != nil {
			log.Error().Err(err).Msg(msgDeleteOldAnalysisResultsFailed)
			return 0, ErrDeleteOldAnalysisResultsFailed
		}
		analysisResultIDs = append(analysisResultIDs, id)
	}
	if len(analysisResultIDs) == 0 {
		return 0, nil
	}

	err = txR.deleteImagesByAnalysisResultIDs(ctx, analysisResultIDs)
	if err != nil {
		return 0, err
	}
	err = txR.deleteChannelResultsByAnalysisResultIDs(ctx, analysisResultIDs)
	if err != nil {
		return 0, err
	}
	err = txR.deleteAnalysisResultWarningsByAnalysisResultIDs(ctx, analysisResultIDs)
	if err != nil {
		return 0, err
	}
	err = txR.deleteAnalysisResultExtraValuesByAnalysisResultIDs(ctx, analysisResultIDs)
	if err != nil {
		return 0, err
	}
	err = txR.deleteReagentInfosByAnalysisResultIDs(ctx, analysisResultIDs)
	if err != nil {
		return 0, err
	}
	query = fmt.Sprintf(`DELETE FROM %s.sk_analysis_results WHERE id IN (?);`, r.dbSchema)
	query, args, _ := sqlx.In(query, analysisResultIDs)
	query = txR.db.Rebind(query)
	result, err := txR.db.ExecContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgDeleteOldAnalysisResultsFailed)
		return 0, ErrDeleteOldAnalysisResultsFailed
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
			log.Error().Err(err).Msg(msgDeleteChannelResultsByAnalysisResultIdsFailed)
			return ErrDeleteChannelResultsByAnalysisResultIdsFailed
		}
		defer rows.Close()

		for rows.Next() {
			var id uuid.UUID
			err = rows.Scan(&id)
			if err != nil {
				log.Error().Err(err).Msg(msgDeleteChannelResultsByAnalysisResultIdsFailed)
				return ErrDeleteChannelResultsByAnalysisResultIdsFailed
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
			log.Error().Err(err).Msg(msgDeleteChannelResultsByAnalysisResultIdsFailed)
			return ErrDeleteChannelResultsByAnalysisResultIdsFailed
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
			log.Error().Err(err).Msg(msgDeleteChannelResultsByAnalysisResultIdsFailed)
			return ErrDeleteChannelResultsByAnalysisResultIdsFailed
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
			log.Error().Err(err).Msg(msgDeleteAnalysisResultWarningsFailed)
			return ErrDeleteAnalysisResultWarningsFailed
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
			log.Error().Err(err).Msg(msgDeleteImagesByAnalysisResultIdsFailed)
			return ErrDeleteImagesByAnalysisResultIdsFailed
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
			log.Error().Err(err).Msg(msgDeleteAnalysisResultExtraValuesByAnalysisResultIdsFailed)
			return ErrDeleteAnalysisResultExtraValuesByAnalysisResultIdsFailed
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
			log.Error().Err(err).Msg(msgDeleteReagentInfosByAnalysisResultIdsFailed)
			return ErrDeleteReagentInfosByAnalysisResultIdsFailed
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
		ID:                       ar.ID,
		WorkingItemID:            ar.AnalysisRequest.WorkItemID,
		DEARawMessageID:          ar.DEARawMessageID.UUID,
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
		Reagents:                 make([]ReagentTO, 0),
		Images:                   make([]ImageTO, 0),
		WarnFlag:                 ar.WarnFlag,
		Warnings:                 ar.Warnings,
	}
	if !ar.DEARawMessageID.Valid {
		return analysisResultTO, ErrMissingDEARawMessageID
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

	for _, ri := range ar.Reagents {
		reagentTO := ReagentTO{
			ID:           ri.ID,
			Manufacturer: ri.Manufacturer,
			SerialNo:     ri.SerialNumber,
			LotNo:        ri.LotNo,
			Name:         ri.Name,
			Type:         ri.Type,
		}

		if len(ri.ControlResults) > 0 {
			reagentTO.ControlResults = convertControlResultsToTOs(ri.ControlResults)
		}

		analysisResultTO.Reagents = append(analysisResultTO.Reagents, reagentTO)
	}

	analysisResultTO.ControlResults = convertControlResultsToTOs(ar.ControlResults)

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
		log.Error().Err(err).Msg(msgGetCerberusQueueItemsFailed)
		return []CerberusQueueItem{}, ErrGetCerberusQueueItemsFailed
	}
	defer rows.Close()

	cerberusQueueItems := make([]CerberusQueueItem, 0)
	for rows.Next() {
		queueItemDAO := cerberusQueueItemDAO{}
		err = rows.StructScan(&queueItemDAO)
		if err != nil {
			log.Error().Err(err).Msg(msgGetCerberusQueueItemsFailed)
			return []CerberusQueueItem{}, ErrGetCerberusQueueItemsFailed
		}

		cerberusQueueItems = append(cerberusQueueItems, convertCerberusQueueItemDAOToCerberusQueueItem(queueItemDAO))
	}

	return cerberusQueueItems, err
}

func (r *analysisRepository) UpdateCerberusQueueItemStatus(ctx context.Context, queueItem CerberusQueueItem) error {
	query := fmt.Sprintf(`UPDATE %s.sk_cerberus_queue_items
			SET last_http_status = :last_http_status, last_error = :last_error, last_error_at = :last_error_at, trial_count = trial_count + 1, retry_not_before = :retry_not_before, raw_response = :raw_response, response_json_message = :response_json_message
			WHERE queue_item_id = :queue_item_id;`, r.dbSchema)
	_, err := r.db.NamedExecContext(ctx, query, convertCerberusQueueItemToCerberusQueueItemDAO(queueItem))
	if err != nil {
		log.Error().Err(err).Msg(msgUpdateCerberusQueueItemStatusFailed)
		return ErrUpdateCerberusQueueItemStatusFailed
	}
	return nil
}

func (r *analysisRepository) GetStuckImageIDsForDEA(ctx context.Context) ([]uuid.UUID, error) {
	query := fmt.Sprintf(`SELECT sari.id FROM %s.sk_analysis_result_images sari WHERE sari.dea_image_id IS NULL AND sari.image_bytes IS NOT NULL;`, r.dbSchema)

	imageIDs := make([]uuid.UUID, 0)

	rows, err := r.db.QueryxContext(ctx, query)
	if err != nil {
		log.Error().Err(err).Msg(msgGetStuckImagesFailed)
		return imageIDs, ErrGetStuckImagesFailed
	}
	defer rows.Close()

	for rows.Next() {
		var id uuid.UUID
		err := rows.Scan(&id)
		if err != nil {
			log.Error().Err(err).Msg(msgScanStuckImageIdFailed)
			return imageIDs, ErrScanStuckImageIdFailed
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
		log.Error().Err(err).Msg(msgGetStuckImagesFailed)
		return imageIDs, ErrGetStuckImagesFailed
	}
	defer rows.Close()

	for rows.Next() {
		var id uuid.UUID
		err := rows.Scan(&id)
		if err != nil {
			log.Error().Err(err).Msg(msgScanStuckImageIdFailed)
			return imageIDs, ErrScanStuckImageIdFailed
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
		log.Error().Err(err).Msg(msgGetStuckImagesFailed)
		return []imageDAO{}, ErrGetStuckImagesFailed
	}
	defer rows.Close()

	images := make([]imageDAO, 0)
	for rows.Next() {
		image := imageDAO{}
		err := rows.StructScan(&image)
		if err != nil {
			log.Error().Err(err).Msg(msgScanStuckImageDataFailed)
			return []imageDAO{}, ErrScanStuckImageDataFailed
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
		log.Error().Err(err).Msg(msgGetStuckImagesFailed)
		return []cerberusImageDAO{}, ErrGetStuckImagesFailed
	}
	defer rows.Close()

	images := make([]cerberusImageDAO, 0)
	for rows.Next() {
		image := cerberusImageDAO{}
		err := rows.StructScan(&image)
		if err != nil {
			log.Error().Err(err).Msg(msgScanStuckImageDataFailed)
			return []cerberusImageDAO{}, ErrScanStuckImageDataFailed
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
		log.Error().Err(err).Msg(msgSaveDEAImageIDFailed)
		return ErrSaveDEAImageIDFailed
	}

	return nil
}

func (r *analysisRepository) IncreaseImageUploadRetryCount(ctx context.Context, imageID uuid.UUID, error string) error {
	query := fmt.Sprintf(`UPDATE %s.sk_analysis_result_images SET upload_retry_count = upload_retry_count + 1, upload_error = $2 WHERE id = $1;`, r.dbSchema)

	_, err := r.db.ExecContext(ctx, query, imageID, error)
	if err != nil {
		log.Error().Err(err).Msg(msgIncreaseImageUploadRetryCountFailed)
		return ErrIncreaseImageUploadRetryCountFailed
	}

	return nil
}

func (r *analysisRepository) MarkImagesAsSyncedToCerberus(ctx context.Context, ids []uuid.UUID) error {
	query := fmt.Sprintf(`UPDATE %s.sk_analysis_result_images SET sync_to_cerberus_needed = false WHERE id IN (?);`, r.dbSchema)

	query, args, _ := sqlx.In(query, ids)
	query = r.db.Rebind(query)

	_, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgMarkImagesAsSyncedToCerberusFailed)
		return ErrMarkImagesAsSyncedToCerberusFailed
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
		log.Error().Err(err).Msg(msgGetUnprocessedAnalysisRequestsFailed)
		return []AnalysisRequest{}, ErrGetUnprocessedAnalysisRequestsFailed
	}
	defer rows.Close()

	analysisRequests := make([]AnalysisRequest, 0)
	for rows.Next() {
		request := analysisRequestDAO{}
		err := rows.StructScan(&request)
		if err != nil {
			log.Error().Err(err).Msg(msgGetUnprocessedAnalysisRequestsFailed)
			return []AnalysisRequest{}, ErrGetUnprocessedAnalysisRequestsFailed
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
		log.Error().Err(err).Msg(msgUnprocessedAnalysisResultIdsFailed)
		return []uuid.UUID{}, ErrUnprocessedAnalysisResultIdsFailed
	}
	defer rows.Close()

	analysisResultIDs := make([]uuid.UUID, 0)
	for rows.Next() {
		var id uuid.UUID
		err := rows.Scan(&id)
		if err != nil {
			log.Error().Err(err).Msg(msgUnprocessedAnalysisResultIdsFailed)
			return []uuid.UUID{}, ErrUnprocessedAnalysisResultIdsFailed
		}

		analysisResultIDs = append(analysisResultIDs, id)
	}

	return analysisResultIDs, err
}

func (r *analysisRepository) MarkAnalysisRequestsAsProcessed(ctx context.Context, analysisRequestIDs []uuid.UUID) error {
	err := utils.Partition(len(analysisRequestIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`UPDATE %s.sk_analysis_requests SET is_processed = true WHERE id IN (?);`, r.dbSchema)

		query, args, _ := sqlx.In(query, analysisRequestIDs[low:high])
		query = r.db.Rebind(query)

		_, err := r.db.ExecContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg(msgMarkAnalysisRequestsAsProcessedFailed)
			return ErrMarkAnalysisRequestsAsProcessedFailed
		}

		return nil
	})

	return err
}

func (r *analysisRepository) MarkAnalysisResultsAsProcessed(ctx context.Context, analysisRequestIDs []uuid.UUID) error {
	err := utils.Partition(len(analysisRequestIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`UPDATE %s.sk_analysis_results SET is_processed = true WHERE id IN (?);`, r.dbSchema)

		query, args, _ := sqlx.In(query, analysisRequestIDs[low:high])
		query = r.db.Rebind(query)

		_, err := r.db.ExecContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg(msgMarkAnalysisResultsAsProcessedFailed)
			return ErrMarkAnalysisResultsAsProcessedFailed
		}
		return nil
	})

	return err
}

func (r *analysisRepository) CreateReagents(ctx context.Context, reagents []Reagent) ([]uuid.UUID, error) {
	if len(reagents) == 0 {
		return []uuid.UUID{}, nil
	}

	reagentDAOs := make([]reagentDAO, len(reagents))

	for i := range reagents {
		reagentDAOs[i] = convertReagentToDAO(reagents[i])
	}

	reagentIDs, err := r.createReagents(ctx, reagentDAOs)
	if err != nil {
		return nil, err
	}

	return reagentIDs, nil
}

func (r *analysisRepository) GetReagentsByIDs(ctx context.Context, reagentIDs []uuid.UUID) (map[uuid.UUID]Reagent, error) {
	reagents := make(map[uuid.UUID]Reagent)

	if len(reagentIDs) == 0 {
		return reagents, nil
	}

	query := `SELECT * FROM %schema_name%.sk_reagents sr WHERE sr.id IN (?);`

	query = strings.ReplaceAll(query, "%schema_name%", r.dbSchema)

	query, args, _ := sqlx.In(query, reagentIDs)
	query = r.db.Rebind(query)

	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgGetReagentsByIDsFailed)
		return reagents, ErrGetReagentsByIDsFailed
	}

	defer rows.Close()

	for rows.Next() {
		reagent := reagentDAO{}
		err := rows.StructScan(&reagent)
		if err != nil {
			log.Error().Err(err).Msg(msgGetReagentsByIDsFailed)
			return reagents, ErrGetReagentsByIDsFailed
		}

		reagents[reagent.ID] = convertReagentDAOToReagent(reagent)
	}

	return reagents, err
}

func (r *analysisRepository) UpdateAnalysisResultDEARawMessageID(ctx context.Context, analysisResultID uuid.UUID, deaRawMessageID uuid.NullUUID) error {
	query := fmt.Sprintf(`UPDATE %s.sk_analysis_results SET dea_raw_message_id = $1 WHERE id = $2;`, r.dbSchema)
	_, err := r.db.ExecContext(ctx, query, deaRawMessageID, analysisResultID)
	if err != nil {
		log.Error().Err(err).Msg(msgUpdateAnalysisResultDEARawMessageIDFailed)
		return ErrUpdateAnalysisResultDEARawMessageIDFailed
	}

	return nil
}

func (r *analysisRepository) CreateControlResultBatch(ctx context.Context, controlResults []ControlResult) ([]ControlResult, error) {
	crs := make([]ControlResult, 0)
	modifiedControlResults := make([]ControlResult, 0)
	for i := range controlResults {
		var crID uuid.UUID

		if controlResults[i].ID == uuid.Nil {
			crID = uuid.New()
			controlResults[i].ID = crID

			crs = append(crs, controlResults[i])
		}

		modifiedControlResults = append(modifiedControlResults, controlResults[i])
	}

	_, err := r.createControlResults(ctx, crs)
	if err != nil {
		return nil, err
	}

	return modifiedControlResults, nil
}

func (r *analysisRepository) UpdateControlResultBatch(ctx context.Context, controlResults []ControlResult) error {
	if len(controlResults) == 0 {
		return nil
	}
	controlResultDAOs := convertControlResultsToDAO(controlResults)
	query := fmt.Sprintf(`UPDATE %s.sk_control_results SET expected_control_result_id = :expected_control_result_id, is_valid = :is_valid, is_compared_to_expected_result = :is_compared_to_expected_result WHERE id = :id;`, r.dbSchema)
	for i := range controlResultDAOs {
		_, err := r.db.NamedExecContext(ctx, query, controlResultDAOs[i])
		if err != nil {
			log.Error().Err(err).Msg(msgUpdateControlResultFailed)
			if IsErrorCode(err, ForeignKeyViolationErrorCode) {
				return ErrAnalyteMappingNotFound
			}
			return ErrUpdateControlResultFailed
		}
	}
	return nil
}

func (r *analysisRepository) GetControlResultsByIDs(ctx context.Context, controlResultIDs []uuid.UUID) (map[uuid.UUID]ControlResult, error) {
	if len(controlResultIDs) == 0 {
		return map[uuid.UUID]ControlResult{}, nil
	}

	query := `SELECT scr.id, scr.analyte_mapping_id, scr.instrument_id, scr.sample_code, scr.expected_control_result_id, scr.is_valid, scr.is_compared_to_expected_result, scr."result", scr.examined_at, scr.created_at,
					sam.id AS "analyte_mapping.id", sam.instrument_id AS "analyte_mapping.instrument_id", sam.instrument_analyte AS "analyte_mapping.instrument_analyte", sam.analyte_id AS "analyte_mapping.analyte_id", sam.result_type AS "analyte_mapping.result_type", 
					sam.control_result_required AS "analyte_mapping.control_result_required", sam.created_at AS "analyte_mapping.created_at", sam.modified_at AS "analyte_mapping.modified_at"
			FROM %schema_name%.sk_control_results scr
			INNER JOIN %schema_name%.sk_analyte_mappings sam ON scr.analyte_mapping_id = sam.id
			WHERE scr.id IN (?);`

	query = strings.ReplaceAll(query, "%schema_name%", r.dbSchema)

	query, args, _ := sqlx.In(query, controlResultIDs)
	query = r.db.Rebind(query)

	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Error().Err(err).Msg(msgGetControlResultsByIDsFailed)
		return map[uuid.UUID]ControlResult{}, ErrGetControlResultsByIDsFailed
	}
	defer rows.Close()

	controlResultDAOs := make([]controlResultDAO, 0)
	for rows.Next() {
		controlResultDao := controlResultDAO{}
		err := rows.StructScan(&controlResultDao)
		if err != nil {
			log.Error().Err(err).Msg(msgGetControlResultsByIDsFailed)
			return map[uuid.UUID]ControlResult{}, ErrGetControlResultsByIDsFailed
		}

		controlResultDAOs = append(controlResultDAOs, controlResultDao)
	}

	controlResults, err := r.gatherAndAttachAllConnectedDataToControlResults(ctx, controlResultDAOs)

	controlResultsMap := make(map[uuid.UUID]ControlResult)
	for i := range controlResults {
		controlResultsMap[controlResults[i].ID] = controlResults[i]
	}

	return controlResultsMap, err
}

func (r *analysisRepository) gatherAndAttachAllConnectedDataToControlResults(ctx context.Context, controlResultDAOs []controlResultDAO) ([]ControlResult, error) {
	controlResults := make([]ControlResult, 0)
	analyteMappingIDs := make([]uuid.UUID, 0)
	controlResultIDs := make([]uuid.UUID, 0)
	if len(controlResultDAOs) == 0 {
		return controlResults, nil
	}

	for i := range controlResultDAOs {
		analyteMappingIDs = append(analyteMappingIDs, controlResultDAOs[i].AnalyteMappingID)
		controlResultIDs = append(controlResultIDs, controlResultDAOs[i].ID)
	}

	extraValues, err := r.getControlResultExtraValues(ctx, controlResultIDs)
	if err != nil {
		return controlResults, err
	}

	imagesMap, err := r.getControlResultImages(ctx, controlResultIDs)
	if err != nil {
		return controlResults, err
	}

	warningsMap, err := r.getControlResultWarnings(ctx, controlResultIDs)
	if err != nil {
		return controlResults, err
	}

	channelResultsMap, err := r.getControlResultChannelResults(ctx, controlResultIDs)
	if err != nil {
		return controlResults, err
	}

	channelResultIDs := make([]uuid.UUID, 0)
	for _, channelResults := range channelResultsMap {
		for _, channelResult := range channelResults {
			channelResultIDs = append(channelResultIDs, channelResult.ID)
		}
	}

	quantitativeValuesByChannelResultID, err := r.getControlResultChannelResultQuantitativeValues(ctx, channelResultIDs)
	if err != nil {
		return controlResults, err
	}

	channelMappingsMap, err := r.getChannelMappings(ctx, analyteMappingIDs)
	if err != nil {
		return controlResults, err
	}
	resultMappingsMap, err := r.getResultMappings(ctx, analyteMappingIDs)
	if err != nil {
		return controlResults, err
	}

	expectedControlResultMappingsMap, err := r.getExpectedControlResultsByAnalyteMappingIds(ctx, analyteMappingIDs)
	if err != nil {
		return controlResults, err
	}

	for i := range controlResultDAOs {
		controlResultDAOs[i].ExtraValues = extraValues[controlResultDAOs[i].ID]
		controlResultDAOs[i].Warnings = warningsMap[controlResultDAOs[i].ID]

		for _, channelResult := range channelResultsMap[controlResultDAOs[i].ID] {
			channelResult.QuantitativeResults = quantitativeValuesByChannelResultID[channelResult.ID]

			for _, image := range imagesMap[controlResultDAOs[i].ID] {
				if !image.ChannelResultID.Valid || (image.ChannelResultID.Valid && image.ChannelResultID.UUID != channelResult.ID) {
					continue
				}
				channelResult.Images = append(channelResult.Images, image)
			}

			controlResultDAOs[i].ChannelResults = append(controlResultDAOs[i].ChannelResults, channelResult)
		}

		controlResult := convertControlResultDAOToControlResult(controlResultDAOs[i])
		controlResult.AnalyteMapping.ChannelMappings = channelMappingsMap[controlResultDAOs[i].AnalyteMappingID]
		controlResult.AnalyteMapping.ResultMappings = resultMappingsMap[controlResultDAOs[i].AnalyteMappingID]
		controlResult.AnalyteMapping.ExpectedControlResults = expectedControlResultMappingsMap[controlResultDAOs[i].AnalyteMappingID]

		controlResults = append(controlResults, controlResult)
	}

	return controlResults, nil
}

func (r *analysisRepository) getControlResultExtraValues(ctx context.Context, controlResultIDs []uuid.UUID) (map[uuid.UUID][]controlResultExtraValueDAO, error) {
	extraValuesMap := make(map[uuid.UUID][]controlResultExtraValueDAO)
	err := utils.Partition(len(controlResultIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`SELECT scre.id, scre.control_result_id, scre."key", scre."value"
		FROM %s.sk_control_result_extravalues scre WHERE scre.control_result_id IN (?);`, r.dbSchema)
		query, args, _ := sqlx.In(query, controlResultIDs[low:high])
		query = r.db.Rebind(query)
		rows, err := r.db.QueryxContext(ctx, query, args...)
		if err != nil {
			if err == sql.ErrNoRows {
				log.Trace().Msg("No control result extra value")
				return nil
			}
			log.Error().Err(err).Msg(msgGetControlResultExtraValuesFailed)
			return ErrGetControlResultExtraValuesFailed
		}
		defer rows.Close()

		for rows.Next() {
			extraValue := controlResultExtraValueDAO{}
			err = rows.StructScan(&extraValue)
			if err != nil {
				log.Error().Err(err).Msg(msgGetControlResultExtraValuesFailed)
				return ErrGetControlResultExtraValuesFailed
			}

			extraValuesMap[extraValue.ControlResultId] = append(extraValuesMap[extraValue.ControlResultId], extraValue)
		}
		return nil
	})

	return extraValuesMap, err
}

func (r *analysisRepository) getControlResultWarnings(ctx context.Context, controlResultIDs []uuid.UUID) (map[uuid.UUID][]controlResultWarningDAO, error) {
	warningsMap := make(map[uuid.UUID][]controlResultWarningDAO)
	err := utils.Partition(len(controlResultIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`SELECT scrw.id, scrw.control_result_id, scrw.warning FROM %s.sk_control_result_warnings scrw WHERE scrw.control_result_id IN (?);`, r.dbSchema)
		query, args, _ := sqlx.In(query, controlResultIDs[low:high])
		query = r.db.Rebind(query)
		rows, err := r.db.QueryxContext(ctx, query, args...)
		if err != nil {
			if err == sql.ErrNoRows {
				log.Trace().Msg("No control result warnings")
				return nil
			}
			log.Error().Err(err).Msg(msgGetControlResultWarningsFailed)
			return ErrGetControlResultWarningsFailed
		}
		defer rows.Close()

		for rows.Next() {
			warning := controlResultWarningDAO{}
			err := rows.StructScan(&warning)
			if err != nil {
				log.Error().Err(err).Msg(msgGetControlResultWarningsFailed)
				return ErrGetControlResultWarningsFailed
			}

			warningsMap[warning.ControlResultId] = append(warningsMap[warning.ControlResultId], warning)
		}
		return nil
	})

	return warningsMap, err
}

func (r *analysisRepository) getControlResultChannelResults(ctx context.Context, controlResultIDs []uuid.UUID) (map[uuid.UUID][]controlResultChannelResultDAO, error) {
	channelResultsMap := make(map[uuid.UUID][]controlResultChannelResultDAO)
	err := utils.Partition(len(controlResultIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`SELECT scrcr.id, scrcr.control_result_id, scrcr.channel_id, scrcr.qualitative_result, scrcr.qualitative_result_edited
		FROM %s.sk_control_result_channel_results scrcr WHERE scrcr.control_result_id IN (?);`, r.dbSchema)
		query, args, _ := sqlx.In(query, controlResultIDs[low:high])
		query = r.db.Rebind(query)
		rows, err := r.db.QueryxContext(ctx, query, args...)
		if err != nil {
			if err == sql.ErrNoRows {
				log.Trace().Msg("No control result channel result")
				return nil
			}
			log.Error().Err(err).Msg(msgGetControlResultChannelResultsFailed)
			return ErrGetControlResultChannelResultsFailed
		}

		defer rows.Close()
		for rows.Next() {
			channelResult := controlResultChannelResultDAO{}
			err = rows.StructScan(&channelResult)
			if err != nil {
				log.Error().Err(err).Msg(msgGetControlResultChannelResultsFailed)
				return ErrGetControlResultChannelResultsFailed
			}

			channelResultsMap[channelResult.ControlResultId] = append(channelResultsMap[channelResult.ControlResultId], channelResult)
		}
		return nil
	})

	return channelResultsMap, err
}

func (r *analysisRepository) getControlResultChannelResultQuantitativeValues(ctx context.Context, channelResultIDs []uuid.UUID) (map[uuid.UUID][]quantitativeChannelResultDAO, error) {
	valuesByChannelResultID := make(map[uuid.UUID][]quantitativeChannelResultDAO)

	err := utils.Partition(len(channelResultIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`SELECT scrcrqv.id, scrcrqv.channel_result_id, scrcrqv.metric, scrcrqv."value"
		FROM %s.sk_control_result_channel_result_quantitative_values scrcrqv WHERE scrcrqv.channel_result_id IN (?);`, r.dbSchema)
		query, args, _ := sqlx.In(query, channelResultIDs[low:high])
		query = r.db.Rebind(query)

		rows, err := r.db.QueryxContext(ctx, query, args...)
		if err != nil {
			if err == sql.ErrNoRows {
				log.Trace().Msg("No control result quantitative channel result")
				return nil
			}
			log.Error().Err(err).Msg(msgGetControlResultQuantitativeChannelResultsFailed)
			return ErrGetControlResultQuantitativeChannelResultsFailed
		}
		defer rows.Close()
		for rows.Next() {
			quantitativeChannelResult := quantitativeChannelResultDAO{}
			err = rows.StructScan(&quantitativeChannelResult)
			if err != nil {
				log.Error().Err(err).Msg(msgGetControlResultQuantitativeChannelResultsFailed)
				return ErrGetControlResultQuantitativeChannelResultsFailed
			}

			valuesByChannelResultID[quantitativeChannelResult.ChannelResultID] = append(valuesByChannelResultID[quantitativeChannelResult.ChannelResultID], quantitativeChannelResult)
		}

		return nil
	})

	return valuesByChannelResultID, err
}

func (r *analysisRepository) getControlResultImages(ctx context.Context, controlResultIDs []uuid.UUID) (map[uuid.UUID][]controlResultImageDAO, error) {
	imagesMap := make(map[uuid.UUID][]controlResultImageDAO)
	err := utils.Partition(len(controlResultIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`SELECT scri.id, scri.control_result_id, scri.channel_result_id, scri.name, scri.description, scri.dea_image_id FROM %s.sk_control_result_images scri WHERE scri.control_result_id IN (?);`, r.dbSchema)
		query, args, _ := sqlx.In(query, controlResultIDs[low:high])
		query = r.db.Rebind(query)
		rows, err := r.db.QueryxContext(ctx, query, args...)
		if err != nil {
			if err == sql.ErrNoRows {
				log.Trace().Msg("No control result images")
				return nil
			}
			log.Error().Err(err).Msg(msgGetControlResultImagesFailed)
			return ErrGetControlResultImagesFailed
		}

		defer rows.Close()
		for rows.Next() {
			image := controlResultImageDAO{}
			err := rows.StructScan(&image)
			if err != nil {
				log.Error().Err(err).Msg(msgGetControlResultImagesFailed)
				return ErrGetControlResultImagesFailed
			}

			imagesMap[image.ControlResultId] = append(imagesMap[image.ControlResultId], image)
		}

		return nil
	})

	return imagesMap, err
}

func (r *analysisRepository) getExpectedControlResultsByAnalyteMappingIds(ctx context.Context, analyteMappingIds []uuid.UUID) (map[uuid.UUID][]ExpectedControlResult, error) {
	expectedControlResultsByAnalyteMappingID := make(map[uuid.UUID][]ExpectedControlResult)
	if analyteMappingIds == nil || len(analyteMappingIds) == 0 {
		return expectedControlResultsByAnalyteMappingID, nil
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

		expectedControlResultsByAnalyteMappingID[dao.AnalyteMappingID] = append(expectedControlResultsByAnalyteMappingID[dao.AnalyteMappingID], convertExpectedControlResultDaoToExpectedControlResult(dao))
	}
	return expectedControlResultsByAnalyteMappingID, nil
}

func convertControlResultsToTOs(controlResults []ControlResult) []ControlResultTO {
	tos := make([]ControlResultTO, 0)

	for _, controlResult := range controlResults {
		controlResultTO := ControlResultTO{
			ID:                         controlResult.ID,
			InstrumentID:               controlResult.InstrumentID,
			SampleCode:                 controlResult.SampleCode,
			AnalyteID:                  controlResult.AnalyteMapping.AnalyteID,
			IsValid:                    controlResult.IsValid,
			IsComparedToExpectedResult: controlResult.IsComparedToExpectedResult,
			Result:                     controlResult.Result,
			ExaminedAt:                 controlResult.ExaminedAt,
			ChannelResults:             make([]ChannelResultTO, 0),
			ExtraValues:                make([]ExtraValueTO, 0),
			Warnings:                   controlResult.Warnings,
		}

		for _, ev := range controlResult.ExtraValues {
			extraValueTO := ExtraValueTO{
				Key:   ev.Key,
				Value: ev.Value,
			}
			controlResultTO.ExtraValues = append(controlResultTO.ExtraValues, extraValueTO)
		}

		for _, cr := range controlResult.ChannelResults {
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
			controlResultTO.ChannelResults = append(controlResultTO.ChannelResults, channelResultTO)
		}

		tos = append(tos, controlResultTO)
	}

	return tos
}

func (r *analysisRepository) SaveCerberusIDForAnalysisResult(ctx context.Context, analysisResultID uuid.UUID, cerberusID uuid.UUID) error {
	query := fmt.Sprintf(`UPDATE %s.sk_analysis_results SET cerberus_id = $2 WHERE id = $1;`, r.dbSchema)

	_, err := r.db.ExecContext(ctx, query, analysisResultID, cerberusID)
	if err != nil {
		log.Error().Err(err).Msg(msgSaveCerberusIdForAnalysisResultFailed)
		return ErrSaveCerberusIdForAnalysisResultFailed
	}

	return nil
}

// TODO: rework it with new validated analyte concept

func (r *analysisRepository) GetAnalysisResultIdsWithoutControlByReagent(ctx context.Context, controlResult ControlResult, reagent Reagent, analysisResultWithoutControlSearchDays int) ([]uuid.UUID, error) {
	analysisResultIds := make([]uuid.UUID, 0)

	preparedValues := map[string]interface{}{
		"manufacturer":               reagent.Manufacturer,
		"lot_no":                     reagent.LotNo,
		"serial":                     reagent.SerialNumber,
		"name":                       reagent.Name,
		"control_analyte_id":         controlResult.AnalyteMapping.AnalyteID,
		"control_analyte_mapping_id": controlResult.AnalyteMapping.ID,
		"sample_code":                controlResult.SampleCode,
		"instrument_id":              controlResult.InstrumentID,
		"analysis_result_without_control_search_days": analysisResultWithoutControlSearchDays,
	}

	query := `WITH analyteMapping AS (select sam.id from %schema_name%.sk_control_mapping_control_analyte scmca
		INNER JOIN %schema_name%.sk_control_mappings scm ON scmca.control_mapping_id = scm.id
		INNER JOIN %schema_name%.sk_analyte_mappings sam ON scm.analyte_id = sam.analyte_id AND scm.instrument_id = sam.instrument_id
		WHERE scmca.control_analyte_id = :control_analyte_id AND scm.instrument_id = :instrument_id),
	assignedControl AS (SELECT DISTINCT sarcrr.analysis_result_id, sarcrr.control_result_id
		FROM %schema_name%.sk_analysis_result_control_result_relations sarcrr
		INNER JOIN %schema_name%.sk_control_results scr ON sarcrr.control_result_id = scr.id
		INNER JOIN %schema_name%.sk_analyte_mappings sam ON scr.analyte_mapping_id = sam.id
		INNER JOIN %schema_name%.sk_reagent_control_result_relations skrcrr ON scr.id = skrcrr.control_result_id
		INNER JOIN %schema_name%.sk_reagents skr ON skrcrr.reagent_id = skr.id
		WHERE skr.manufacturer = :manufacturer AND skr.lot_no = :lot_no AND skr.serial = :serial AND skr.name = :name AND scr.sample_code = :sample_code
		  AND scr.analyte_mapping_id = :control_analyte_mapping_id AND scr.instrument_id = :instrument_id)
	SELECT skar.id FROM %schema_name% .sk_analysis_results skar
		INNER JOIN %schema_name%.sk_analysis_result_reagent_relations skarr ON skar.id = skarr.analysis_result_id
		INNER JOIN %schema_name%.sk_reagents skr ON skarr.reagent_id = skr.id
		INNER JOIN analyteMapping am ON skar.analyte_mapping_id = am.id
		LEFT JOIN assignedControl ac ON skar.id = ac.analysis_result_id
	WHERE skr.manufacturer = :manufacturer AND skr.lot_no = :lot_no AND skr.serial = :serial AND skr.name = :name AND skar.instrument_id = :instrument_id
		AND ac.control_result_id IS NULL AND skar.yielded_at >= (current_date - make_interval(days := :analysis_result_without_control_search_days));`
	query = strings.ReplaceAll(query, "%schema_name%", r.dbSchema)
	rows, err := r.db.NamedQueryContext(ctx, query, preparedValues)
	if err != nil {
		log.Error().Err(err).Msg(msgLoadAnalysisResultIdsWithoutControlByReagentFailed)
		return analysisResultIds, ErrLoadAnalysisResultIdsWithoutControlByReagentFailed
	}

	defer rows.Close()

	for rows.Next() {
		var analysisResultId uuid.UUID
		err := rows.Scan(&analysisResultId)
		if err != nil {
			log.Error().Err(err).Msg(msgLoadAnalysisResultIdsWithoutControlByReagentFailed)
			return analysisResultIds, ErrLoadAnalysisResultIdsWithoutControlByReagentFailed
		}
		analysisResultIds = append(analysisResultIds, analysisResultId)
	}

	return analysisResultIds, nil
}

func (r *analysisRepository) GetAnalysisResultIdsWhereLastestControlIsInvalid(ctx context.Context, controlResult ControlResult, reagent Reagent, analysisResultWithInvalidControlSearchDays int) ([]uuid.UUID, error) {
	analysisResultIds := make([]uuid.UUID, 0)
	preparedValues := map[string]interface{}{
		"manufacturer":               reagent.Manufacturer,
		"lot_no":                     reagent.LotNo,
		"serial":                     reagent.SerialNumber,
		"sample_code":                controlResult.SampleCode,
		"control_analyte_mapping_id": controlResult.AnalyteMapping.ID,
		"instrument_id":              controlResult.InstrumentID,
		"analysis_result_with_invalid_control_search_days": analysisResultWithInvalidControlSearchDays,
	}

	query := `WITH latestControl AS (SELECT skcr.id, skcr.is_valid, skcr.is_compared_to_expected_result
		FROM %schema_name%.sk_control_results skcr
		INNER JOIN %schema_name%.sk_analyte_mappings sam ON skcr.analyte_mapping_id = sam.id
		INNER JOIN %schema_name%.sk_reagent_control_result_relations skrcrr ON skcr.id = skrcrr.control_result_id
		INNER JOIN %schema_name%.sk_reagents skr ON skrcrr.reagent_id = skr.id
		WHERE skr.manufacturer = :manufacturer AND skr.lot_no = :lot_no AND skr.serial = :serial AND skr.name = '' AND skcr.sample_code = :sample_code 
			AND skcr.analyte_mapping_id = :control_analyte_mapping_id AND skcr.instrument_id = :instrument_id
		ORDER BY skcr.sample_code, skcr.analyte_mapping_id, skcr. examined_at desc, skcr.created_at desc limit 1)
	SELECT sarcrr.analysis_result_id 
	FROM %schema_name%.sk_analysis_result_control_result_relations sarcrr 
		INNER JOIN latestControl ON sarcrr.control_result_id = latestControl.id 
		INNER JOIN %schema_name% .sk_analysis_results skar ON skar.id = sarcrr.analysis_result_id
	WHERE (latestControl.is_compared_to_expected_result = false OR latestControl.is_valid = false) AND skar.yielded_at >= (current_date - make_interval(days := :analysis_result_with_invalid_control_search_days));`
	query = strings.ReplaceAll(query, "%schema_name%", r.dbSchema)
	rows, err := r.db.NamedQueryContext(ctx, query, preparedValues)
	if err != nil {
		log.Error().Err(err).Msg(msgLoadAnalysisResultIdsWhereLatestControlResultIsInValidFailed)
		return analysisResultIds, ErrLoadAnalysisResultIdsWhereLatestControlResultIsInValidFailed
	}

	defer rows.Close()

	for rows.Next() {
		var analysisResultId uuid.UUID
		err := rows.Scan(&analysisResultId)
		if err != nil {
			log.Error().Err(err).Msg(msgLoadAnalysisResultIdsWhereLatestControlResultIsInValidFailed)
			return analysisResultIds, ErrLoadAnalysisResultIdsWhereLatestControlResultIsInValidFailed
		}
		analysisResultIds = append(analysisResultIds, analysisResultId)
	}

	return analysisResultIds, err
}

// TODO: rework it with new validated analyte concept

func (r *analysisRepository) GetLatestControlResultsByReagent(ctx context.Context, reagent Reagent, resultYieldTime *time.Time, analyteMapping AnalyteMapping, instrumentId uuid.UUID, controlResultSearchDays int) ([]ControlResult, error) {
	controlResults := make([]ControlResult, 0)
	preparedValues := map[string]interface{}{
		"manufacturer":      reagent.Manufacturer,
		"lot_no":            reagent.LotNo,
		"serial":            reagent.SerialNumber,
		"name":              reagent.Name,
		"result_analyte_id": analyteMapping.AnalyteID,
		"instrument_id":     instrumentId,
	}

	query := `WITH controlAnalyteMapping AS (select sam.id from %schema_name%.sk_control_mapping_control_analyte scmca
		INNER JOIN %schema_name%.sk_control_mappings scm ON scmca.control_mapping_id = scm.id
		INNER JOIN %schema_name%.sk_analyte_mappings sam ON scmca.control_analyte_id = sam.analyte_id AND scm.instrument_id = sam.instrument_id
	WHERE scm.analyte_id = :result_analyte_id AND scm.instrument_id = :instrument_id AND scm.deleted_at IS NULL AND sam.deleted_at IS NULL)
	SELECT DISTINCT ON (skcr.sample_code, skcr.analyte_mapping_id) skcr.id, skcr.sample_code, skcr.analyte_mapping_id, skcr.instrument_id, skcr.expected_control_result_id, skcr.is_valid, skcr.is_compared_to_expected_result, skcr.result, skcr.examined_at, skcr.created_at,
    	sam.id AS "analyte_mapping.id", sam.instrument_id AS "analyte_mapping.instrument_id", sam.instrument_analyte AS "analyte_mapping.instrument_analyte", sam.analyte_id AS "analyte_mapping.analyte_id", sam.result_type AS "analyte_mapping.result_type", sam.created_at AS "analyte_mapping.created_at", sam.modified_at AS "analyte_mapping.modified_at"
    FROM %schema_name%.sk_control_results skcr
        INNER JOIN controlAnalyteMapping cam ON skcr.analyte_mapping_id = cam.id
		INNER JOIN %schema_name%.sk_analyte_mappings sam ON cam.id = sam.id
        INNER JOIN %schema_name%.sk_reagent_control_result_relations skrcrr ON skcr.id = skrcrr.control_result_id
        INNER JOIN %schema_name%.sk_reagents skr ON skrcrr.reagent_id = skr.id
    WHERE skr.manufacturer = :manufacturer AND skr.lot_no = :lot_no AND skr.serial = :serial AND skr.name = :name AND skcr.instrument_id = :instrument_id`
	if resultYieldTime != nil {
		preparedValues["result_yield_time"] = resultYieldTime
		preparedValues["yield_time_lookback"] = resultYieldTime.Add(time.Hour * 24 * time.Duration(-controlResultSearchDays))
		query += ` AND skcr.examined_at < :result_yield_time AND skcr.examined_at >= :yield_time_lookback`
	}
	query += ` ORDER BY skcr.sample_code, skcr.analyte_mapping_id, skcr. examined_at desc, skcr.created_at desc;`
	query = strings.ReplaceAll(query, "%schema_name%", r.dbSchema)
	rows, err := r.db.NamedQueryContext(ctx, query, preparedValues)
	if err != nil {
		log.Error().Err(err).Msg(msgLoadLatestControlResultIdFailed)
		return controlResults, ErrLoadLatestControlResultIdFailed
	}

	defer rows.Close()

	controlResultDAOs := make([]controlResultDAO, 0)
	for rows.Next() {
		var controlResultDao controlResultDAO
		err := rows.StructScan(&controlResultDao)
		if err != nil {
			log.Error().Err(err).Msg(msgLoadLatestControlResultIdFailed)
			return controlResults, ErrLoadLatestControlResultIdFailed
		}
		controlResultDAOs = append(controlResultDAOs, controlResultDao)
	}

	return r.gatherAndAttachAllConnectedDataToControlResults(ctx, controlResultDAOs)
}

func (r *analysisRepository) GetControlResultsToValidate(ctx context.Context, analyteMappingIds []uuid.UUID) ([]ControlResult, error) {
	controlResults := make([]ControlResult, 0)

	query := fmt.Sprintf(`SELECT skcr.id, skcr.sample_code, skcr.analyte_mapping_id, skcr.instrument_id, skcr.expected_control_result_id, skcr.is_valid, skcr.is_compared_to_expected_result, skcr.result, skcr.examined_at, skcr.created_at,
		sam.id AS "analyte_mapping.id", sam.instrument_id AS "analyte_mapping.instrument_id", sam.instrument_analyte AS "analyte_mapping.instrument_analyte", sam.analyte_id AS "analyte_mapping.analyte_id", sam.result_type AS "analyte_mapping.result_type",
		sam.control_result_required AS "analyte_mapping.control_result_required", sam.created_at AS "analyte_mapping.created_at", sam.modified_at AS "analyte_mapping.modified_at"
	FROM %s.sk_control_results skcr
	INNER JOIN %s.sk_analyte_mappings sam ON skcr.analyte_mapping_id = sam.id
    INNER JOIN %s.sk_expected_control_result secr ON sam.id = secr.analyte_mapping_id AND skcr.sample_code = secr.sample_code
	WHERE skcr.expected_control_result_id IS NULL`, r.dbSchema, r.dbSchema, r.dbSchema)
	if len(analyteMappingIds) > 0 {
		query += " AND skcr.analyte_mapping_id IN (?) "
	}

	var rows *sqlx.Rows
	var err error
	if len(analyteMappingIds) > 0 {
		var args []interface{}
		query, args, _ = sqlx.In(query, analyteMappingIds)
		query = r.db.Rebind(query)
		rows, err = r.db.QueryxContext(ctx, query, args...)
	} else {
		rows, err = r.db.QueryxContext(ctx, query)
	}

	if err != nil {
		log.Error().Err(err).Msg(msgLoadNotValidatedControlResultsFailed)
		return controlResults, ErrLoadNotValidatedControlResultsFailed
	}

	defer rows.Close()

	controlResultDAOs := make([]controlResultDAO, 0)
	analyteMappingIDs := make([]uuid.UUID, 0)
	for rows.Next() {
		var controlResultDao controlResultDAO
		err := rows.StructScan(&controlResultDao)
		if err != nil {
			log.Error().Err(err).Msg(msgLoadNotValidatedControlResultsFailed)
			return controlResults, ErrLoadNotValidatedControlResultsFailed
		}
		controlResultDAOs = append(controlResultDAOs, controlResultDao)
		analyteMappingIDs = append(analyteMappingIDs, controlResultDao.AnalyteMappingID)
	}

	expectedControlResultMappingsMap, err := r.getExpectedControlResultsByAnalyteMappingIds(ctx, analyteMappingIDs)
	if err != nil {
		return controlResults, err
	}

	for i := range controlResultDAOs {
		controlResult := convertControlResultDAOToControlResult(controlResultDAOs[i])
		controlResult.AnalyteMapping.ExpectedControlResults = expectedControlResultMappingsMap[controlResult.AnalyteMapping.ID]
		controlResults = append(controlResults, controlResult)
	}

	return controlResults, nil
}

func (r *analysisRepository) MarkReagentControlResultRelationsAsProcessed(ctx context.Context, controlResultID uuid.UUID, reagentIDs []uuid.UUID) error {
	err := utils.Partition(len(reagentIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`UPDATE %s.sk_reagent_control_result_relations SET is_processed = true WHERE control_result_id = ? AND reagent_id IN (?);`, r.dbSchema)

		query, args, _ := sqlx.In(query, controlResultID, reagentIDs[low:high])
		query = r.db.Rebind(query)

		_, err := r.db.ExecContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg(msgMarkAnalysisResultControlResultRelationAsProcessedFailed)
			return ErrMarkAnalysisResultControlResultRelationAsProcessedFailed
		}
		return nil
	})

	return err
}

func (r *analysisRepository) MarkAnalysisResultControlResultRelationsAsProcessed(ctx context.Context, controlResultID uuid.UUID, analysisResultIDs []uuid.UUID) error {
	err := utils.Partition(len(analysisResultIDs), maxParams, func(low int, high int) error {
		query := fmt.Sprintf(`UPDATE %s.sk_analysis_result_control_result_relations SET is_processed = true WHERE control_result_id = ? AND analysis_result_id IN (?);`, r.dbSchema)

		query, args, _ := sqlx.In(query, controlResultID, analysisResultIDs[low:high])
		query = r.db.Rebind(query)

		_, err := r.db.ExecContext(ctx, query, args...)
		if err != nil {
			log.Error().Err(err).Msg(msgMarkAnalysisResultControlResultRelationAsProcessedFailed)
			return ErrMarkAnalysisResultControlResultRelationAsProcessedFailed
		}
		return nil
	})

	return err
}

func (r *analysisRepository) CreateTransaction() (db.DbConnection, error) {
	return r.db.CreateTransactionConnector()
}

func (r *analysisRepository) WithTransaction(tx db.DbConnection) AnalysisRepository {
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

func convertAnalysisResultToDAO(analysisResult AnalysisResult) analysisResultDAO {
	analysisResultDAO := analysisResultDAO{
		ID:               analysisResult.ID,
		AnalyteMappingID: analysisResult.AnalyteMapping.ID,
		InstrumentID:     analysisResult.Instrument.ID,
		ResultMode:       analysisResult.ResultMode,
		SampleCode:       analysisResult.SampleCode,
		InstrumentRunID:  analysisResult.InstrumentRunID,
		DEARawMessageID:  analysisResult.DEARawMessageID,
		MessageInID:      analysisResult.MessageInID,
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
		DEARawMessageID: analysisResultDAO.DEARawMessageID,
		MessageInID:     analysisResultDAO.MessageInID,
		Result:          analysisResultDAO.Result,
		ResultMode:      analysisResultDAO.ResultMode,
		Status:          analysisResultDAO.Status,
		ValidUntil:      analysisResultDAO.ValidUntil,
		Operator:        analysisResultDAO.Operator,
		InstrumentRunID: analysisResultDAO.InstrumentRunID,
		Edited:          analysisResultDAO.Edited,
		EditReason:      utils.NullStringToString(analysisResultDAO.EditReason),
		IsInvalid:       analysisResultDAO.IsInvalid,
		Warnings:        convertAnalysisWarningDAOsToWarnings(analysisResultDAO.Warnings),
		ChannelResults:  convertChannelResultDAOsToChannelResults(analysisResultDAO.ChannelResults),
		ExtraValues:     convertExtraValueDAOsToExtraValues(analysisResultDAO.ExtraValues),
		Reagents:        convertReagentDAOsToReagentList(analysisResultDAO.Reagents),
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

func convertReagentToDAO(reagent Reagent) reagentDAO {
	dao := reagentDAO{
		ID:           reagent.ID,
		Manufacturer: reagent.Manufacturer,
		SerialNumber: reagent.SerialNumber,
		LotNo:        reagent.LotNo,
		Name:         reagent.Name,
		Type:         reagent.Type,
	}
	if reagent.ExpirationDate != nil {
		dao.ExpirationDate = sql.NullTime{
			Time:  *reagent.ExpirationDate,
			Valid: true,
		}
	}

	return dao
}

func convertControlResultsToDAO(controlResults []ControlResult) []controlResultDAO {
	controlResultDAOs := make([]controlResultDAO, len(controlResults))
	for i := range controlResults {
		controlResultDAOs[i] = controlResultDAO{
			ID:                         controlResults[i].ID,
			SampleCode:                 controlResults[i].SampleCode,
			AnalyteMappingID:           controlResults[i].AnalyteMapping.ID,
			InstrumentID:               controlResults[i].InstrumentID,
			ExpectedControlResultId:    controlResults[i].ExpectedControlResultId,
			IsValid:                    controlResults[i].IsValid,
			IsComparedToExpectedResult: controlResults[i].IsComparedToExpectedResult,
			Result:                     controlResults[i].Result,
			ExaminedAt:                 controlResults[i].ExaminedAt,
		}
	}

	return controlResultDAOs
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

func convertCerberusQueueItemToCerberusQueueItemDAO(cerberusQueueItem CerberusQueueItem) cerberusQueueItemDAO {
	return cerberusQueueItemDAO{
		ID:                  cerberusQueueItem.ID,
		JsonMessage:         cerberusQueueItem.JsonMessage,
		LastHTTPStatus:      cerberusQueueItem.LastHTTPStatus,
		LastError:           cerberusQueueItem.LastError,
		LastErrorAt:         utils.TimePointerToNullTime(cerberusQueueItem.LastErrorAt),
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
		LastErrorAt:         utils.NullTimeToTimePointer(cerberusQueueItemDAO.LastErrorAt),
		TrialCount:          cerberusQueueItemDAO.TrialCount,
		RetryNotBefore:      cerberusQueueItemDAO.RetryNotBefore,
		RawResponse:         cerberusQueueItemDAO.RawResponse,
		ResponseJsonMessage: cerberusQueueItemDAO.ResponseJsonMessage,
	}
}

func convertReagentDAOsToReagentList(reagentDAOs []reagentDAO) []Reagent {
	reagentList := make([]Reagent, len(reagentDAOs))
	for i := range reagentDAOs {
		reagentList[i] = convertReagentDAOToReagent(reagentDAOs[i])
	}
	return reagentList
}

func convertReagentDAOToReagent(reagentDAO reagentDAO) Reagent {
	reagent := Reagent{
		ID:           reagentDAO.ID,
		Manufacturer: reagentDAO.Manufacturer,
		SerialNumber: reagentDAO.SerialNumber,
		LotNo:        reagentDAO.LotNo,
		Type:         reagentDAO.Type,
		Name:         reagentDAO.Name,
		CreatedAt:    reagentDAO.CreatedAt,
	}
	if reagentDAO.ExpirationDate.Valid {
		reagent.ExpirationDate = &reagentDAO.ExpirationDate.Time
	}
	return reagent
}

func convertControlResultDAOToControlResult(controlResultDao controlResultDAO) ControlResult {
	controlResult := ControlResult{
		ID:                         controlResultDao.ID,
		SampleCode:                 controlResultDao.SampleCode,
		AnalyteMapping:             convertAnalyteMappingDaoToAnalyteMapping(controlResultDao.AnalyteMapping),
		Result:                     controlResultDao.Result,
		ExpectedControlResultId:    controlResultDao.ExpectedControlResultId,
		IsValid:                    controlResultDao.IsValid,
		IsComparedToExpectedResult: controlResultDao.IsComparedToExpectedResult,
		ExaminedAt:                 controlResultDao.ExaminedAt,
		InstrumentID:               controlResultDao.InstrumentID,
		Warnings:                   convertControlResultWarningDAOsToWarnings(controlResultDao.Warnings),
		ChannelResults:             convertControlResultChannelResultDAOsToChannelResults(controlResultDao.ChannelResults),
		ExtraValues:                convertControlResultExtraValueDAOsToExtraValues(controlResultDao.ExtraValues),
	}

	return controlResult
}

func convertAnalysisWarningDAOsToWarnings(warningDAOs []warningDAO) []string {
	warnings := make([]string, len(warningDAOs))
	for i := range warningDAOs {
		warnings[i] = warningDAOs[i].Warning
	}
	return warnings
}
func convertControlResultWarningDAOsToWarnings(warningDAOs []controlResultWarningDAO) []string {
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

func convertControlResultExtraValueDAOsToExtraValues(extraValueDAOs []controlResultExtraValueDAO) []ExtraValue {
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

func convertControlResultChannelResultDAOsToChannelResults(channelResultDAOs []controlResultChannelResultDAO) []ChannelResult {
	channelResults := make([]ChannelResult, len(channelResultDAOs))
	for i, channelResultDAO := range channelResultDAOs {
		channelResults[i] = convertControlResultChannelResultDAOsToChannelResult(channelResultDAO)
	}
	return channelResults
}

func convertControlResultChannelResultDAOsToChannelResult(channelResultDAO controlResultChannelResultDAO) ChannelResult {
	return ChannelResult{
		ID:                    channelResultDAO.ID,
		ChannelID:             channelResultDAO.ChannelID,
		QualitativeResult:     channelResultDAO.QualitativeResult,
		QualitativeResultEdit: channelResultDAO.QualitativeResultEdit,
		QuantitativeResults:   convertQuantitativeResultDAOsToQuantitativeResults(channelResultDAO.QuantitativeResults),
		Images:                convertControlResultImageDAOsToImages(channelResultDAO.Images),
	}
}

func convertChannelResultToControlResultChannelResultDAO(channelResult ChannelResult, controlResultID uuid.UUID) controlResultChannelResultDAO {
	return controlResultChannelResultDAO{
		ID:                    channelResult.ID,
		ControlResultId:       controlResultID,
		ChannelID:             channelResult.ChannelID,
		QualitativeResult:     channelResult.QualitativeResult,
		QualitativeResultEdit: channelResult.QualitativeResultEdit,
	}
}

func convertChannelResultsToControlResultChannelResultDAOs(channelResults []ChannelResult, controlResultID uuid.UUID) []controlResultChannelResultDAO {
	channelResultDAOs := make([]controlResultChannelResultDAO, len(channelResults))
	for i := range channelResults {
		channelResultDAOs[i] = convertChannelResultToControlResultChannelResultDAO(channelResults[i], controlResultID)
	}
	return channelResultDAOs
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
			Description: utils.NullStringToStringPointer(imageDAO.Description),
			DeaImageID:  imageDAO.DeaImageID,
		}
	}
	return images
}

func convertControlResultImageDAOsToImages(imageDAOs []controlResultImageDAO) []Image {
	images := make([]Image, len(imageDAOs))
	for i, imageDAO := range imageDAOs {
		images[i] = Image{
			ID:          imageDAO.ID,
			Name:        imageDAO.Name,
			Description: utils.NullStringToStringPointer(imageDAO.Description),
			DeaImageID:  imageDAO.DeaImageID,
		}
	}
	return images
}
