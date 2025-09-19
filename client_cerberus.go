package skeleton

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/blutspende/bloodlab-common/utils"
	"github.com/blutspende/skeleton/middleware"
	"net/http"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

const (
	MsgSendResultBatchFailed                    = "send result batch failed"
	MsgBasepathNotSet                           = "basepath for cerberus must be set. check your configuration for CerberusURL"
	MsgFailedToCallCerberusAPI                  = "Failed to call Cerberus API"
	MsgFailedToUnmarshalError                   = "Failed to unmarshal error of response"
	MsgUnexpectedErrorFromCerberus              = "unexpected error from cerberus"
	MsgEmptyAnalysisResultsBatch                = "Send analysis results batch called with empty array"
	MsgSendSampleSeenMessageBatchFailed         = "send sample seen message batch failed"
	MsgFailedToPrepareData                      = "Failed to prepare data for sending"
	MsgFailedToSetOnlineStatus                  = "Failed to set instrument online status"
	MsgFailedToCallImageBatchAPI                = "Failed to call Cerberus API (/v1/analysis-results/image/batch)"
	MsgFailedToCallVerifyAPI                    = "Failed to call Cerberus API (/v1/instrument/verify)"
	MsgFailedToCallExpectedControlResultsAPI    = "Failed to call Cerberus API (/v1/expected-control-results/verify)"
	MsgFailedToCallSyncAPI                      = "Failed to call Cerberus API (/v1/instrument-drivers/analysis-requests/sync)"
	MsgFailedToCallLogsAPI                      = "Failed to call Cerberus API (/v1/instruments/logs)"
	MsgFailedToCallRegisterManufacturerTestsAPI = "Failed to call Cerberus API (/v1/instrument-drivers/manufacturer-tests)"
	MsgFailedToCallSampleSeenAPI                = "Failed to call Cerberus API (/v1/instruments/sample-seen)"
)

var (
	ErrSendResultBatchFailed            = errors.New(MsgSendResultBatchFailed)
	ErrBasePathNotSet                   = errors.New(MsgBasepathNotSet)
	ErrSendSampleSeenMessageBatchFailed = errors.New(MsgSendSampleSeenMessageBatchFailed)
)

type CerberusClient interface {
	RegisterInstrumentDriver(name, displayName string, extraValueKeys []string, protocols []supportedProtocolTO, tests []supportedManufacturerTestTO, encodings []string, reagentManufacturers []string) error
	RegisterManufacturerTests(driverName string, tests []supportedManufacturerTestTO) error
	SendAnalysisResultBatch(analysisResults []AnalysisResultTO) (AnalysisResultBatchResponse, error)
	SendAnalysisResultImageBatch(images []WorkItemResultImageTO) error
	SendSampleSeenMessageBatch(messages []SampleSeenMessage) error
	VerifyInstrumentHash(hash string) error
	VerifyExpectedControlResultsHash(hash string) error
	SyncAnalysisRequests(workItemIDs []uuid.UUID, syncType string) error
	SendConsoleLog(consoleLogDTOs []ConsoleLogDTO)
	SetInstrumentOnlineStatus(instrumentId uuid.UUID, status InstrumentStatus) error
}

type cerberusClient struct {
	client      *resty.Client
	cerberusUrl string
	driverName  string
}

type registerInstrumentDriverTO struct {
	Name                 string                        `json:"name" binding:"required"`
	DisplayName          string                        `json:"display_name"`
	ExtraValueKeys       []string                      `json:"extraValueKeys,omitempty"`
	Protocols            []supportedProtocolTO         `json:"protocols"`
	ManufacturerTests    []supportedManufacturerTestTO `json:"manufacturerTests"`
	Encodings            []string                      `json:"encodings"`
	ReagentManufacturers []string                      `json:"reagentManufacturers"`
}

type registerManufacturerTestTO struct {
	DriverName        string                        `json:"driverName"`
	ManufacturerTests []supportedManufacturerTestTO `json:"manufacturerTests"`
}

type ExtraValueTO struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type createAnalysisResultResponseItemTO struct {
	ID         uuid.NullUUID `json:"id"`
	WorkItemID uuid.UUID     `json:"workItemId"`
	Error      *string       `json:"error"`
}

type ChannelResultTO struct {
	ChannelID             uuid.UUID         `json:"channelId"`
	QualitativeResult     string            `json:"qualitativeResult"`
	QualitativeResultEdit bool              `json:"edited"`
	QuantitativeResults   map[string]string `json:"quantitativeResults"`
	Images                []ImageTO         `json:"images"`
}

type AnalysisResultTO struct {
	ID                       uuid.UUID         `json:"id"`
	WorkingItemID            uuid.UUID         `json:"workItemId"`
	DEARawMessageID          uuid.UUID         `json:"deaRawMessageId"`
	ValidUntil               time.Time         `json:"validUntil"`
	Status                   string            `json:"status"`
	Mode                     string            `json:"mode"` // ResultMode
	ResultYieldDateTime      *time.Time        `json:"resultYieldDateTime"`
	ExaminedMaterial         uuid.UUID         `json:"examinedMaterial"`
	Result                   string            `json:"result"`
	Operator                 string            `json:"operator"`
	TechnicalReleaseDateTime *time.Time        `json:"technicalReleaseDateTime"`
	InstrumentID             uuid.UUID         `json:"instrumentId"`
	InstrumentRunID          uuid.UUID         `json:"instrumentRunId"`
	Edited                   bool              `json:"resultEdit"`
	EditReason               string            `json:"editReason"`
	IsInvalid                bool              `json:"isInvalid"`
	ChannelResults           []ChannelResultTO `json:"channelResults"`
	ExtraValues              []ExtraValueTO    `json:"extraValues"`
	Reagents                 []ReagentTO       `json:"reagents"`
	ControlResults           []ControlResultTO `json:"controlResults"`
	Images                   []ImageTO         `json:"images"`
	WarnFlag                 bool              `json:"warnFlag"`
	Warnings                 []string          `json:"warnings"`
}

type ImageTO struct {
	ID          uuid.UUID `json:"imageId"`
	Name        string    `json:"name"`
	Description *string   `json:"description,omitempty"`
}

type ReagentTO struct {
	ID             uuid.UUID         `json:"id"`
	Manufacturer   string            `json:"manufacturer"`
	Name           string            `json:"name"`
	SerialNo       string            `json:"serialNo"`
	LotNo          string            `json:"lotNo"`
	Type           ReagentType       `json:"type"`
	ControlResults []ControlResultTO `json:"controlResults"`
}

type ControlResultTO struct {
	ID                         uuid.UUID         `json:"id"`
	InstrumentID               uuid.UUID         `json:"instrumentID"`
	SampleCode                 string            `json:"sampleCode"`
	AnalyteID                  uuid.UUID         `json:"analyteID"`
	IsValid                    bool              `json:"isValid"`
	IsComparedToExpectedResult bool              `json:"isComparedToExpectedResult"`
	Result                     string            `json:"result"`
	ExaminedAt                 time.Time         `json:"examinedAt"`
	ChannelResults             []ChannelResultTO `json:"channelResults"`
	ExtraValues                []ExtraValueTO    `json:"extraValues"`
	Warnings                   []string          `json:"warnings"`
}

type StandaloneControlResultTO struct {
	ControlResultTO
	Reagents  []ReagentTO `json:"reagents"`
	ResultIDs []uuid.UUID `json:"resultIds"`
}

type WorkItemResultImageTO struct {
	WorkItemID          uuid.UUID  `json:"workItemId"`
	ResultYieldDateTime *time.Time `json:"resultYieldDateTime"`
	ChannelID           *uuid.UUID `json:"channelId"`
	Image               ImageTO    `json:"image"`
}

type LogLevel string // @Name LogLevel

const (
	Debug LogLevel = "debug"
	Info  LogLevel = "info"
	Error LogLevel = "error"
)

type ConsoleLogDTO struct {
	InstrumentID uuid.UUID `json:"instrumentId" swaggertype:"string" format:"uuid"`   // The instrument ID
	CreatedAt    time.Time `json:"timestamp" swaggertype:"string" format:"date-time"` // The log timestamp
	Level        LogLevel  `json:"messageCategory"`                                   // The log level
	Message      string    `json:"message"`                                           // The log message
	MessageType  string    `json:"messageType"`                                       // The log message type
} // @Name consoleLogDTO

type VerifyInstrumentTO struct {
	Hash string `json:"hash"`
}

type instrumentStatusTO struct {
	Status InstrumentStatus `json:"status"`
}

type sampleSeenMessageTO struct {
	InstrumentID uuid.UUID `json:"instrumentID"`
	ModuleName   string    `json:"moduleName"`
	SampleCode   string    `json:"sampleCode"`
	SeenAt       time.Time `json:"seenAt"`
}

func NewCerberusClient(cerberusUrl string, restyClient *resty.Client) (CerberusClient, error) {
	if cerberusUrl == "" {
		return nil, ErrBasePathNotSet
	}

	return &cerberusClient{
		client:      restyClient,
		cerberusUrl: cerberusUrl,
	}, nil
}

func (c *cerberusClient) RegisterInstrumentDriver(name, displayName string, extraValueKeys []string,
	protocols []supportedProtocolTO, tests []supportedManufacturerTestTO, encodings []string, reagentManufacturers []string) error {
	resp, err := c.client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(registerInstrumentDriverTO{
			Name:                 name,
			DisplayName:          displayName,
			ExtraValueKeys:       extraValueKeys,
			Protocols:            protocols,
			ManufacturerTests:    tests,
			Encodings:            encodings,
			ReagentManufacturers: reagentManufacturers,
		}).
		Post(c.cerberusUrl + "/v1/instrument-drivers")

	if err != nil {
		log.Error().Err(err).Msg(MsgFailedToCallCerberusAPI)
		return err
	}

	if resp.IsError() {
		errReps := middleware.ClientError{}
		err = json.Unmarshal(resp.Body(), &errReps)
		if err != nil {
			log.Error().Err(err).Msg(MsgFailedToUnmarshalError)
			return err
		}
		return errors.New(errReps.Message)
	}
	c.driverName = name
	return nil
}

func (c *cerberusClient) RegisterManufacturerTests(driverName string, tests []supportedManufacturerTestTO) error {
	var errResponse clientError
	resp, err := c.client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(registerManufacturerTestTO{
			DriverName:        driverName,
			ManufacturerTests: tests,
		}).
		SetError(&errResponse).
		Post(c.cerberusUrl + "/v1/instrument-drivers/manufacturer-tests")

	if err != nil {
		log.Error().Err(err).Msg(MsgFailedToCallRegisterManufacturerTestsAPI)
		return err
	}

	if resp.IsError() {
		log.Error().Str("Message", errResponse.Message).Msg(MsgFailedToCallRegisterManufacturerTestsAPI)
		return errors.New(errResponse.Message)
	}

	return nil
}

// SendAnalysisResultBatch Submit a list of AnalysisResults to Cerberus
func (c *cerberusClient) SendAnalysisResultBatch(analysisResults []AnalysisResultTO) (AnalysisResultBatchResponse, error) {
	if len(analysisResults) < 1 {
		log.Warn().Msg(MsgEmptyAnalysisResultsBatch)
		return AnalysisResultBatchResponse{}, nil
	}

	analysisResultBatchItemInfoList := make([]AnalysisResultBatchItemInfo, len(analysisResults))

	var hasError bool
	for i := range analysisResults {
		analysisResultBatchItemInfoList[i] = AnalysisResultBatchItemInfo{
			AnalysisResult: &analysisResults[i],
		}
	}

	if hasError {
		response := AnalysisResultBatchResponse{
			AnalysisResultBatchItemInfoList: analysisResultBatchItemInfoList,
			ErrorMessage:                    MsgFailedToPrepareData,
		}
		return response, nil
	}

	resp, err := c.client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(analysisResults).
		Post(c.cerberusUrl + "/v1/analysis-results/batch")

	if err != nil {
		response := AnalysisResultBatchResponse{
			AnalysisResultBatchItemInfoList: analysisResultBatchItemInfoList,
			ErrorMessage:                    err.Error(),
		}

		return response, fmt.Errorf("%s (%w)", ErrSendResultBatchFailed, err)
		// log.Error().Err(err).Msg(i18n.MsgSendResultBatchFailed)
		//requestBody, _ := json.Marshal(analysisResultsTOs)
		//TODO:Better soltion for request-logging c.ciaHistoryService.Create(model.TYPE_AnalysisResultBatch, err.Error(), string(requestBody), 0, nil, analysisResultIDs)
	}

	switch {
	case resp.StatusCode() == http.StatusCreated, resp.StatusCode() == http.StatusAccepted:
		responseItems := make([]createAnalysisResultResponseItemTO, 0)
		err = json.Unmarshal(resp.Body(), &responseItems)
		if err != nil {
			response := AnalysisResultBatchResponse{
				AnalysisResultBatchItemInfoList: analysisResultBatchItemInfoList,
				HTTPStatusCode:                  resp.StatusCode(),
				ErrorMessage:                    err.Error(),
				RawResponse:                     string(resp.Body()),
			}

			return response, err
		}

		for i, responseItem := range responseItems {
			analysisResultBatchItemInfoList[i].ErrorMessage = utils.StringPointerToString(responseItem.Error)
			analysisResultBatchItemInfoList[i].CerberusAnalysisResultID = utils.NullUUIDToUUIDPointer(responseItem.ID)
		}

		response := AnalysisResultBatchResponse{
			AnalysisResultBatchItemInfoList: analysisResultBatchItemInfoList,
			HTTPStatusCode:                  resp.StatusCode(),
			RawResponse:                     string(resp.Body()),
		}

		return response, nil
	case resp.StatusCode() == http.StatusInternalServerError:
		errReps := middleware.ClientError{}
		err = json.Unmarshal(resp.Body(), &errReps)
		if err != nil {
			err = fmt.Errorf("can not unmarshal error of response (%w)", err)
			response := AnalysisResultBatchResponse{
				AnalysisResultBatchItemInfoList: analysisResultBatchItemInfoList,
				HTTPStatusCode:                  resp.StatusCode(),
				ErrorMessage:                    err.Error(),
				RawResponse:                     string(resp.Body()),
			}
			return response, err
		}
		err = errors.New(errReps.Message)
		response := AnalysisResultBatchResponse{
			AnalysisResultBatchItemInfoList: analysisResultBatchItemInfoList,
			HTTPStatusCode:                  resp.StatusCode(),
			ErrorMessage:                    err.Error(),
			RawResponse:                     string(resp.Body()),
		}
		return response, err
	default:
		err = fmt.Errorf("%s %d", MsgUnexpectedErrorFromCerberus, resp.StatusCode())
		response := AnalysisResultBatchResponse{
			AnalysisResultBatchItemInfoList: analysisResultBatchItemInfoList,
			HTTPStatusCode:                  resp.StatusCode(),
			ErrorMessage:                    err.Error(),
			RawResponse:                     string(resp.Body()),
		}
		return response, err
	}
}

func (c *cerberusClient) SendAnalysisResultImageBatch(images []WorkItemResultImageTO) error {
	_, err := c.client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(images).
		Post(c.cerberusUrl + "/v1/analysis-results/image/batch")

	if err != nil {
		log.Error().Err(err).Msg(MsgFailedToCallImageBatchAPI)
		return err
	}

	return nil
}

func (c *cerberusClient) VerifyInstrumentHash(hash string) error {
	verifyTO := VerifyInstrumentTO{Hash: hash}
	var errResponse clientError
	resp, err := c.client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(verifyTO).
		SetError(&errResponse).
		Post(c.cerberusUrl + "/v1/instruments/verify")

	if err != nil {
		log.Error().Err(err).Msg(MsgFailedToCallVerifyAPI)
		return err
	}

	if resp.IsError() {
		log.Error().Str("Message", errResponse.Message).Msg(MsgFailedToCallVerifyAPI)
		return errors.New(errResponse.Message)
	}

	return nil
}

func (c *cerberusClient) VerifyExpectedControlResultsHash(hash string) error {
	verifyTO := VerifyInstrumentTO{Hash: hash}
	var errResponse clientError
	resp, err := c.client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(verifyTO).
		SetError(&errResponse).
		Post(c.cerberusUrl + "/v1/expected-control-results/verify")

	if err != nil {
		log.Error().Err(err).Msg(MsgFailedToCallExpectedControlResultsAPI)
		return err
	}

	if resp.IsError() {
		log.Error().Str("Message", errResponse.Message).Msg(MsgFailedToCallExpectedControlResultsAPI)
		return errors.New(errResponse.Message)
	}

	return nil
}

func (c *cerberusClient) SyncAnalysisRequests(workItemIDs []uuid.UUID, syncType string) error {
	queryParams := make(map[string]string)
	queryParams["driverName"] = c.driverName
	queryParams["syncType"] = syncType
	resp, err := c.client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(workItemIDs).
		SetQueryParams(queryParams).
		Post(c.cerberusUrl + "/v1/instrument-drivers/analysis-requests/sync")

	if err != nil {
		log.Error().Err(err).Msg(MsgFailedToCallSyncAPI)
		return fmt.Errorf("%s %d", MsgUnexpectedErrorFromCerberus, resp.StatusCode())
	}

	return nil
}

func (c *cerberusClient) SendConsoleLog(consoleLogDTOs []ConsoleLogDTO) {
	var errResponse clientError
	resp, err := c.client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(consoleLogDTOs).
		SetError(&errResponse).
		Post(c.cerberusUrl + "/v1/instruments/logs")

	if err != nil {
		log.Error().Err(err).Msg(MsgFailedToCallLogsAPI)
	}

	if resp.IsError() {
		log.Error().Str("Message", errResponse.Message).Msg(MsgFailedToCallLogsAPI)
	}
}

func (c *cerberusClient) SetInstrumentOnlineStatus(instrumentId uuid.UUID, status InstrumentStatus) error {
	var errResponse clientError
	resp, err := c.client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(instrumentStatusTO{Status: status}).
		SetError(&errResponse).
		Put(c.cerberusUrl + "/v1/instruments/" + instrumentId.String() + "/status")

	if err != nil {
		log.Error().Err(err).Str("InstrumentId", instrumentId.String()).Msg(MsgFailedToSetOnlineStatus)
		return err
	}

	if resp.IsError() {
		log.Error().Str("Message", errResponse.Message).Str("InstrumentId", instrumentId.String()).Msg(MsgFailedToSetOnlineStatus)
		return errors.New(errResponse.Message)
	}

	return nil
}

func (c *cerberusClient) SendSampleSeenMessageBatch(messages []SampleSeenMessage) error {
	dtos := make([]sampleSeenMessageTO, len(messages))
	for i := range messages {
		dtos[i] = sampleSeenMessageTO{
			InstrumentID: messages[i].InstrumentID,
			ModuleName:   messages[i].ModuleName,
			SampleCode:   messages[i].SampleCode,
			SeenAt:       messages[i].SeenAt,
		}
	}
	var errResponse clientError
	resp, err := c.client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(dtos).
		SetError(&errResponse).
		Post(c.cerberusUrl + "/v1/instruments/sample-seen")
	if resp == nil {
		log.Error().Msg(MsgFailedToCallSampleSeenAPI)
		return ErrSendSampleSeenMessageBatchFailed
	}
	if err != nil {
		log.Error().Err(err).Msg(MsgFailedToCallSampleSeenAPI)
		return ErrSendSampleSeenMessageBatchFailed
	}

	if resp.IsError() {
		log.Error().Str("message", errResponse.Message).Int("statusCode", resp.StatusCode()).Msg(MsgSendSampleSeenMessageBatchFailed)
		return ErrSendSampleSeenMessageBatchFailed
	}

	return nil
}
