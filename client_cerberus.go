package skeleton

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/go-resty/resty/v2"
	"github.com/rs/zerolog/log"
)

const (
	MsgSendResultBatchFailed      = "send result batch failed"
	MsgSyncAnalysisRequestsFailed = "sync analysis requests failed"
)

var (
	ErrSendResultBatchFailed      = errors.New(MsgSendResultBatchFailed)
	ErrSyncAnalysisRequestsFailed = errors.New(MsgSyncAnalysisRequestsFailed)
)

type CerberusClient interface {
	RegisterInstrumentDriver(name, displayName string, extraValueKeys []string, protocols []supportedProtocolTO, tests []supportedManufacturerTestTO, encodings []string) error
	SendAnalysisResultBatch(analysisResults []AnalysisResultTO) (AnalysisResultBatchResponse, error)
	SendAnalysisResultImageBatch(images []WorkItemResultImageTO) error
	VerifyInstrumentHash(hash string) error
	SyncAnalysisRequests(workItemIDs []uuid.UUID, syncType string) error
}

type cerberusClient struct {
	client      *resty.Client
	cerberusUrl string
	driverName  string
}

type registerInstrumentDriverTO struct {
	Name              string                        `json:"name" binding:"required"`
	DisplayName       string                        `json:"display_name"`
	ExtraValueKeys    []string                      `json:"extraValueKeys,omitempty"`
	Protocols         []supportedProtocolTO         `json:"protocols"`
	ManufacturerTests []supportedManufacturerTestTO `json:"manufacturerTests"`
	Encodings         []string                      `json:"encodings"`
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
	WorkingItemID            uuid.UUID         `json:"workItemId"`
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
	ReagentInfos             []ReagentInfoTO   `json:"reagentInfos"`
	Images                   []ImageTO         `json:"images"`
	WarnFlag                 bool              `json:"warnFlag"`
	Warnings                 []string          `json:"warnings"`
}

type ImageTO struct {
	ID          uuid.UUID `json:"imageId"`
	Name        string    `json:"name"`
	Description *string   `json:"description,omitempty"`
}

type ReagentInfoTO struct {
	SerialNumber            string    `json:"serialNo"`
	Name                    string    `json:"name"`
	Code                    string    `json:"code"`
	LotNo                   string    `json:"lotNo"`
	ShelfLife               time.Time `json:"shelfLife"`
	ExpiryDateTime          time.Time `json:"expiryDateTime"`
	ManufacturerName        string    `json:"manufacturer"`
	ReagentManufacturerDate time.Time `json:"reagentManufacturerDate"`
	ReagentType             string    `json:"reagentType"`
	UseUntil                time.Time `json:"useUntil"`
	DateCreated             time.Time `json:"dateCreated"`
}

type WorkItemResultImageTO struct {
	WorkItemID          uuid.UUID  `json:"workItemId"`
	ResultYieldDateTime *time.Time `json:"resultYieldDateTime"`
	ChannelID           *uuid.UUID `json:"channelId"`
	Image               ImageTO    `json:"image"`
}

type VerifyInstrumentTO struct {
	Hash string `json:"hash"`
}

func NewCerberusClient(cerberusUrl string, restyClient *resty.Client) (CerberusClient, error) {
	if cerberusUrl == "" {
		return nil, fmt.Errorf("basepath for cerberus must be set. check your configurationf or CerberusURL")
	}

	return &cerberusClient{
		client:      restyClient,
		cerberusUrl: cerberusUrl,
	}, nil
}

func (c *cerberusClient) RegisterInstrumentDriver(name, displayName string, extraValueKeys []string,
	protocols []supportedProtocolTO, tests []supportedManufacturerTestTO, encodings []string) error {
	resp, err := c.client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(registerInstrumentDriverTO{
			Name:              name,
			DisplayName:       displayName,
			ExtraValueKeys:    extraValueKeys,
			Protocols:         protocols,
			ManufacturerTests: tests,
			Encodings:         encodings,
		}).
		Post(c.cerberusUrl + "/v1/instrument-drivers")

	if err != nil {
		log.Error().Err(err).Msg("Failed to call Cerberus API")
		return err
	}

	if resp.IsError() {
		errReps := clientError{}
		err = json.Unmarshal(resp.Body(), &errReps)
		if err != nil {
			log.Error().Err(err).Msg("Failed to unmarshal error of response")
			return err
		}
		return errors.New(errReps.Message)
	}
	c.driverName = name
	return nil
}

// SendAnalysisResultBatch Submit a list of AnalysisResults to Cerberus
func (c *cerberusClient) SendAnalysisResultBatch(analysisResults []AnalysisResultTO) (AnalysisResultBatchResponse, error) {
	if len(analysisResults) < 1 {
		log.Warn().Msg("Send analysis results batch called with empty array")
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
			ErrorMessage:                    "Failed to prepare data for sending",
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
			analysisResultBatchItemInfoList[i].ErrorMessage = stringPointerToString(responseItem.Error)
			analysisResultBatchItemInfoList[i].CerberusAnalysisResultID = nullUUIDToUUIDPointer(responseItem.ID)
		}

		response := AnalysisResultBatchResponse{
			AnalysisResultBatchItemInfoList: analysisResultBatchItemInfoList,
			HTTPStatusCode:                  resp.StatusCode(),
			RawResponse:                     string(resp.Body()),
		}

		return response, nil
	case resp.StatusCode() == http.StatusInternalServerError:
		errReps := clientError{}
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
		err = fmt.Errorf("unexpected error from cerberus %d", resp.StatusCode())
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
		log.Error().Err(err).Msg("Failed to call Cerberus API (/v1/analysis-results/image/batch)")
		return err
	}

	return nil
}
func (c *cerberusClient) VerifyInstrumentHash(hash string) error {
	verifyTO := VerifyInstrumentTO{Hash: hash}
	resp, err := c.client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(verifyTO).
		Post(c.cerberusUrl + "/v1/instruments/verify")

	if err != nil {
		log.Error().Err(err).Msg("Failed to call Cerberus API (/v1/instrument/verify)")
		return err
	}

	if resp.IsError() {
		log.Error().Int("Code", resp.StatusCode()).Msg("Failed to call Cerberus API (/v1/instrument/verify)")
		return fmt.Errorf("unexpected error from cerberus %d", resp.StatusCode())
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
		log.Error().Err(err).Msg("Failed to call Cerberus API (/v1/instrument-drivers/analysis-requests/sync)")
		return fmt.Errorf("unexpected error from cerberus %d", resp.StatusCode())
	}

	return nil
}
