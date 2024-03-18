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
	MsgSendResultBatchFailed = "send result batch failed"
)

var (
	ErrSendResultBatchFailed = errors.New(MsgSendResultBatchFailed)
)

type CerberusClient interface {
	RegisterInstrument(instrument Instrument) error
	RegisterInstrumentDriver(name, apiVersion string, apiPort uint16, tlsEnabled bool) error
	SendAnalysisResultBatch(analysisResults []AnalysisResultTO) (AnalysisResultBatchResponse, error)
	SendAnalysisResultImageBatch(images []WorkItemResultImageTO) error
}

type cerberusClient struct {
	client      *resty.Client
	cerberusUrl string
}

type ciaInstrumentTO struct {
	ID   uuid.UUID `json:"id"`
	Name string    `json:"name"`
}

type registerInstrumentDriverTO struct {
	Name       string `json:"name" binding:"required"`
	APIVersion string `json:"apiVersion" binding:"required"`
	APIPort    uint16 `json:"apiPort" binding:"required"`
	TLSEnabled bool   `json:"tlsEnabled" default:"false"`
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

func NewCerberusClient(cerberusUrl string, restyClient *resty.Client) (CerberusClient, error) {
	if cerberusUrl == "" {
		return nil, fmt.Errorf("basepath for cerberus must be set. check your configurationf or CerberusURL")
	}

	return &cerberusClient{
		client:      restyClient,
		cerberusUrl: cerberusUrl,
	}, nil
}

// RegisterInstrument Update cerberus with changed instrument-information
func (c *cerberusClient) RegisterInstrument(instrument Instrument) error {
	ciaInstrumentTO := ciaInstrumentTO{
		ID:   instrument.ID,
		Name: instrument.Name,
	}

	resp, err := c.client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(ciaInstrumentTO).
		Post(c.cerberusUrl + "/v1/instruments")

	if err != nil {
		log.Error().Err(err).Msg("Failed to call Cerberus API")
		return err
	}

	if resp.StatusCode() != http.StatusNoContent {
		errReps := clientError{}
		err = json.Unmarshal(resp.Body(), &errReps)
		if err != nil {
			log.Error().Err(err).Msg("Failed to unmarshal error of response")
			return err
		}
		return errors.New(errReps.Message)
	}

	return nil
}

func (c *cerberusClient) RegisterInstrumentDriver(name, apiVersion string, apiPort uint16, tlsEnabled bool) error {
	resp, err := c.client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(registerInstrumentDriverTO{
			Name:       name,
			APIVersion: apiVersion,
			APIPort:    apiPort,
			TLSEnabled: tlsEnabled,
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
