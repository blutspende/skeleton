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

type Cerberus interface {
	RegisterInstrument(instrument Instrument) error
	SendAnalysisResultBatch(analysisResults []AnalysisResult) (AnalysisResultBatchResponse, error)
}

type cerberus struct {
	client      *resty.Client
	cerberusUrl string
}

type errorResponseTO struct {
	Code    string   `json:"code"`
	Message string   `json:"message"`
	Errors  []string `json:"errors"`
}

type ciaInstrumentTO struct {
	ID   uuid.UUID `json:"id"`
	Name string    `json:"name"`
}

type extraValueTO struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type createAnalysisResultResponseItemTO struct {
	ID         uuid.NullUUID `json:"id"`
	WorkItemID uuid.UUID     `json:"workItemId"`
	Error      *string       `json:"error"`
}

type channelResultTO struct {
	ChannelID             uuid.UUID         `json:"channelId"`
	QualitativeResult     string            `json:"qualitativeResult"`
	QualitativeResultEdit bool              `json:"edited"`
	QuantitativeResults   map[string]string `json:"quantitativeResults"`
	Images                []imageV1TO       `json:"images"`
}

type analysisResultTO struct {
	WorkingItemID            uuid.UUID       `json:"workItemId"`
	ValidUntil               time.Time       `json:"validUntil"`
	Status                   string          `json:"status"`
	Mode                     string          `json:"mode"` // ResultMode
	ResultYieldDateTime      time.Time       `json:"resultYieldDateTime"`
	ExaminedMaterial         uuid.UUID       `json:"examinedMaterial"`
	Result                   string          `json:"result"`
	Operator                 string          `json:"operator"`
	TechnicalReleaseDateTime time.Time       `json:"technicalReleaseDateTime"`
	InstrumentID             uuid.UUID       `json:"instrumentId"`
	InstrumentRunID          uuid.UUID       `json:"instrumentRunId" `
	ReagentInfos             []reagentInfoTO `json:"reagentInfos"`
	// OrderRef                 string              `json:"orderRef"`
	RunCounter  int            `json:"runCounter" `
	ExtraValues []extraValueTO `json:"extraValues"`
	Edited      bool           `json:"resultEdit"`
	EditReason  string         `json:"editReason"`
	//TODO:REMOVE : WarnFlag                 bool                `json:"warnFlag"`
	Warnings       []string          `json:"warnings"`
	ChannelResults []channelResultTO `json:"channelResults"`
	Images         []imageV1TO       `json:"images"`
}

type imageV1TO struct {
	ID          uuid.UUID `json:"id"`
	Name        string    `json:"name"`
	Description *string   `json:"description,omitempty"`
}

type reagentInfoTO struct {
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

func NewCerberusClient(cerberusUrl string, restyClient *resty.Client) (Cerberus, error) {
	if cerberusUrl == "" {
		return nil, fmt.Errorf("basepath for cerberus must be set. check your configurationf or CerberusURL")
	}

	return &cerberus{
		client:      restyClient,
		cerberusUrl: cerberusUrl,
	}, nil
}

// RegisterInstrument Update cerberus with changed instrument-information
func (c *cerberus) RegisterInstrument(instrument Instrument) error {
	ciaInstrumentTO := ciaInstrumentTO{
		ID:   instrument.ID,
		Name: instrument.Name,
	}

	resp, err := c.client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(ciaInstrumentTO).
		Post(c.cerberusUrl + "/v1/instruments")

	if err != nil && resp == nil {
		log.Error().Err(err).Msg("Failed to call Cerberus API")
		return err
	}

	if resp.StatusCode() != http.StatusNoContent {
		errReps := errorResponseTO{}
		err = json.Unmarshal(resp.Body(), &errReps)
		if err != nil {
			log.Error().Err(err).Msg("Failed to unmarshal error of response")
			return err
		}
		return errors.New(errReps.Message + "(" + errReps.Code + ")")
	}

	return nil
}

// SendAnalysisResultBatch Submit a list of AnalysisResults to Cerberus
func (cia *cerberus) SendAnalysisResultBatch(analysisResults []AnalysisResult) (AnalysisResultBatchResponse, error) {
	if len(analysisResults) < 1 {
		return AnalysisResultBatchResponse{}, nil
	}

	analysisResultsTOs := make([]analysisResultTO, len(analysisResults))
	analysisResultBatchItemInfoList := make([]AnalysisResultBatchItemInfo, len(analysisResults))

	var hasError bool
	for i, ar := range analysisResults {
		info := AnalysisResultBatchItemInfo{
			AnalysisResult: &analysisResults[i],
		}

		analysisResultTO := analysisResultTO{
			WorkingItemID:            ar.AnalysisRequest.WorkItemID,
			ValidUntil:               ar.ValidUntil,
			ResultYieldDateTime:      ar.ResultYieldDateTime,
			ExaminedMaterial:         ar.AnalysisRequest.MaterialID,
			Result:                   ar.Result,
			Operator:                 ar.Operator,
			TechnicalReleaseDateTime: ar.TechnicalReleaseDateTime,
			InstrumentID:             ar.Instrument.ID,
			InstrumentRunID:          ar.InstrumentRunID,
			ReagentInfos:             []reagentInfoTO{},
			RunCounter:               ar.RunCounter,
			ExtraValues:              []extraValueTO{},
			Edited:                   ar.Edited,
			EditReason:               ar.EditReason,
			Warnings:                 ar.Warnings,
			ChannelResults:           []channelResultTO{},
			//TODO Images                  : ar.Images,
		}

		switch ar.Status {
		case Preliminary:
			analysisResultTO.Status = "PRE"
		case Final:
			analysisResultTO.Status = "FIN"
		default:
			hasError = true
			info.ErrorMessage = fmt.Sprintf("Invalid result-status (%s)", ar.Status)
			log.Debug().Msg(info.ErrorMessage)
		}

		switch ar.ResultMode {
		case Simulation:
			//analysisResultTO.Mode = "SIMULATION"
			analysisResultTO.Mode = "TEST"
		case Qualify:
			//analysisResultTO.Mode = "QUALIFY"
			analysisResultTO.Mode = "VALIDATION"
		case Production:
			analysisResultTO.Mode = "PRODUCTION"
		default:
			hasError = true
			info.ErrorMessage = fmt.Sprintf("Invalid result-mode (%s)", ar.Instrument.ResultMode)
			log.Debug().Msg(info.ErrorMessage)
		}

		for _, ev := range ar.ExtraValues {
			extraValueTO := extraValueTO{
				Key:   ev.Key,
				Value: ev.Value,
			}
			analysisResultTO.ExtraValues = append(analysisResultTO.ExtraValues, extraValueTO)
		}

		for _, cr := range ar.ChannelResults {
			channelResultTO := channelResultTO{
				ChannelID:             cr.ChannelID,
				QualitativeResult:     cr.QualitativeResult,
				QualitativeResultEdit: cr.QualitativeResultEdit,
				QuantitativeResults:   cr.QuantitativeResults,
				// TODO: Images
			}
			analysisResultTO.ChannelResults = append(analysisResultTO.ChannelResults, channelResultTO)
		}

		for _, ri := range ar.ReagentInfos {
			reagentInfoTO := reagentInfoTO{
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
				hasError = true
				info.ErrorMessage = fmt.Sprintf("Invalid reagent type (%s)", ri.ReagentType)
				log.Debug().Msg(info.ErrorMessage)
			}
		}

		analysisResultBatchItemInfoList[i] = info
		analysisResultsTOs[i] = analysisResultTO
	}

	if hasError {
		response := AnalysisResultBatchResponse{
			AnalysisResultBatchItemInfoList: analysisResultBatchItemInfoList,
			ErrorMessage:                    "Failed to prepare data for sending",
		}
		return response, nil
	}

	resp, err := cia.client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(analysisResultsTOs).
		Post(cia.cerberusUrl + "/v1/analysis-results/batch")

	if err != nil {
		response := AnalysisResultBatchResponse{
			AnalysisResultBatchItemInfoList: analysisResultBatchItemInfoList,
			ErrorMessage:                    err.Error(),
		}

		return response, fmt.Errorf("%s (%w)", ErrSendResultBatchFailed, err)
		// log.Error().Err(err).Msg(i18n.MsgSendResultBatchFailed)
		//requestBody, _ := json.Marshal(analysisResultsTOs)
		//TODO:Better soltion for request-logging cia.ciaHistoryService.Create(model.TYPE_AnalysisResultBatch, err.Error(), string(requestBody), 0, nil, analysisResultIDs)
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
		errReps := errorResponseTO{}
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

/*
func (cia *cerberusV1) PostAnalysisResult(analysisResult v1.AnalysisResult) (v1.AnalysisResultBatchResponse, error) {
		var responseBodyStr string
		requestBody, _ := json.Marshal(cia.mapAnalysisResultToAnalysisResultDTO(analysisResult))
		requestBodyStr := string(requestBody)

		resp, err := cia.client.R().
			SetHeader("Content-Type", "application/json").
			SetBody(cia.mapAnalysisResultToAnalysisResultDTO(analysisResult)).
			Post(cia.config.CerberusURL + "/v1/analysis-results")

		if err != nil {
			log.Error().Err(err).Msg("Can not call internal cerberus api")
			cia.ciaHistoryService.Create(model.TYPE_AnalysisResultBatch, err.Error(), requestBodyStr, 0, nil, []uuid.UUID{analysisResult.ID})
			return nil, err
		}

		respBodyBytes := resp.Body()
		responseBodyStr = string(respBodyBytes)

		if resp.StatusCode() == http.StatusInternalServerError {
			errReps := errorResponseV1TO{}
			err = json.Unmarshal(resp.Body(), &errReps)
			if err != nil {
				log.Error().Err(err).Msg("Can not unmarshal error of resp")
				return nil, err
			}
			return nil, errors.New(errReps.Message)
		}

		_, err = cia.ciaHistoryService.Create(model.TYPE_AnalysisResultBatch, requestBodyStr, responseBodyStr, resp.StatusCode(), nil, []uuid.UUID{analysisResult.ID})
		if err != nil {
			log.Error().Err(err).Msg("Can not create cia http history")
		}
		if resp.StatusCode() == http.StatusAccepted {
			err = v1.ErrSendResultBatchPartiallyFailed
		}

		return resp, nil
	return nil, nil
}
*/
/*
	func (cia *cerberusV1) mapAnalysisResultToAnalysisResultDTO(analysisRes v1.AnalysisResult) analysisResultV1TO {
		return analysisResultV1TO{
			WorkingItemID:            analysisRes.WorkItemID,
			ValidUntil:               analysisRes.ValidUntil,
			Status:                   analysisRes.Status.ToString(),
			Mode:                     analysisRes.ResultMode.ToString(),
			ResultYieldDateTime:      analysisRes.ResultYieldDateTime,
			ExaminedMaterial:         analysisRes.ExaminedMaterial,
			Result:                   fmt.Sprint(analysisRes.Result),
			Operator:                 analysisRes.Operator,
			TechnicalReleaseDateTime: analysisRes.TechnicalReleaseDateTime,
			InstrumentID:             analysisRes.InstrumentID,
			InstrumentRunID:          analysisRes.InstrumentRunID,
			ReagentInfos:             cia.mapReagentInfoToReagentInfoDTO(analysisRes.ReagentInfos),
			RunCounter:               analysisRes.RunCounter,
			ExtraValues:              cia.mapExtraValuesToExtraValuesDTO(analysisRes.ExtraValues),
			Edited:                   analysisRes.Edited,
			EditReason:               analysisRes.EditReason,
			WarnFlag:                 analysisRes.WarnFlag,
			Warnings:                 analysisRes.Warnings,
			ChannelResults:           cia.mapChannelResultsToChannelResultsDTO(analysisRes.ChannelResults),
			Images:                   cia.mapImageToImageDTO(analysisRes.Images),
		}
	}

func (cia *cerberusV1) mapImageToImageDTO(images []v1.Image) []imageV1TO {
	imagesDTO := make([]imageV1TO, 0)
	for _, image := range images {
		imagesDTO = append(imagesDTO, imageV1TO{
			ID:          image.ID,
			Name:        image.Name,
			Description: image.Description,
		})
	}

	return imagesDTO
}

func (cia *cerberusV1) mapReagentInfoToReagentInfoDTO(reagentInfos []v1.ReagentInfo) []reagentInfoV1TO {
	reagentInfoList := make([]reagentInfoV1TO, 0)

	for _, reagentInfoItem := range reagentInfos {
		reagentInfoList = append(reagentInfoList, reagentInfoV1TO{
			SerialNumber:            reagentInfoItem.SerialNumber,
			Name:                    reagentInfoItem.Name,
			ShelfLife:               reagentInfoItem.ShelfLife,
			ManufacturerName:        reagentInfoItem.ManufacturerName,
			ReagentManufacturerDate: reagentInfoItem.ReagentManufacturerDate,
			ReagentType:             reagentInfoItem.ReagentType.ToString(),
			DateCreated:             reagentInfoItem.DateCreated,
		})
	}

	return reagentInfoList
}

// TODO: Eliminate and move to model
func (cia *cerberusV1) mapExtraValuesToExtraValuesDTO(extraValues []v1.ExtraValue) []extraValueV1TO {
	extraValueList := make([]extraValueV1TO, 0)

	for _, extraValueItem := range extraValues {
		extraValueList = append(extraValueList, extraValueV1TO{
			Key:   extraValueItem.Key,
			Value: extraValueItem.Value,
		})
	}

	return extraValueList
}

func (cia *cerberusV1) mapChannelResultsToChannelResultsDTO(channels []v1.ChannelResult) []channelResultV1TO {
	channelResultList := make([]channelResultV1TO, 0)

	for _, channel := range channels {
		channelResultList = append(channelResultList, channelResultV1TO{
			ChannelID:             channel.ChannelID,
			QualitativeResult:     channel.QualitativeResult,
			QualitativeResultEdit: channel.QualitativeResultEdit,
			QuantitativeResults:   channel.QuantitativeResults,
			Images:                cia.mapImageToImageDTO(channel.Images),
		})
	}

	return channelResultList
}
*/
