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

type CerberusV1 interface {
	RegisterInstrument(instrument Instrument) error
	PostAnalysisResultBatch(analysisResults []AnalysisResult) ([]AnalysisResultCreateStatusV1, error)
}

type cerberusV1 struct {
	client      *resty.Client
	cerberusUrl string
}

type errorResponseV1TO struct {
	Code    string   `json:"code"`
	Message string   `json:"message"`
	Errors  []string `json:"errors,omitempty"`
}

type ciaInstrumentV1TO struct {
	ID   uuid.UUID `json:"id"`
	Name string    `json:"name"`
}

type extraValueV1TO struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type createAnalysisResultResponseItemV1TO struct {
	ID         uuid.NullUUID `json:"id"`
	WorkItemID uuid.UUID     `json:"workItemId"`
	Error      *string       `json:"error"`
}

type channelResultV1TO struct {
	ChannelID             uuid.UUID         `json:"channelId"`
	QualitativeResult     string            `json:"qualitativeResult"`
	QualitativeResultEdit bool              `json:"edited"`
	QuantitativeResults   map[string]string `json:"quantitativeResults"`
	Images                []imageV1TO       `json:"images"`
}

type analysisResultV1TO struct {
	WorkingItemID            uuid.UUID         `json:"workItemId"`
	ValidUntil               time.Time         `json:"validUntil"`
	Status                   string            `json:"status"`
	Mode                     string            `json:"mode"`
	ResultYieldDateTime      time.Time         `json:"resultYieldDateTime"`
	ExaminedMaterial         uuid.UUID         `json:"examinedMaterial"`
	Result                   string            `json:"result"`
	Operator                 string            `json:"operator"`
	TechnicalReleaseDateTime time.Time         `json:"technicalReleaseDateTime"`
	InstrumentID             uuid.UUID         `json:"instrumentId"`
	InstrumentRunID          uuid.UUID         `json:"instrumentRunId" `
	ReagentInfos             []reagentInfoV1TO `json:"reagentInfos"`
	// OrderRef                 string              `json:"orderRef"`
	RunCounter  int              `json:"runCounter" `
	ExtraValues []extraValueV1TO `json:"extraValues"`
	Edited      bool             `json:"resultEdit"`
	EditReason  string           `json:"editReason"`
	//TODO:REMOVE : WarnFlag                 bool                `json:"warnFlag"`
	Warnings       []string            `json:"warnings"`
	ChannelResults []channelResultV1TO `json:"channelResults"`
	Images         []imageV1TO         `json:"images"`
}

type imageV1TO struct {
	ID          uuid.UUID `json:"id"`
	Name        string    `json:"name"`
	Description *string   `json:"description,omitempty"`
}

type reagentInfoV1TO struct {
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

func NewCerberusV1Client(cerberusUrl string, restyClient *resty.Client) (CerberusV1, error) {

	if cerberusUrl == "" {
		return nil, fmt.Errorf("basepath for cerberus must be set. check your configurationf or CerberusURL")
	}

	return &cerberusV1{
		client:      restyClient,
		cerberusUrl: cerberusUrl,
		//TODO REMOVE ciaHistoryService: ciaHistoryService,
	}, nil
}

// RegisterInstrument UPdate cerberus with changed instrument-information
func (cia *cerberusV1) RegisterInstrument(instrument Instrument) error {
	instrumentDTO := ciaInstrumentV1TO{
		ID:   instrument.ID,
		Name: instrument.Name,
	}

	resp, err := cia.client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(instrumentDTO).
		Post(cia.cerberusUrl + "/v1/instruments")

	if err != nil && resp == nil {
		log.Error().Err(err).Msg("Can not call internal cerberus api")
		return err
	}

	if resp.StatusCode() != http.StatusNoContent {
		errReps := errorResponseV1TO{}
		err = json.Unmarshal(resp.Body(), &errReps)
		if err != nil {
			log.Error().Err(err).Msg("Can not unmarshal error of resp")
			return err
		}
		return errors.New(errReps.Message + "(" + errReps.Code + ")")
	}

	return nil
}

// PostAnalysisResultBatch Submit a list of Analysisresults to Cerberus
func (cia *cerberusV1) PostAnalysisResultBatch(analysisResults []AnalysisResult) ([]AnalysisResultCreateStatusV1, error) {

	if len(analysisResults) == 0 {
		return []AnalysisResultCreateStatusV1{}, nil
	}

	analysisResultsTOs := make([]analysisResultV1TO, 0)

	for _, ar := range analysisResults {

		analysisResultTO := analysisResultV1TO{
			WorkingItemID:            ar.AnalysisRequest.WorkItemID,
			ValidUntil:               ar.ValidUntil,
			Status:                   "",
			Mode:                     "",
			ResultYieldDateTime:      ar.ResultYieldDateTime,
			ExaminedMaterial:         ar.AnalysisRequest.MaterialID,
			Result:                   ar.Result,
			Operator:                 ar.Operator,
			TechnicalReleaseDateTime: ar.TechnicalReleaseDateTime,
			InstrumentID:             ar.Instrument.ID,
			InstrumentRunID:          ar.InstrumentRunID,
			ReagentInfos:             []reagentInfoV1TO{},
			RunCounter:               ar.RunCounter,
			ExtraValues:              []extraValueV1TO{},
			Edited:                   ar.Edited,
			EditReason:               ar.EditReason,
			Warnings:                 ar.Warnings,
			ChannelResults:           []channelResultV1TO{},
			//TODO Images                  : ar.Images,
		}

		switch ar.Status {
		case Preliminary:
			analysisResultTO.Status = "PRE"
		case Final:
			analysisResultTO.Status = "FIN"
		default:
			return nil, fmt.Errorf("Invalid result-status '%s'", ar.Status) // TODO: Can-it later
		}

		switch ar.Instrument.ResultMode {
		case Simulation:
			analysisResultTO.Mode = "SIMULATION"
		case Qualify:
			analysisResultTO.Mode = "QUALIFY"
		case Production:
			analysisResultTO.Mode = "PRODUCTION"
		default:
			return nil, fmt.Errorf("Invalid result-mode '%s'", ar.Instrument.ResultMode) // TODO: Can-it later
		}

		for _, ev := range ar.ExtraValues {
			extraValueTO := extraValueV1TO{
				Key:   ev.Key,
				Value: ev.Value,
			}
			analysisResultTO.ExtraValues = append(analysisResultTO.ExtraValues, extraValueTO)
		}

		for _, cr := range ar.ChannelResults {
			channelResultTO := channelResultV1TO{
				ChannelID:             cr.ChannelID,
				QualitativeResult:     cr.QualitativeResult,
				QualitativeResultEdit: cr.QualitativeResultEdit,
				QuantitativeResults:   cr.QuantitativeResults,
				// TODO: Images
			}
			analysisResultTO.ChannelResults = append(analysisResultTO.ChannelResults, channelResultTO)
		}

		for _, ri := range ar.ReagentInfos {
			reagentInfoTO := reagentInfoV1TO{
				SerialNumber:            ri.SerialNumber,
				Name:                    ri.Name,
				Code:                    ri.Code,
				LotNo:                   ri.LotNo,
				ReagentManufacturerDate: ri.ReagentManufacturerDate,
				UseUntil:                ri.UseUntil,
				ShelfLife:               ri.ShelfLife,
				//TODO Add an expiry : ExpiryDateTime: (this has to be added to database as well)
				ManufacturerName: ri.ManufacturerName,
				ReagentType:      "",
				DateCreated:      ri.DateCreated,
			}

			switch ri.ReagentType {
			case Reagent:
				reagentInfoTO.ReagentType = "Reagent"
			case Diluent:
				reagentInfoTO.ReagentType = "Diluent"
			default:
			}
		}

		analysisResultsTOs = append(analysisResultsTOs, analysisResultTO)
	}

	resp, err := cia.client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(analysisResultsTOs).
		Post(cia.cerberusUrl + "/v1/analysis-results/batch")

	if err != nil {
		return nil, fmt.Errorf("%s (%w)", ErrSendResultBatchFailed, err)
		// log.Error().Err(err).Msg(i18n.MsgSendResultBatchFailed)
		//requestBody, _ := json.Marshal(analysisResultsTOs)
		//TODO:Better soltion for request-logging cia.ciaHistoryService.Create(model.TYPE_AnalysisResultBatch, err.Error(), string(requestBody), 0, nil, analysisResultIDs)
	}

	switch {
	case resp.StatusCode() == http.StatusCreated, resp.StatusCode() == http.StatusAccepted:
		responseItems := []createAnalysisResultResponseItemV1TO{}
		err = json.Unmarshal(resp.Body(), responseItems)
		if err != nil {
			return nil, err
		}

		returnAnalysisResultStatus := []AnalysisResultCreateStatusV1{}
		for i, responseItem := range responseItems {
			analysisResultStatus := AnalysisResultCreateStatusV1{
				AnalyisResult:            &analysisResults[i],
				Success:                  responseItem.ID.Valid,
				ErrorMessage:             "",
				CerberusAnalysisResultID: responseItem.ID,
			}
			if responseItem.Error != nil {
				analysisResultStatus.ErrorMessage = *responseItem.Error
			}
			returnAnalysisResultStatus = append(returnAnalysisResultStatus)
		}
		return returnAnalysisResultStatus, nil
	case resp.StatusCode() == http.StatusInternalServerError:
		errReps := errorResponseV1TO{}
		err = json.Unmarshal(resp.Body(), &errReps)
		if err != nil {
			return nil, fmt.Errorf("can not unmarshal error of resp (%w)", err)
		}
		return nil, errors.New(errReps.Message)
	default:
		return nil, fmt.Errorf("unexpected error from cerberus %d", resp.StatusCode())
	}
}

/*
func (cia *cerberusV1) PostAnalysisResult(analysisResult v1.AnalysisResult) (v1.AnalysisResultCreateStatusV1, error) {
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
