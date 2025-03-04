package skeleton

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/google/uuid"
	longpollclient "github.com/jcuga/golongpoll/client"
	"github.com/rs/zerolog/log"
	"net/http"
	"net/url"
	"time"
)

type InstrumentMessageType string

const (
	MessageTypeCreate InstrumentMessageType = "CREATE"
	MessageTypeUpdate InstrumentMessageType = "UPDATE"
	MessageTypeDelete InstrumentMessageType = "DELETE"
)

type InstrumentMessageTO struct {
	MessageType  InstrumentMessageType `json:"messageType" binding:"required"`
	InstrumentId uuid.UUID             `json:"instrumentId" binding:"required"`
	Instrument   *instrumentTO         `json:"instrument,omitempty"`
}

type ReprocessMessageType string

const (
	MessageTypeRetransmitResult      ReprocessMessageType = "RETRANSMIT_RESULT"
	MessageTypeReprocessBySampleCode ReprocessMessageType = "REPROCESS_BY_SAMPLE_CODE"
	MessageTypeReprocessByBatchIds   ReprocessMessageType = "REPROCESS_BY_BATCH_IDS"
)

type ReprocessMessageTO struct {
	MessageType ReprocessMessageType `json:"messageType" binding:"required"`
	ReprocessId interface{}          `json:"id" binding:"required"`
}

type LongPollClient interface {
	GetInstrumentConfigsChan() chan InstrumentMessageTO
	GetReprocessEventsChan() chan ReprocessMessageTO
	GetAnalysisRequestsChan() chan []AnalysisRequest
	GetRevokedWorkItemIDsChan() chan []uuid.UUID
	GetReexaminedWorkItemIDsChan() chan []uuid.UUID
	StartInstrumentConfigsLongPolling(ctx context.Context)
	StartReprocessEventsLongPolling(ctx context.Context)
	StartAnalysisRequestLongPolling(ctx context.Context)
	StartRevokedReexaminedWorkItemIDsLongPolling(ctx context.Context)
}

type longPollClient struct {
	restyClient               *resty.Client
	serviceName               string
	cerberusUrl               string
	timeoutSeconds            uint
	instrumentsChan           chan InstrumentMessageTO
	reprocessChan             chan ReprocessMessageTO
	analysisRequestsChan      chan []AnalysisRequest
	revokedWorkItemIDsChan    chan []uuid.UUID
	reexaminedWorkItemIDsChan chan []uuid.UUID
}

func NewLongPollClient(restyClient *resty.Client, serviceName, cerberusUrl string, timeoutSeconds uint) LongPollClient {
	return &longPollClient{
		restyClient:               restyClient,
		serviceName:               serviceName,
		cerberusUrl:               cerberusUrl,
		instrumentsChan:           make(chan InstrumentMessageTO),
		reprocessChan:             make(chan ReprocessMessageTO, 100),
		analysisRequestsChan:      make(chan []AnalysisRequest),
		revokedWorkItemIDsChan:    make(chan []uuid.UUID),
		reexaminedWorkItemIDsChan: make(chan []uuid.UUID),
		timeoutSeconds:            timeoutSeconds,
	}
}

func (l *longPollClient) GetInstrumentConfigsChan() chan InstrumentMessageTO {
	return l.instrumentsChan
}

func (l *longPollClient) GetReprocessEventsChan() chan ReprocessMessageTO {
	return l.reprocessChan
}

func (l *longPollClient) GetAnalysisRequestsChan() chan []AnalysisRequest {
	return l.analysisRequestsChan
}

func (l *longPollClient) GetRevokedWorkItemIDsChan() chan []uuid.UUID {
	return l.revokedWorkItemIDsChan
}

func (l *longPollClient) GetReexaminedWorkItemIDsChan() chan []uuid.UUID {
	return l.reexaminedWorkItemIDsChan
}

func (l *longPollClient) StartInstrumentConfigsLongPolling(ctx context.Context) {
	u, err := url.Parse(l.cerberusUrl + "/v1/instruments/poll-config")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to parse URL for long-poll")
	}

	httpClient := &http.Client{
		Transport: &RestyRoundTripper{restyClient: l.restyClient},
	}

	c, err := longpollclient.NewClient(longpollclient.ClientOptions{
		SubscribeUrl:       *u,
		Category:           fmt.Sprintf("%s:%s", l.serviceName, syncTypeInstrument),
		PollTimeoutSeconds: l.timeoutSeconds,
		HttpClient:         httpClient,
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to create long-poll client")
		return
	}

	for event := range c.Start(time.Now().UTC().AddDate(0, 0, -1)) {
		select {
		case <-ctx.Done():
			log.Info().Msg("Long poll gracefully stopped")
			return
		default:
			data, ok := event.Data.(map[string]interface{})
			if !ok {
				log.Error().Msg("Unexpected event data type")
				return
			}

			jsonData, err := json.Marshal(data)
			if err != nil {
				log.Error().Err(err).Msg("Failed to marshal event data")
				return
			}

			var instrumentMessageTO InstrumentMessageTO
			if err := json.Unmarshal(jsonData, &instrumentMessageTO); err != nil {
				log.Error().Err(err).Msg("Failed to unmarshal event data to InstrumentMessageTO")
				return
			}

			log.Debug().
				Str("MessageType", string(instrumentMessageTO.MessageType)).
				Str("InstrumentId", instrumentMessageTO.InstrumentId.String()).
				Msg("Received event mapped to InstrumentMessageTO")

			l.instrumentsChan <- instrumentMessageTO
		}
	}
}

func (l *longPollClient) StartReprocessEventsLongPolling(ctx context.Context) {
	u, err := url.Parse(l.cerberusUrl + "/v1/instruments/poll-reprocess")
	if err != nil {
		log.Fatal().Err(err).Msg("start reprocess long polling failed")
	}

	httpClient := &http.Client{
		Transport: &RestyRoundTripper{restyClient: l.restyClient},
	}

	c, err := longpollclient.NewClient(longpollclient.ClientOptions{
		SubscribeUrl:       *u,
		Category:           fmt.Sprintf("%s:%s", l.serviceName, syncTypeReprocess),
		PollTimeoutSeconds: l.timeoutSeconds,
		HttpClient:         httpClient,
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to create long-poll client")
		return
	}

	for event := range c.Start(time.Now().UTC().AddDate(0, 0, -1)) {
		select {
		case <-ctx.Done():
			log.Info().Msg("Long poll gracefully stopped")
			return
		default:
			data, ok := event.Data.(map[string]interface{})
			if !ok {
				log.Error().Msg("Unexpected event data type")
				return
			}

			jsonData, err := json.Marshal(data)
			if err != nil {
				log.Error().Err(err).Msg("Failed to marshal event data")
				return
			}

			var reprocessMessageTO ReprocessMessageTO
			if err := json.Unmarshal(jsonData, &reprocessMessageTO); err != nil {
				log.Error().Err(err).Msg("Failed to unmarshal event data to ReprocessMessageTO")
				return
			}

			log.Debug().
				Str("MessageType", string(reprocessMessageTO.MessageType)).
				Interface("ReprocessId", reprocessMessageTO.ReprocessId).
				Msg("Received event mapped to ReprocessMessageTO")

			l.reprocessChan <- reprocessMessageTO
		}
	}
}

func (l *longPollClient) StartAnalysisRequestLongPolling(ctx context.Context) {
	u, err := url.Parse(l.cerberusUrl + "/v1/instrument-drivers/analysis-requests")
	if err != nil {
		log.Fatal().Err(err).Msg("start analysis requests long polling failed")
	}

	httpClient := &http.Client{
		Transport: &RestyRoundTripper{restyClient: l.restyClient},
	}

	c, err := longpollclient.NewClient(longpollclient.ClientOptions{
		SubscribeUrl:       *u,
		Category:           fmt.Sprintf("%s:%s", l.serviceName, syncTypeNewWorkItem),
		PollTimeoutSeconds: l.timeoutSeconds,
		HttpClient:         httpClient,
	})
	if err != nil {
		log.Error().Err(err).Msg("create longpoll client failed")
		return
	}
	for event := range c.Start(time.Now().UTC().AddDate(0, 0, -1)) {
		select {
		case <-ctx.Done():
			log.Info().Msg("Long poll gracefully stopped")
			return
		default:
			data, ok := event.Data.([]interface{})
			if !ok {
				log.Error().Msg("unexpected event data type")
				continue
			}

			jsonData, err := json.Marshal(data)
			if err != nil {
				log.Error().Err(err).Interface("data", data).Msg("marshal event data failed")
				continue
			}

			var analysisRequestTOs []analysisRequestTO
			err = json.Unmarshal(jsonData, &analysisRequestTOs)
			if err != nil {
				log.Error().Err(err).Interface("jsonData", jsonData).Msg("unmarshal event data to analysis requests failed")
				continue
			}
			analysisRequests := make([]AnalysisRequest, len(analysisRequestTOs))
			for i := range analysisRequestTOs {
				analysisRequests[i] = convertTOToAnalysisRequest(analysisRequestTOs[i])
			}
			l.analysisRequestsChan <- analysisRequests
		}
	}
}

const revokedReexaminedCategory = "revoked-reexamined"

func (l *longPollClient) StartRevokedReexaminedWorkItemIDsLongPolling(ctx context.Context) {
	u, err := url.Parse(l.cerberusUrl + "/v1/instrument-drivers/analysis-requests/revoked-reexamined")
	if err != nil {
		log.Fatal().Err(err).Msg("start revoked work item IDs long polling failed")
	}

	httpClient := &http.Client{
		Transport: &RestyRoundTripper{restyClient: l.restyClient},
	}

	c, err := longpollclient.NewClient(longpollclient.ClientOptions{
		SubscribeUrl:       *u,
		Category:           fmt.Sprintf("%s:%s", l.serviceName, revokedReexaminedCategory),
		PollTimeoutSeconds: l.timeoutSeconds,
		HttpClient:         httpClient,
	})
	if err != nil {
		log.Error().Err(err).Msg("create longpoll client failed")
		return
	}

	for event := range c.Start(time.Now().UTC().AddDate(0, 0, -1)) {
		select {
		case <-ctx.Done():
			log.Info().Msg("Long poll gracefully stopped")
			return
		default:
			data, ok := event.Data.(interface{})
			if !ok {
				log.Error().Msg("unexpected event data type")
				continue
			}
			jsonData, err := json.Marshal(data)
			if err != nil {
				log.Error().Err(err).Interface("data", data).Msg("marshal event data failed")
				continue
			}

			var workItemIDsTO workItemIDLongPollTO
			err = json.Unmarshal(jsonData, &workItemIDsTO)
			if err != nil {
				log.Error().Err(err).Interface("jsonData", jsonData).Msg("unmarshal event data to UUIDs failed")
				continue
			}
			switch workItemIDsTO.Type {
			case syncTypeRevocation:
				l.revokedWorkItemIDsChan <- workItemIDsTO.WorkItemIDs
			case syncTypeReexamine:
				l.reexaminedWorkItemIDsChan <- workItemIDsTO.WorkItemIDs
			default:
				log.Error().Str("type", workItemIDsTO.Type).Msg("unexpected type of work item IDs in revoke-reexamine job")
			}
		}
	}
}

func convertTOToAnalysisRequest(to analysisRequestTO) AnalysisRequest {
	analysisRequest := AnalysisRequest{
		WorkItemID:     to.WorkItemID,
		AnalyteID:      to.AnalyteID,
		SampleCode:     to.SampleCode,
		MaterialID:     to.MaterialID,
		LaboratoryID:   to.LaboratoryID,
		ValidUntilTime: to.ValidUntilTime,
		ExtraValues:    convertExtraValueTOsToExtraValues(to.ExtraValues),
	}
	if to.Subject != nil {
		analysisRequest.SubjectInfo = &SubjectInfo{
			Type:         "",
			DateOfBirth:  to.Subject.DateOfBirth,
			FirstName:    to.Subject.FirstName,
			LastName:     to.Subject.LastName,
			DonorID:      to.Subject.DonorID,
			DonationID:   to.Subject.DonationID,
			DonationType: to.Subject.DonationType,
			Pseudonym:    to.Subject.Pseudonym,
		}
		switch to.Subject.Type {
		case "DONOR":
			analysisRequest.SubjectInfo.Type = Donor
		case "PERSONAL":
			analysisRequest.SubjectInfo.Type = Personal
		case "PSEUDONYMIZED":
			analysisRequest.SubjectInfo.Type = Pseudonym
		case "":
			analysisRequest.SubjectInfo = nil
		default:
			log.Error().Str("workItemID", to.WorkItemID.String()).
				Str("subjectID", to.Subject.ID.String()).
				Msgf("Invalid subject Type provided (%+v)", to.Subject.Type)
			analysisRequest.SubjectInfo = nil
		}
	}

	return analysisRequest
}

type workItemIDLongPollTO struct {
	Type        string      `json:"type"`
	WorkItemIDs []uuid.UUID `json:"workItemIDs"`
}

type clientError struct {
	MessageKey    string            `json:"messageKey"`
	MessageParams map[string]string `json:"messageParams"`
	Message       string            `json:"message"`
	Errors        []clientError     `json:"errors"`
}

type subjectType string

const (
	donor     subjectType = "DONOR"
	personal  subjectType = "PERSONAL"
	pseudonym subjectType = "PSEUDONYMIZED"
)

type subjectTO struct {
	ID           uuid.UUID   `json:"-"`
	Type         subjectType `json:"type"`
	DateOfBirth  *time.Time  `json:"dateOfBirth" format:"date"`
	FirstName    *string     `json:"firstName"`
	LastName     *string     `json:"lastName"`
	DonorID      *string     `json:"donorId"`
	DonationID   *string     `json:"donationId"`
	DonationType *string     `json:"donationType"`
	Pseudonym    *string     `json:"pseudonym"`
}

type extraValueTO struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func convertExtraValueTOsToExtraValues(extraValueTOs []extraValueTO) []ExtraValue {
	extraValues := make([]ExtraValue, len(extraValueTOs))
	for i := range extraValueTOs {
		extraValues[i] = convertExtraValueTOToExtraValues(extraValueTOs[i])
	}
	return extraValues
}
func convertExtraValueTOToExtraValues(to extraValueTO) ExtraValue {
	return ExtraValue{
		Key:   to.Key,
		Value: to.Value,
	}
}

type analysisRequestTO struct {
	WorkItemID     uuid.UUID      `json:"workItemId"`
	AnalyteID      uuid.UUID      `json:"analyteId"`
	SampleCode     string         `json:"sampleCode"`
	MaterialID     uuid.UUID      `json:"materialId"`
	LaboratoryID   uuid.UUID      `json:"laboratoryId"`
	ValidUntilTime time.Time      `json:"validUntilTime"`
	Subject        *subjectTO     `json:"subject"`
	ExtraValues    []extraValueTO `json:"extraValues"`
}
