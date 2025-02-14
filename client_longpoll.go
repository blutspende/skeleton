package skeleton

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/google/uuid"
	"github.com/jcuga/golongpoll"
	longpollclient "github.com/jcuga/golongpoll/client"
	"github.com/rs/zerolog/log"
	"net/http"
	"net/url"
	"time"
)

type MessageType string

const (
	MessageTypeCreate MessageType = "CREATE"
	MessageTypeUpdate MessageType = "UPDATE"
	MessageTypeDelete MessageType = "DELETE"
)

type InstrumentMessageTO struct {
	MessageType  MessageType   `json:"messageType" binding:"required"`
	InstrumentId uuid.UUID     `json:"instrumentId" binding:"required"`
	Instrument   *instrumentTO `json:"instrument,omitempty"`
}

type LongPollClient interface {
	StartInstrumentLongPoll(ctx context.Context)
	GetAnalysisRequestsChan() chan []AnalysisRequest
	GetRevokedWorkItemIDsChan() chan []uuid.UUID
	GetReexaminedWorkItemIDsChan() chan []uuid.UUID
	StartAnalysisRequestLongPolling(ctx context.Context)
	StartRevokedWorkItemIDsLongPolling(ctx context.Context)
	StartReexaminedWorkItemIDsLongPolling(ctx context.Context)
}

type longPollClient struct {
	restyClient               *resty.Client
	instrumentService         InstrumentService
	serviceName               string
	cerberusUrl               string
	timeoutSeconds            uint
	analysisRequestsChan      chan []AnalysisRequest
	revokedWorkItemIDsChan    chan []uuid.UUID
	reexaminedWorkItemIDsChan chan []uuid.UUID
}

func NewLongPollClient(restyClient *resty.Client, instrumentService InstrumentService, serviceName, cerberusUrl string, timeoutSeconds uint) LongPollClient {
	return &longPollClient{
		restyClient:               restyClient,
		instrumentService:         instrumentService,
		serviceName:               serviceName,
		cerberusUrl:               cerberusUrl,
		analysisRequestsChan:      make(chan []AnalysisRequest),
		revokedWorkItemIDsChan:    make(chan []uuid.UUID),
		reexaminedWorkItemIDsChan: make(chan []uuid.UUID),
		timeoutSeconds:            timeoutSeconds,
	}
}

func (l *longPollClient) StartInstrumentLongPoll(ctx context.Context) {
	longPollPath, err := url.JoinPath(l.cerberusUrl, "/v1/instruments/poll-config")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to parse URL for long-poll")
	}
	u, err := url.Parse(longPollPath)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to parse URL for long-poll")
	}

	log.Info().Msg("Sending long-poll request...")

	// Create an HTTP client for long polling
	httpClient := &http.Client{
		Transport: &RestyRoundTripper{restyClient: l.restyClient},
	}

	c, err := longpollclient.NewClient(longpollclient.ClientOptions{
		SubscribeUrl:       *u,
		Category:           l.serviceName,
		LoggingEnabled:     true,
		PollTimeoutSeconds: l.timeoutSeconds,
		HttpClient:         httpClient,
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to create long-poll client")
		return
	}

	// Poll events in a separate goroutine with context for graceful shutdown
	go func() {
		for event := range c.Start(time.Now()) {
			select {
			case <-ctx.Done():
				log.Info().Msg("Long poll gracefully stopped")
				return
			default:
				l.handleEvent(ctx, event)
			}
		}
	}()
}

func (l *longPollClient) handleEvent(ctx context.Context, event *golongpoll.Event) {
	// Assert event data as map[string]interface{}
	data, ok := event.Data.(map[string]interface{})
	if !ok {
		log.Error().Msg("Unexpected event data type")
		return
	}

	// Convert map back to JSON bytes
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal event data")
		return
	}

	var messageTO InstrumentMessageTO
	// Deserialize JSON to InstrumentMessageTO struct
	if err := json.Unmarshal(jsonData, &messageTO); err != nil {
		log.Error().Err(err).Msg("Failed to unmarshal event data to InstrumentMessageTO")
		return
	}

	log.Debug().
		Str("MessageType", string(messageTO.MessageType)).
		Str("InstrumentId", messageTO.InstrumentId.String()).
		Msg("Received event mapped to InstrumentMessageTO")

	// Handle the received message
	err = l.processInstrumentMessage(ctx, messageTO)
	if err != nil {
		log.Error().Err(err).Interface("Message", messageTO).Msg("Failed to process event")
		// TODO handle, failed chan or something
	}
}

func (l *longPollClient) processInstrumentMessage(ctx context.Context, message InstrumentMessageTO) error {
	switch message.MessageType {
	case MessageTypeCreate:
		log.Info().Msgf("Processing creation event for InstrumentId: %s", message.InstrumentId)
		instrument := convertInstrumentTOToInstrument(*message.Instrument)
		id, err := l.instrumentService.CreateInstrument(ctx, instrument)
		if err != nil {
			log.Error().Err(err).Msg("Failed to create instrument")
			return err
		}
		log.Info().Interface("UUID", id).Msg("Created instrument")
	case MessageTypeUpdate:
		log.Info().Msgf("Processing update event for InstrumentId: %s", message.InstrumentId)
		instrument := convertInstrumentTOToInstrument(*message.Instrument)
		err := l.instrumentService.UpdateInstrument(ctx, instrument)
		if err != nil {
			log.Error().Err(err).Msg("Failed to update instrument")
			return err
		}
		log.Info().Msg("Updated instrument")
	case MessageTypeDelete:
		log.Info().Msgf("Processing deletion event for InstrumentId: %s", message.InstrumentId)
		err := l.instrumentService.DeleteInstrument(ctx, message.InstrumentId)
		if err != nil {
			log.Error().Err(err).Msg("Failed to delete instrument")
			return err
		}
		log.Info().Msg("Deleted instrument")
	default:
		log.Warn().Msgf("Unknown message type for InstrumentId: %s", message.InstrumentId)
	}
	return nil
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

func (l *longPollClient) StartRevokedWorkItemIDsLongPolling(ctx context.Context) {
	u, err := url.Parse(l.cerberusUrl + "/v1/instrument-drivers/analysis-requests/revoked")
	if err != nil {
		log.Fatal().Err(err).Msg("start revoked work item IDs long polling failed")
	}

	httpClient := &http.Client{
		Transport: &RestyRoundTripper{restyClient: l.restyClient},
	}

	c, err := longpollclient.NewClient(longpollclient.ClientOptions{
		SubscribeUrl:       *u,
		Category:           fmt.Sprintf("%s:%s", l.serviceName, syncTypeRevocation),
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

			var revokedWorkItemIDs []uuid.UUID
			err = json.Unmarshal(jsonData, &revokedWorkItemIDs)
			if err != nil {
				log.Error().Err(err).Interface("jsonData", jsonData).Msg("unmarshal event data to UUIDs failed")
				continue
			}
			l.revokedWorkItemIDsChan <- revokedWorkItemIDs
		}
	}
}

func (l *longPollClient) StartReexaminedWorkItemIDsLongPolling(ctx context.Context) {
	u, err := url.Parse(l.cerberusUrl + "/v1/instrument-drivers/analysis-requests/reexamined")
	if err != nil {
		log.Fatal().Err(err).Msg("start reexamined work item IDs long polling failed")
	}

	httpClient := &http.Client{
		Transport: &RestyRoundTripper{restyClient: l.restyClient},
	}

	c, err := longpollclient.NewClient(longpollclient.ClientOptions{
		SubscribeUrl:       *u,
		Category:           fmt.Sprintf("%s:%s", l.serviceName, syncTypeReexamine),
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
				return
			}

			jsonData, err := json.Marshal(data)
			if err != nil {
				log.Error().Err(err).Interface("data", data).Msg("marshal event data failed")
				return
			}

			var reexaminedWorkItemIDs []uuid.UUID
			err = json.Unmarshal(jsonData, &reexaminedWorkItemIDs)
			if err != nil {
				log.Error().Err(err).Interface("jsonData", jsonData).Msg("unmarshal event data to UUIDs failed")
				return
			}
			l.reexaminedWorkItemIDsChan <- reexaminedWorkItemIDs
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
