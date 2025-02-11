package skeleton

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/jcuga/golongpoll"
	"net/http"
	"net/url"
	"time"

	"github.com/go-resty/resty/v2"
	longpollclient "github.com/jcuga/golongpoll/client"
	"github.com/rs/zerolog/log"
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
	StartLongPoll(ctx context.Context)
}

type longPollClient struct {
	restyClient       *resty.Client
	instrumentService InstrumentService
	serviceName       string
	cerberusUrl       string
}

func NewLongPollClient(restyClient *resty.Client, instrumentService InstrumentService, serviceName, cerberusUrl string) LongPollClient {
	return &longPollClient{
		restyClient:       restyClient,
		instrumentService: instrumentService,
		serviceName:       serviceName,
		cerberusUrl:       cerberusUrl,
	}
}

func (l *longPollClient) StartLongPoll(ctx context.Context) {
	longPollPath, err := url.JoinPath(l.cerberusUrl, "/v1/instruments/poll-config")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to parse URL for long-poll")
		return
	}
	u, err := url.Parse(longPollPath)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to parse URL for long-poll")
		return
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
		PollTimeoutSeconds: 90,
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
	l.processInstrumentMessage(ctx, messageTO)
}

func (l *longPollClient) processInstrumentMessage(ctx context.Context, message InstrumentMessageTO) {
	switch message.MessageType {
	case MessageTypeCreate:
		log.Info().Msgf("Processing creation event for InstrumentId: %s", message.InstrumentId)
		instrument := convertInstrumentTOToInstrument(*message.Instrument)
		id, err := l.instrumentService.CreateInstrument(ctx, instrument)
		if err != nil {
			log.Error().Err(err).Msg("Failed to create instrument")
			//TODO: handle, failed chan or something
		}
		log.Info().Interface("UUID", id).Msg("Created instrument")
	case MessageTypeUpdate:
		log.Info().Msgf("Processing update event for InstrumentId: %s", message.InstrumentId)
		instrument := convertInstrumentTOToInstrument(*message.Instrument)
		err := l.instrumentService.UpdateInstrument(ctx, instrument)
		if err != nil {
			log.Error().Err(err).Msg("Failed to update instrument")
			//TODO: handle, failed chan or something
		}
		log.Info().Msg("Updated instrument")
	case MessageTypeDelete:
		log.Info().Msgf("Processing deletion event for InstrumentId: %s", message.InstrumentId)
		err := l.instrumentService.DeleteInstrument(ctx, message.InstrumentId)
		if err != nil {
			log.Error().Err(err).Msg("Failed to delete instrument")
			//TODO: handle, failed chan or something
		}
	default:
		log.Warn().Msgf("Unknown message type for InstrumentId: %s", message.InstrumentId)
	}
}
