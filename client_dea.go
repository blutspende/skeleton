package skeleton

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"net/http"
	"time"
)

var (
	ErrRegisterSampleCodesFailed          = errors.New("register sample codes to instrument message failed")
	ErrUploadInstrumentMessageToDEAFailed = errors.New("upload instrument message to DEA failed")
	ErrUploadImageToDEAFailed             = errors.New("upload image to DEA failed")
)

type DeaClientV1 interface {
	RegisterSampleCodes(messageID uuid.UUID, sampleCodes []string) error
	UploadImage(fileData []byte, name string) (uuid.UUID, error)
	UploadInstrumentMessage(message SaveInstrumentMessageTO) (uuid.UUID, error)
}

type deaClientV1 struct {
	client *resty.Client
	deaUrl string
}

func NewDEAClient(deaUrl string, restyClient *resty.Client) (DeaClientV1, error) {
	if deaUrl == "" {
		return nil, fmt.Errorf("basepath for dea must be set. check your configuration or DeaURL")
	}
	client := *restyClient
	clientCopy := client.SetRetryResetReaders(true)
	return &deaClientV1{
		deaUrl: deaUrl,
		client: clientCopy,
	}, nil
}

func (dea *deaClientV1) RegisterSampleCodes(messageID uuid.UUID, sampleCodes []string) error {
	var errResponse clientError
	resp, err := dea.client.R().
		SetBody(sampleCodes).
		SetError(&errResponse).
		Post(dea.deaUrl + "/v1/instrument-messages/" + messageID.String() + "/samplecodes")
	if err != nil {
		log.Error().Err(err).Msg("Can not call internal dea api")
		return ErrRegisterSampleCodesFailed
	}
	if resp == nil {
		return ErrRegisterSampleCodesFailed
	}
	if resp.IsError() {
		log.Error().Msg(errResponse.Message)
		return ErrRegisterSampleCodesFailed
	}

	return nil
}

func (dea *deaClientV1) UploadImage(fileData []byte, name string) (imageID uuid.UUID, err error) {
	// TODO - as soon as Resty v3 is released, remove this.
	// When the service 2 service auth retry mechanism triggers, but OIDC provider is not available (response is nil),
	// and the request has multi-part form data that is wrapped in an io.Reader that needs to be reset,
	// then due to a bug in Resty library, a panic occurs.
	defer func() {
		r := recover()
		if panicErr, ok := r.(error); ok {
			log.Error().Err(err).Msg("panic recovered")
			err = panicErr
		}
	}()
	var resp *resty.Response
	resp, err = dea.client.R().
		SetFileReader("file", name, bytes.NewReader(fileData)).
		Post(dea.deaUrl + "/v1/image/upload")
	if err != nil {
		log.Error().Err(err).Msg("Can not call internal dea api")
		return
	}
	if resp == nil {
		return uuid.Nil, ErrUploadImageToDEAFailed
	}

	if resp.StatusCode() != http.StatusOK {
		log.Error().Int("lenOfFileBytes", len(fileData)).Str("responseBody", string(resp.Body())).Msg("Invalid request to dea")
		return uuid.Nil, fmt.Errorf("can not upload image. Invalid request data: %s", string(resp.Body()))
	}

	imageID, err = uuid.Parse(string(resp.Body()))
	if err != nil {
		log.Error().Err(err).Str("responseBody", string(resp.Body())).Msg("Can not parse bytes into uuid.")
		return
	}

	return
}

func (dea *deaClientV1) UploadInstrumentMessage(message SaveInstrumentMessageTO) (uuid.UUID, error) {
	var errResponse clientError
	var id uuid.UUID
	resp, err := dea.client.R().
		SetResult(&id).
		SetBody(message).
		SetError(&errResponse).
		Post(dea.deaUrl + "/v1/instrument-messages")
	if err != nil {
		log.Error().Err(err).Msg(ErrUploadInstrumentMessageToDEAFailed.Error())
		return id, ErrUploadInstrumentMessageToDEAFailed
	}
	if resp == nil {
		return id, ErrUploadInstrumentMessageToDEAFailed
	}

	if resp.StatusCode() != http.StatusCreated {
		log.Error().Str("message", errResponse.Message).Msg(ErrUploadInstrumentMessageToDEAFailed.Error())
		return id, ErrUploadInstrumentMessageToDEAFailed
	}

	return id, nil
}

type SaveInstrumentMessageTO struct {
	ID                 uuid.UUID     `json:"id"`
	InstrumentID       uuid.UUID     `json:"instrumentId"`
	InstrumentModuleID uuid.NullUUID `json:"instrumentModuleId"`
	IsIncoming         bool          `json:"isIncoming"`
	Raw                string        `json:"raw"`
	ReceivedAt         time.Time     `json:"receivedAt"`
}
