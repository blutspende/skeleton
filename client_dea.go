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
	UploadImages(images []*Image) (imageIDs []uuid.UUID, err error)
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

func (dea *deaClientV1) UploadImages(images []*Image) (imageIDs []uuid.UUID, err error) {
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

	multipartFields := make([]*resty.MultipartField, len(images))
	for i := range images {
		multipartFields[i] = &resty.MultipartField{
			Param:    "file",
			FileName: images[i].Name,
			Reader:   bytes.NewReader(images[i].ImageBytes),
		}
	}
	var response []fileSavedResponseItem
	var resp *resty.Response
	resp, err = dea.client.R().
		SetMultipartFields(multipartFields...).
		SetResult(&response).
		Post(dea.deaUrl + "/v1/image/batch")
	if err != nil {
		log.Error().Err(err).Msg("Can not call internal dea api")
		return
	}
	if resp == nil {
		return nil, ErrUploadImageToDEAFailed
	}

	if resp.StatusCode() != http.StatusCreated {
		log.Error().Str("responseBody", string(resp.Body())).Msg("Invalid request to dea")
		return nil, fmt.Errorf("can not upload images. Invalid request data: %s", string(resp.Body()))
	}
	imageIDs = make([]uuid.UUID, len(response))
	for i := range response {
		imageIDs[i] = response[i].ID
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

type fileSavedResponseItem struct {
	ID       uuid.UUID `json:"id"`
	FileName string    `json:"fileName"`
}
