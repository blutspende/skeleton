package skeleton

import (
	"bytes"
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"net/http"
)

type DeaClientV1 interface {
	UploadImage(fileData []byte, name string) (uuid.UUID, error)
	UploadFile(fileData []byte, name string) (uuid.UUID, error)
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
		return
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

type savedFileResponseItem struct {
	ID       uuid.UUID `json:"id"`
	FileName string    `json:"fileName"`
}

func (dea *deaClientV1) UploadFile(fileData []byte, name string) (fileID uuid.UUID, err error) {
	var response []savedFileResponseItem
	var errResponse clientError
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
	resp, err = dea.client.SetRetryResetReaders(true).R().
		SetFileReader("file", name, bytes.NewReader(fileData)).
		SetResult(&response).
		SetError(&errResponse).
		Post(dea.deaUrl + "/v1/files")
	if err != nil {
		log.Error().Err(err).Msg("upload file to DEA failed")
		return
	}
	if resp == nil {
		return
	}

	if resp.StatusCode() != http.StatusCreated {
		log.Error().Int("lenOfFileBytes", len(fileData)).Str("message", errResponse.Message).Msg("upload file to DEA failed")
		err = fmt.Errorf(errResponse.Message)
		return
	}

	if len(response) != 1 {
		err = fmt.Errorf("DEA returned %d response items for single file upload", len(response))
		return
	}
	fileID = response[0].ID

	return
}
