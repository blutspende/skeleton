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

	return &deaClientV1{
		deaUrl: deaUrl,
		client: restyClient,
	}, nil
}

func (dea *deaClientV1) UploadImage(fileData []byte, name string) (uuid.UUID, error) {
	var imageID uuid.UUID

	resp, err := dea.client.R().
		SetFileReader("file", name, bytes.NewReader(fileData)).
		Post(dea.deaUrl + "/v1/image/upload")
	if err != nil {
		log.Error().Err(err).Msg("Can not call internal dea api")
		return imageID, err
	}

	if resp.StatusCode() != http.StatusOK {
		log.Error().Int("lenOfFileBytes", len(fileData)).Str("responseBody", string(resp.Body())).Msg("Invalid request to dea")
		return uuid.Nil, fmt.Errorf("can not upload image. Invalid request data: %s", string(resp.Body()))
	}

	imageID, err = uuid.Parse(string(resp.Body()))
	if err != nil {
		log.Error().Err(err).Str("responseBody", string(resp.Body())).Msg("Can not parse bytes into uuid.")
		return imageID, err
	}

	return imageID, nil
}

type savedFileResponseItem struct {
	ID       uuid.UUID `json:"id"`
	FileName string    `json:"fileName"`
}

func (dea *deaClientV1) UploadFile(fileData []byte, name string) (uuid.UUID, error) {
	var response []savedFileResponseItem
	var errResponse clientError
	resp, err := dea.client.R().
		SetFileReader("file", name, bytes.NewReader(fileData)).
		SetResult(&response).
		SetError(&errResponse).
		Post(dea.deaUrl + "/v1/files")
	if err != nil {
		log.Error().Err(err).Msg("upload file to DEA failed")
		return uuid.Nil, err
	}

	if resp.StatusCode() != http.StatusCreated {
		log.Error().Int("lenOfFileBytes", len(fileData)).Str("message", errResponse.Message).Msg("upload file to DEA failed")
		return uuid.Nil, fmt.Errorf(errResponse.Message)
	}

	if len(response) != 1 {
		return uuid.Nil, fmt.Errorf("DEA returned %d response items for single file upload", len(response))
	}
	return response[0].ID, nil
}
