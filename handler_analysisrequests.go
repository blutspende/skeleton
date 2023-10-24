package skeleton

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"net/http"
	"time"
)

const (
	MsgCanNotBindRequestBody = "can not bind request body"
	keyBadRequest            = "badRequest"
)

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

type analysisRequestTO struct {
	WorkItemID     uuid.UUID  `json:"workItemId"`
	AnalyteID      uuid.UUID  `json:"analyteId"`
	SampleCode     string     `json:"sampleCode"`
	MaterialID     uuid.UUID  `json:"materialId"`
	LaboratoryID   uuid.UUID  `json:"laboratoryId"`
	ValidUntilTime time.Time  `json:"validUntilTime"`
	Subject        *subjectTO `json:"subject"`
}

type analysisRequestStatusTO struct {
	WorkItemID uuid.UUID `json:"workitemId"`
	Error      string    `json:"error,omitempty"`
}

type analyteUsageResponseItem struct {
	AnalyteID   uuid.UUID    `json:"analyteId"`
	Instruments []nameIDPair `json:"instruments"`
}

type nameIDPair struct {
	ID   uuid.UUID `json:"id"`
	Name string    `json:"name"`
}

// CreateAnalysisRequestBatch
// @Summary Create a batch of Analysis Request and process them
// @Description Create multiple analysis requests. If results are present then cerberus will receive a response for results.
// @Tags AnalysisRequest
// @Produce json
// @Accept json
// @Param AnalysisRequests body []model.AnalysisRequest true "Array of AnalysisRequest"
// @Success 200 {object} analysisRequestResponse "OK"
// @Failure 400 {object} model.HTTPError "Bad Request"
// @Router /v1/int/analysisRequest/batch [POST]
// @Router /v1/analysis-request/batch [POST]
func (api *api) CreateAnalysisRequestBatch(c *gin.Context) {
	requestStart := time.Now()
	api.createAnalysisRequestMutex.Lock()
	defer api.createAnalysisRequestMutex.Unlock()

	var analysisRequestTOs []analysisRequestTO
	err := c.BindJSON(&analysisRequestTOs)
	if err != nil {
		log.Error().Err(err).Msg(ErrInvalidRequestBody.Message)
		c.AbortWithStatusJSON(http.StatusBadRequest, ErrInvalidRequestBody)
		return
	}

	log.Debug().Msgf("Got %d analysis requests", len(analysisRequestTOs))

	// Map TO to model
	analysisRequests := make([]AnalysisRequest, 0, len(analysisRequestTOs))
	for i := range analysisRequestTOs {
		analysisRequest := AnalysisRequest{
			ID:             uuid.Nil,
			WorkItemID:     analysisRequestTOs[i].WorkItemID,
			AnalyteID:      analysisRequestTOs[i].AnalyteID,
			SampleCode:     analysisRequestTOs[i].SampleCode,
			MaterialID:     analysisRequestTOs[i].MaterialID,
			LaboratoryID:   analysisRequestTOs[i].LaboratoryID,
			ValidUntilTime: analysisRequestTOs[i].ValidUntilTime,
			CreatedAt:      time.Time{},
		}
		if analysisRequestTOs[i].Subject != nil {
			analysisRequest.SubjectInfo = &SubjectInfo{
				Type:         "",
				DateOfBirth:  analysisRequestTOs[i].Subject.DateOfBirth,
				FirstName:    analysisRequestTOs[i].Subject.FirstName,
				LastName:     analysisRequestTOs[i].Subject.LastName,
				DonorID:      analysisRequestTOs[i].Subject.DonorID,
				DonationID:   analysisRequestTOs[i].Subject.DonationID,
				DonationType: analysisRequestTOs[i].Subject.DonationType,
				Pseudonym:    analysisRequestTOs[i].Subject.Pseudonym,
			}
			switch analysisRequestTOs[i].Subject.Type {
			case "DONOR":
				analysisRequest.SubjectInfo.Type = Donor
			case "PERSONAL":
				analysisRequest.SubjectInfo.Type = Personal
			case "PSEUDONYMIZED":
				analysisRequest.SubjectInfo.Type = Pseudonym
			case "":
				analysisRequest.SubjectInfo = nil
			default:
				log.Error().Err(err).Str("workItemID", analysisRequestTOs[i].WorkItemID.String()).
					Str("subjectID", analysisRequestTOs[i].Subject.ID.String()).
					Msgf("Invalid subject Type provided (%+v)", analysisRequestTOs[i].Subject.Type)
				//TODO: Add logcom: notify some groups
				analysisRequest.SubjectInfo = nil
			}
		}
		analysisRequests = append(analysisRequests, analysisRequest)
	}

	analysisRequestStatus, err := api.analysisService.CreateAnalysisRequests(c, analysisRequests)
	if err != nil {
		log.Error().Err(err).Msg("")
		c.JSON(http.StatusInternalServerError, ErrInternalServerError)
		return
	}

	analysisRequestStatusTO := make([]analysisRequestStatusTO, len(analysisRequestStatus))
	for i := range analysisRequestStatus {
		analysisRequestStatusTO[i].WorkItemID = analysisRequestStatus[i].WorkItemID
		if analysisRequestStatus[i].Error != nil {
			analysisRequestStatusTO[i].Error = analysisRequestStatus[i].Error.Error()
		}
	}

	log.Trace().
		Int64("Execution-time (ms)", time.Now().Sub(requestStart).Milliseconds()).
		Msg("createAnalysisRequestBatch")

	c.JSON(http.StatusOK, analysisRequestStatusTO)
}

// RevokeAnalysisRequestBatch
// @Summary Revoke a batch of Analysis Request by Work Item ID
// @Description Revoke multiple analysis requests by work item id.
// @Tags AnalysisRequest
// @Produce json
// @Accept json
// @Param WorkItemIDs body []uuid.UUID true "Array of work item id"
// @Success 204 "No Content"
// @Router /v1/analysis-request/batch [DELETE]
func (api *api) RevokeAnalysisRequestBatch(c *gin.Context) {
	var workItemIDs []uuid.UUID
	err := c.ShouldBindJSON(&workItemIDs)
	if err != nil {
		log.Error().Err(err).Msg(ErrInvalidRequestBody.Message)
		c.AbortWithStatusJSON(http.StatusBadRequest, ErrInvalidRequestBody)
		return
	}

	err = api.analysisService.RevokeAnalysisRequests(c, workItemIDs)
	if err != nil {
		log.Error().Err(err).Msg("Failed to revoke analysis requests")
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}

	c.Status(http.StatusNoContent)
}

func (api *api) ReexamineAnalysisRequestBatch(c *gin.Context) {
	var workItemIDs []uuid.UUID
	err := c.ShouldBindJSON(&workItemIDs)
	if err != nil {
		log.Error().Err(err).Msg(ErrInvalidRequestBody.Message)
		c.AbortWithStatusJSON(http.StatusBadRequest, ErrInvalidRequestBody)
		return
	}

	err = api.analysisService.ReexamineAnalysisRequestsBatch(c, workItemIDs)
	if err != nil {
		log.Error().Err(err).Msg("Failed to reexamine analysis requests")
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}

	c.Status(http.StatusNoContent)
}

func (api *api) CheckAnalytesUsage(c *gin.Context) {
	var ids []uuid.UUID
	err := c.ShouldBindJSON(&ids)
	if err != nil {
		log.Error().Err(err).Msg(MsgCanNotBindRequestBody)
		c.AbortWithStatusJSON(http.StatusBadRequest, clientError{
			Message:    MsgCanNotBindRequestBody,
			MessageKey: keyBadRequest,
		})
		return
	}

	analyteUsage, err := api.instrumentService.CheckAnalytesUsage(c, ids)
	if err != nil {
		log.Error().Err(err).Send()
		c.AbortWithStatusJSON(http.StatusInternalServerError, nil)
	}
	response := make([]analyteUsageResponseItem, 0)
	for analyteID, instruments := range analyteUsage {
		if len(instruments) == 0 {
			continue
		}
		respItem := analyteUsageResponseItem{
			AnalyteID: analyteID,
		}
		for _, instrument := range instruments {
			respItem.Instruments = append(respItem.Instruments, nameIDPair{
				ID:   instrument.ID,
				Name: instrument.Name,
			})
		}
		response = append(response, respItem)
	}

	c.JSON(http.StatusOK, response)
}
