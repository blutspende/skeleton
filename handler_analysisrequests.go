package skeleton

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"net/http"
	"time"
)

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

	log.Debug().Msg("CreateAnalysisRequestBatch - desperate debug session: entering this http handler")

	var analysisRequestTOs []analysisRequestTO
	err := c.BindJSON(&analysisRequestTOs)
	if err != nil {
		log.Error().Err(err).Msg(ErrInvalidRequestBody.Message)
		c.JSON(http.StatusBadRequest, ErrInvalidRequestBody)
		return
	}

	// Map TO to model
	analysisRequests := make([]AnalysisRequest, len(analysisRequestTOs))
	for i := range analysisRequestTOs {
		analysisRequests[i].ID = uuid.Nil
		analysisRequests[i].WorkItemID = analysisRequestTOs[i].WorkItemID
		analysisRequests[i].AnalyteID = analysisRequestTOs[i].AnalyteID
		analysisRequests[i].SampleCode = analysisRequestTOs[i].SampleCode
		analysisRequests[i].MaterialID = analysisRequestTOs[i].MaterialID
		analysisRequests[i].LaboratoryID = analysisRequestTOs[i].LaboratoryID
		analysisRequests[i].ValidUntilTime = analysisRequestTOs[i].ValidUntilTime
		analysisRequests[i].CreatedAt = time.Time{}
		if analysisRequestTOs[i].Subject != nil {
			subject := SubjectInfo{
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
				subject.Type = Donor
			case "PERSONAL":
				subject.Type = Personal
			case "PSEUDONYMIZED":
				subject.Type = Pseudonym
			default:
				log.Error().Err(err).Msg(ErrInvalidSubjectTypeProvidedInAnalysisRequest.Message)
				//TODO: Add logcom: notify some groups
				c.JSON(http.StatusBadRequest, ErrInvalidSubjectTypeProvidedInAnalysisRequest)
				return
			}
		}
	}

	analysisRequestStatus, err := api.analysisService.CreateAnalysisRequests(c, analysisRequests)
	if err != nil {
		log.Error().Err(err).Msg("")
		c.JSON(http.StatusInternalServerError, ErrInternalServerError)
		return
	}

	analysisRequestStatusTO := make([]analysisRequestStatusTO, len(analysisRequestStatus))
	for i := range analysisRequestStatus {
		analysisRequestStatusTO[i].WorkItemID = analysisRequestStatus[i].AnalysisRequest.WorkItemID
		if analysisRequestStatus[i].Error != nil {
			analysisRequestStatusTO[i].Error = analysisRequestStatus[i].Error.Error()
		}
	}

	log.Debug().
		Int64("Execution-time (ms)", time.Now().Sub(requestStart).Milliseconds()).
		Msg("createAnalysisRequestBatch")

	c.JSON(http.StatusOK, analysisRequestStatusTO)
}
