# skeleton

Skeleton for Bloodlab drivermodel

## API 
### Analysis-Request
Received by skeleton from cerberus on /v1/analysisRequest/batch [POST]

```json body
workItemId     uuid.UUID Reference to the WorkitemId
analyteId      uuid.UUID Reference to the Analyte 
sampleCode     string The Samplecode
materialId    uuid.UUID Reference to the (ordered) MaterialId
laboratoryId   uuid.UUID Laboratory 
validUntilTime time.Time Upper time-limit until which the request should be regarded as valid
subject        (obsolete)
```

### Analysis-Result
Each request is supposed to be answered by a Result-Response whenever results are available

workItemId uuid.UUID  Reference to the Workitem
validUntil time.Time  Time-Limit until when the result can be regarded as valid 
status enum "FIN" = Final results, "PRE" = Preliminary results
mode enum "TEST" = Testsystem, do not process further, "VAL" = Validation mode = Process but dont export, "PROD" = do everything
resultYieldDateTime time.Time When the result was yield
examinedMaterial uuid.UUID The material (different from request) that was actually examined
result string 
operator string
technicalReleaseDateTime time.time
instrumentId uuid.UUID
instrumentRunId uuid.UUID
reagentInfos
runCounter  int
extraValues 
resultEdit
editReason
warnings []string
channelResults []channelResultV1TO
images         []imageV1TO         

### UI 
