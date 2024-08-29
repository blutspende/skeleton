# skeleton

Skeleton for Bloodlab drivermodel

### Receiving a Transmission and finding the right instrument
```golang
remoteAddress, _ := session.RemoteAddress()
instrument, err := s.skeleton.GetInstrumentByIP(remoteAddress)
```

## API 
### Analysis-Request
Received by skeleton from cerberus on /v1/analysis-requests/batch [POST]

```text 
workItemId     uuid.UUID Reference to the WorkitemId
analyteId      uuid.UUID Reference to the Analyte 
sampleCode     string The Samplecode
materialId     uuid.UUID Reference to the (ordered) MaterialId
laboratoryId   uuid.UUID Laboratory 
validUntilTime time.Time Upper time-limit until which the request should be regarded as valid
subject        (obsolete)
extraValues    []ExtraValue List of key-value pairs for extra information of analysis request
```

### Analysis-Result
Each request is supposed to be answered by a Result-Response whenever results are available

```text
workItemId               uuid.UUID  Reference to the Workitem
validUntil               time.Time  Time-Limit until when the result can be regarded as valid 
status                   enum "FIN" = Final results, "PRE" = Preliminary results
mode                     enum "TEST" = Testsystem, do not process further, "VAL" = Validation mode = Process but dont export, "PROD" = do everything
resultYieldDateTime      *time.Time When the result was yield
examinedMaterial         uuid.UUID The material (different from request) that was actually examined
result                   string 
operator                 string
technicalReleaseDateTime *time.time
instrumentId             uuid.UUID
instrumentRunId          uuid.UUID
resultEdit               bool
editReason               string
isInvalid                bool
channelResults           []ChannelResultTO
extraValues              []ExtraValueTO
reagentInfos             []ReagentInfoTO
images                   []ImageTO
warnFlag                 bool
warnings                 []string
```

### UI 


### Versioning strategy
The versioning of this library is semantic with <major>.<minor>.<micro>-versions. When contributing changes increment 
  1. <micro>-version for every fix that changes nothing in the structure nor behaviour of the library e.g. adding tests, adding debug output etc.
  2. <minor>-version for every change in the behaviour of existing functonality.
  3. <major>-version for every change in the interface itself. 
