package skeleton

import (
	"context"
	"fmt"
	"github.com/blutspende/bloodlab-common/messagestatus"
	"github.com/blutspende/bloodlab-common/messagetype"
	"github.com/google/uuid"
	"regexp"
	"strings"
	"time"
)

type MessageService interface {
	AddAnalysisRequestsToMessageOutOrder(ctx context.Context, messageOutOrderID uuid.UUID, analysisRequestIDs []uuid.UUID) error
	DeleteRevokedUnsentOrderMessagesByAnalysisRequestIDs(ctx context.Context, analysisRequestIDs []uuid.UUID) ([]uuid.UUID, error)
	GetMessageInsByIDs(ctx context.Context, messageIDs []uuid.UUID) (map[uuid.UUID]MessageIn, error)
	GetUnprocessedMessageIns(ctx context.Context, limit, offset int, cutoffTime time.Time) ([]MessageIn, error)
	GetUnprocessedMessageInsByInstrumentID(ctx context.Context, instrumentID uuid.UUID, limit, offset int) ([]MessageIn, error)
	GetUnsyncedMessageIns(ctx context.Context, limit, offset int, cutoffTime time.Time) ([]MessageIn, error)
	SaveMessageIn(ctx context.Context, message MessageIn) (uuid.UUID, error)
	SaveMessageOut(ctx context.Context, message MessageOut) (uuid.UUID, error)
	SaveMessageOutBatch(ctx context.Context, message []MessageOut) ([]uuid.UUID, error)
	GetUnprocessedMessageOuts(ctx context.Context, limit, offset int, cutoffTime time.Time) ([]MessageOut, error)
	GetUnprocessedMessageOutsByInstrumentID(ctx context.Context, instrumentID uuid.UUID, limit, offset int) ([]MessageOut, error)
	GetUnsyncedMessageOuts(ctx context.Context, limit, offset int, cutoffTime time.Time) ([]MessageOut, error)
	UpdateMessageIn(ctx context.Context, message MessageIn) error
	UpdateMessageOut(ctx context.Context, message MessageOut) error
	GetMessageOutOrdersBySampleCodesAndRequestMappingIDs(ctx context.Context, sampleCodes []string, analyteIDs []uuid.UUID, includePending bool) (map[string]map[uuid.UUID][]MessageOutOrder, error)
	GetTestCodesToRevokeBySampleCodes(ctx context.Context, instrumentID uuid.UUID, analysisRequestIDs []uuid.UUID) (map[string][]string, error)
	EnqueueMessageInsForArchiving(messages ...MessageIn)
	EnqueueMessageOutsForArchiving(messages ...MessageOut)
	StartDEAArchiving(ctx context.Context)
}

type messageService struct {
	deaClient                 DeaClientV1
	messageInRepository       MessageInRepository
	messageOutRepository      MessageOutRepository
	messageOutOrderRepository MessageOutOrderRepository
	messageInArchivingChan    chan []MessageIn
	messageOutArchivingChan   chan []MessageOut
	serviceName               string
}

func (s *messageService) AddAnalysisRequestsToMessageOutOrder(ctx context.Context, messageOutOrderID uuid.UUID, analysisRequestIDs []uuid.UUID) error {
	return s.messageOutOrderRepository.AddAnalysisRequestsToMessageOutOrder(ctx, messageOutOrderID, analysisRequestIDs)
}

func (s *messageService) DeleteRevokedUnsentOrderMessagesByAnalysisRequestIDs(ctx context.Context, analysisRequestIDs []uuid.UUID) ([]uuid.UUID, error) {
	tx, err := s.messageOutRepository.CreateTransaction()
	if err != nil {
		return nil, err
	}
	messageIDs, err := s.messageOutRepository.WithTransaction(tx).GetFullyRevokedUnsentMessageIDsByAnalysisRequestIDs(ctx, analysisRequestIDs)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	err = s.messageOutRepository.WithTransaction(tx).DeleteByIDs(ctx, messageIDs)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	err = tx.Commit()
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	return messageIDs, nil
}

func (s *messageService) GetMessageInsByIDs(ctx context.Context, messageIDs []uuid.UUID) (map[uuid.UUID]MessageIn, error) {
	messages, err := s.messageInRepository.GetByIDs(ctx, messageIDs)
	if err != nil {
		return nil, err
	}
	messagesByIDs := make(map[uuid.UUID]MessageIn)
	for i := range messages {
		messagesByIDs[messages[i].ID] = messages[i]
	}

	return messagesByIDs, nil
}

func (s *messageService) GetUnprocessedMessageIns(ctx context.Context, limit, offset int, cutoffTime time.Time) ([]MessageIn, error) {
	return s.messageInRepository.GetUnprocessed(ctx, limit, offset, cutoffTime)
}

func (s *messageService) GetUnprocessedMessageInsByInstrumentID(ctx context.Context, instrumentID uuid.UUID, limit, offset int) ([]MessageIn, error) {
	return s.messageInRepository.GetUnprocessedByInstrumentID(ctx, instrumentID, limit, offset)
}

func (s *messageService) GetUnsyncedMessageIns(ctx context.Context, limit, offset int, cutoffTime time.Time) ([]MessageIn, error) {
	return s.messageInRepository.GetUnsynced(ctx, limit, offset, cutoffTime)
}

func (s *messageService) SaveMessageIn(ctx context.Context, message MessageIn) (uuid.UUID, error) {
	if !isStatusValid(message.Status) {
		message.Status = messagestatus.Stored
	}
	if !isTypeValid(message.Type) {
		message.Type = messagetype.Unidentified
	}

	return s.messageInRepository.Create(ctx, message)
}

func (s *messageService) SaveMessageOut(ctx context.Context, message MessageOut) (uuid.UUID, error) {
	if !isStatusValid(message.Status) {
		message.Status = messagestatus.Stored
	}
	if !isTypeValid(message.Type) {
		message.Type = messagetype.Unidentified
	}

	messageIDs, err := s.messageOutRepository.CreateBatch(ctx, []MessageOut{message})
	if err != nil {
		return uuid.Nil, err
	}
	message.ID = messageIDs[0]
	s.EnqueueMessageOutsForArchiving(message)

	return message.ID, nil
}

func (s *messageService) SaveMessageOutBatch(ctx context.Context, messages []MessageOut) ([]uuid.UUID, error) {
	if len(messages) == 0 {
		return nil, nil
	}
	tx, err := s.messageOutRepository.CreateTransaction()
	if err != nil {
		return nil, err
	}
	messageIDs, err := s.messageOutRepository.WithTransaction(tx).CreateBatch(ctx, messages)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	for i := range messages {
		if !isStatusValid(messages[i].Status) {
			messages[i].Status = messagestatus.Stored
		}
		if !isTypeValid(messages[i].Type) {
			messages[i].Type = messagetype.Unidentified
		}
		messages[i].ID = messageIDs[i]
		for j := range messages[i].MessageOutOrders {
			if messages[i].MessageOutOrders[j].ID == uuid.Nil {
				messages[i].MessageOutOrders[j].ID = uuid.New()
			}
			messages[i].MessageOutOrders[j].MessageOutID = messages[i].ID
		}
		_, err = s.messageOutOrderRepository.WithTransaction(tx).CreateBatch(ctx, messages[i].MessageOutOrders)
		if err != nil {
			_ = tx.Rollback()
			return nil, err
		}
		for j := range messages[i].MessageOutOrders {
			err = s.messageOutOrderRepository.WithTransaction(tx).AddAnalysisRequestsToMessageOutOrder(ctx, messages[i].MessageOutOrders[j].ID, messages[i].MessageOutOrders[j].AnalysisRequestIDs)
			if err != nil {
				_ = tx.Rollback()
				return nil, err
			}
		}

	}
	err = tx.Commit()
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	s.EnqueueMessageOutsForArchiving(messages...)

	return messageIDs, nil
}

func (s *messageService) GetUnprocessedMessageOuts(ctx context.Context, limit, offset int, cutoffTime time.Time) ([]MessageOut, error) {
	return s.messageOutRepository.GetUnprocessed(ctx, limit, offset, cutoffTime)
}

func (s *messageService) GetUnprocessedMessageOutsByInstrumentID(ctx context.Context, instrumentID uuid.UUID, limit, offset int) ([]MessageOut, error) {
	return s.messageOutRepository.GetUnprocessedByInstrumentID(ctx, instrumentID, limit, offset)
}

func (s *messageService) GetUnsyncedMessageOuts(ctx context.Context, limit, offset int, cutoffTime time.Time) ([]MessageOut, error) {
	return s.messageOutRepository.GetUnsynced(ctx, limit, offset, cutoffTime)
}

func (s *messageService) UpdateMessageIn(ctx context.Context, message MessageIn) error {
	return s.messageInRepository.Update(ctx, message)
}

func (s *messageService) UpdateMessageOut(ctx context.Context, message MessageOut) error {
	return s.messageOutRepository.Update(ctx, message)
}

func (s *messageService) GetMessageOutOrdersBySampleCodesAndRequestMappingIDs(ctx context.Context, sampleCodes []string, analyteIDs []uuid.UUID, includePending bool) (map[string]map[uuid.UUID][]MessageOutOrder, error) {
	return s.messageOutOrderRepository.GetBySampleCodesAndRequestMappingIDs(ctx, sampleCodes, analyteIDs, includePending)
}

func (s *messageService) GetTestCodesToRevokeBySampleCodes(ctx context.Context, instrumentID uuid.UUID, analysisRequestIDs []uuid.UUID) (map[string][]string, error) {
	return s.messageOutOrderRepository.GetTestCodesToRevokeBySampleCodes(ctx, instrumentID, analysisRequestIDs)
}

func (s *messageService) EnqueueMessageInsForArchiving(messages ...MessageIn) {
	if len(messages) == 0 {
		return
	}
	s.messageInArchivingChan <- messages
}

func (s *messageService) EnqueueMessageOutsForArchiving(messages ...MessageOut) {
	if len(messages) == 0 {
		return
	}
	s.messageOutArchivingChan <- messages
}

const deaArchivingRetryTimeoutSeconds = 30

func (s *messageService) StartDEAArchiving(ctx context.Context) {
	for {
		select {
		case messagesToArchive := <-s.messageInArchivingChan:
			failedMessagesByIDs := make(map[uuid.UUID]MessageIn)
			for i := range messagesToArchive {
				deaID, err := s.uploadRawMessageToDEA(messagesToArchive[i].Raw)
				if err != nil {
					errorMsg := err.Error()
					messagesToArchive[i].Status = messagestatus.Error
					messagesToArchive[i].Error = &errorMsg
					messagesToArchive[i].RetryCount += 1
					failedMessagesByIDs[messagesToArchive[i].ID] = messagesToArchive[i]
				}
				messagesToArchive[i].DEARawMessageID = uuid.NullUUID{
					UUID:  deaID,
					Valid: deaID != uuid.Nil,
				}
				err = s.messageInRepository.UpdateDEAInfo(ctx, messagesToArchive[i])
				if err != nil {
					failedMessagesByIDs[messagesToArchive[i].ID] = messagesToArchive[i]
				}
			}
			if len(failedMessagesByIDs) == 0 {
				continue
			}
			failedMessages := make([]MessageIn, len(failedMessagesByIDs))
			counter := 0
			for _, message := range failedMessagesByIDs {
				failedMessages[counter] = message
				counter++
			}
			time.AfterFunc(time.Second*deaArchivingRetryTimeoutSeconds, func() {
				s.EnqueueMessageInsForArchiving(failedMessages...)
			})
		case messagesToArchive := <-s.messageOutArchivingChan:
			failedMessagesByIDs := make(map[uuid.UUID]MessageOut)
			for i := range messagesToArchive {
				deaID, err := s.uploadRawMessageToDEA(messagesToArchive[i].Raw)
				if err != nil {
					errorMsg := err.Error()
					messagesToArchive[i].Error = &errorMsg
					messagesToArchive[i].RetryCount += 1
					failedMessagesByIDs[messagesToArchive[i].ID] = messagesToArchive[i]
				}
				messagesToArchive[i].DEARawMessageID = uuid.NullUUID{
					UUID:  deaID,
					Valid: deaID != uuid.Nil,
				}
				err = s.messageOutRepository.UpdateDEAInfo(ctx, messagesToArchive[i])
				if err != nil {
					failedMessagesByIDs[messagesToArchive[i].ID] = messagesToArchive[i]
				}
			}
			if len(failedMessagesByIDs) == 0 {
				continue
			}
			failedMessages := make([]MessageOut, len(failedMessagesByIDs))
			counter := 0
			for _, message := range failedMessagesByIDs {
				failedMessages[counter] = message
				counter++
			}
			time.AfterFunc(time.Second*deaArchivingRetryTimeoutSeconds, func() {
				s.EnqueueMessageOutsForArchiving(failedMessages...)
			})
		case <-ctx.Done():
			return
		}
	}
}

var nonSpecialCharactersRegex = regexp.MustCompile("[^A-Za-z0-9]+")

func (s *messageService) uploadRawMessageToDEA(rawMessageBytes []byte) (uuid.UUID, error) {
	return s.deaClient.UploadFile(rawMessageBytes, generateRawMessageFileName(s.serviceName, time.Now().UTC()))
}

func generateRawMessageFileName(serviceName string, ts time.Time) string {
	strippedServiceName := nonSpecialCharactersRegex.ReplaceAllString(serviceName, "_")
	formattedTs := strings.ReplaceAll(ts.Format("2006-01-02-15-04-05.000000"), ".", "_")
	return fmt.Sprintf("%s_%s", strippedServiceName, formattedTs)
}

func NewMessageService(deaClient DeaClientV1, messageInRepository MessageInRepository, messageOutRepository MessageOutRepository, messageOutOrderRepository MessageOutOrderRepository, serviceName string) MessageService {
	return &messageService{
		deaClient:                 deaClient,
		messageInRepository:       messageInRepository,
		messageOutRepository:      messageOutRepository,
		messageOutOrderRepository: messageOutOrderRepository,
		messageInArchivingChan:    make(chan []MessageIn, 100),
		messageOutArchivingChan:   make(chan []MessageOut, 100),
		serviceName:               serviceName,
	}
}

func isStatusValid(status messagestatus.MessageStatus) bool {
	return status == messagestatus.Sent || status == messagestatus.Error || status == messagestatus.Processed || status == messagestatus.Stored
}

func isTypeValid(messageType messagetype.MessageType) bool {
	return messageType == messagetype.Query || messageType == messagetype.Order || messageType == messagetype.Result ||
		messageType == messagetype.Acknowledgement || messageType == messagetype.Cancellation || messageType == messagetype.Reorder ||
		messageType == messagetype.Diagnostics || messageType == messagetype.Unidentified
}
