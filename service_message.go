package skeleton

import (
	"context"
	"github.com/blutspende/bloodlab-common/encoding"
	"github.com/blutspende/bloodlab-common/messagestatus"
	"github.com/blutspende/bloodlab-common/messagetype"
	"github.com/blutspende/bloodlab-common/utils"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"time"
)

type MessageService interface {
	AddAnalysisRequestsToMessageOutOrder(ctx context.Context, messageOutOrderID uuid.UUID, analysisRequestIDs []uuid.UUID) error
	DeleteRevokedUnsentOrderMessagesByAnalysisRequestIDs(ctx context.Context, analysisRequestIDs []uuid.UUID) ([]uuid.UUID, error)
	GetMessageInsByDEAIDs(ctx context.Context, deaIDs []uuid.UUID) ([]MessageIn, error)
	GetMessageInsByIDs(ctx context.Context, messageIDs []uuid.UUID) ([]MessageIn, error)
	GetMessageInsBySampleCode(ctx context.Context, sampleCode string) ([]MessageIn, error)
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
	GetMessageOutOrdersBySampleCodesAndRequestMappingIDs(ctx context.Context, sampleCodes []string, instrumentID uuid.UUID, includePending bool) (map[string]map[uuid.UUID][]MessageOutOrder, error)
	GetTestCodesToRevokeBySampleCodes(ctx context.Context, instrumentID uuid.UUID, analysisRequestIDs []uuid.UUID) (map[string][]string, error)
	EnqueueMessageInsForArchiving(messages ...MessageIn)
	EnqueueMessageOutsForArchiving(messages ...MessageOut)
	StartDEAArchiving(ctx context.Context, maxRetries int)
	DeleteOldMessageInRecords(ctx context.Context, cleanupDays int, limit int) (int64, error)
	DeleteOldMessageOutRecords(ctx context.Context, cleanupDays int, limit int) (int64, error)
	RegisterSampleCodesToMessageIn(ctx context.Context, messageID uuid.UUID, sampleCodes []string) error
	RegisterSampleCodesToMessageOut(ctx context.Context, messageID uuid.UUID, sampleCodes []string) error
	StartSampleCodeRegisteringToDEA(ctx context.Context)
}

type messageService struct {
	deaClient                  DeaClientV1
	messageInRepository        MessageInRepository
	messageOutRepository       MessageOutRepository
	messageOutOrderRepository  MessageOutOrderRepository
	messageInArchivingChan     chan []MessageIn
	messageOutArchivingChan    chan []MessageOut
	messageInSampleCodeIDChan  chan []sampleCodesWithIDsAndMessageID
	messageOutSampleCodeIDChan chan []sampleCodesWithIDsAndMessageID
	serviceName                string
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

func (s *messageService) GetMessageInsBySampleCode(ctx context.Context, sampleCode string) ([]MessageIn, error) {
	messageIDs, err := s.messageInRepository.GetMessageInIDsBySampleCode(ctx, sampleCode)
	if err != nil {
		return nil, err
	}

	return s.messageInRepository.GetByIDs(ctx, messageIDs)
}

func (s *messageService) GetMessageInsByDEAIDs(ctx context.Context, deaIDs []uuid.UUID) ([]MessageIn, error) {
	return s.messageInRepository.GetByDEAIDs(ctx, deaIDs)
}

func (s *messageService) GetMessageInsByIDs(ctx context.Context, messageIDs []uuid.UUID) ([]MessageIn, error) {
	return s.messageInRepository.GetByIDs(ctx, messageIDs)
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
	var err error
	message.CreatedAt = time.Now().UTC()
	if !isStatusValid(message.Status) {
		message.Status = messagestatus.Stored
	}
	if !isTypeValid(message.Type) {
		message.Type = messagetype.Unidentified
	}
	message.ID, err = s.messageInRepository.Create(ctx, message)
	if err != nil {
		return uuid.Nil, err
	}
	s.EnqueueMessageInsForArchiving(message)

	return message.ID, nil
}

func (s *messageService) SaveMessageOut(ctx context.Context, message MessageOut) (uuid.UUID, error) {
	messageIDs, err := s.SaveMessageOutBatch(ctx, []MessageOut{message})
	if err != nil {
		return uuid.Nil, err
	}

	return messageIDs[0], nil
}

func (s *messageService) SaveMessageOutBatch(ctx context.Context, messages []MessageOut) ([]uuid.UUID, error) {
	ts := time.Now().UTC()
	if len(messages) == 0 {
		return nil, nil
	}
	for i := range messages {
		if !isStatusValid(messages[i].Status) {
			messages[i].Status = messagestatus.Stored
		}
		if !isTypeValid(messages[i].Type) {
			messages[i].Type = messagetype.Unidentified
		}
		if messages[i].CreatedAt.IsZero() {
			messages[i].CreatedAt = ts
		}
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

func (s *messageService) GetMessageOutOrdersBySampleCodesAndRequestMappingIDs(ctx context.Context, sampleCodes []string, instrumentID uuid.UUID, includePending bool) (map[string]map[uuid.UUID][]MessageOutOrder, error) {
	return s.messageOutOrderRepository.GetBySampleCodesAndRequestMappingIDs(ctx, sampleCodes, instrumentID, includePending)
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

const deaRetryTimeoutSeconds = 30

func (s *messageService) StartDEAArchiving(ctx context.Context, maxRetries int) {
	for {
		select {
		case messagesToArchive := <-s.messageInArchivingChan:
			failedMessagesByIDs := make(map[uuid.UUID]MessageIn)
			for i := range messagesToArchive {
				raw, err := encoding.ConvertFromEncodingToUtf8(messagesToArchive[i].Raw, messagesToArchive[i].Encoding)
				if err != nil {
					log.Error().Err(err).Interface("encoding", messagesToArchive[i].Encoding).Interface("messageID", messagesToArchive[i].ID).Msg("failed to encode instrument message to UTF-8")
					raw = string(messagesToArchive[i].Raw)
				}
				deaID, err := s.uploadRawMessageToDEA(SaveInstrumentMessageTO{
					ID:           messagesToArchive[i].ID,
					InstrumentID: messagesToArchive[i].InstrumentID,
					IsIncoming:   true,
					Raw:          raw,
					ReceivedAt:   messagesToArchive[i].CreatedAt,
				})
				if err != nil {
					errorMsg := err.Error()
					messagesToArchive[i].Status = messagestatus.Error
					messagesToArchive[i].Error = &errorMsg
					messagesToArchive[i].RetryCount += 1
					if messagesToArchive[i].RetryCount <= maxRetries {
						failedMessagesByIDs[messagesToArchive[i].ID] = messagesToArchive[i]
					}
				}
				messagesToArchive[i].deaRawMessageID = utils.UUIDToNullUUID(deaID)
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
			time.AfterFunc(time.Second*deaRetryTimeoutSeconds, func() {
				s.EnqueueMessageInsForArchiving(failedMessages...)
			})
		case messagesToArchive := <-s.messageOutArchivingChan:
			failedMessagesByIDs := make(map[uuid.UUID]MessageOut)
			for i := range messagesToArchive {
				raw, err := encoding.ConvertFromEncodingToUtf8(messagesToArchive[i].Raw, messagesToArchive[i].Encoding)
				if err != nil {
					log.Error().Err(err).Interface("encoding", messagesToArchive[i].Encoding).Interface("messageID", messagesToArchive[i].ID).Msg("failed to encode instrument message to UTF-8")
					raw = string(messagesToArchive[i].Raw)
				}
				deaID, err := s.uploadRawMessageToDEA(SaveInstrumentMessageTO{
					ID:           messagesToArchive[i].ID,
					InstrumentID: messagesToArchive[i].InstrumentID,
					IsIncoming:   false,
					Raw:          raw,
					ReceivedAt:   messagesToArchive[i].CreatedAt,
				})
				if err != nil {
					errorMsg := err.Error()
					messagesToArchive[i].Error = &errorMsg
					messagesToArchive[i].RetryCount += 1
					failedMessagesByIDs[messagesToArchive[i].ID] = messagesToArchive[i]
				}
				messagesToArchive[i].deaRawMessageID = utils.UUIDToNullUUID(deaID)
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
			time.AfterFunc(time.Second*deaRetryTimeoutSeconds, func() {
				s.EnqueueMessageOutsForArchiving(failedMessages...)
			})
		case <-ctx.Done():
			return
		}
	}
}

func (s *messageService) uploadRawMessageToDEA(message SaveInstrumentMessageTO) (uuid.UUID, error) {
	return s.deaClient.UploadInstrumentMessage(message)
}

func (s *messageService) RegisterSampleCodesToMessageIn(ctx context.Context, messageID uuid.UUID, sampleCodes []string) error {
	ids, err := s.messageInRepository.RegisterSampleCodes(ctx, messageID, sampleCodes)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	s.messageInSampleCodeIDChan <- []sampleCodesWithIDsAndMessageID{
		{
			MessageID:            messageID,
			MessageSampleCodeIDs: ids,
			SampleCodes:          sampleCodes,
		},
	}

	return nil
}

func (s *messageService) RegisterSampleCodesToMessageOut(ctx context.Context, messageID uuid.UUID, sampleCodes []string) error {
	ids, err := s.messageOutRepository.RegisterSampleCodes(ctx, messageID, sampleCodes)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	s.messageOutSampleCodeIDChan <- []sampleCodesWithIDsAndMessageID{
		{
			MessageID:            messageID,
			MessageSampleCodeIDs: ids,
			SampleCodes:          sampleCodes,
		},
	}

	return nil
}

func (s *messageService) getMessageOutsByIDs(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]MessageOut, error) {
	messages, err := s.messageOutRepository.GetByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	messagesByIDs := make(map[uuid.UUID]MessageOut)
	for i := range messages {
		messagesByIDs[messages[i].ID] = messages[i]
	}

	return messagesByIDs, nil
}

func (s *messageService) StartSampleCodeRegisteringToDEA(ctx context.Context) {
	go s.addUnsentMessageInSampleCodesToQueue(ctx)
	go s.addUnsentMessageOutSampleCodesToQueue(ctx)
	go s.startMessageOutSampleCodeCleanupJob(ctx)
	go s.startMessageInSampleCodeCleanupJob(ctx)
	messageInBatchChan := make(chan []sampleCodesWithIDsAndMessageID, 1)
	messageOutBatchChan := make(chan []sampleCodesWithIDsAndMessageID, 1)
	go startMessageSampleCodeBatching(ctx, s.messageInSampleCodeIDChan, messageInBatchChan)
	go startMessageSampleCodeBatching(ctx, s.messageOutSampleCodeIDChan, messageOutBatchChan)
	for {
		select {
		case sampleCodesWithMessageIDAndIDs := <-messageOutBatchChan:
			messagesToRetry := make([]sampleCodesWithIDsAndMessageID, 0)
			messageIDs := make([]uuid.UUID, len(sampleCodesWithMessageIDAndIDs))
			for i := range sampleCodesWithMessageIDAndIDs {
				messageIDs[i] = sampleCodesWithMessageIDAndIDs[i].MessageID
			}
			messagesByIDs, err := s.getMessageOutsByIDs(ctx, messageIDs)
			if err != nil {
				log.Error().Err(err).Msg("register message out sample codes failed")
				time.AfterFunc(deaRetryTimeoutSeconds*time.Second, func() {
					s.messageOutSampleCodeIDChan <- sampleCodesWithMessageIDAndIDs
				})
				continue
			}
			for i := range sampleCodesWithMessageIDAndIDs {
				messageOut := messagesByIDs[sampleCodesWithMessageIDAndIDs[i].MessageID]
				if !messageOut.deaRawMessageID.Valid {
					messagesToRetry = append(messagesToRetry, sampleCodesWithMessageIDAndIDs[i])
					continue
				}
				err = s.deaClient.RegisterSampleCodes(messageOut.deaRawMessageID.UUID, sampleCodesWithMessageIDAndIDs[i].SampleCodes)
				if err != nil {
					log.Error().Err(err).Msg("register message out sample codes failed")
					messagesToRetry = append(messagesToRetry, sampleCodesWithMessageIDAndIDs[i])
					continue
				}
				err = s.messageOutRepository.MarkSampleCodesAsUploadedByIDs(ctx, sampleCodesWithMessageIDAndIDs[i].MessageSampleCodeIDs)
				if err != nil {
					log.Error().Err(err).Msg("register message in sample codes failed")
					messagesToRetry = append(messagesToRetry, sampleCodesWithMessageIDAndIDs[i])
					continue
				}
			}
			if len(messagesToRetry) == 0 {
				continue
			}
			time.AfterFunc(deaRetryTimeoutSeconds*time.Second, func() {
				s.messageOutSampleCodeIDChan <- messagesToRetry
			})
		case sampleCodesWithMessageIDAndIDs := <-messageInBatchChan:
			messagesToRetry := make([]sampleCodesWithIDsAndMessageID, 0)
			messageIDs := make([]uuid.UUID, len(sampleCodesWithMessageIDAndIDs))
			for i := range sampleCodesWithMessageIDAndIDs {
				messageIDs[i] = sampleCodesWithMessageIDAndIDs[i].MessageID
			}
			messages, err := s.GetMessageInsByIDs(ctx, messageIDs)
			if err != nil {
				log.Error().Err(err).Msg("register message in sample codes failed")
				time.AfterFunc(deaRetryTimeoutSeconds*time.Second, func() {
					s.messageInSampleCodeIDChan <- sampleCodesWithMessageIDAndIDs
				})
				continue
			}
			messagesByIDs := make(map[uuid.UUID]MessageIn)
			for i := range messages {
				messagesByIDs[messages[i].ID] = messages[i]
			}
			for i := range sampleCodesWithMessageIDAndIDs {
				messageIn := messagesByIDs[sampleCodesWithMessageIDAndIDs[i].MessageID]
				if !messageIn.deaRawMessageID.Valid {
					messagesToRetry = append(messagesToRetry, sampleCodesWithMessageIDAndIDs[i])
					continue
				}
				err := s.deaClient.RegisterSampleCodes(messageIn.deaRawMessageID.UUID, sampleCodesWithMessageIDAndIDs[i].SampleCodes)
				if err != nil {
					log.Error().Err(err).Msg("register message in sample codes failed")
					messagesToRetry = append(messagesToRetry, sampleCodesWithMessageIDAndIDs[i])
					continue
				}
				err = s.messageInRepository.MarkSampleCodesAsUploadedByIDs(ctx, sampleCodesWithMessageIDAndIDs[i].MessageSampleCodeIDs)
				if err != nil {
					log.Error().Err(err).Msg("register message in sample codes failed")
					messagesToRetry = append(messagesToRetry, sampleCodesWithMessageIDAndIDs[i])
				}
			}
			if len(messagesToRetry) == 0 {
				continue
			}
			time.AfterFunc(deaRetryTimeoutSeconds*time.Second, func() {
				s.messageInSampleCodeIDChan <- messagesToRetry
			})
		case <-ctx.Done():
			return
		}
	}
}

func startMessageSampleCodeBatching(ctx context.Context, listeningChan chan []sampleCodesWithIDsAndMessageID, batchChan chan []sampleCodesWithIDsAndMessageID) {
	batchSize := 50
	timeout := 20 * time.Second
	batch := make([]sampleCodesWithIDsAndMessageID, 0)
	for {
		select {
		case <-ctx.Done():
			return
		case sampleCodesWithMessageIDAndIDs := <-listeningChan:
			batch = append(batch, sampleCodesWithMessageIDAndIDs...)
			if len(batch) < batchSize {
				continue
			}
			batchChan <- batch
			batch = make([]sampleCodesWithIDsAndMessageID, 0)
		case <-time.After(timeout):
			if len(batch) == 0 {
				continue
			}
			batchChan <- batch
			batch = make([]sampleCodesWithIDsAndMessageID, 0)
		}
	}
}

func (s *messageService) startMessageInSampleCodeCleanupJob(ctx context.Context) {
	for {
		affected, err := s.deleteOldMessageInSampleCodes(ctx)
		if err != nil {
			log.Error().Err(err).Msg("delete old message in sample codes on startup failed")
			break
		}
		if affected < messageBatchSize {
			break
		}
	}
	for {
		select {
		case <-time.After(time.Hour):
			for {
				affected, err := s.deleteOldMessageInSampleCodes(ctx)
				if err != nil {
					log.Error().Err(err).Msg("delete old message in sample codes failed")
					break
				}
				if affected < messageBatchSize {
					break
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *messageService) deleteOldMessageInSampleCodes(ctx context.Context) (int, error) {
	ids, err := s.messageInRepository.GetMessageInSampleCodeIDsToDelete(ctx, messageBatchSize)
	if err != nil {
		return 0, err
	}
	err = s.messageInRepository.DeleteMessageInSampleCodesByIDs(ctx, ids)
	if err != nil {
		return 0, err
	}

	return len(ids), nil
}

func (s *messageService) startMessageOutSampleCodeCleanupJob(ctx context.Context) {
	for {
		affected, err := s.deleteOldMessageOutSampleCodes(ctx)
		if err != nil {
			log.Error().Err(err).Msg("delete old message out sample codes on startup failed")
			break
		}
		if affected < messageBatchSize {
			break
		}
	}
	for {
		select {
		case <-time.After(time.Hour):
			for {
				affected, err := s.deleteOldMessageOutSampleCodes(ctx)
				if err != nil {
					log.Error().Err(err).Msg("delete old message out sample codes failed")
					break
				}
				if affected < messageBatchSize {
					break
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *messageService) deleteOldMessageOutSampleCodes(ctx context.Context) (int, error) {
	ids, err := s.messageOutRepository.GetMessageOutSampleCodeIDsToDelete(ctx, messageBatchSize)
	if err != nil {
		return 0, err
	}
	err = s.messageOutRepository.DeleteMessageOutSampleCodesByIDs(ctx, ids)
	if err != nil {
		return 0, err
	}

	return len(ids), nil
}

func (s *messageService) addUnsentMessageInSampleCodesToQueue(ctx context.Context) {
	unsentMessageInSampleCodes, err := s.messageInRepository.GetUnsentMessageInSampleCodes(ctx)
	if err != nil {
		log.Error().Err(err).Msg("register unprocessed sample codes on startup failed")
		return
	}
	unsentSampleCodes := make([]sampleCodesWithIDsAndMessageID, 0)
	for messageID, sampleCodeByID := range unsentMessageInSampleCodes {
		sampleCodes := sampleCodesWithIDsAndMessageID{
			MessageID: messageID,
		}
		for id, sampleCode := range sampleCodeByID {
			sampleCodes.MessageSampleCodeIDs = append(sampleCodes.MessageSampleCodeIDs, id)
			sampleCodes.SampleCodes = append(sampleCodes.SampleCodes, sampleCode)
		}
		if len(sampleCodes.SampleCodes) == 0 {
			continue
		}
		unsentSampleCodes = append(unsentSampleCodes, sampleCodes)
	}
	_ = utils.Partition(len(unsentMessageInSampleCodes), messageBatchSize, func(low int, high int) error {
		s.messageInSampleCodeIDChan <- unsentSampleCodes[low:high]
		return nil
	})
}

func (s *messageService) addUnsentMessageOutSampleCodesToQueue(ctx context.Context) {
	unsentMessageOutSampleCodes, err := s.messageOutRepository.GetUnsentMessageOutSampleCodes(ctx)
	if err != nil {
		log.Error().Err(err).Msg("register unprocessed sample codes on startup failed")
		return
	}
	unsentSampleCodes := make([]sampleCodesWithIDsAndMessageID, 0)
	for messageID, sampleCodeByID := range unsentMessageOutSampleCodes {
		sampleCodes := sampleCodesWithIDsAndMessageID{
			MessageID: messageID,
		}
		for id, sampleCode := range sampleCodeByID {
			sampleCodes.MessageSampleCodeIDs = append(sampleCodes.MessageSampleCodeIDs, id)
			sampleCodes.SampleCodes = append(sampleCodes.SampleCodes, sampleCode)
		}
		if len(sampleCodes.SampleCodes) == 0 {
			continue
		}
		unsentSampleCodes = append(unsentSampleCodes, sampleCodes)
	}
	_ = utils.Partition(len(unsentMessageOutSampleCodes), messageBatchSize, func(low int, high int) error {
		s.messageOutSampleCodeIDChan <- unsentSampleCodes[low:high]
		return nil
	})
}

func (s *messageService) DeleteOldMessageInRecords(ctx context.Context, cleanupDays int, limit int) (int64, error) {
	deletedRows, err := s.messageInRepository.DeleteOldMessageInRecords(ctx, cleanupDays, limit)
	if err != nil {
		return deletedRows, err
	}
	return deletedRows, nil
}

func (s *messageService) DeleteOldMessageOutRecords(ctx context.Context, cleanupDays int, limit int) (int64, error) {
	deletedRows, err := s.messageOutRepository.DeleteOldMessageOutRecords(ctx, cleanupDays, limit)
	if err != nil {
		return deletedRows, err
	}
	return deletedRows, nil
}

func NewMessageService(deaClient DeaClientV1, messageInRepository MessageInRepository, messageOutRepository MessageOutRepository, messageOutOrderRepository MessageOutOrderRepository, serviceName string) MessageService {
	return &messageService{
		deaClient:                  deaClient,
		messageInRepository:        messageInRepository,
		messageOutRepository:       messageOutRepository,
		messageOutOrderRepository:  messageOutOrderRepository,
		messageInArchivingChan:     make(chan []MessageIn, 100),
		messageOutArchivingChan:    make(chan []MessageOut, 100),
		messageInSampleCodeIDChan:  make(chan []sampleCodesWithIDsAndMessageID, 100),
		messageOutSampleCodeIDChan: make(chan []sampleCodesWithIDsAndMessageID, 100),
		serviceName:                serviceName,
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

type sampleCodesWithIDsAndMessageID struct {
	SampleCodes          []string
	MessageID            uuid.UUID
	MessageSampleCodeIDs []uuid.UUID
}
