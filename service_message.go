package skeleton

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"regexp"
	"strings"
	"time"
)

type MessageService interface {
	GetMessageInByID(ctx context.Context, messageID uuid.UUID) (MessageIn, error)
	GetMessageInsByIDs(ctx context.Context, messageIDs []uuid.UUID) (map[uuid.UUID]MessageIn, error)
	GetMessageOutByID(ctx context.Context, messageID uuid.UUID) (MessageOut, error)
	SaveMessageIn(ctx context.Context, message MessageIn) (uuid.UUID, error)
	SaveMessageOut(ctx context.Context, message MessageOut) (uuid.UUID, error)
	UpdateMessageIn(ctx context.Context, message MessageIn) error
	UpdateMessageOut(ctx context.Context, message MessageOut) error
	LinkSampleCodesToMessageIn(ctx context.Context, sampleCodes []string, messageInID uuid.UUID) error
	LinkSampleCodesToMessageOut(ctx context.Context, sampleCodes []string, messageOutID uuid.UUID) error
	EnqueueMessageInsForArchiving(...MessageIn)
	EnqueueMessageOutsForArchiving(...MessageOut)
	StartDEAArchiving(ctx context.Context)
}

type messageService struct {
	deaClient               DeaClientV1
	messageInRepository     MessageInRepository
	messageOutRepository    MessageOutRepository
	messageInArchivingChan  chan []MessageIn
	messageInProcessingChan chan []MessageIn
	messageOutArchivingChan chan []MessageOut
	serviceName             string
}

func (s *messageService) GetMessageInByID(ctx context.Context, messageID uuid.UUID) (MessageIn, error) {
	return s.messageInRepository.GetByID(ctx, messageID)
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

func (s *messageService) GetMessageOutByID(ctx context.Context, messageID uuid.UUID) (MessageOut, error) {
	//TODO implement me
	panic("implement me")
}

func (s *messageService) SaveMessageIn(ctx context.Context, message MessageIn) (uuid.UUID, error) {
	return s.messageInRepository.Create(ctx, message)
}

func (s *messageService) SaveMessageOut(ctx context.Context, message MessageOut) (uuid.UUID, error) {
	return s.messageOutRepository.Create(ctx, message)

}

func (s *messageService) UpdateMessageIn(ctx context.Context, message MessageIn) error {
	return s.messageInRepository.Update(ctx, message)
}

func (s *messageService) UpdateMessageOut(ctx context.Context, message MessageOut) error {
	return s.messageOutRepository.Update(ctx, message)
}

func (s *messageService) LinkSampleCodesToMessageIn(ctx context.Context, sampleCodes []string, messageInID uuid.UUID) error {
	return s.messageInRepository.LinkSampleCodes(ctx, sampleCodes, messageInID)
}

func (s *messageService) LinkSampleCodesToMessageOut(ctx context.Context, sampleCodes []string, messageOutID uuid.UUID) error {
	return s.messageOutRepository.LinkSampleCodes(ctx, sampleCodes, messageOutID)
}

func (s *messageService) EnqueueMessageInsForArchiving(in ...MessageIn) {
	//TODO implement me
	panic("implement me")
}

func (s *messageService) EnqueueMessageOutsForArchiving(out ...MessageOut) {
	//TODO implement me
	panic("implement me")
}

func (s *messageService) StartDEAArchiving(ctx context.Context) {
	for {
		select {
		case messagesToArchive := <-s.messageInArchivingChan:
			failedMessages := make([]MessageIn, 0)
			for i := range messagesToArchive {
				deaID, err := s.uploadRawMessageToDEA(messagesToArchive[i].Raw)
				if err != nil {
					errorMsg := err.Error()
					messagesToArchive[i].Status = MessageStatusError
					messagesToArchive[i].Error = &errorMsg
					messagesToArchive[i].RetryCount += 1
					failedMessages = append(failedMessages, messagesToArchive[i])
				}
				messagesToArchive[i].DEARawMessageID = uuid.NullUUID{
					UUID:  deaID,
					Valid: deaID != uuid.Nil,
				}
				err = s.messageInRepository.Update(ctx, messagesToArchive[i])
				if err != nil {
					failedMessages = append(failedMessages, messagesToArchive[i])
				}
				if len(failedMessages) == 0 {
					continue
				}
				time.AfterFunc(time.Minute, func() { // TODO - configurable
					s.EnqueueMessageInsForArchiving(failedMessages...)
				})
			}
		case messagesToArchive := <-s.messageOutArchivingChan:
			failedMessages := make([]MessageOut, 0)
			for i := range messagesToArchive {
				deaID, err := s.uploadRawMessageToDEA(messagesToArchive[i].Raw)
				if err != nil {
					errorMsg := err.Error()
					messagesToArchive[i].Status = MessageStatusError
					messagesToArchive[i].Error = &errorMsg
					messagesToArchive[i].RetryCount += 1
					failedMessages = append(failedMessages, messagesToArchive[i])
				}
				messagesToArchive[i].DEARawMessageID = uuid.NullUUID{
					UUID:  deaID,
					Valid: deaID != uuid.Nil,
				}
				err = s.messageOutRepository.Update(ctx, messagesToArchive[i])
				if err != nil {
					failedMessages = append(failedMessages, messagesToArchive[i])
				}
				if len(failedMessages) == 0 {
					continue
				}
				time.AfterFunc(time.Minute, func() { // TODO - configurable
					s.EnqueueMessageOutsForArchiving(failedMessages...)
				})
			}
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

func NewMessageService(deaClient DeaClientV1, messageInRepository MessageInRepository, messageOutRepository MessageOutRepository, serviceName string) MessageService {
	return &messageService{
		deaClient:            deaClient,
		messageInRepository:  messageInRepository,
		messageOutRepository: messageOutRepository,
		serviceName:          serviceName,
	}
}
