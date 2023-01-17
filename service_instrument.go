package skeleton

import (
	"context"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/config"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"time"
)

type InstrumentService interface {
	CreateInstrument(ctx context.Context, instrument Instrument) (uuid.UUID, error)
	GetInstruments(ctx context.Context) ([]Instrument, error)
	GetInstrumentByID(ctx context.Context, id uuid.UUID) (Instrument, error)
	GetInstrumentByIP(ctx context.Context, ip string) (Instrument, error)
	UpdateInstrument(ctx context.Context, instrument Instrument) error
	DeleteInstrument(ctx context.Context, id uuid.UUID) error
	GetSupportedProtocols(ctx context.Context) ([]SupportedProtocol, error)
	GetProtocolAbilities(ctx context.Context, protocolID uuid.UUID) ([]ProtocolAbility, error)
	GetManufacturerTests(ctx context.Context, instrumentID uuid.UUID, protocolID uuid.UUID) ([]SupportedManufacturerTests, error)
	UpsertSupportedProtocol(ctx context.Context, id uuid.UUID, name string, description string) error
	UpsertProtocolAbilities(ctx context.Context, protocolID uuid.UUID, protocolAbilities []ProtocolAbility) error
	UpdateInstrumentStatus(ctx context.Context, id uuid.UUID, status InstrumentStatus) error
	EnqueueUnsentInstrumentsToCerberus(ctx context.Context)
}

type instrumentService struct {
	config               *config.Configuration
	instrumentRepository InstrumentRepository
	manager              Manager
	instrumentCache      InstrumentCache
	cerberusClient       Cerberus
}

func NewInstrumentService(config *config.Configuration, instrumentRepository InstrumentRepository, manager Manager, instrumentCache InstrumentCache, cerberusClient Cerberus) InstrumentService {
	service := &instrumentService{
		config:               config,
		instrumentRepository: instrumentRepository,
		manager:              manager,
		instrumentCache:      instrumentCache,
		cerberusClient:       cerberusClient,
	}

	manager.RegisterInstrumentQueueListener(service, InstrumentAddedEvent, InstrumentUpdatedEvent, InstrumentAddRetryEvent)

	return service
}

func (s *instrumentService) CreateInstrument(ctx context.Context, instrument Instrument) (uuid.UUID, error) {
	transaction, err := s.instrumentRepository.CreateTransaction()
	if err != nil {
		return uuid.Nil, err
	}
	id, err := s.instrumentRepository.WithTransaction(transaction).CreateInstrument(ctx, instrument)
	if err != nil {
		_ = transaction.Rollback()
		return uuid.Nil, err
	}
	analyteMappingIDs, err := s.instrumentRepository.WithTransaction(transaction).CreateAnalyteMappings(ctx, instrument.AnalyteMappings, id)
	for i, analyteMappingID := range analyteMappingIDs {
		_, err = s.instrumentRepository.WithTransaction(transaction).CreateChannelMappings(ctx, instrument.AnalyteMappings[i].ChannelMappings, analyteMappingID)
		if err != nil {
			_ = transaction.Rollback()
			return uuid.Nil, err
		}
		_, err = s.instrumentRepository.WithTransaction(transaction).CreateResultMappings(ctx, instrument.AnalyteMappings[i].ResultMappings, analyteMappingID)
		if err != nil {
			_ = transaction.Rollback()
			return uuid.Nil, err
		}
	}
	requestMappingIDs, err := s.instrumentRepository.WithTransaction(transaction).CreateRequestMappings(ctx, instrument.RequestMappings, id)
	if err != nil {
		_ = transaction.Rollback()
		return uuid.Nil, err
	}
	analyteIDsByRequestMappingIDs := make(map[uuid.UUID][]uuid.UUID)
	for i, requestMappingID := range requestMappingIDs {
		analyteIDsByRequestMappingIDs[requestMappingID] = instrument.RequestMappings[i].AnalyteIDs
	}
	err = s.instrumentRepository.WithTransaction(transaction).UpsertRequestMappingAnalytes(ctx, analyteIDsByRequestMappingIDs)
	if err != nil {
		_ = transaction.Rollback()
		return uuid.Nil, err
	}
	err = transaction.Commit()
	if err != nil {
		return uuid.Nil, err
	}
	s.manager.EnqueueInstrument(id, InstrumentAddedEvent)
	return id, nil
}

func (s *instrumentService) GetInstruments(ctx context.Context) ([]Instrument, error) {
	if instruments := s.instrumentCache.GetAll(); len(instruments) > 0 {
		return instruments, nil
	}

	instruments, err := s.instrumentRepository.GetInstruments(ctx)
	if err != nil {
		return nil, err
	}
	instrumentIDs := make([]uuid.UUID, len(instruments))
	instrumentsByIDs := make(map[uuid.UUID]*Instrument)
	analyteMappingsByIDs := make(map[uuid.UUID]*AnalyteMapping)
	for i := range instruments {
		protocol, err := s.instrumentRepository.GetProtocolByID(ctx, instruments[i].ProtocolID)
		if err != nil {
			return nil, err
		}
		instruments[i].ProtocolName = protocol.Name

		instrumentIDs[i] = instruments[i].ID
		instrumentsByIDs[instruments[i].ID] = &instruments[i]
	}
	analyteMappingsByInstrumentID, err := s.instrumentRepository.GetAnalyteMappings(ctx, instrumentIDs)
	if err != nil {
		return nil, err
	}
	analyteMappingsIDs := make([]uuid.UUID, 0)
	analyteMappingIDInstrumentIDMap := make(map[uuid.UUID]uuid.UUID)

	for instrumentID, analyteMappings := range analyteMappingsByInstrumentID {
		instrumentsByIDs[instrumentID].AnalyteMappings = analyteMappings
		for i := range instrumentsByIDs[instrumentID].AnalyteMappings {
			analyteMappingsIDs = append(analyteMappingsIDs, analyteMappings[i].ID)
			analyteMappingIDInstrumentIDMap[analyteMappings[i].ID] = instrumentID
			analyteMappingsByIDs[analyteMappings[i].ID] = &instrumentsByIDs[instrumentID].AnalyteMappings[i]
		}
	}
	channelMappingsByAnalyteMappingID, err := s.instrumentRepository.GetChannelMappings(ctx, analyteMappingsIDs)
	if err != nil {
		return nil, err
	}
	for analyteMappingID, channelMappings := range channelMappingsByAnalyteMappingID {
		analyteMappingsByIDs[analyteMappingID].ChannelMappings = channelMappings
	}

	resultMappingsByAnalyteMappingID, err := s.instrumentRepository.GetResultMappings(ctx, analyteMappingsIDs)
	if err != nil {
		return nil, err
	}
	for analyteMappingID, resultMappings := range resultMappingsByAnalyteMappingID {
		analyteMappingsByIDs[analyteMappingID].ResultMappings = resultMappings
	}

	requestMappingsByInstrumentID, err := s.instrumentRepository.GetRequestMappings(ctx, instrumentIDs)
	if err != nil {
		return nil, err
	}
	requestMappingIDs := make([]uuid.UUID, 0)
	requestMappingsByIDs := make(map[uuid.UUID]*RequestMapping)
	for instrumentID, requestMappings := range requestMappingsByInstrumentID {
		instrumentsByIDs[instrumentID].RequestMappings = requestMappings
		for i := range instrumentsByIDs[instrumentID].RequestMappings {
			requestMappingIDs = append(requestMappingIDs, requestMappings[i].ID)
			requestMappingsByIDs[requestMappings[i].ID] = &instrumentsByIDs[instrumentID].RequestMappings[i]
		}
	}

	requestMappingAnalyteIDs, err := s.instrumentRepository.GetRequestMappingAnalytes(ctx, requestMappingIDs)
	if err != nil {
		return nil, err
	}

	for requestMappingID, analyteIDs := range requestMappingAnalyteIDs {
		requestMappingsByIDs[requestMappingID].AnalyteIDs = analyteIDs
	}

	s.instrumentCache.Set(instruments)

	return instruments, nil
}

func (s *instrumentService) GetInstrumentByID(ctx context.Context, id uuid.UUID) (Instrument, error) {
	if instrument, ok := s.instrumentCache.GetByID(id); ok {
		return instrument, nil
	}

	instrument, err := s.instrumentRepository.GetInstrumentByID(ctx, id)
	if err != nil {
		return instrument, err
	}
	instrumentIDs := []uuid.UUID{instrument.ID}
	analyteMappingsByInstrumentID, err := s.instrumentRepository.GetAnalyteMappings(ctx, instrumentIDs)
	if err != nil {
		return instrument, err
	}
	analyteMappingsIDs := make([]uuid.UUID, 0)
	analyteMappingIDInstrumentIDMap := make(map[uuid.UUID]uuid.UUID)
	analyteMappingsByIDs := make(map[uuid.UUID]*AnalyteMapping)

	for instrumentID, analyteMappings := range analyteMappingsByInstrumentID {
		instrument.AnalyteMappings = analyteMappings
		for i := range instrument.AnalyteMappings {
			analyteMappingsIDs = append(analyteMappingsIDs, analyteMappings[i].ID)
			analyteMappingIDInstrumentIDMap[analyteMappings[i].ID] = instrumentID
			analyteMappingsByIDs[analyteMappings[i].ID] = &instrument.AnalyteMappings[i]
		}
	}
	channelMappingsByAnalyteMappingID, err := s.instrumentRepository.GetChannelMappings(ctx, analyteMappingsIDs)
	if err != nil {
		return instrument, err
	}
	for analyteMappingID, channelMappings := range channelMappingsByAnalyteMappingID {
		analyteMappingsByIDs[analyteMappingID].ChannelMappings = channelMappings
	}

	resultMappingsByAnalyteMappingID, err := s.instrumentRepository.GetResultMappings(ctx, analyteMappingsIDs)
	if err != nil {
		return instrument, err
	}
	for analyteMappingID, resultMappings := range resultMappingsByAnalyteMappingID {
		analyteMappingsByIDs[analyteMappingID].ResultMappings = resultMappings
	}

	requestMappingsByInstrumentID, err := s.instrumentRepository.GetRequestMappings(ctx, instrumentIDs)
	if err != nil {
		return instrument, err
	}
	requestMappingIDs := make([]uuid.UUID, 0)
	requestMappingsByIDs := make(map[uuid.UUID]*RequestMapping)
	for _, requestMappings := range requestMappingsByInstrumentID {
		instrument.RequestMappings = requestMappings
		for i := range instrument.RequestMappings {
			requestMappingIDs = append(requestMappingIDs, requestMappings[i].ID)
			requestMappingsByIDs[requestMappings[i].ID] = &instrument.RequestMappings[i]
		}
	}

	requestMappingAnalyteIDs, err := s.instrumentRepository.GetRequestMappingAnalytes(ctx, requestMappingIDs)
	if err != nil {
		return instrument, err
	}

	for requestMappingID, analyteIDs := range requestMappingAnalyteIDs {
		requestMappingsByIDs[requestMappingID].AnalyteIDs = analyteIDs
	}

	return instrument, nil
}

func (s *instrumentService) GetInstrumentByIP(ctx context.Context, ip string) (Instrument, error) {
	if instrument, ok := s.instrumentCache.GetByIP(ip); ok {
		return instrument, nil
	}
	return s.instrumentRepository.GetInstrumentByIP(ctx, ip)
}

func (s *instrumentService) UpdateInstrument(ctx context.Context, instrument Instrument) error {
	oldInstrument, err := s.GetInstrumentByID(ctx, instrument.ID)
	if err != nil {
		return err
	}
	err = s.instrumentRepository.UpdateInstrument(ctx, instrument)
	if err != nil {
		return err
	}
	deletedAnalyteMappingIDs := make([]uuid.UUID, 0)
	deletedChannelMappingIDs := make([]uuid.UUID, 0)
	deletedResultMappingIDs := make([]uuid.UUID, 0)
	deletedRequestMappingIDs := make([]uuid.UUID, 0)

	for _, oldAnalyteMapping := range oldInstrument.AnalyteMappings {
		analyteMappingFound := false
		for _, newAnalyteMapping := range instrument.AnalyteMappings {
			if oldAnalyteMapping.ID == newAnalyteMapping.ID {
				for _, oldChannelMapping := range oldAnalyteMapping.ChannelMappings {
					channelMappingFound := false
					for _, newChannelMapping := range newAnalyteMapping.ChannelMappings {
						if oldChannelMapping.ID == newChannelMapping.ID {
							channelMappingFound = true
							break
						}
					}
					if !channelMappingFound {
						deletedChannelMappingIDs = append(deletedChannelMappingIDs, oldChannelMapping.ID)
					}
				}
				for _, oldResultMapping := range oldAnalyteMapping.ResultMappings {
					resultMappingFound := false
					for _, newResultMapping := range newAnalyteMapping.ResultMappings {
						if oldResultMapping.ID == newResultMapping.ID {
							resultMappingFound = true
							break
						}
					}
					if !resultMappingFound {
						deletedResultMappingIDs = append(deletedResultMappingIDs, oldResultMapping.ID)
					}
				}
				analyteMappingFound = true
				break
			}
		}
		if !analyteMappingFound {
			deletedAnalyteMappingIDs = append(deletedAnalyteMappingIDs, oldAnalyteMapping.ID)
		}
	}
	for _, oldRequestMapping := range oldInstrument.RequestMappings {
		found := false
		for _, newRequestMapping := range instrument.RequestMappings {
			if oldRequestMapping.ID == newRequestMapping.ID {
				found = true
				break
			}
		}
		if !found {
			deletedRequestMappingIDs = append(deletedRequestMappingIDs, oldRequestMapping.ID)
		}
	}

	err = s.instrumentRepository.DeleteAnalyteMappings(ctx, deletedAnalyteMappingIDs)
	if err != nil {
		return err
	}
	err = s.instrumentRepository.DeleteChannelMappings(ctx, deletedChannelMappingIDs)
	if err != nil {
		return err
	}
	err = s.instrumentRepository.DeleteResultMappings(ctx, deletedResultMappingIDs)
	if err != nil {
		return err
	}
	err = s.instrumentRepository.DeleteRequestMappings(ctx, deletedRequestMappingIDs)
	if err != nil {
		return err
	}

	newAnalyteMappings := make([]AnalyteMapping, 0)
	for _, analyteMapping := range instrument.AnalyteMappings {
		if analyteMapping.ID == uuid.Nil {
			newAnalyteMappings = append(newAnalyteMappings, analyteMapping)
		} else {
			err = s.instrumentRepository.UpdateAnalyteMapping(ctx, analyteMapping)
			if err != nil {
				return err
			}
			newChannelMappings := make([]ChannelMapping, 0)
			for _, channelMapping := range analyteMapping.ChannelMappings {
				if channelMapping.ID == uuid.Nil {
					newChannelMappings = append(newChannelMappings, channelMapping)
				} else {
					err = s.instrumentRepository.UpdateChannelMapping(ctx, channelMapping)
					if err != nil {
						return err
					}
				}
			}
			_, err = s.instrumentRepository.CreateChannelMappings(ctx, newChannelMappings, analyteMapping.ID)
			if err != nil {
				return err
			}

			newResultMappings := make([]ResultMapping, 0)
			for _, resultMapping := range analyteMapping.ResultMappings {
				if resultMapping.ID == uuid.Nil {
					newResultMappings = append(newResultMappings, resultMapping)
				} else {
					err = s.instrumentRepository.UpdateResultMapping(ctx, resultMapping)
					if err != nil {
						return err
					}
				}
			}
			_, err = s.instrumentRepository.CreateResultMappings(ctx, newResultMappings, analyteMapping.ID)
			if err != nil {
				return err
			}
		}
	}
	analyteMappingIDs, err := s.instrumentRepository.CreateAnalyteMappings(ctx, newAnalyteMappings, instrument.ID)
	if err != nil {
		return err
	}
	for i, analyteMappingID := range analyteMappingIDs {
		_, err = s.instrumentRepository.CreateChannelMappings(ctx, newAnalyteMappings[i].ChannelMappings, analyteMappingID)
		if err != nil {
			return err
		}
		_, err = s.instrumentRepository.CreateResultMappings(ctx, newAnalyteMappings[i].ResultMappings, analyteMappingID)
		if err != nil {
			return err
		}
	}
	newRequestMappings := make([]RequestMapping, 0)
	for _, requestMapping := range instrument.RequestMappings {
		if requestMapping.ID == uuid.Nil {
			newRequestMappings = append(newRequestMappings, requestMapping)
		} else {
			err = s.instrumentRepository.UpdateRequestMapping(ctx, requestMapping)
			if err != nil {
				return err
			}
			err = s.instrumentRepository.UpsertRequestMappingAnalytes(ctx, map[uuid.UUID][]uuid.UUID{
				requestMapping.ID: requestMapping.AnalyteIDs,
			})
		}
	}
	requestMappingIDs, err := s.instrumentRepository.CreateRequestMappings(ctx, newRequestMappings, instrument.ID)
	if err != nil {
		return err
	}
	requestMappingsAnalytes := make(map[uuid.UUID][]uuid.UUID)
	for i := range requestMappingIDs {
		requestMappingsAnalytes[requestMappingIDs[i]] = newRequestMappings[i].AnalyteIDs
	}
	err = s.instrumentRepository.UpsertRequestMappingAnalytes(ctx, requestMappingsAnalytes)
	if err != nil {
		return err
	}
	s.manager.EnqueueInstrument(instrument.ID, InstrumentUpdatedEvent)
	return nil
}

func (s *instrumentService) DeleteInstrument(ctx context.Context, id uuid.UUID) error {
	err := s.instrumentRepository.DeleteInstrument(ctx, id)
	if err != nil {
		return err
	}
	s.instrumentCache.Invalidate()
	return nil
}

func (s *instrumentService) GetSupportedProtocols(ctx context.Context) ([]SupportedProtocol, error) {
	supportedProtocols, err := s.instrumentRepository.GetSupportedProtocols(ctx)
	if err != nil {
		return nil, err
	}
	for i := range supportedProtocols {
		abilities, err := s.instrumentRepository.GetProtocolAbilities(ctx, supportedProtocols[i].ID)
		if err != nil {
			return nil, err
		}
		supportedProtocols[i].ProtocolAbilities = abilities
	}
	return supportedProtocols, nil
}

func (s *instrumentService) GetProtocolAbilities(ctx context.Context, protocolID uuid.UUID) ([]ProtocolAbility, error) {
	return s.instrumentRepository.GetProtocolAbilities(ctx, protocolID)
}

func (s *instrumentService) GetManufacturerTests(ctx context.Context, instrumentID uuid.UUID, protocolID uuid.UUID) ([]SupportedManufacturerTests, error) {
	tests, err := s.manager.GetCallbackHandler().GetManufacturerTestList(instrumentID, protocolID)
	if err != nil {
		return nil, err
	}
	if len(tests) < 1 {
		return []SupportedManufacturerTests{}, nil
	}
	return tests, nil
}

func (s *instrumentService) UpsertSupportedProtocol(ctx context.Context, id uuid.UUID, name string, description string) error {
	return s.instrumentRepository.UpsertSupportedProtocol(ctx, id, name, description)
}

func (s *instrumentService) UpsertProtocolAbilities(ctx context.Context, protocolID uuid.UUID, protocolAbilities []ProtocolAbility) error {
	return s.instrumentRepository.UpsertProtocolAbilities(ctx, protocolID, protocolAbilities)
}

func (s *instrumentService) UpdateInstrumentStatus(ctx context.Context, id uuid.UUID, status InstrumentStatus) error {
	return s.instrumentRepository.UpdateInstrumentStatus(ctx, id, status)
}

func (s *instrumentService) EnqueueUnsentInstrumentsToCerberus(ctx context.Context) {
	instrumentIDs, err := s.instrumentRepository.GetUnsentToCerberus(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to collect unsent instruments")
	}

	for i := range instrumentIDs {
		s.manager.EnqueueInstrument(instrumentIDs[i], InstrumentAddRetryEvent)
	}
}

func (s *instrumentService) ProcessInstrumentEvent(instrumentID uuid.UUID, event instrumentEventType) {
	if event.IsOneOf(InstrumentAddedEvent | InstrumentUpdatedEvent) {
		log.Debug().Msg("Invalidating instrument cache")
		s.instrumentCache.Invalidate()
		log.Trace().Msg("Invalidated instrument cache")

		log.Debug().Str("instrumentID", instrumentID.String()).Msg("Registering instrument in Cerberus")
		if retry, err := s.registerInstrument(context.Background(), instrumentID); err != nil {
			if retry {
				s.retryInstrumentRegistration(context.Background(), instrumentID)
			}
		}
	} else if event.IsExactly(InstrumentAddRetryEvent) {
		log.Debug().Str("instrumentID", instrumentID.String()).Msg("Retrying to register instrument in Cerberus")
		_, _ = s.registerInstrument(context.Background(), instrumentID)
	}
}

func (s *instrumentService) registerInstrument(ctx context.Context, instrumentID uuid.UUID) (bool, error) {
	instrument, err := s.instrumentRepository.GetInstrumentByID(ctx, instrumentID)
	if err != nil {
		log.Error().Err(err).Str("instrumentID", instrumentID.String()).Msg("failed to get instrument!")
		return false, err
	}

	err = s.cerberusClient.RegisterInstrument(instrument)
	if err != nil {
		log.Error().Err(err).Msg("Failed to send instrument to cerberus: " + instrumentID.String())
		return true, err
	}

	err = s.instrumentRepository.MarkAsSentToCerberus(ctx, instrumentID)
	if err != nil {
		log.Error().Err(err).Msg("Failed to mark instrument as sent to cerberus: " + instrumentID.String())
		return false, err
	}

	return false, nil
}

func (s *instrumentService) retryInstrumentRegistration(ctx context.Context, id uuid.UUID) {
	log.Debug().Msg("Starting instrument registration retry task")
	timeoutContext, cancel := context.WithTimeout(ctx, 48*time.Hour)
	ticker := time.NewTicker(time.Duration(s.config.InstrumentTransferRetryDelayInMs) * time.Millisecond)
	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-timeoutContext.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				if retry, err := s.registerInstrument(timeoutContext, id); err != nil {
					if retry {
						break
					}
				}
				cancel()
			}
		}
	}()
}
