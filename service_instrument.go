package skeleton

import (
	"context"
	"github.com/google/uuid"
)

type InstrumentService interface {
	CreateInstrument(ctx context.Context, instrument Instrument) (uuid.UUID, error)
	GetInstruments(ctx context.Context) ([]Instrument, error)
	GetInstrumentByID(ctx context.Context, id uuid.UUID) (Instrument, error)
	UpdateInstrument(ctx context.Context, instrument Instrument) error
	DeleteInstrument(ctx context.Context, id uuid.UUID) error
	GetSupportedProtocols(ctx context.Context) ([]SupportedProtocol, error)
	GetProtocolAbilities(ctx context.Context, protocolID uuid.UUID) ([]ProtocolAbility, error)
}

type instrumentService struct {
	instrumentRepository InstrumentRepository
}

func NewInstrumentService(instrumentRepository InstrumentRepository) InstrumentService {
	return &instrumentService{
		instrumentRepository: instrumentRepository,
	}
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
	return id, nil
}

func (s *instrumentService) GetInstruments(ctx context.Context) ([]Instrument, error) {
	instruments, err := s.instrumentRepository.GetInstruments(ctx)
	if err != nil {
		return nil, err
	}
	instrumentIDs := make([]uuid.UUID, len(instruments))
	instrumentsByIDs := make(map[uuid.UUID]*Instrument)
	analyteMappingsByIDs := make(map[uuid.UUID]*AnalyteMapping)
	for i := range instruments {
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
	return instruments, nil
}

func (s *instrumentService) GetInstrumentByID(ctx context.Context, id uuid.UUID) (Instrument, error) {
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
	return nil
}

func (s *instrumentService) DeleteInstrument(ctx context.Context, id uuid.UUID) error {
	return s.instrumentRepository.DeleteInstrument(ctx, id)
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
