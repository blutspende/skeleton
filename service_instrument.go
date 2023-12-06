package skeleton

import (
	"context"
	"time"

	"github.com/DRK-Blutspende-BaWueHe/logcom-api/logcom"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/config"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/db"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type InstrumentService interface {
	CreateInstrument(ctx context.Context, instrument Instrument) (uuid.UUID, error)
	GetInstruments(ctx context.Context) ([]Instrument, error)
	GetInstrumentByID(ctx context.Context, tx db.DbConnector, id uuid.UUID, bypassCache bool) (Instrument, error)
	GetInstrumentByIP(ctx context.Context, ip string) (Instrument, error)
	UpdateInstrument(ctx context.Context, instrument Instrument) error
	DeleteInstrument(ctx context.Context, id uuid.UUID) error
	GetSupportedProtocols(ctx context.Context) ([]SupportedProtocol, error)
	GetProtocolAbilities(ctx context.Context, protocolID uuid.UUID) ([]ProtocolAbility, error)
	GetManufacturerTests(ctx context.Context, instrumentID uuid.UUID, protocolID uuid.UUID) ([]SupportedManufacturerTests, error)
	GetEncodings(ctx context.Context, protocolID uuid.UUID) ([]string, error)
	UpsertSupportedProtocol(ctx context.Context, id uuid.UUID, name string, description string, abilities []ProtocolAbility, settings []ProtocolSetting) error
	UpdateInstrumentStatus(ctx context.Context, id uuid.UUID, status InstrumentStatus) error
	EnqueueUnsentInstrumentsToCerberus(ctx context.Context)
	CheckAnalytesUsage(ctx context.Context, analyteIDs []uuid.UUID) (map[uuid.UUID][]Instrument, error)
	HidePassword(ctx context.Context, instrument *Instrument) error
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
	if err != nil {
		_ = transaction.Rollback()
		return uuid.Nil, err
	}
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
	var err error
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

	settingsMap, err := s.instrumentRepository.GetInstrumentsSettings(ctx, instrumentIDs)
	if err != nil {
		return nil, err
	}
	for instrumentID, settings := range settingsMap {
		if _, ok := instrumentsByIDs[instrumentID]; !ok {
			continue
		}
		instrumentsByIDs[instrumentID].Settings = settings
	}

	s.instrumentCache.Set(instruments)

	return instruments, nil
}

func (s *instrumentService) GetInstrumentByID(ctx context.Context, tx db.DbConnector, id uuid.UUID, bypassCache bool) (Instrument, error) {
	var err error
	if !bypassCache {
		if instrument, ok := s.instrumentCache.GetByID(id); ok {
			return instrument, nil
		}
	}

	instrument, err := s.instrumentRepository.WithTransaction(tx).GetInstrumentByID(ctx, id)
	if err != nil {
		return instrument, err
	}
	instrumentIDs := []uuid.UUID{instrument.ID}
	analyteMappingsByInstrumentID, err := s.instrumentRepository.WithTransaction(tx).GetAnalyteMappings(ctx, instrumentIDs)
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
	channelMappingsByAnalyteMappingID, err := s.instrumentRepository.WithTransaction(tx).GetChannelMappings(ctx, analyteMappingsIDs)
	if err != nil {
		return instrument, err
	}
	for analyteMappingID, channelMappings := range channelMappingsByAnalyteMappingID {
		analyteMappingsByIDs[analyteMappingID].ChannelMappings = channelMappings
	}

	resultMappingsByAnalyteMappingID, err := s.instrumentRepository.WithTransaction(tx).GetResultMappings(ctx, analyteMappingsIDs)
	if err != nil {
		return instrument, err
	}
	for analyteMappingID, resultMappings := range resultMappingsByAnalyteMappingID {
		analyteMappingsByIDs[analyteMappingID].ResultMappings = resultMappings
	}

	requestMappingsByInstrumentID, err := s.instrumentRepository.WithTransaction(tx).GetRequestMappings(ctx, instrumentIDs)
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

	requestMappingAnalyteIDs, err := s.instrumentRepository.WithTransaction(tx).GetRequestMappingAnalytes(ctx, requestMappingIDs)
	if err != nil {
		return instrument, err
	}

	for requestMappingID, analyteIDs := range requestMappingAnalyteIDs {
		requestMappingsByIDs[requestMappingID].AnalyteIDs = analyteIDs
	}

	settingsMap, err := s.instrumentRepository.WithTransaction(tx).GetInstrumentsSettings(ctx, instrumentIDs)
	if err != nil {
		return instrument, err
	}

	if instrumentSettings, ok := settingsMap[id]; ok {
		instrument.Settings = instrumentSettings
	}

	return instrument, nil
}

func (s *instrumentService) GetInstrumentByIP(ctx context.Context, ip string) (Instrument, error) {
	var err error
	if instrument, ok := s.instrumentCache.GetByIP(ip); ok {
		return instrument, nil
	}

	instrument, err := s.instrumentRepository.GetInstrumentByIP(ctx, ip)
	if err != nil {
		return instrument, err
	}
	instrumentIDs := []uuid.UUID{instrument.ID}

	protocol, err := s.instrumentRepository.GetProtocolByID(ctx, instrument.ProtocolID)
	if err != nil {
		return instrument, err
	}
	instrument.ProtocolName = protocol.Name

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
	tx, err := s.instrumentRepository.CreateTransaction()
	if err != nil {
		return db.ErrBeginTransactionFailed
	}

	oldInstrument, err := s.GetInstrumentByID(ctx, tx, instrument.ID, false)
	if err != nil {
		return err
	}

	err = s.instrumentRepository.WithTransaction(tx).UpdateInstrument(ctx, instrument)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	deletedAnalyteMappingIDs := make([]uuid.UUID, 0)
	deletedChannelMappingIDs := make([]uuid.UUID, 0)
	deletedResultMappingIDs := make([]uuid.UUID, 0)
	deletedRequestMappingIDs := make([]uuid.UUID, 0)
	deletedSettingIDs := make([]uuid.UUID, 0)

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
	for _, oldSetting := range oldInstrument.Settings {
		found := false
		for i := range instrument.Settings {
			if instrument.Settings[i].ID == oldSetting.ID {
				found = true
				break
			}
		}
		if !found {
			deletedSettingIDs = append(deletedSettingIDs, oldSetting.ID)
		}
	}
	err = s.instrumentRepository.WithTransaction(tx).DeleteAnalyteMappings(ctx, deletedAnalyteMappingIDs)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	err = s.instrumentRepository.WithTransaction(tx).DeleteChannelMappings(ctx, deletedChannelMappingIDs)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	err = s.instrumentRepository.WithTransaction(tx).DeleteResultMappings(ctx, deletedResultMappingIDs)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	err = s.instrumentRepository.WithTransaction(tx).DeleteRequestMappings(ctx, deletedRequestMappingIDs)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	err = s.instrumentRepository.WithTransaction(tx).DeleteInstrumentSettings(ctx, deletedSettingIDs)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	newAnalyteMappings := make([]AnalyteMapping, 0)
	for _, analyteMapping := range instrument.AnalyteMappings {
		if analyteMapping.ID == uuid.Nil {
			newAnalyteMappings = append(newAnalyteMappings, analyteMapping)
		} else {
			err = s.instrumentRepository.WithTransaction(tx).UpdateAnalyteMapping(ctx, analyteMapping)
			if err != nil {
				_ = tx.Rollback()
				return err
			}
			newChannelMappings := make([]ChannelMapping, 0)
			for _, channelMapping := range analyteMapping.ChannelMappings {
				if channelMapping.ID == uuid.Nil {
					newChannelMappings = append(newChannelMappings, channelMapping)
				} else {
					err = s.instrumentRepository.WithTransaction(tx).UpdateChannelMapping(ctx, channelMapping)
					if err != nil {
						_ = tx.Rollback()
						return err
					}
				}
			}
			_, err = s.instrumentRepository.WithTransaction(tx).CreateChannelMappings(ctx, newChannelMappings, analyteMapping.ID)
			if err != nil {
				_ = tx.Rollback()
				return err
			}

			newResultMappings := make([]ResultMapping, 0)
			for _, resultMapping := range analyteMapping.ResultMappings {
				if resultMapping.ID == uuid.Nil {
					newResultMappings = append(newResultMappings, resultMapping)
				} else {
					err = s.instrumentRepository.WithTransaction(tx).UpdateResultMapping(ctx, resultMapping)
					if err != nil {
						_ = tx.Rollback()
						return err
					}
				}
			}
			_, err = s.instrumentRepository.WithTransaction(tx).CreateResultMappings(ctx, newResultMappings, analyteMapping.ID)
			if err != nil {
				_ = tx.Rollback()
				return err
			}
		}
	}
	analyteMappingIDs, err := s.instrumentRepository.WithTransaction(tx).CreateAnalyteMappings(ctx, newAnalyteMappings, instrument.ID)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	for i, analyteMappingID := range analyteMappingIDs {
		_, err = s.instrumentRepository.WithTransaction(tx).CreateChannelMappings(ctx, newAnalyteMappings[i].ChannelMappings, analyteMappingID)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		_, err = s.instrumentRepository.WithTransaction(tx).CreateResultMappings(ctx, newAnalyteMappings[i].ResultMappings, analyteMappingID)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	newRequestMappings := make([]RequestMapping, 0)
	for _, requestMapping := range instrument.RequestMappings {
		if requestMapping.ID == uuid.Nil {
			newRequestMappings = append(newRequestMappings, requestMapping)
		} else {
			err = s.instrumentRepository.WithTransaction(tx).UpdateRequestMapping(ctx, requestMapping)
			if err != nil {
				_ = tx.Rollback()
				return err
			}
			err = s.instrumentRepository.WithTransaction(tx).UpsertRequestMappingAnalytes(ctx, map[uuid.UUID][]uuid.UUID{
				requestMapping.ID: requestMapping.AnalyteIDs,
			})
			if err != nil {
				_ = tx.Rollback()
				return err
			}
		}
	}
	requestMappingIDs, err := s.instrumentRepository.WithTransaction(tx).CreateRequestMappings(ctx, newRequestMappings, instrument.ID)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	requestMappingsAnalytes := make(map[uuid.UUID][]uuid.UUID)
	for i := range requestMappingIDs {
		requestMappingsAnalytes[requestMappingIDs[i]] = newRequestMappings[i].AnalyteIDs
	}
	err = s.instrumentRepository.WithTransaction(tx).UpsertRequestMappingAnalytes(ctx, requestMappingsAnalytes)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	protocolSettings, err := s.instrumentRepository.GetProtocolSettings(ctx, instrument.ProtocolID)
	if err != nil {
		return err
	}
	for i := range instrument.Settings {
		isSettingUpdateExcluded := false
		for _, protocolSetting := range protocolSettings {
			if instrument.Settings[i].ProtocolSettingID != protocolSetting.ID {
				continue
			}
			if protocolSetting.Type != Password {
				break
			}

			if instrument.Settings[i].Value == "" {
				isSettingUpdateExcluded = true
			}
			break
		}
		if isSettingUpdateExcluded {
			continue
		}
		err = s.instrumentRepository.WithTransaction(tx).UpsertInstrumentSetting(ctx, instrument.ID, instrument.Settings[i])
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	newInstrument, err := s.GetInstrumentByID(ctx, tx, instrument.ID, true)
	if err != nil {
		return err
	}

	err = logcom.SendAuditLogWithModification(ctx, "INSTRUMENT", oldInstrument.Name+"("+oldInstrument.ID.String()+")", oldInstrument, newInstrument)
	if err != nil {
		log.Error().Err(err).Msg("Failed to audit instrument update")
		_ = tx.Rollback()
		return ErrFailedToAudit
	}

	err = tx.Commit()
	if err != nil {
		_ = tx.Rollback()
		return db.ErrCommitTransactionFailed
	}

	s.instrumentCache.Invalidate()
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
		settings, err := s.instrumentRepository.GetProtocolSettings(ctx, supportedProtocols[i].ID)
		if err != nil {
			return nil, err
		}
		supportedProtocols[i].ProtocolSettings = settings
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
	for i := range tests {
		if tests[i].Channels == nil {
			tests[i].Channels = make([]string, 0)
		}
		if tests[i].ValidResultValues == nil {
			tests[i].ValidResultValues = make([]string, 0)
		}
	}
	return tests, nil
}

func (s *instrumentService) GetEncodings(ctx context.Context, protocolID uuid.UUID) ([]string, error) {
	encodings, err := s.manager.GetCallbackHandler().GetEncodingList(protocolID)
	if err != nil {
		return nil, err
	}
	if len(encodings) < 1 {
		return s.instrumentRepository.GetEncodings(ctx)
	}
	return encodings, nil
}

func (s *instrumentService) UpsertSupportedProtocol(ctx context.Context, id uuid.UUID, name string, description string, abilities []ProtocolAbility, settings []ProtocolSetting) error {
	tx, err := s.instrumentRepository.CreateTransaction()
	if err != nil {
		return err
	}
	oldSettings, err := s.instrumentRepository.WithTransaction(tx).GetProtocolSettings(ctx, id)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	deletedSettingIDs := make([]uuid.UUID, 0)
	for i := range oldSettings {
		found := false
		for j := range settings {
			if oldSettings[i].ID == settings[j].ID {
				found = true
				break
			}
		}
		if !found {
			deletedSettingIDs = append(deletedSettingIDs, oldSettings[i].ID)
		}
	}
	err = s.instrumentRepository.WithTransaction(tx).UpsertSupportedProtocol(ctx, id, name, description)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	err = s.instrumentRepository.WithTransaction(tx).UpsertProtocolAbilities(ctx, id, abilities)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	err = s.instrumentRepository.WithTransaction(tx).DeleteProtocolSettings(ctx, deletedSettingIDs)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	for i := range settings {
		err = s.instrumentRepository.WithTransaction(tx).UpsertProtocolSetting(ctx, id, settings[i])
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Error().Err(err).Msg(msgUpsertSupportedProtocolFailed)
		_ = tx.Rollback()
		return ErrUpsertProtocolAbilitiesFailed
	}
	return nil
}

func (s *instrumentService) UpsertProtocolAbilities(ctx context.Context, protocolID uuid.UUID, protocolAbilities []ProtocolAbility) error {
	return s.instrumentRepository.UpsertProtocolAbilities(ctx, protocolID, protocolAbilities)
}

func (s *instrumentService) UpdateInstrumentStatus(ctx context.Context, id uuid.UUID, status InstrumentStatus) error {
	err := s.instrumentRepository.UpdateInstrumentStatus(ctx, id, status)
	if err != nil {
		return err
	}

	s.instrumentCache.Invalidate()

	return nil
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

func (s *instrumentService) CheckAnalytesUsage(ctx context.Context, analyteIDs []uuid.UUID) (map[uuid.UUID][]Instrument, error) {
	return s.instrumentRepository.CheckAnalytesUsage(ctx, analyteIDs)
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

func (s *instrumentService) HidePassword(ctx context.Context, instrument *Instrument) error {
	protocolSettings, err := s.instrumentRepository.GetProtocolSettings(ctx, instrument.ProtocolID)
	if err != nil {
		return err
	}
	passwordProtocolSettingsMap := make(map[uuid.UUID]any)
	for _, protocolSetting := range protocolSettings {
		if protocolSetting.Type == Password {
			passwordProtocolSettingsMap[protocolSetting.ID] = nil
		}
	}

	if len(passwordProtocolSettingsMap) == 0 {
		return nil
	}

	for i := range instrument.Settings {
		if _, ok := passwordProtocolSettingsMap[instrument.Settings[i].ProtocolSettingID]; ok {
			instrument.Settings[i].Value = ""
		}
	}

	return nil
}
