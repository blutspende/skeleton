package skeleton

import (
	"context"

	"github.com/blutspende/skeleton/db"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type InstrumentService interface {
	CreateInstrument(ctx context.Context, instrument Instrument) (uuid.UUID, error)
	GetInstruments(ctx context.Context) ([]Instrument, error)
	GetInstrumentByID(ctx context.Context, tx db.DbConnection, id uuid.UUID, bypassCache bool) (Instrument, error)
	GetInstrumentByIP(ctx context.Context, ip string) (Instrument, error)
	UpdateInstrument(ctx context.Context, instrument Instrument, userId uuid.UUID) error
	DeleteInstrument(ctx context.Context, id uuid.UUID) error
	GetExpectedControlResultsByInstrumentId(ctx context.Context, instrumentId uuid.UUID) ([]ExpectedControlResult, error)
	GetNotSpecifiedExpectedControlResultsByInstrumentId(ctx context.Context, instrumentId uuid.UUID) ([]NotSpecifiedExpectedControlResult, error)
	CreateExpectedControlResults(ctx context.Context, expectedControlResults []ExpectedControlResult, userId uuid.UUID) error
	UpdateExpectedControlResults(ctx context.Context, instrumentId uuid.UUID, expectedControlResultMapByID []ExpectedControlResult, userId uuid.UUID) error
	DeleteExpectedControlResult(ctx context.Context, expectedControlResultId uuid.UUID, userId uuid.UUID) error
	GetSupportedProtocols(ctx context.Context) ([]SupportedProtocol, error)
	GetManufacturerTests(ctx context.Context) ([]SupportedManufacturerTests, error)
	UpsertSupportedProtocol(ctx context.Context, id uuid.UUID, name string, description string, abilities []ProtocolAbility, settings []ProtocolSetting) error
	UpsertManufacturerTests(ctx context.Context, manufacturerTests []SupportedManufacturerTests) error
	UpdateInstrumentStatus(ctx context.Context, id uuid.UUID, status InstrumentStatus) error
	CheckAnalytesUsage(ctx context.Context, analyteIDs []uuid.UUID) (map[uuid.UUID][]Instrument, error)
}

type instrumentService struct {
	sortingRuleService   SortingRuleService
	instrumentRepository InstrumentRepository
	instrumentCache      InstrumentCache
	cerberusClient       CerberusClient
	manager              Manager
}

func NewInstrumentService(
	sortingRuleService SortingRuleService,
	instrumentRepository InstrumentRepository,
	manager Manager,
	instrumentCache InstrumentCache,
	cerberusClient CerberusClient) InstrumentService {
	service := &instrumentService{
		sortingRuleService:   sortingRuleService,
		instrumentRepository: instrumentRepository,
		manager:              manager,
		instrumentCache:      instrumentCache,
		cerberusClient:       cerberusClient,
	}

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

	if instrument.ConnectionMode == FTP && instrument.FTPConfig != nil {
		instrument.FTPConfig.InstrumentId = id
		err = s.instrumentRepository.WithTransaction(transaction).CreateFtpConfig(ctx, *instrument.FTPConfig)
		if err != nil {
			_ = transaction.Rollback()
			return uuid.Nil, err
		}
	}

	analyteMappingIDs, err := s.instrumentRepository.WithTransaction(transaction).UpsertAnalyteMappings(ctx, instrument.AnalyteMappings, id)
	if err != nil {
		_ = transaction.Rollback()
		return uuid.Nil, err
	}
	for i, analyteMappingID := range analyteMappingIDs {
		_, err = s.instrumentRepository.WithTransaction(transaction).UpsertChannelMappings(ctx, instrument.AnalyteMappings[i].ChannelMappings, analyteMappingID)
		if err != nil {
			_ = transaction.Rollback()
			return uuid.Nil, err
		}

		_, err = s.instrumentRepository.WithTransaction(transaction).UpsertResultMappings(ctx, instrument.AnalyteMappings[i].ResultMappings, analyteMappingID)
		if err != nil {
			_ = transaction.Rollback()
			return uuid.Nil, err
		}

		// TODO: is this even possible? to have mappings and analytes for a newly created instrument?
		err = s.instrumentRepository.WithTransaction(transaction).CreateValidatedAnalyteIDs(ctx, analyteMappingID, instrument.AnalyteMappings[i].ValidatedAnalyteIDs)
		if err != nil {
			_ = transaction.Rollback()
			return uuid.Nil, err
		}
	}

	err = s.instrumentRepository.WithTransaction(transaction).UpsertRequestMappings(ctx, instrument.RequestMappings, id)
	if err != nil {
		_ = transaction.Rollback()
		return uuid.Nil, err
	}
	analyteIDsByRequestMappingIDs := make(map[uuid.UUID][]uuid.UUID)
	for _, mapping := range instrument.RequestMappings {
		analyteIDsByRequestMappingIDs[mapping.ID] = mapping.AnalyteIDs
	}
	err = s.instrumentRepository.WithTransaction(transaction).UpsertRequestMappingAnalytes(ctx, analyteIDsByRequestMappingIDs)
	if err != nil {
		_ = transaction.Rollback()
		return uuid.Nil, err
	}

	protocolSettings, err := s.instrumentRepository.GetProtocolSettings(ctx, instrument.ProtocolID)
	if err != nil {
		return uuid.Nil, err
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
		err = s.instrumentRepository.WithTransaction(transaction).UpsertInstrumentSetting(ctx, instrument.ID, instrument.Settings[i])
		if err != nil {
			_ = transaction.Rollback()
			return uuid.Nil, err
		}
	}

	for i := range instrument.SortingRules {
		instrument.SortingRules[i].InstrumentID = id
		err = s.sortingRuleService.WithTransaction(transaction).UpsertWithTx(ctx, &instrument.SortingRules[i])
		if err != nil {
			_ = transaction.Rollback()
			return uuid.Nil, err
		}
	}

	instrumentHash := HashInstrument(instrument)
	err = s.cerberusClient.VerifyInstrumentHash(instrumentHash)
	if err != nil {
		log.Error().Err(err).Msg("Failed to verify instrument hash")
		_ = transaction.Rollback()
		return uuid.Nil, err
	}

	err = transaction.Commit()
	if err != nil {
		return uuid.Nil, err
	}
	s.instrumentCache.Invalidate()
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

		if instruments[i].ConnectionMode == FTP {
			ftpConfig, err := s.instrumentRepository.GetFtpConfigByInstrumentId(ctx, instruments[i].ID)
			if err == nil {
				instruments[i].FTPConfig = &ftpConfig
			} else if err != nil && err != ErrFtpConfigNotFound {
				return instruments, err
			}
		}

		instrumentIDs[i] = instruments[i].ID
		instrumentsByIDs[instruments[i].ID] = &instruments[i]
	}
	analyteMappingsByInstrumentID, err := s.instrumentRepository.GetAnalyteMappings(ctx, instrumentIDs)
	if err != nil {
		return nil, err
	}
	analyteMappingIDs := make([]uuid.UUID, 0)
	analyteMappingIDInstrumentIDMap := make(map[uuid.UUID]uuid.UUID)

	for instrumentID, analyteMappings := range analyteMappingsByInstrumentID {
		instrumentsByIDs[instrumentID].AnalyteMappings = analyteMappings
		for i := range instrumentsByIDs[instrumentID].AnalyteMappings {
			analyteMappingIDs = append(analyteMappingIDs, analyteMappings[i].ID)
			analyteMappingIDInstrumentIDMap[analyteMappings[i].ID] = instrumentID
			analyteMappingsByIDs[analyteMappings[i].ID] = &instrumentsByIDs[instrumentID].AnalyteMappings[i]
		}
	}

	validatedAnalyteIDsMapByAnalyteMappingIDs, err := s.instrumentRepository.GetValidatedAnalyteIDsByAnalyteMappingID(ctx, analyteMappingIDs)
	if err != nil {
		return nil, err
	}
	for analyteMappingID, validatedAnalyteIDs := range validatedAnalyteIDsMapByAnalyteMappingIDs {
		analyteMappingsByIDs[analyteMappingID].ValidatedAnalyteIDs = validatedAnalyteIDs
	}

	channelMappingsByAnalyteMappingID, err := s.instrumentRepository.GetChannelMappings(ctx, analyteMappingIDs)
	if err != nil {
		return nil, err
	}
	for analyteMappingID, channelMappings := range channelMappingsByAnalyteMappingID {
		analyteMappingsByIDs[analyteMappingID].ChannelMappings = channelMappings
	}

	resultMappingsByAnalyteMappingID, err := s.instrumentRepository.GetResultMappings(ctx, analyteMappingIDs)
	if err != nil {
		return nil, err
	}
	for analyteMappingID, resultMappings := range resultMappingsByAnalyteMappingID {
		analyteMappingsByIDs[analyteMappingID].ResultMappings = resultMappings
	}

	expectedControlResultMappingsByAnalyteMappingID, err := s.instrumentRepository.GetExpectedControlResultsByAnalyteMappingIds(ctx, analyteMappingIDs)
	if err != nil {
		return nil, err
	}
	for analyteMappingID, expectedControlResults := range expectedControlResultMappingsByAnalyteMappingID {
		analyteMappingsByIDs[analyteMappingID].ExpectedControlResults = expectedControlResults
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

	sortingRulesMap, err := s.sortingRuleService.GetByInstrumentIDs(ctx, instrumentIDs)
	if err != nil {
		return nil, err
	}
	for instrumentID, sortingRules := range sortingRulesMap {
		if _, ok := instrumentsByIDs[instrumentID]; !ok {
			continue
		}
		instrumentsByIDs[instrumentID].SortingRules = sortingRules
	}

	s.instrumentCache.Set(instruments)

	return instruments, nil
}

func (s *instrumentService) GetInstrumentByID(ctx context.Context, tx db.DbConnection, id uuid.UUID, bypassCache bool) (Instrument, error) {
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

	if instrument.ConnectionMode == FTP {
		ftpConf, err := s.instrumentRepository.WithTransaction(tx).GetFtpConfigByInstrumentId(ctx, instrument.ID)
		if err == nil {
			instrument.FTPConfig = &ftpConf
		} else if err != nil && err != ErrFtpConfigNotFound {
			return instrument, err
		}
	}

	protocol, err := s.instrumentRepository.WithTransaction(tx).GetProtocolByID(ctx, instrument.ProtocolID)
	if err != nil {
		return instrument, err
	}
	instrument.ProtocolName = protocol.Name

	instrumentIDs := []uuid.UUID{instrument.ID}
	analyteMappingsByInstrumentID, err := s.instrumentRepository.WithTransaction(tx).GetAnalyteMappings(ctx, instrumentIDs)
	if err != nil {
		return instrument, err
	}
	analyteMappingIDs := make([]uuid.UUID, 0)
	analyteMappingIDInstrumentIDMap := make(map[uuid.UUID]uuid.UUID)
	analyteMappingsByIDs := make(map[uuid.UUID]*AnalyteMapping)

	for instrumentID, analyteMappings := range analyteMappingsByInstrumentID {
		instrument.AnalyteMappings = analyteMappings
		for i := range instrument.AnalyteMappings {
			analyteMappingIDs = append(analyteMappingIDs, analyteMappings[i].ID)
			analyteMappingIDInstrumentIDMap[analyteMappings[i].ID] = instrumentID
			analyteMappingsByIDs[analyteMappings[i].ID] = &instrument.AnalyteMappings[i]
		}
	}

	validatedAnalyteIDsMapByAnalyteMappingIDs, err := s.instrumentRepository.WithTransaction(tx).GetValidatedAnalyteIDsByAnalyteMappingID(ctx, analyteMappingIDs)
	if err != nil {
		return instrument, err
	}
	for analyteMappingID, validatedAnalyteIDs := range validatedAnalyteIDsMapByAnalyteMappingIDs {
		analyteMappingsByIDs[analyteMappingID].ValidatedAnalyteIDs = validatedAnalyteIDs
	}

	channelMappingsMapByAnalyteMappingIDs, err := s.instrumentRepository.WithTransaction(tx).GetChannelMappings(ctx, analyteMappingIDs)
	if err != nil {
		return instrument, err
	}
	for analyteMappingID, channelMappings := range channelMappingsMapByAnalyteMappingIDs {
		analyteMappingsByIDs[analyteMappingID].ChannelMappings = channelMappings
	}

	resultMappingsByAnalyteMappingID, err := s.instrumentRepository.WithTransaction(tx).GetResultMappings(ctx, analyteMappingIDs)
	if err != nil {
		return instrument, err
	}
	for analyteMappingID, resultMappings := range resultMappingsByAnalyteMappingID {
		analyteMappingsByIDs[analyteMappingID].ResultMappings = resultMappings
	}

	expectedControlResultMappingsByAnalyteMappingID, err := s.instrumentRepository.WithTransaction(tx).GetExpectedControlResultsByAnalyteMappingIds(ctx, analyteMappingIDs)
	if err != nil {
		return instrument, err
	}
	for analyteMappingID, expectedControlResults := range expectedControlResultMappingsByAnalyteMappingID {
		analyteMappingsByIDs[analyteMappingID].ExpectedControlResults = expectedControlResults
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

	sortingRulesMap, err := s.sortingRuleService.WithTransaction(tx).GetByInstrumentIDs(ctx, instrumentIDs)
	if err != nil {
		return instrument, err
	}
	for _, sortingRules := range sortingRulesMap {
		instrument.SortingRules = sortingRules
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

	if instrument.ConnectionMode == FTP {
		ftpConf, err := s.instrumentRepository.GetFtpConfigByInstrumentId(ctx, instrument.ID)
		if err == nil {
			instrument.FTPConfig = &ftpConf
		} else if err != nil && err != ErrFtpConfigNotFound {
			return instrument, err
		}
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
	analyteMappingIDs := make([]uuid.UUID, 0)
	analyteMappingIDInstrumentIDMap := make(map[uuid.UUID]uuid.UUID)
	analyteMappingsByIDs := make(map[uuid.UUID]*AnalyteMapping)

	for instrumentID, analyteMappings := range analyteMappingsByInstrumentID {
		instrument.AnalyteMappings = analyteMappings
		for i := range instrument.AnalyteMappings {
			analyteMappingIDs = append(analyteMappingIDs, analyteMappings[i].ID)
			analyteMappingIDInstrumentIDMap[analyteMappings[i].ID] = instrumentID
			analyteMappingsByIDs[analyteMappings[i].ID] = &instrument.AnalyteMappings[i]
		}
	}

	validatedAnalyteIDsMapByAnalyteMappingIDs, err := s.instrumentRepository.GetValidatedAnalyteIDsByAnalyteMappingID(ctx, analyteMappingIDs)
	if err != nil {
		return instrument, err
	}
	for analyteMappingID, validatedAnalyteIDs := range validatedAnalyteIDsMapByAnalyteMappingIDs {
		analyteMappingsByIDs[analyteMappingID].ValidatedAnalyteIDs = validatedAnalyteIDs
	}

	channelMappingsByAnalyteMappingID, err := s.instrumentRepository.GetChannelMappings(ctx, analyteMappingIDs)
	if err != nil {
		return instrument, err
	}
	for analyteMappingID, channelMappings := range channelMappingsByAnalyteMappingID {
		analyteMappingsByIDs[analyteMappingID].ChannelMappings = channelMappings
	}

	resultMappingsByAnalyteMappingID, err := s.instrumentRepository.GetResultMappings(ctx, analyteMappingIDs)
	if err != nil {
		return instrument, err
	}
	for analyteMappingID, resultMappings := range resultMappingsByAnalyteMappingID {
		analyteMappingsByIDs[analyteMappingID].ResultMappings = resultMappings
	}

	expectedControlResultMappingsByAnalyteMappingID, err := s.instrumentRepository.GetExpectedControlResultsByAnalyteMappingIds(ctx, analyteMappingIDs)
	if err != nil {
		return instrument, err
	}
	for analyteMappingID, expectedControlResults := range expectedControlResultMappingsByAnalyteMappingID {
		analyteMappingsByIDs[analyteMappingID].ExpectedControlResults = expectedControlResults
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

	settingsMap, err := s.instrumentRepository.GetInstrumentsSettings(ctx, instrumentIDs)
	if err != nil {
		return instrument, err
	}
	if _, ok := settingsMap[instrument.ID]; ok {
		instrument.Settings = settingsMap[instrument.ID]
	}

	sortingRulesMap, err := s.sortingRuleService.GetByInstrumentIDs(ctx, instrumentIDs)
	if err != nil {
		return instrument, err
	}
	for _, sortingRules := range sortingRulesMap {
		instrument.SortingRules = sortingRules
	}

	return instrument, nil
}

func (s *instrumentService) UpdateInstrument(ctx context.Context, instrument Instrument, userId uuid.UUID) error {
	tx, err := s.instrumentRepository.CreateTransaction()
	if err != nil {
		return db.ErrBeginTransactionFailed
	}

	oldInstrument, err := s.GetInstrumentByID(ctx, tx, instrument.ID, false)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	if instrument.ConnectionMode == FTP && instrument.FTPConfig != nil {
		err = s.instrumentRepository.WithTransaction(tx).DeleteFtpConfig(ctx, instrument.ID)
		if err != nil {
			_ = tx.Rollback()
			return err
		}

		err = s.instrumentRepository.WithTransaction(tx).CreateFtpConfig(ctx, *instrument.FTPConfig)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	if oldInstrument.ConnectionMode == FTP && instrument.ConnectionMode != FTP {
		err = s.instrumentRepository.WithTransaction(tx).DeleteFtpConfig(ctx, instrument.ID)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
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
	deletedValidatedAnalyteIDs := make(map[uuid.UUID][]uuid.UUID)
	createdValidatedAnalyteIDs := make(map[uuid.UUID][]uuid.UUID)

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

				// Extract removed validation links
				for _, oldValidatedAnalyteID := range oldAnalyteMapping.ValidatedAnalyteIDs {
					validatedAnalyteIDFound := false
					for _, newValidatedAnalyteID := range newAnalyteMapping.ValidatedAnalyteIDs {
						if oldValidatedAnalyteID == newValidatedAnalyteID {
							validatedAnalyteIDFound = true
							break
						}
					}
					if !validatedAnalyteIDFound {
						deletedValidatedAnalyteIDs[oldAnalyteMapping.ID] = append(deletedValidatedAnalyteIDs[oldAnalyteMapping.ID], oldValidatedAnalyteID)
					}
				}
				// Extract added validation links
				for _, newValidatedAnalyteID := range newAnalyteMapping.ValidatedAnalyteIDs {
					validatedAnalyteIDFound := false
					for _, oldValidatedAnalyteID := range oldAnalyteMapping.ValidatedAnalyteIDs {
						if newValidatedAnalyteID == oldValidatedAnalyteID {
							validatedAnalyteIDFound = true
							break
						}
					}
					if !validatedAnalyteIDFound {
						createdValidatedAnalyteIDs[oldAnalyteMapping.ID] = append(createdValidatedAnalyteIDs[newAnalyteMapping.ID], newValidatedAnalyteID)
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

	// Delete removed validated analyte links
	for analyteMappingID, validatedAnalyteIDs := range deletedValidatedAnalyteIDs {
		if len(validatedAnalyteIDs) == 0 {
			continue
		}
		err = s.instrumentRepository.WithTransaction(tx).DeleteValidatedAnalyteIDsByAnalyteMappingID(ctx, analyteMappingID, validatedAnalyteIDs)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	err = s.instrumentRepository.WithTransaction(tx).DeleteAnalyteMappings(ctx, deletedAnalyteMappingIDs)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	err = s.instrumentRepository.WithTransaction(tx).DeleteExpectedControlResultsByAnalyteMappingIDs(ctx, deletedAnalyteMappingIDs, userId)
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

	_, err = s.instrumentRepository.WithTransaction(tx).UpsertAnalyteMappings(ctx, instrument.AnalyteMappings, instrument.ID)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	for _, analyteMapping := range instrument.AnalyteMappings {
		_, err = s.instrumentRepository.WithTransaction(tx).UpsertChannelMappings(ctx, analyteMapping.ChannelMappings, analyteMapping.ID)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		_, err = s.instrumentRepository.WithTransaction(tx).UpsertResultMappings(ctx, analyteMapping.ResultMappings, analyteMapping.ID)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	// Create added validated analyte links
	for analyteMappingID, validatedAnalyteIDs := range createdValidatedAnalyteIDs {
		if len(validatedAnalyteIDs) == 0 {
			continue
		}
		err = s.instrumentRepository.WithTransaction(tx).CreateValidatedAnalyteIDs(ctx, analyteMappingID, validatedAnalyteIDs)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	newRequestMappingAnalytes := make(map[uuid.UUID]any)
	for _, requestMapping := range instrument.RequestMappings {
		for _, analyteID := range requestMapping.AnalyteIDs {
			newRequestMappingAnalytes[analyteID] = requestMapping.ID
		}
	}
	deletedRequestMappingAnalyteIDsByRequestMappingsIDs := make(map[uuid.UUID][]uuid.UUID)
	for _, requestMapping := range oldInstrument.RequestMappings {
		for _, analyteID := range requestMapping.AnalyteIDs {
			if _, ok := newRequestMappingAnalytes[analyteID]; ok {
				continue
			}
			deletedRequestMappingAnalyteIDsByRequestMappingsIDs[requestMapping.ID] = append(deletedRequestMappingAnalyteIDsByRequestMappingsIDs[requestMapping.ID], analyteID)
		}
	}
	for requestMappingID, deletedRequestMappingAnalyteIDs := range deletedRequestMappingAnalyteIDsByRequestMappingsIDs {
		err = s.instrumentRepository.WithTransaction(tx).DeleteRequestMappingAnalytes(ctx, requestMappingID, deletedRequestMappingAnalyteIDs)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	err = s.instrumentRepository.WithTransaction(tx).UpsertRequestMappings(ctx, instrument.RequestMappings, instrument.ID)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	requestMappingsAnalytes := make(map[uuid.UUID][]uuid.UUID)
	for i := range instrument.RequestMappings {
		requestMappingsAnalytes[instrument.RequestMappings[i].ID] = instrument.RequestMappings[i].AnalyteIDs
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
	deletedSortingRules := make([]SortingRule, 0)
	for i := range oldInstrument.SortingRules {
		isRuleFound := false
		for j := range instrument.SortingRules {
			if instrument.SortingRules[j].ID == oldInstrument.SortingRules[i].ID {
				isRuleFound = true
				if instrument.SortingRules[j].Priority != j {
					instrument.SortingRules[j].Priority = j
				}
				break
			}
		}
		if isRuleFound {
			continue
		}

		deletedSortingRules = append(deletedSortingRules, oldInstrument.SortingRules[i])
	}
	err = s.sortingRuleService.WithTransaction(tx).DeleteSortingRulesWithTx(ctx, deletedSortingRules)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	for i := range instrument.SortingRules {
		instrument.SortingRules[i].InstrumentID = instrument.ID
		err = s.sortingRuleService.WithTransaction(tx).UpsertWithTx(ctx, &instrument.SortingRules[i])
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	//r.SetTx(r.CreateTransaction())
	newInstrument, err := s.GetInstrumentByID(ctx, tx, instrument.ID, true)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	//r.ClearTx()

	instrumentHash := HashInstrument(newInstrument)
	err = s.cerberusClient.VerifyInstrumentHash(instrumentHash)
	if err != nil {
		log.Error().Err(err).Msg("Failed to verify instrument hash")
		_ = tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		_ = tx.Rollback()
		return db.ErrCommitTransactionFailed
	}

	s.instrumentCache.Invalidate()

	return nil
}

func (s *instrumentService) DeleteInstrument(ctx context.Context, id uuid.UUID) error {
	tx, err := s.instrumentRepository.CreateTransaction()
	err = s.instrumentRepository.WithTransaction(tx).DeleteFtpConfig(ctx, id)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	err = s.instrumentRepository.WithTransaction(tx).DeleteInstrument(ctx, id)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	err = s.instrumentRepository.WithTransaction(tx).DeleteAllValidatedAnalyteIDsByInstrumentID(ctx, id)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	deletionHash := HashDeletedInstrument(id)
	err = s.cerberusClient.VerifyInstrumentHash(deletionHash)
	if err != nil {
		log.Error().Err(err).Msg("Failed to verify instrument hash")
		_ = tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		_ = tx.Rollback()
		return db.ErrCommitTransactionFailed
	}

	s.instrumentCache.Invalidate()
	return nil
}

func (s *instrumentService) GetExpectedControlResultsByInstrumentId(ctx context.Context, instrumentId uuid.UUID) ([]ExpectedControlResult, error) {
	expectedControlResults, err := s.instrumentRepository.GetExpectedControlResultsByInstrumentId(ctx, instrumentId)
	if err != nil {
		return nil, err
	}

	return expectedControlResults, nil
}

func (s *instrumentService) GetNotSpecifiedExpectedControlResultsByInstrumentId(ctx context.Context, instrumentId uuid.UUID) ([]NotSpecifiedExpectedControlResult, error) {
	notSpecifiedExpectedControlResults, err := s.instrumentRepository.GetNotSpecifiedExpectedControlResultsByInstrumentId(ctx, instrumentId)
	if err != nil {
		return nil, err
	}

	return notSpecifiedExpectedControlResults, nil
}

func (s *instrumentService) CreateExpectedControlResults(ctx context.Context, expectedControlResults []ExpectedControlResult, userId uuid.UUID) error {
	tx, err := s.instrumentRepository.CreateTransaction()

	analyteMappingIds := make([]uuid.UUID, 0)

	for i := range expectedControlResults {
		expectedControlResults[i].CreatedBy = userId
		analyteMappingIds = append(analyteMappingIds, expectedControlResults[i].AnalyteMappingId)
	}

	_, err = s.instrumentRepository.WithTransaction(tx).CreateExpectedControlResults(ctx, expectedControlResults)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	expectedControlResultsHash := HashExpectedControlResults(expectedControlResults)
	err = s.cerberusClient.VerifyExpectedControlResultsHash(expectedControlResultsHash)
	if err != nil {
		log.Error().Err(err).Msg("Failed to verify instrument hash")
		_ = tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		_ = tx.Rollback()
		return db.ErrCommitTransactionFailed
	}

	s.manager.SendAnalyteMappingsToValidateControlResults(analyteMappingIds)

	return nil
}

func (s *instrumentService) UpdateExpectedControlResults(ctx context.Context, instrumentId uuid.UUID, expectedControlResults []ExpectedControlResult, userId uuid.UUID) error {
	tx, err := s.instrumentRepository.CreateTransaction()

	createExpectedControlResults := make([]ExpectedControlResult, 0)
	updateExpectedControlResults := make([]ExpectedControlResult, 0)
	expectedControlResultsForValidation := make([]ExpectedControlResult, 0)
	expectedControlResultIdsToDelete := make([]uuid.UUID, 0)

	expectedControlResultsMapBySampleCode := make(map[string]bool)
	updatedSampleCodes := make([]string, 0)
	analyteMappingIds := make([]uuid.UUID, 0)

	for _, expectedControlResult := range expectedControlResults {
		if _, ok := expectedControlResultsMapBySampleCode[expectedControlResult.SampleCode]; !ok {
			expectedControlResultsMapBySampleCode[expectedControlResult.SampleCode] = true
			updatedSampleCodes = append(updatedSampleCodes, expectedControlResult.SampleCode)
		}
		expectedControlResultsForValidation = append(expectedControlResultsForValidation, expectedControlResult)
	}

	existingExpectedControlResultMapByID, err := s.instrumentRepository.GetExpectedControlResultsByInstrumentIdAndSampleCodes(ctx, instrumentId, updatedSampleCodes)
	if err != nil {
		return err
	}

	for _, existingExpectedControlResult := range existingExpectedControlResultMapByID {
		for _, expectedControlResult := range expectedControlResults {
			if existingExpectedControlResult.ID == expectedControlResult.ID {
				updateExpectedControlResults = append(updateExpectedControlResults, expectedControlResult)
				analyteMappingIds = append(analyteMappingIds, expectedControlResult.AnalyteMappingId)
			}
		}
	}

	for _, expectedControlResult := range expectedControlResults {
		alreadyExists := false
		for id, existingExpectedControlResult := range existingExpectedControlResultMapByID {
			if id == expectedControlResult.ID || (existingExpectedControlResult.SampleCode == expectedControlResult.SampleCode && existingExpectedControlResult.AnalyteMappingId == expectedControlResult.AnalyteMappingId) {
				alreadyExists = true
				delete(existingExpectedControlResultMapByID, existingExpectedControlResult.ID)
				break
			}
		}

		if !alreadyExists {
			expectedControlResult.CreatedBy = userId
			createExpectedControlResults = append(createExpectedControlResults, expectedControlResult)
			analyteMappingIds = append(analyteMappingIds, expectedControlResult.AnalyteMappingId)
		}
	}

	for _, deleteExpectedControlResult := range existingExpectedControlResultMapByID {
		expectedControlResultIdsToDelete = append(expectedControlResultIdsToDelete, deleteExpectedControlResult.ID)
	}

	_, err = s.instrumentRepository.WithTransaction(tx).CreateExpectedControlResults(ctx, createExpectedControlResults)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	err = s.instrumentRepository.WithTransaction(tx).UpdateExpectedControlResults(ctx, updateExpectedControlResults)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	err = s.instrumentRepository.WithTransaction(tx).DeleteExpectedControlResults(ctx, expectedControlResultIdsToDelete, userId)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	expectedControlResultsHash := HashExpectedControlResults(expectedControlResults)
	err = s.cerberusClient.VerifyExpectedControlResultsHash(expectedControlResultsHash)
	if err != nil {
		log.Error().Err(err).Msg("Failed to verify instrument hash")
		_ = tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		_ = tx.Rollback()
		return db.ErrCommitTransactionFailed
	}

	s.manager.SendAnalyteMappingsToValidateControlResults(analyteMappingIds)

	return nil
}

func (s *instrumentService) DeleteExpectedControlResult(ctx context.Context, expectedControlResultId uuid.UUID, userId uuid.UUID) error {
	tx, err := s.instrumentRepository.CreateTransaction()

	err = s.instrumentRepository.WithTransaction(tx).DeleteExpectedControlResults(ctx, []uuid.UUID{expectedControlResultId}, userId)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	deletionHash := HashDeletedExpectedControlResult(expectedControlResultId, userId)
	err = s.cerberusClient.VerifyExpectedControlResultsHash(deletionHash)
	if err != nil {
		log.Error().Err(err).Msg("Failed to verify deleted expected control result hash")
		_ = tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		_ = tx.Rollback()
		return db.ErrCommitTransactionFailed
	}

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

func (s *instrumentService) GetManufacturerTests(ctx context.Context) ([]SupportedManufacturerTests, error) {
	tests, err := s.instrumentRepository.GetManufacturerTests(ctx)
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

func (s *instrumentService) UpsertManufacturerTests(ctx context.Context, manufacturerTests []SupportedManufacturerTests) error {
	tx, err := s.instrumentRepository.CreateTransaction()
	if err != nil {
		return err
	}

	err = s.instrumentRepository.WithTransaction(tx).UpsertManufacturerTests(ctx, manufacturerTests)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Error().Err(err).Msg(msgUpsertManufacturerTestsFailed)
		_ = tx.Rollback()
		return ErrUpsertManufacturerTestsFailed
	}
	return nil
}

func (s *instrumentService) UpdateInstrumentStatus(ctx context.Context, id uuid.UUID, status InstrumentStatus) error {
	err := s.instrumentRepository.UpdateInstrumentStatus(ctx, id, status)
	if err != nil {
		return err
	}

	s.instrumentCache.Invalidate()

	return nil
}

func (s *instrumentService) CheckAnalytesUsage(ctx context.Context, analyteIDs []uuid.UUID) (map[uuid.UUID][]Instrument, error) {
	return s.instrumentRepository.CheckAnalytesUsage(ctx, analyteIDs)
}
