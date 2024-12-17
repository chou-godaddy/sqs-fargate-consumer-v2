package builders

import (
	"errors"
	"fmt"
	logging "github.com/gdcorp-domains/fulfillment-golang-logging"
	"github.com/gdcorp-domains/fulfillment-registrar-domains/models"
	"strconv"

	actionsModels "github.com/gdcorp-domains/fulfillment-ags3-workflow/models"
	"github.com/gdcorp-domains/fulfillment-go-grule-engine/repository"
	"github.com/gdcorp-domains/fulfillment-goapimodels/rules/agentservices"
	"github.com/gdcorp-domains/fulfillment-rules/rule"
	"github.com/gdcorp-domains/fulfillment-rules/rule/state"
	workflowModels "sqs-fargate-consumer-v2/internal/workflow/models"
)

// unlockDomainActionCreator creates UnlockDomain actions
type unlockDomainActionCreator struct {
}

func newUnlockDomainActionCreator() *unlockDomainActionCreator {
	return &unlockDomainActionCreator{}
}

// BuildActionCreate creates a new ActionCreate
func (cr *unlockDomainActionCreator) BuildActionCreate(agsEvent agentservices.AgentEvent, registrar, tld string, sourceAPI string, customerID string) actionsModels.ActionCreate {
	workflowInput := workflowModels.DomainEventInput{
		CustomerID: &customerID,
		AgentMessage: &agentservices.AgentMessage{
			AgentEvent: &agentservices.AgentEvent{
				ID:                  agsEvent.ID,
				ResourceID:          agsEvent.ResourceID,
				ResourceType:        agsEvent.ResourceType,
				CurrentStatus:       agsEvent.CurrentStatus,
				PreviousStatus:      agsEvent.PreviousStatus,
				RegistryAPI:         agsEvent.RegistryAPI,
				InternalRegistrarID: agsEvent.InternalRegistrarID,
				RetryCount:          agsEvent.RetryCount,
			},
			Registrar: registrar,
			Tld:       tld,
		},
		RegistrarBackend: models.AGS2,
		UpdateLocksActionInput: &workflowModels.UpdateLocksActionInput{
			DomainsToUpdate: []string{agsEvent.ResourceData},
			Lock:            false,
			Registrar:       registrar,
		},
	}
	action := actionsModels.ActionCreate{
		Type:       workflowModels.UnlockDomainActionType,
		Version:    workflowModels.CurrentVersion,
		SourceAPI:  sourceAPI,
		Input:      workflowInput,
		ResourceID: strconv.Itoa(agsEvent.ResourceID),
	}

	return action
}

// BuildDataContext creates a new data context
func (cr *unlockDomainActionCreator) BuildDataContext(rulesetName string, workerDependencies, domainInput interface{}, logger logging.Logger) (repository.IDataContext, error) {
	deps, ok := getDependencies(workerDependencies)
	if !ok {
		return nil, fmt.Errorf("Failed to retrieve dependencies")
	}

	domainEvent, ok := getDomainInput(domainInput)
	if !ok {
		return nil, fmt.Errorf("Failed to retrieve dependencies")
	}

	dataCtx, err := repository.NewDataContext(logger, domainEvent.AgentMessage.Registrar, rulesetName)
	if err != nil {
		return nil, fmt.Errorf("Failed to create new data context, err: %s", err)
	}

	metaStore := state.NewMetaStore()
	dataCtx.Add(rule.MetaStoreKey, metaStore)
	logger.Debugf("Add new metastore %p in dataCtx %p", metaStore, dataCtx)

	logger = logger.WithFields(map[string]interface{}{
		"MetastoreAddr": fmt.Sprintf("%p", metaStore),
		"DataCtxAddr":   fmt.Sprintf("%p", dataCtx),
	})

	dataCtx.Add(rule.LoggerKey, logger)

	if domainEvent.UpdateLocksActionInput != nil {
		var eppStatusListStr []string
		for _, eppStatus := range domainEvent.LocksToAddOrRemove {
			eppStatusListStr = append(eppStatusListStr, string(eppStatus))
		}
		dataCtx.Add(rule.EPPStatusRemoveKey, &eppStatusListStr)
	} else {
		return nil, errors.New("UpdateLocksActionInput is empty")
	}

	dataAccessor := NewDataAccessor(deps, metaStore, domainEvent.AgentMessage.Registrar, domainEvent.CustomerID, logger).GetDataAccessor(domainEvent.RegistrarBackend)

	dataCtx.Add(rule.RegistryContactsClientKey, deps.GetRegistryContactsClient())
	dataCtx.Add(rule.RegistryDomainsClientKey, deps.GetRegistryDomainsClient())
	dataCtx.Add(rule.AgentMessageKey, domainEvent.AgentMessage)
	dataCtx.Add(rule.SwitchboardAccessorKey, deps.GetSwitchboardAccessor())
	dataCtx.Add(rule.DBConnectionKey, deps.GetDBConnection())
	dataCtx.Add(rule.ShopperAPIClientKey, deps.GetShopperAPIClient())
	dataCtx.Add(rule.ActionsAPIClientKey, deps.GetActionAPIClient())
	dataCtx.Add(rule.RegistrarConfigCacheKey, deps.GetRegistrarConfigCache())
	dataCtx.Add(rule.RegistryConfigCacheKey, deps.GetRegistryConfigCache())
	dataCtx.Add(rule.DataAccessorKey, dataAccessor)
	dataCtx.Add(rule.MSMQClientKey, deps.GetMSMQClient())
	dataCtx.Add(rule.CustomerIDKey, domainEvent.CustomerID)
	dataCtx.Add("TreeName", rulesetName)
	return dataCtx, nil
}
