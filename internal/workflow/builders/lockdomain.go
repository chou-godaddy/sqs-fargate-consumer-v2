package builders

import (
	"errors"
	"fmt"
	"strconv"

	logging "github.com/gdcorp-domains/fulfillment-golang-logging"
	"github.com/gdcorp-domains/fulfillment-registrar-domains/models"
	fulfillment_worker_helper "github.com/gdcorp-domains/fulfillment-worker-helper"

	"sqs-fargate-consumer-v2/internal/dependencies"
	workflowModels "sqs-fargate-consumer-v2/internal/workflow/models"

	actionsModels "github.com/gdcorp-domains/fulfillment-ags3-workflow/models"
	"github.com/gdcorp-domains/fulfillment-go-grule-engine/repository"
	"github.com/gdcorp-domains/fulfillment-goapimodels/rules/agentservices"
	"github.com/gdcorp-domains/fulfillment-rules/rule"
	"github.com/gdcorp-domains/fulfillment-rules/rule/state"
)

// lockDomainActionCreator creates LockDomain actions
type lockDomainActionCreator struct {
}

func newLockDomainActionCreator() fulfillment_worker_helper.Builders {
	return &lockDomainActionCreator{}
}

// BuildActionCreate creates a new ActionCreate
func (cr *lockDomainActionCreator) BuildActionCreate(agsEvent agentservices.AgentEvent, registrar, tld string, sourceAPI string, customerID string) actionsModels.ActionCreate {
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
			Lock:            true,
			Registrar:       registrar,
		},
	}
	action := actionsModels.ActionCreate{
		Type:       workflowModels.LockDomainActionType,
		Version:    workflowModels.CurrentVersion,
		SourceAPI:  sourceAPI,
		Input:      workflowInput,
		ResourceID: strconv.Itoa(agsEvent.ResourceID),
	}

	return action
}

// BuildDataContext creates a new data context
func (cr *lockDomainActionCreator) BuildDataContext(rulesetName string, workerDependencies, domainInput interface{}, logger logging.Logger) (repository.IDataContext, error) {
	deps, ok := getDependencies(workerDependencies)
	if !ok {
		return nil, fmt.Errorf("Failed to retrieve dependencies")
	}

	domainEvent, ok := getDomainInput(domainInput)
	if !ok {
		return nil, fmt.Errorf("Failed to retrieve input")
	}

	dataCtx, err := repository.NewDataContext(logger, domainEvent.AgentMessage.Registrar, rulesetName)
	if err != nil {
		return nil, fmt.Errorf("Failed to create new data context, err: %s", err)
	}

	if domainEvent.UpdateLocksActionInput != nil {
		var eppStatusListStr []string
		for _, eppStatus := range domainEvent.LocksToAddOrRemove {
			eppStatusListStr = append(eppStatusListStr, string(eppStatus))
		}
		dataCtx.Add(rule.EPPStatusAddKey, &eppStatusListStr)
	} else {
		return nil, errors.New("UpdateLocksActionInput is empty")
	}

	metaStore := state.NewMetaStore()

	dataAccessor := NewDataAccessor(deps, metaStore, domainEvent.AgentMessage.Registrar, domainEvent.CustomerID, logger).GetDataAccessor(domainEvent.RegistrarBackend)

	dataCtx.Add(rule.MetaStoreKey, metaStore)
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

func getDependencies(d interface{}) (deps dependencies.WorkflowDependencies, ok bool) {
	deps, ok = d.(dependencies.WorkflowDependencies)
	return
}

func getDomainInput(d interface{}) (domainEventInput *workflowModels.DomainEventInput, ok bool) {
	domainEventInput, ok = d.(*workflowModels.DomainEventInput)
	return
}
