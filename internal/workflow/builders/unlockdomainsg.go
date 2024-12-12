package builders

import (
	actionsModels "github.com/gdcorp-domains/fulfillment-ags3-workflow/models"
	"github.com/gdcorp-domains/fulfillment-goapimodels/rules/agentservices"
	workflowModels "sqs-fargate-consumer-v2/internal/workflow/models"
)

// unlockDomainSGActionCreator creates UnlockDomainSG actions
type unlockDomainSGActionCreator struct {
	*unlockDomainActionCreator
}

func newUnlockDomainSGActionCreator() *unlockDomainSGActionCreator {
	return &unlockDomainSGActionCreator{
		unlockDomainActionCreator: newUnlockDomainActionCreator(),
	}
}

func (cr *unlockDomainSGActionCreator) getUnlockDomainActionCreator() *unlockDomainActionCreator {
	if cr.unlockDomainActionCreator == nil {
		return newUnlockDomainActionCreator()
	}
	return cr.unlockDomainActionCreator
}

// BuildActionCreate creates a new ActionCreate
func (cr *unlockDomainSGActionCreator) BuildActionCreate(agsEvent agentservices.AgentEvent, registrar, tld string, sourceAPI string, customerID string) actionsModels.ActionCreate {
	action := cr.getUnlockDomainActionCreator().BuildActionCreate(agsEvent, registrar, tld, sourceAPI, customerID)
	action.Type = workflowModels.UnlockDomainSGActionType
	return action
}
