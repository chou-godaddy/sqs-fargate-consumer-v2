package builders

import (
	"context"
	"fmt"
	"os"

	actionsModels "github.com/gdcorp-domains/fulfillment-ags3-workflow/models"
	httpclient "github.com/gdcorp-domains/fulfillment-golang-httpclient"
	logging "github.com/gdcorp-domains/fulfillment-golang-logging"
	workerHelper "github.com/gdcorp-domains/fulfillment-worker-helper"

	workflowModels "sqs-fargate-consumer-v2/internal/workflow/models"
)

type builderRetriever struct {
}

// NewBuilderRetriever ...
func NewBuilderRetriever() workerHelper.BuilderRetriever {
	return &builderRetriever{}
}

// Retrieve method
func (retriever *builderRetriever) Retrieve(actionType string) (workerHelper.Builders, error) {
	switch actionType {
	case workflowModels.LockDomainActionType:
		return newLockDomainActionCreator(), nil
	case workflowModels.UnlockDomainActionType:
		return newUnlockDomainActionCreator(), nil
	}
	return nil, fmt.Errorf("action type %s has not been implemented", actionType)
}

type loggerAndContextDecorator struct {
}

// NewLoggerContextDecorator ...
func NewLoggerContextDecorator() workerHelper.LoggerContextDecorator {
	return &loggerAndContextDecorator{}
}

// DecorateLoggerAndContext decorates the logger and context to contain the extra fields
func (builder *loggerAndContextDecorator) DecorateLoggerAndContext(ctx context.Context, rulesetConfig *workerHelper.RulesetConfig, action *actionsModels.Action, logger logging.Logger) (context.Context, logging.Logger) {
	ctx = context.WithValue(ctx, httpclient.RequestIDKey, action.RequestID)
	ctx = context.WithValue(ctx, httpclient.EnvIDKey, os.Getenv("ENV"))
	fields := map[string]interface{}{}
	if action != nil {
		fields["X-Request-Id"] = action.RequestID
		fields["ActionID"] = action.ActionID
	}
	fields["RulesetName"] = rulesetConfig.Name
	fields["RulesetVersion"] = rulesetConfig.Version

	if action != nil && action.Input != nil {
		domainEventInput := action.Input.(*workflowModels.DomainEventInput)
		if domainEventInput != nil {
			fields["RegistrarBackend"] = string(domainEventInput.RegistrarBackend)
		}
		if domainEventInput.AgentMessage != nil {
			fields["TLD"] = domainEventInput.AgentMessage.Tld
			fields["Registrar"] = domainEventInput.AgentMessage.Registrar

			if domainEventInput.AgentMessage.AgentEvent != nil {
				ctx = context.WithValue(ctx, httpclient.EventIDKey, domainEventInput.AgentMessage.AgentEvent.ID)

				fields["InternalRegistrarID"] = domainEventInput.AgentMessage.AgentEvent.InternalRegistrarID
				fields["RegistryAPI"] = domainEventInput.AgentMessage.AgentEvent.RegistryAPI
				fields["ResourceID"] = domainEventInput.AgentMessage.AgentEvent.ResourceID
				fields["ResourceType"] = string(domainEventInput.AgentMessage.AgentEvent.ResourceType)
				fields["CurrentStatus"] = domainEventInput.AgentMessage.AgentEvent.CurrentStatus
				fields["PreviousStatus"] = domainEventInput.AgentMessage.AgentEvent.PreviousStatus
				fields["RetryCount"] = domainEventInput.AgentMessage.AgentEvent.RetryCount
			}
		}
		// TODO: Add more fields for Domainbox or add them to workflow_sync_run
	}
	return ctx, logger.WithFields(fields)
}
