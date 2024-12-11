package workflow

import (
	"context"
	"encoding/json"
	"sqs-fargate-consumer-v2/internal/dependencies"
	"sqs-fargate-consumer-v2/internal/workflow/builders"

	actionmodel "github.com/gdcorp-domains/fulfillment-ags3-workflow/models"
	actionstatus "github.com/gdcorp-domains/fulfillment-ags3-workflow/models/status"

	"github.com/gdcorp-domains/fulfillment-go-grule-engine/repository"

	"github.com/gdcorp-domains/fulfillment-rules/ruleset/businesscontext"
	workflowHelper "github.com/gdcorp-domains/fulfillment-worker-helper"
	bufferMessageModels "sqs-fargate-consumer-v2/internal/models"
	workflowModels "sqs-fargate-consumer-v2/internal/workflow/models"
)

type RegistrarDomainsWorker struct {
	Deps dependencies.WorkflowDependencies
}

func NewRegistrarDomainsWorker(deps dependencies.WorkflowDependencies) *RegistrarDomainsWorker {
	return &RegistrarDomainsWorker{
		Deps: deps,
	}
}

func (w *RegistrarDomainsWorker) HandleWorkflowEvent(ctx context.Context, message *bufferMessageModels.Message) error {
	w.Deps.GetLogger().Infof("Processing message %s for event source %s = %s", message.MessageID, *message.ReceiptHandle, message.Body)
	eventMessage := actionmodel.EventMessage{}
	if err := json.Unmarshal(message.Body, &eventMessage); err != nil {
		return err
	}

	action, code, err := w.Deps.GetActionAPIClient().GetAction(ctx, eventMessage.ActionID, eventMessage.CustomerID, "input", "customerId")
	if err != nil {
		w.Deps.GetLogger().Errorf("Failed to get action record with actionID %s, customerID %s, http code: %d, err: %s", eventMessage.ActionID, eventMessage.CustomerID, code, err)
		return err
	}

	if action.Status == actionstatus.Success {
		w.Deps.GetLogger().Errorf("Action record with actionID %s, customerID %s is in SUCCESS status, skipping", eventMessage.ActionID, eventMessage.CustomerID)
		return nil
	}

	domainEventInput := workflowModels.DomainEventInput{}
	action.RequestID = eventMessage.RequestID
	// Unmarshal the input from action
	if action.Input != nil {
		b, _ := json.Marshal(action.Input)
		err := json.Unmarshal(b, &domainEventInput)
		if err != nil {
			w.Deps.GetLogger().Errorf("failed to unmarshal action input due to error: %s", err.Error())
			return err
		}
	}
	action.Input = &domainEventInput

	helper := workflowHelper.NewWorkflowHelper(action, w.Deps.GetBaseDependencies(), w.Deps.GetLogger())

	factory := builders.NewBuilderRetriever()

	loggerContextDecorator := builders.NewLoggerContextDecorator()

	engineParams, err := helper.InitEngine(ctx, w.Deps, loggerContextDecorator, factory)
	if err != nil {
		return err
	}

	bc, err := InitializeBusinessContextByType(engineParams.RulesetConfig, engineParams.DataCtx)
	if err != nil {
		w.Deps.GetLogger().Errorf("failed to initialize business context: %s", err)
		return err
	}

	err = w.Deps.AddBusinessContextToKnowledgeBase(engineParams.RulesetConfig.Name, engineParams.RulesetConfig.Version, bc)
	if err != nil {
		w.Deps.GetLogger().Errorf("failed to add business context to knowledge base: %s", err)
		return err
	}

	err = engineParams.Engine.Execute(ctx, engineParams.DataCtx)
	if err != nil {
		return err
	}

	domainEventInput.LastFunctionGraph = engineParams.DataCtx.Get("FunctionGraph").(*string)

	err = helper.UpdateAction(ctx, domainEventInput, actionstatus.Success)
	if err != nil {
		return err
	}

	return nil
}

// InitializeBusinessContextByType initializes businessContext by dataContext
func InitializeBusinessContextByType(ruleset *workflowHelper.RulesetConfig, dc repository.IDataContext) (businesscontext.BusinessContext, error) {
	bcRetriever := businesscontext.NewBusinessContextRetriever()
	return bcRetriever.InitializeBusinessContextBasedOnOperation(dc, ruleset.OperationType)
}
