package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"sqs-fargate-consumer-v2/internal/dependencies"
	"sqs-fargate-consumer-v2/internal/workflow/builders"

	actionmodel "github.com/gdcorp-domains/fulfillment-ags3-workflow/models"
	actionstatus "github.com/gdcorp-domains/fulfillment-ags3-workflow/models/status"

	"github.com/gdcorp-domains/fulfillment-go-grule-engine/repository"

	bufferMessageModels "sqs-fargate-consumer-v2/internal/models"
	workflowModels "sqs-fargate-consumer-v2/internal/workflow/models"

	"github.com/gdcorp-domains/fulfillment-go-grule-engine/grule"
	"github.com/gdcorp-domains/fulfillment-rules/ruleset/businesscontext"
	workflowHelper "github.com/gdcorp-domains/fulfillment-worker-helper"
	"github.com/google/uuid"
)

type EventWrapper struct {
	Detail struct {
		CustomerID uuid.UUID `json:"customerId"`
		ActionID   uuid.UUID `json:"actionId"`
		RequestID  string    `json:"requestId"`
	} `json:"detail"`
}

type RegistrarDomainsWorker struct {
	Deps dependencies.WorkflowDependencies
}

func NewRegistrarDomainsWorker(deps dependencies.WorkflowDependencies) *RegistrarDomainsWorker {
	return &RegistrarDomainsWorker{
		Deps: deps,
	}
}

func (w *RegistrarDomainsWorker) HandleWorkflowEvent(ctx context.Context, message *bufferMessageModels.Message) error {
	logger := w.Deps.GetLogger().WithFields(map[string]interface{}{
		"MessageID":         message.MessageID,
		"QueueName":         message.QueueName,
		"ProcessorGroupID":  message.ProcessorGroupID,
		"ProcessorWorkerID": message.ProcessorWorkerID,
		"ReceiptHandle":     *message.ReceiptHandle,
	})

	logger.Infof("Processing message %s for event source %s = %s", message.MessageID, *message.ReceiptHandle, message.Body)

	var wrapper EventWrapper
	if err := json.Unmarshal([]byte(message.Body), &wrapper); err != nil {
		return err
	}

	eventMessage := actionmodel.EventMessage{
		CustomerID: wrapper.Detail.CustomerID,
		ActionID:   wrapper.Detail.ActionID,
		RequestID:  wrapper.Detail.RequestID,
	}

	action, code, err := w.Deps.GetActionAPIClient().GetAction(ctx, eventMessage.ActionID, eventMessage.CustomerID, "input", "customerId")
	if err != nil {
		logger.Errorf("Failed to get action record with actionID %s, customerID %s, http code: %d, err: %s", eventMessage.ActionID, eventMessage.CustomerID, code, err)
		return err
	}

	if action.Status == actionstatus.Success {
		logger.Errorf("Action record with actionID %s, customerID %s is in SUCCESS status, skipping", eventMessage.ActionID, eventMessage.CustomerID)
		return nil
	}

	domainEventInput := workflowModels.DomainEventInput{}
	action.RequestID = eventMessage.RequestID
	// Unmarshal the input from action
	if action.Input != nil {
		b, _ := json.Marshal(action.Input)
		err := json.Unmarshal(b, &domainEventInput)
		if err != nil {
			logger.Errorf("failed to unmarshal action input due to error: %s", err.Error())
			return err
		}
	}
	action.Input = &domainEventInput

	helper := workflowHelper.NewWorkflowHelper(action, w.Deps.GetBaseDependencies(), logger)

	factory := builders.NewBuilderRetriever()

	loggerContextDecorator := builders.NewLoggerContextDecorator()

	engineParams, err := helper.InitEngine(ctx, w.Deps, loggerContextDecorator, factory)
	if err != nil {
		return err
	}

	// Get original knowledge base from the same source InitEngine used
	originalKB, found := w.Deps.GetKnowledgeBase(engineParams.RulesetConfig.Name, engineParams.RulesetConfig.Version)
	if !found {
		return fmt.Errorf("knowledge base not found")
	}

	// Create fresh knowledge base
	freshKB := &grule.KnowledgeBase{
		Name:          originalKB.Name,
		Version:       originalKB.Version,
		RuleNode:      originalKB.RuleNode,
		PanicRecovery: originalKB.PanicRecovery,
	}

	bc, err := InitializeBusinessContextByType(engineParams.RulesetConfig, engineParams.Engine.GetDataContext())
	if err != nil {
		logger.Errorf("failed to initialize business context: %s", err)
		return err
	}

	logger = logger.WithFields(map[string]interface{}{
		"DataContextAddr":     fmt.Sprintf("%p", engineParams.Engine.GetDataContext()),
		"BusinessContextAddr": fmt.Sprintf("%p", bc),
	})

	// Init new engine
	engineParams.Engine = grule.NewEngine(freshKB)
	engineParams.Engine.SetBusinessContext(bc)

	logger.Infof("Before Execute - DataCtx: %p, BusinessCtx %p", engineParams.Engine.GetDataContext(), bc)
	err = engineParams.Engine.Execute(ctx)
	if err != nil {
		return err
	}

	domainEventInput.LastFunctionGraph = engineParams.Engine.GetDataContext().Get("FunctionGraph").(*string)

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
