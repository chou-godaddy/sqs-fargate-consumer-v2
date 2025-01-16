package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"sqs-fargate-consumer-v2/internal/dependencies"
	"sqs-fargate-consumer-v2/internal/workflow/builders"
	"time"

	actionmodel "github.com/gdcorp-domains/fulfillment-ags3-workflow/models"
	actionstatus "github.com/gdcorp-domains/fulfillment-ags3-workflow/models/status"
	logging "github.com/gdcorp-domains/fulfillment-golang-logging"
	"go.elastic.co/apm"

	"github.com/gdcorp-domains/fulfillment-go-grule-engine/repository"

	bufferMessageModels "sqs-fargate-consumer-v2/internal/models"
	workflowModels "sqs-fargate-consumer-v2/internal/workflow/models"

	"github.com/gdcorp-domains/fulfillment-rules/rule"
	"github.com/gdcorp-domains/fulfillment-rules/ruleset/businesscontext"
	workflowHelper "github.com/gdcorp-domains/fulfillment-worker-helper"
	"github.com/google/uuid"
)

type messageExecutionContext struct {
	MessageID       string
	DataCtxAddr     string
	MetastoreAddr   string
	BusinessCtxAddr string
	nonBlocking     chan struct{}
}

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
	logger := ctx.Value("logger").(logging.Logger)

	logger.Infof("Starting HandleWorkflowEvent execution")

	var wrapper EventWrapper
	if err := json.Unmarshal([]byte(message.Body), &wrapper); err != nil {
		return err
	}

	// Create execution context
	execCtx := &messageExecutionContext{
		MessageID:   message.MessageID,
		nonBlocking: make(chan struct{}),
	}
	close(execCtx.nonBlocking)

	// Enhance logger with execution context
	logger = logger.WithFields(map[string]interface{}{
		"ReceiptHandle":    *message.ReceiptHandle,
		"ExecutionContext": fmt.Sprintf("%p", execCtx),
	})

	logger.Infof("Processing message %s for event source %s = %s", message.MessageID, *message.ReceiptHandle, message.Body)

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

	logger.Infof("Initializing engine")
	engineParams, err := helper.InitEngine(ctx, w.Deps, loggerContextDecorator, factory)
	if err != nil {
		return err
	}

	// Track isolated resources
	execCtx.DataCtxAddr = fmt.Sprintf("%p", engineParams.Engine.GetDataContext())
	if ms := engineParams.Engine.GetDataContext().Get(rule.MetaStoreKey); ms != nil {
		execCtx.MetastoreAddr = fmt.Sprintf("%p", ms)
	}

	logger.Infof("Initializing business context")
	bc, err := InitializeBusinessContextByType(engineParams.RulesetConfig, engineParams.Engine.GetDataContext())
	if err != nil {
		logger.Errorf("failed to initialize business context: %s", err)
		return err
	}

	execCtx.BusinessCtxAddr = fmt.Sprintf("%p", bc)

	logger = logger.WithFields(map[string]interface{}{
		"DataContextAddr":     execCtx.DataCtxAddr,
		"BusinessContextAddr": execCtx.BusinessCtxAddr,
		"MetastoreAddr":       execCtx.MetastoreAddr,
		"MessageExecCtxAddr":  fmt.Sprintf("%p", execCtx),
	})

	engineParams.Engine.GetDataContext().Add("Logger", logger)

	logger.Infof("Created isolated execution context for message %s", execCtx.MessageID)

	// Init new engine with business context
	engineParams.Engine.SetBusinessContext(bc)
	defer engineParams.Engine.GetDataContext().Clear()

	updatedCtx := engineParams.Context
	if w.Deps.GetAPMTracer() != nil {
		apmTransaction := w.Deps.GetAPMTracer().StartTransaction(engineParams.RulesetConfig.Name, "WorkflowRun")
		updatedCtx = apm.ContextWithTransaction(updatedCtx, apmTransaction)
		defer w.Deps.GetAPMTracer().Flush(execCtx.nonBlocking)
		defer apmTransaction.End()

		var span *apm.Span
		span, updatedCtx = apm.StartSpan(updatedCtx, "rulesetExecutionSpan", "ruleset")
		defer span.End()
	}

	logger.Infof("Executing engine with isolated contexts")
	now := time.Now()
	err = engineParams.Engine.Execute(updatedCtx)
	if err != nil {
		logger.Errorf("Rule execution failed: %s", err.Error())
		return err
	}

	domainEventInput.LastFunctionGraph = engineParams.Engine.GetDataContext().Get("FunctionGraph").(*string)

	err = helper.UpdateAction(ctx, domainEventInput, actionstatus.Success)
	if err != nil {
		logger.Errorf("Failed to update action: %s", err.Error())
		return err
	}

	processingDuration := time.Since(now)

	logger.Infof("Successfully completed message execution with context %p in %v", execCtx, processingDuration)
	return nil
}

// InitializeBusinessContextByType initializes businessContext by dataContext
func InitializeBusinessContextByType(ruleset *workflowHelper.RulesetConfig, dc repository.IDataContext) (businesscontext.BusinessContext, error) {
	bcRetriever := businesscontext.NewBusinessContextRetriever()
	return bcRetriever.InitializeBusinessContextBasedOnOperation(dc, ruleset.OperationType)
}
