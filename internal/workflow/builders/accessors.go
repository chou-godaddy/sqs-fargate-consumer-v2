package builders

import (
	"sqs-fargate-consumer-v2/internal/dependencies"

	logging "github.com/gdcorp-domains/fulfillment-golang-logging"
	"github.com/gdcorp-domains/fulfillment-registrar-domains/models"
	"github.com/gdcorp-domains/fulfillment-rules/accessors/dataaccessor/agentservicesaccessor"
	"github.com/gdcorp-domains/fulfillment-rules/accessors/datainterface"
	"github.com/gdcorp-domains/fulfillment-rules/rule/state"
)

// DataAccessor ...
type DataAccessor struct {
	deps            dependencies.WorkflowDependencies
	metastore       *state.MetaStore
	Registrar       string
	CustomerID      *string
	decoratedLogger logging.Logger
}

// NewDataAccessor initializes data accessor depending on backend type
func NewDataAccessor(deps dependencies.WorkflowDependencies, metastore *state.MetaStore, registrar string, customerID *string, decoratedLogger logging.Logger) *DataAccessor {
	return &DataAccessor{
		deps:            deps,
		metastore:       metastore,
		Registrar:       registrar,
		CustomerID:      customerID,
		decoratedLogger: decoratedLogger,
	}
}

// GetDataAccessor retrieves data accessor interface based on backend type
func (accessor *DataAccessor) GetDataAccessor(backendType models.RegistrarBackendType) datainterface.DataAccessor {
	deps := accessor.deps
	switch backendType {
	case models.AGS2:
		managerUserID, _ := deps.GetSBConfigGetter().GetManagerUserID()
		internationContactWebSvc := deps.GetSBConfigGetter().GetUseInternationalContactWebService()
		return agentservicesaccessor.NewAgentServicesDataAccessor(agentservicesaccessor.AgentServicesDataAccessor{
			DBConnection:                      deps.GetDBConnection(),
			ManagerUserID:                     managerUserID,
			Logger:                            accessor.decoratedLogger,
			Metastore:                         accessor.metastore,
			UseInternationalContactWebService: internationContactWebSvc,
			InternationalContactsAPIClient:    deps.GetIntlContactsAPIClient(),
		}, deps.GetActionAPIClient())
	default:
		panic("unhandled backend type")
	}
}
