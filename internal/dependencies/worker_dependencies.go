package dependencies

import (
	"sqs-fargate-consumer-v2/internal/config"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	actionapi "github.com/gdcorp-domains/fulfillment-ags3-workflow/client"
	msmqclient "github.com/gdcorp-domains/fulfillment-generic-queue-client/client"
	"github.com/gdcorp-domains/fulfillment-go-grule-engine/grule"
	intlcontacts "github.com/gdcorp-domains/fulfillment-golang-clients/internationalcontactsapi"
	"github.com/gdcorp-domains/fulfillment-golang-clients/shopperapi"
	"github.com/gdcorp-domains/fulfillment-golang-clients/switchboard"
	logging "github.com/gdcorp-domains/fulfillment-golang-logging"
	sqlinterfaces "github.com/gdcorp-domains/fulfillment-golang-sql-interfaces"
	registrarconfig "github.com/gdcorp-domains/fulfillment-registrar-config"
	contactverification "github.com/gdcorp-domains/fulfillment-registrar-contact-verification"
	registrycontacts "github.com/gdcorp-domains/fulfillment-registry-contacts"
	registrydomains "github.com/gdcorp-domains/fulfillment-registry-domains"
	rgclient "github.com/gdcorp-domains/fulfillment-rg-client"
	workerhelper "github.com/gdcorp-domains/fulfillment-worker-helper"
	workflowHelper "github.com/gdcorp-domains/fulfillment-worker-helper"
)

// WorkflowDependencies defines the interface used to pass dependencies down to the workflows
type WorkflowDependencies interface {
	GetBaseDependencies() workflowHelper.BaseWorkerDependencies
	GetConfig() *config.Config
	GetRegistrarConfigCache() registrarconfig.Cache
	GetActionAPIClient() actionapi.Client
	GetLogger() logging.Logger
	GetRegistryContactsClient() registrycontacts.Client
	GetRegistryDomainsClient() registrydomains.Client
	GetKnowledgeBase(name, version string) (*grule.KnowledgeBase, bool)
	GetSwitchboardAccessor() switchboard.Accessor
	GetSBConfigGetter() workerhelper.SBConfigGetter
	GetDBConnection() sqlinterfaces.DatabaseConnection
	GetShopperAPIClient() shopperapi.Client
	GetIntlContactsAPIClient() intlcontacts.Client
	GetRegistryConfigCache() rgclient.Cache
	GetMSMQClient() msmqclient.MSMQClient
	GetDomainStatusMapping() workerhelper.AGSStatusMapping
	GetManagerUserID() *string
	GetRulesetConfigMaps() map[string]workflowHelper.RulesetConfig
	GetSQSClient() *sqs.Client
	GetContactVerificationAPIClient() contactverification.Client
}
