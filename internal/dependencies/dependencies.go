package dependencies

import (
	"context"
	"fmt"
	"sqs-fargate-consumer-v2/internal/config"

	"net/url"

	registrycontacts "github.com/gdcorp-domains/fulfillment-registry-contacts"

	"github.com/gdcorp-domains/fulfillment-rules/ruleset/businesscontext"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	actionapi "github.com/gdcorp-domains/fulfillment-ags3-workflow/client"
	msmqclient "github.com/gdcorp-domains/fulfillment-generic-queue-client/client"
	goapi "github.com/gdcorp-domains/fulfillment-go-api"
	"github.com/gdcorp-domains/fulfillment-go-grule-engine/grule"
	intlcontacts "github.com/gdcorp-domains/fulfillment-golang-clients/internationalcontactsapi"

	"github.com/gdcorp-domains/fulfillment-golang-clients/shopperapi"
	"github.com/gdcorp-domains/fulfillment-golang-clients/switchboard"
	logging "github.com/gdcorp-domains/fulfillment-golang-logging"
	sqlinterfaces "github.com/gdcorp-domains/fulfillment-golang-sql-interfaces"
	registrarconfig "github.com/gdcorp-domains/fulfillment-registrar-config"

	registrydomains "github.com/gdcorp-domains/fulfillment-registry-domains"
	rgclient "github.com/gdcorp-domains/fulfillment-rg-client"
	"github.com/gdcorp-domains/fulfillment-rules/ruleset/godaddy"
	sqldata "github.com/gdcorp-domains/fulfillment-sql-data-api-client"
	workerhelper "github.com/gdcorp-domains/fulfillment-worker-helper"
	switchboardclient "github.com/gdcorp-uxp/switchboard-client/go/switchboard"
)

// Dependencies encapsulates all dependencies to be carried through request processing.
type Dependencies struct {
	*goapi.StaticDependencies
	Config                  *config.Config
	RegistryContactsClient  registrycontacts.Client
	RegistryDomainsClient   registrydomains.Client
	RegistrarConfigCache    registrarconfig.Cache
	ActionAPIClient         actionapi.Client
	KnowledgeLibrary        grule.KnowledgeLibraryManager
	SwitchboardAccessor     switchboard.Accessor
	SBConfigGetter          workerhelper.SBConfigGetter
	DBConnection            sqlinterfaces.DatabaseConnection
	ShopperAPIClient        shopperapi.Client
	IntlContactsAPIClient   intlcontacts.Client
	DomainStatusMapping     workerhelper.AGSStatusMapping
	DomainBoxRulesetMapping workerhelper.DomainBoxMapping
	RegistryConfigCache     rgclient.Cache
	MSMQClient              msmqclient.MSMQClient
	ManagerUserID           *string
	RulesetConfigMaps       map[string]workerhelper.RulesetConfig
	SQSClient               *sqs.Client
	CloudwatchClient        *cloudwatch.Client
}

// Initialize takes the static dependencies and does any one-time setup
func (dep *Dependencies) Initialize(static *goapi.StaticDependencies) {
	dep.StaticDependencies = static
	iamSSOClient := dep.StaticDependencies.HTTPClients.WithIAMSSO

	registryDomainsClientURL, _ := url.Parse(dep.Config.RegistryDomainsURL)
	dep.RegistryDomainsClient = registrydomains.NewClient(registryDomainsClientURL, iamSSOClient)

	registryContactsClientURL, _ := url.Parse(dep.Config.RegistryContactsURL)
	dep.RegistryContactsClient = registrycontacts.NewClient(registryContactsClientURL, iamSSOClient)

	actionAPIURL, _ := url.Parse(dep.Config.ActionAPIURL)
	dep.ActionAPIClient = actionapi.NewClient(actionAPIURL, iamSSOClient)

	dep.KnowledgeLibrary = grule.NewKnowledgeLibrary()
	for _, r := range godaddy.RuleSet() {
		dep.KnowledgeLibrary.AddRulesToKnowledgeBase(r.Name(), r.Version(), r.Root())
	}

	cfg := dep.Config
	sbClient, err := SetupSwitchboardClient(cfg.APIName, cfg.SwitchBoardApplicationName, cfg.Env, cfg.SSO.IAMConfig.PrimaryRegion, cfg.SSO.IAMConfig.SecondaryRegions[0])
	if err != nil {
		panic(err)
	}
	dep.SwitchboardAccessor = switchboard.NewAccessor(sbClient, cfg.SwitchBoardApplicationName)
	dep.SBConfigGetter = workerhelper.NewSBConfigGetter(dep.SwitchboardAccessor)

	sqldataURL, err := url.Parse(dep.Config.SQLDATAAPIURL)
	if err != nil {
		panic(err)
	}
	dep.DBConnection = sqldata.NewWrapper(sqldata.NewClient(sqldataURL, dep.Config.DBName, iamSSOClient))

	ShopperAPIURL, err := url.Parse(dep.Config.ShopperAPIURL)
	if err != nil {
		panic(err)
	}
	dep.ShopperAPIClient = shopperapi.NewClient(ShopperAPIURL, static.HTTPClients.WithCertSSO)

	intlContactsAPIURL, _ := url.Parse(dep.Config.IntlContactsAPIURL)
	dep.IntlContactsAPIClient = intlcontacts.NewClient(intlContactsAPIURL, static.HTTPClients.WithClientCert)

	msmqQueueURL, _ := url.Parse(dep.Config.MSMQURL)
	dep.MSMQClient = msmqclient.NewMSMQClient(msmqQueueURL, static.HTTPClients.Plain)

	// Start switchboard client
	if err := dep.SwitchboardAccessor.StartClient(); err != nil {
		panic(fmt.Errorf("switchboard.StartClient() failed with error: %s", err))
	}

	// Load AGSDomainStatusMapping
	agsStatusMapping, err := dep.SBConfigGetter.GetAGSStatusMap(workerhelper.AGSDomainStatusMapping)
	if err != nil {
		panic(fmt.Errorf("SBConfigGetter.GetStatusMap() failed with error: %s", err))
	}

	dep.DomainStatusMapping = agsStatusMapping

	// Load DomainBoxRulesetMapping
	domainBoxRulesetMapping, err := dep.SBConfigGetter.GetDomainBoxRulesetMap(workerhelper.DomainBoxRulesetMapping)
	if err != nil {
		panic(fmt.Errorf("SBConfigGetter.GetDomainBoxRulesetMapping() failed with error: %s", err))
	}
	dep.DomainBoxRulesetMapping = domainBoxRulesetMapping

	// Load ManagerUserID
	managerUserID, err := dep.SBConfigGetter.GetManagerUserID()
	if err != nil {
		panic(fmt.Errorf("SBConfigGetter.GetManagerUserID() failed with error: %s", err))
	}
	dep.ManagerUserID = managerUserID

	// Load all RulesetConfigMaps
	sbConfigGetter := workerhelper.NewSBConfigGetter(dep.SwitchboardAccessor)
	rulesetConfigMaps, err := sbConfigGetter.GetAllRulesetMaps()
	if err != nil {
		panic(fmt.Errorf("sbConfigGetter.GetAllRulesetMaps() failed with error: %s", err))
	}
	dep.RulesetConfigMaps = rulesetConfigMaps

	// Load AWS configuration
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		panic(fmt.Errorf("Unable to load SDK config: %v", err))
	}

	// Initialize AWS clients
	dep.SQSClient = sqs.NewFromConfig(awsCfg)
	dep.CloudwatchClient = cloudwatch.NewFromConfig(awsCfg)
}

// SetupSwitchboardClient sets up a switchboard client
func SetupSwitchboardClient(appName, application, env, ssoPrimaryRegion, ssoSecondaryRegion string) (switchboardclient.Client, error) {
	var switchboardEnv switchboardclient.Environment
	switch env {
	case "dev":
		switchboardEnv = switchboardclient.EnvDevelopment
	case "test":
		switchboardEnv = switchboardclient.EnvTest
	case "ote", "prod":
		switchboardEnv = switchboardclient.EnvProduction
	default:
		return nil, fmt.Errorf("unsupported env %s for switchboard client", env)
	}

	sbClientConfig := switchboardclient.ClientConfiguration[switchboardclient.AWSIAMAuthOptions]{
		CallingService: appName,
		Environment:    switchboardEnv,
		AuthOptions: switchboardclient.AWSIAMAuthOptions{
			PrimaryRegion:   ssoPrimaryRegion,
			SecondaryRegion: ssoSecondaryRegion,
		},
		DataRetrieval: map[string]switchboardclient.DataRetrievalOptions{
			application: {},
		},
	}
	client, err := switchboardclient.CreateClient(sbClientConfig)
	if err != nil {
		return nil, fmt.Errorf("switchboard.CreateClient() failed with error: %s", err)
	}

	return client, nil
}

// New constructs a new Dependencies, or panics on error
func New(config *config.Config) *Dependencies {

	if err := config.RegistrarConfig.Initialize(); err != nil {
		panic(err)
	}

	registrarConfigCache, err := registrarconfig.NewCache(config.RegistrarConfig)
	if err != nil {
		panic(err)
	}

	if err := config.RegistryConfig.Initialize(); err != nil {
		panic(err)
	}

	registryConfigCache, err := rgclient.NewCache(config.RegistryConfig)
	if err != nil {
		panic(err)
	}

	return &Dependencies{
		Config:               config,
		RegistrarConfigCache: registrarConfigCache,
		RegistryConfigCache:  registryConfigCache,
	}
}

// GetConfig returns the config
func (dep Dependencies) GetConfig() *config.Config {
	return dep.Config
}

// GetLogger returns the logger
func (dep Dependencies) GetLogger() logging.Logger {
	return dep.Logger
}

// GetRegistrarConfigCache returns registrar config cache
func (dep Dependencies) GetRegistrarConfigCache() registrarconfig.Cache {
	return dep.RegistrarConfigCache
}

// GetRegistryContactsClient returns the registry contacts client
func (dep Dependencies) GetRegistryContactsClient() registrycontacts.Client {
	return dep.RegistryContactsClient
}

// GetRegistryDomainsClient returns the registry domains client
func (dep Dependencies) GetRegistryDomainsClient() registrydomains.Client {
	return dep.RegistryDomainsClient
}

// GetActionAPIClient returns the action api client
func (dep Dependencies) GetActionAPIClient() actionapi.Client {
	return dep.ActionAPIClient
}

// GetKnowledgeBase returns the knowledge base for rules
func (dep Dependencies) GetKnowledgeBase(name, version string) (*grule.KnowledgeBase, bool) {
	return dep.KnowledgeLibrary.GetKnowledgeBase(name, version)
}

// AddBusinessContextToKnowledgeBase sets businessCtx in knowledge lib
func (dep Dependencies) AddBusinessContextToKnowledgeBase(name, version string, businessCtx businesscontext.BusinessContext) error {
	return dep.KnowledgeLibrary.AddBusinessContextToKnowledgeBase(name, version, businessCtx)
}

// GetSwitchboardAccessor returns the switchboard accessor
func (dep Dependencies) GetSwitchboardAccessor() switchboard.Accessor {
	return dep.SwitchboardAccessor
}

// GetSBConfigGetter returns the switchboard accessor
func (dep Dependencies) GetSBConfigGetter() workerhelper.SBConfigGetter {
	return dep.SBConfigGetter
}

// GetDBConnection returns the DBConnetion
func (dep Dependencies) GetDBConnection() sqlinterfaces.DatabaseConnection {
	return dep.DBConnection
}

// GetShopperAPIClient returns the shopper API client
func (dep Dependencies) GetShopperAPIClient() shopperapi.Client {
	return dep.ShopperAPIClient
}

// GetIntlContactsAPIClient returns the intl contacts API client
func (dep Dependencies) GetIntlContactsAPIClient() intlcontacts.Client {
	return dep.IntlContactsAPIClient
}

// GetDomainStatusMapping returns AGSDomainStatusMapping from switchboard settings
func (dep Dependencies) GetDomainStatusMapping() workerhelper.AGSStatusMapping {
	return dep.DomainStatusMapping
}

// GetRegistryConfigCache returns registry config cache
func (dep Dependencies) GetRegistryConfigCache() rgclient.Cache {
	return dep.RegistryConfigCache
}

// GetMSMQClient returns msmq client
func (dep Dependencies) GetMSMQClient() msmqclient.MSMQClient {
	return dep.MSMQClient
}

// GetBaseDependencies returns ...
func (dep Dependencies) GetBaseDependencies() workerhelper.BaseWorkerDependencies {
	return workerhelper.BaseWorkerDependencies{
		Config: workerhelper.Config{
			APIName: dep.Config.APIName,
			Env:     dep.Config.Env,
			Region:  dep.Config.Region,
		},
		ActionAPIClient:     dep.ActionAPIClient,
		SwitchboardAccessor: dep.SwitchboardAccessor,
		SBConfigGetter:      workerhelper.NewSBConfigGetter(dep.SwitchboardAccessor),
		KnowledgeLibrary:    dep.KnowledgeLibrary,
	}
}

// GetManagerUserID returns the managerUserID
func (dep *Dependencies) GetManagerUserID() *string {
	return dep.ManagerUserID
}

// GetRulesetConfigMaps returns the ruleset config maps
func (dep *Dependencies) GetRulesetConfigMaps() map[string]workerhelper.RulesetConfig {
	return dep.RulesetConfigMaps
}
