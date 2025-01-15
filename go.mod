module sqs-fargate-consumer-v2

go 1.22.0

toolchain go1.22.9

require (
	github.com/aws/aws-sdk-go-v2 v1.32.5
	github.com/aws/aws-sdk-go-v2/config v1.28.3
	github.com/aws/aws-sdk-go-v2/service/cloudwatch v1.42.4
	github.com/aws/aws-sdk-go-v2/service/sqs v1.37.0
	github.com/gdcorp-domains/fulfillment-ags3-workflow v1.0.148
	github.com/gdcorp-domains/fulfillment-generic-queue-client v1.0.4
	github.com/gdcorp-domains/fulfillment-go-api v1.0.88
	github.com/gdcorp-domains/fulfillment-go-grule-engine v1.0.19
	github.com/gdcorp-domains/fulfillment-goapimodels v1.0.112
	github.com/gdcorp-domains/fulfillment-golang-clients v1.0.132
	github.com/gdcorp-domains/fulfillment-golang-httpclient v1.0.85
	github.com/gdcorp-domains/fulfillment-golang-logging v1.0.21
	github.com/gdcorp-domains/fulfillment-golang-sql-interfaces v1.0.11
	github.com/gdcorp-domains/fulfillment-registrar-config v1.0.58
	github.com/gdcorp-domains/fulfillment-registrar-contact-verification v1.0.62
	github.com/gdcorp-domains/fulfillment-registrar-domains v1.0.14
	github.com/gdcorp-domains/fulfillment-registry-contacts v1.0.23
	github.com/gdcorp-domains/fulfillment-registry-domains v1.0.105
	github.com/gdcorp-domains/fulfillment-rg-client v1.0.59
	github.com/gdcorp-domains/fulfillment-rules v1.0.381
	github.com/gdcorp-domains/fulfillment-sql-data-api-client v1.0.41
	github.com/gdcorp-domains/fulfillment-worker-helper v1.0.17
	github.com/gdcorp-uxp/switchboard-client/go/switchboard v1.9.0
	github.com/google/uuid v1.6.0
	go.uber.org/ratelimit v0.3.1
)

require (
	github.com/andybalholm/brotli v1.1.0 // indirect
	github.com/armon/go-radix v1.0.0 // indirect
	github.com/asaskevich/govalidator v0.0.0-20230301143203-a9d515a09cc2 // indirect
	github.com/aws/aws-lambda-go v1.41.0 // indirect
	github.com/aws/aws-sdk-go v1.55.5 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.17.46 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.20 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.24 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.24 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.12.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.24.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.28.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.33.1 // indirect
	github.com/aws/aws-xray-sdk-go v1.8.4 // indirect
	github.com/aws/smithy-go v1.22.1 // indirect
	github.com/awslabs/aws-lambda-go-api-proxy v0.13.3 // indirect
	github.com/awslabs/kinesis-aggregation/go v0.0.0-20230808105340-e631fe742486 // indirect
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/bradfitz/gomemcache v0.0.0-20230905024940-24af94b03874 // indirect
	github.com/bytedance/sonic v1.11.8 // indirect
	github.com/bytedance/sonic/loader v0.1.1 // indirect
	github.com/cloudwego/base64x v0.1.4 // indirect
	github.com/cloudwego/iasm v0.2.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/denisenkom/go-mssqldb v0.0.0-20200428022330-06a60b6afbbc // indirect
	github.com/elastic/go-licenser v0.4.1 // indirect
	github.com/elastic/go-sysinfo v1.14.0 // indirect
	github.com/elastic/go-windows v1.0.1 // indirect
	github.com/facebookgo/stack v0.0.0-20160209184415-751773369052 // indirect
	github.com/fsnotify/fsnotify v1.8.0 // indirect
	github.com/gabriel-vasile/mimetype v1.4.6 // indirect
	github.com/gdcorp-domains/fulfillment-command-factory v0.14.51 // indirect
	github.com/gdcorp-domains/fulfillment-contact-eligibility v1.0.11 // indirect
	github.com/gdcorp-domains/fulfillment-database-writer-worker v0.0.0-20230614225414-7ace5e88530d // indirect
	github.com/gdcorp-domains/fulfillment-domain-eligibility v1.0.7 // indirect
	github.com/gdcorp-domains/fulfillment-gin-writer v1.0.25 // indirect
	github.com/gdcorp-domains/fulfillment-ginutils v1.0.16 // indirect
	github.com/gdcorp-domains/fulfillment-go-filebuffer v1.0.0 // indirect
	github.com/gdcorp-domains/fulfillment-go2epp v1.1.45 // indirect
	github.com/gdcorp-domains/fulfillment-golang-middleware v1.0.54 // indirect
	github.com/gdcorp-domains/fulfillment-golang-sso-auth v1.0.60 // indirect
	github.com/gdcorp-domains/fulfillment-golang-utility v1.0.10 // indirect
	github.com/gdcorp-domains/fulfillment-gosecrets v1.0.17 // indirect
	github.com/gdcorp-domains/fulfillment-gotranslate v1.0.3 // indirect
	github.com/gdcorp-domains/fulfillment-govalidate v1.0.61 // indirect
	github.com/gdcorp-domains/fulfillment-openapihandler v1.0.7 // indirect
	github.com/gdcorp-domains/fulfillment-registrar-contacts v1.0.2 // indirect
	github.com/gdcorp-domains/fulfillment-registry-hosts v1.0.23 // indirect
	github.com/gdcorp-domains/fulfillment-structiterator v1.0.11 // indirect
	github.com/gin-contrib/sse v0.1.0 // indirect
	github.com/gin-contrib/static v0.0.1 // indirect
	github.com/gin-gonic/gin v1.10.0 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/go-playground/validator/v10 v10.22.1 // indirect
	github.com/goccy/go-json v0.10.3 // indirect
	github.com/godaddy/cobhan-go v0.4.3 // indirect
	github.com/golang-sql/civil v0.0.0-20190719163853-cb61b32ac6fe // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/harlow/kinesis-consumer v0.3.5 // indirect
	github.com/jcchavezs/porto v0.6.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/juju/xml v0.0.0-20160224194805-b5bf18ebd8b8 // indirect
	github.com/klauspost/compress v1.17.8 // indirect
	github.com/klauspost/cpuid/v2 v2.2.7 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mazen160/go-random v0.0.0-20210308102632-d2b501c85c03 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mohae/deepcopy v0.0.0-20170929034955-c48cc78d4826 // indirect
	github.com/mr-tron/base58 v1.2.0 // indirect
	github.com/patrickmn/go-cache v2.1.0+incompatible // indirect
	github.com/pelletier/go-toml/v2 v2.2.2 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rs/cors v1.8.1 // indirect
	github.com/rs/cors/wrapper/gin v0.0.0-20221003140808-fcebdb403f4d // indirect
	github.com/santhosh-tekuri/jsonschema v1.2.4 // indirect
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/ugorji/go/codec v1.2.12 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasthttp v1.54.0 // indirect
	go.elastic.co/apm v1.15.0 // indirect
	go.elastic.co/apm/module/apmgin v1.11.0 // indirect
	go.elastic.co/apm/module/apmhttp v1.15.0 // indirect
	go.elastic.co/fastjson v1.3.0 // indirect
	golang.org/x/arch v0.8.0 // indirect
	golang.org/x/crypto v0.28.0 // indirect
	golang.org/x/exp v0.0.0-20240416160154-fe59bbe5cc7f // indirect
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616 // indirect
	golang.org/x/mod v0.21.0 // indirect
	golang.org/x/net v0.30.0 // indirect
	golang.org/x/sync v0.8.0 // indirect
	golang.org/x/sys v0.26.0 // indirect
	golang.org/x/text v0.19.0 // indirect
	golang.org/x/tools v0.26.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241104194629-dd2ea8efbc28 // indirect
	google.golang.org/grpc v1.64.1 // indirect
	google.golang.org/protobuf v1.35.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	howett.net/plist v1.0.1 // indirect
)

replace github.com/gdcorp-domains/fulfillment-go-grule-engine v1.0.19 => /Users/chou/Desktop/godaddy/gdcorp-domains/fulfillment-go-grule-engine

replace github.com/gdcorp-domains/fulfillment-rules v1.0.381 => /Users/chou/Desktop/godaddy/gdcorp-domains/fulfillment-rules

replace github.com/gdcorp-domains/fulfillment-worker-helper v1.0.17 => /Users/chou/Desktop/godaddy/gdcorp-domains/fulfillment-worker-helper
