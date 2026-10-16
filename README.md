# Project 4 – Event-Driven Orders Application (AWS SAM, multi-Region)

Orders come in through API Gateway, are queued in SQS, processed idempotently by Lambda, announced as events on
Amazon EventBridge (through a **global endpoint**), fanned out to a fulfilment queue and an SNS topic, and the whole
application runs in **two AWS Regions**. It is deployed from your computer or, on every push to `main`, from GitHub Actions.

> **Status – read this first.** The template, workflow and Lambda code were checked offline only: the YAML parses and
> every reference resolves, the JavaScript passes a syntax check, and the idempotency / concurrency / batch-failure logic
> passes an in-memory test. **Nothing has been deployed to a real AWS account** (the SAM CLI, cfn-lint and npm were not
> reachable from the build environment). Expect to fix a small thing or two on the first real deployment; §7 lists the
> parts I am least sure about.

---

## 1. Project scope and AWS services

### 1.1 What it does

```mermaid
flowchart LR
  C([Client]) -- "POST /orders" --> APIGW["API Gateway<br/>validates body, answers 202"]
  APIGW -- SendMessage --> OQ[("SQS orders queue")]
  OQ -. "3 failed receives" .-> DLQ[("Dead-letter queue")]
  OQ --> CO["Lambda CreateOrder<br/>idempotent on orderId"]
  CO <--> OT[("DynamoDB Orders")]
  CO -- "PutEvents OrderCreated" --> GE{{"EventBridge<br/>global endpoint"}}
  GE --> PB["Event bus (primary Region)"]
  GE -. "failover + replication" .-> SB["Event bus (secondary Region)"]
  PB -- rule --> FQ[("SQS fulfilment queue")] --> FO["Lambda FulfilOrder"] --> FT[("DynamoDB Fulfilment")]
  PB -- rule --> SNS["SNS order notifications"]
```

The diagram shows one Region. The **same stack is deployed in both Regions**, so each Region has its own API Gateway,
Lambda functions, event bus, SQS queues, SNS topic, DynamoDB tables and dashboard.

| Item | Detail |
|---|---|
| **Write path** | `POST /orders` → API Gateway validates the JSON → message on SQS → API answers **202 Accepted** immediately (it does not wait for Lambda). |
| **Idempotency** | The client sends a unique `orderId`. CreateOrder claims it with a DynamoDB conditional write; duplicates return the stored result and trigger no second event. |
| **Events** | After the order is stored, CreateOrder publishes `OrderCreated` through the EventBridge global endpoint. |
| **Fan-out** | Rule 1 → SQS → **FulfilOrder** Lambda → *Fulfilment* table. Rule 2 → **SNS** topic (optional e-mail subscription). |
| **Failure handling** | Source queue has a dead-letter queue: after **3** failed receives a message is moved there. The fulfilment queue has one too. |
| **Read path** | `GET /orders/{orderId}` → Lambda → order status + fulfilment record (the client polls this after the 202). |
| **Observability** | CloudWatch dashboard: EventBridge `MatchedEvents`, SQS `NumberOfMessagesSent`, SNS `NumberOfMessagesPublished` (+ DLQ depth); X-Ray tracing on Lambda. |
| **Disaster recovery** | EventBridge **global endpoint** + Route 53 health check: if ingestion in the primary Region degrades, publishers are routed to the secondary Region. Replication keeps both buses in step. |
| **Delivery** | GitHub Actions on every push to `main`, credentials from repository secrets. |

### 1.2 Where each requirement lives

| # | Requirement | Implemented in |
|---|---|---|
| 1 | Custom bus + `OrderCreated` event from CreateOrder | `OrderBus`, `src/create-order/index.js` (`publishOrderCreated`) |
| 2 | SQS queue + rule + `FulfilOrder` + new DynamoDB table | `FulfilmentQueue`, `OrderToFulfilmentRule`, `FulfilOrderFunction`, `FulfilmentTable` |
| 3 | SNS topic + rule | `OrderNotificationsTopic`, `OrderNotificationRule` (input transformer builds the message) |
| 4 | CloudWatch dashboard | `OrdersDashboard` |
| 5 | GitHub Actions deployment | `.github/workflows/deploy.yml` |
| 6 | API Gateway → SQS → CreateOrder (async, validated) | `OrdersApi` (JSON-schema model + direct SQS integration), `OrdersQueue`, event source of `CreateOrderFunction` |
| 7 | DLQ, redrive after 3 attempts | `OrdersDeadLetterQueue` + `RedrivePolicy.maxReceiveCount: 3` on `OrdersQueue` |
| 8 | Idempotent processing, no race conditions | `processOrder()` in `src/create-order/index.js` (conditional `PutItem` + lease takeover) |
| 9 | Active-active multi-Region with global endpoints | `RegionRole` parameter, `GlobalEndpoint`, `EndpointHealthCheck`, `EndpointHighLatencyAlarm`, `EndpointReplicationRole`; `EndpointId` passed to `PutEvents` |

### 1.3 AWS services used (short résumé)

| Service | Role |
|---|---|
| **AWS SAM / CloudFormation** | Defines and deploys everything from `template.yaml` (one stack per Region). |
| **Amazon API Gateway (REST)** | Public API; validates requests; writes straight to SQS (no Lambda in the write path). |
| **Amazon SQS** | Orders queue, fulfilment queue, and a dead-letter queue for each. |
| **AWS Lambda (Node.js 22)** | `CreateOrder`, `FulfilOrder`, `GetOrder`. |
| **Amazon DynamoDB** | `orders` table (idempotency + state) and `fulfilment` table. |
| **Amazon EventBridge** | Custom event bus, rules, and the **global endpoint** (with event replication). |
| **Amazon SNS** | Order notification topic (optional e-mail subscriber). |
| **Amazon CloudWatch** | Dashboard, logs (7-day retention), the failover alarm. |
| **Amazon Route 53** | One *health check* (type CloudWatch metric) that tells the global endpoint when to fail over. No hosted zone, no domain. |
| **AWS IAM** | Least-privilege roles (Lambda, API Gateway → SQS, endpoint replication). |
| **AWS X-Ray** | Active tracing on the functions. |
| **Amazon S3** | SAM's managed artifact bucket (one per Region) holding the packaged code. |

---

## 2. Free tier: restrictions you will run into

Short version: **the application is cheap, but it is not 100 % free.** Costs come from EventBridge custom events, and
the multi-Region design multiplies usage. At lab volume (hundreds of orders) expect cents, not dollars.

What I **verified on the AWS pricing pages**:

- **EventBridge custom events: $1.00 per million, no free tier.** Publishing to the global endpoint is a custom event. There is *no extra charge for the global endpoint itself*, but delivery to another event bus is also $1.00 per million, and cross-Region transfer can add data-transfer charges. With replication on, every order is billed roughly twice.
- **Route 53 health check: $0.50 per month** for a CloudWatch-metric health check (billed like an AWS-endpoint check). The same page also says up to 50 AWS-endpoint health checks per account are free, so it may cost nothing; check your bill.

What I am stating from general knowledge (**confirm on the AWS Free Tier page**):

| Service | Free allowance (typical) | Effect here |
|---|---|---|
| Lambda | 1 M requests + 400 000 GB-s / month, always free | Fine. |
| SQS | 1 M requests / month, always free | Fine; EventBridge→SQS deliveries count too. |
| SNS | 1 M publishes, 1 000 e-mail deliveries / month | Fine. |
| DynamoDB | 25 GB + 25 provisioned RCU / 25 WCU | Tables use **provisioned 5/5** (4 tables = 20/20), because *on-demand has no free tier*. Tiny capacity: bursts are throttled and retried through SQS. |
| API Gateway (REST) | 1 M calls / month for the first 12 months (older accounts) | Stage throttled to 10 req/s as a safety net. |
| CloudWatch | 10 alarms, 3 dashboards, basic metrics, 5 GB logs | 1 alarm + 1 dashboard per Region. |
| X-Ray | 100 000 traces / month | Fine. |

Restrictions and traps specific to this project:

1. **Two Regions = double work.** With replication, *both* Regions process every order (two Lambda runs, two fulfilment rows, two notifications). Usage allowances are per account, not per Region, so they are shared.
2. **Duplicate e-mails.** Both Regions' SNS topics publish each order. Subscribe `NotificationEmail` in one Region only, or accept two mails.
3. **Low Lambda concurrency on new accounts** (often 10 in total). `QueueConsumerConcurrency` defaults to **2** so throttled pollers do not drive messages to the DLQ. Check *Service Quotas → Lambda → Concurrent executions*.
4. **Account plan.** Accounts created since mid-July 2025 can be on the credit-based *Free plan*, where some services/features may be restricted until you upgrade. I could not confirm whether EventBridge global endpoints or Route 53 health checks are on its allowed list. If stack creation fails with a plan/permission message, that is the cause (Billing → *Account plan*).
5. **Region support.** Check that your two Regions support global endpoints (defaults `eu-central-1` / `eu-west-1`). The two Regions must be different, and the bus must have the **same name** in both.
6. **True active-active routing is not free.** Sending *client traffic* to both Regions needs Route 53 records (a domain, hosted zone, latency/weighted routing). This project gives you two independent regional API URLs; see §7.
7. **Set a budget alert first:** Billing → *Budgets* → *Zero-spend budget*.

---

## 3. Prerequisites

- An AWS account and an IAM user for deployments (access keys; `AdministratorAccess` is simplest for a lab).
- [AWS CLI v2](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html), [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html), Node.js 22, Git, a GitHub account.
- **Windows users (PowerShell):** the commands below are single-line so they work in PowerShell, cmd and bash. Use `curl.exe` (not the `curl` alias). Build with Docker: `sam build --use-container` (needs Docker Desktop), because the CreateOrder function includes a native library (AWS CRT) that must be built for Linux. Deploying through GitHub Actions (§5) avoids this entirely.

Project layout:

```
.
├── template.yaml                  # all AWS resources (steps 1-4, 6-9)
├── samconfig.toml                 # stack name, Regions, defaults for sam deploy
├── events/order.json              # sample request body (+ invalid-order.json for the 400 test)
├── src/
│   ├── create-order/              # CreateOrder (idempotent, publishes via global endpoint) + package.json (CRT)
│   ├── fulfil-order/              # FulfilOrder (SQS consumer)
│   └── get-order/                 # GET /orders/{orderId}
├── .github/workflows/deploy.yml   # CI/CD (step 5)
└── README.md
```

---

## 4. Deploy procedure – AWS (from your computer)

Region names below are the defaults (`eu-central-1` = **primary**, `eu-west-1` = **secondary**); change them in
`samconfig.toml` if you like.

### Step 4.1 – Configure the AWS CLI

```
aws configure
aws sts get-caller-identity
```

Enter the access key, secret key, default region `eu-central-1`, output `json`. The second command shows which account you are about to use.

### Step 4.2 – Validate and build

```
sam validate --lint
sam build --use-container
```

(`sam build` without `--use-container` is fine on Linux x86_64.) `sam build` runs `npm install` for each function.

### Step 4.3 – Deploy the primary Region only (single-Region mode, steps 1-8)

This is the safest first run: no global endpoint, no Route 53, events go to the local bus.

```
sam deploy
```

Accept the defaults (stack `orders-app`, IAM role creation **Y**). Add an e-mail for notifications with
`sam deploy --parameter-overrides "RegionRole=primary EnableGlobalEndpoint=false NotificationEmail=you@example.com"`,
then click the confirmation link AWS mails you. Skip to §4.5 to test; come back here for the multi-Region upgrade.

### Step 4.4 – Go multi-Region (step 9): secondary → primary → secondary

The global endpoint needs the event bus of **both** Regions, and the secondary Lambdas need the endpoint id, so the
order is fixed.

**1) Secondary Region first** (creates its bus, queues, API, …):

```
sam deploy --config-env secondary
```

**2) Primary Region, now with the global endpoint** (creates the CloudWatch alarm, Route 53 health check, replication role and the endpoint):

```
sam deploy --parameter-overrides "RegionRole=primary PeerRegion=eu-west-1 EnableGlobalEndpoint=true"
```

Read the endpoint id:

```
aws cloudformation describe-stacks --stack-name orders-app --region eu-central-1 --query "Stacks[0].Outputs[?OutputKey=='GlobalEndpointId'].OutputValue" --output text
```

(The primary CreateOrder already uses it – the id is wired in with `!GetAtt`.)

**3) Secondary Region again**, so its Lambda publishes through the endpoint too (replace `<id>`):

```
sam deploy --config-env secondary --parameter-overrides "RegionRole=secondary PeerRegion=eu-central-1 GlobalEndpointId=<id>"
```

Until step 3 is done the secondary publishes to its own local bus – nothing breaks, it just is not using the endpoint yet.

### Step 4.5 – Test

Get the API URL (repeat with `--region eu-west-1` for the secondary Region):

```
aws cloudformation describe-stacks --stack-name orders-app --region eu-central-1 --query "Stacks[0].Outputs[?OutputKey=='ApiUrl'].OutputValue" --output text
```

**a) Create an order** – answers immediately with `202 Accepted`:

```
curl.exe -i -X POST "<ApiUrl>/orders" -H "Content-Type: application/json" --data "@events/order.json"
```

**b) Check the result** (after a few seconds):

```
curl.exe "<ApiUrl>/orders/ord-1001-demo"
```

You should see `"status":"COMPLETED"` and a `fulfilment` object with `"status":"FULFILLED"`.

**c) Validation** – a body without `customerId` must return `400` and never reach SQS:

```
curl.exe -i -X POST "<ApiUrl>/orders" -H "Content-Type: application/json" --data "@events/invalid-order.json"
```

**d) Idempotency** – send the **same** `events/order.json` three more times (each answers 202). Then confirm there is still exactly one order, one fulfilment row and one notification:

```
aws dynamodb scan --table-name orders-app-fulfilment --select COUNT --region eu-central-1
```

(Change `orderId` in the file to create a different order.)

**e) Dead-letter queue** – put a poison message directly on the queue and watch it move after 3 failed receives (about 3–4 minutes, the visibility timeout is 60 s):

```
aws sqs send-message --queue-url <OrdersQueueUrl output> --message-body "not json" --region eu-central-1
aws sqs get-queue-attributes --queue-url <OrdersDeadLetterQueueUrl output> --attribute-names ApproximateNumberOfMessages --region eu-central-1
```

**f) Multi-Region** – post an order to the **secondary** API URL. Because the event goes through the global endpoint and is replicated, the order record is in the secondary Region's `orders` table, and a fulfilment row appears in **both** Regions' `orders-app-fulfilment` tables.

**g) Dashboard** – open the `DashboardUrl` output (CloudWatch → Dashboards): MatchedEvents, NumberOfMessagesSent, NumberOfMessagesPublished. Metrics appear after a few minutes.

---

## 5. Deploy procedure – GitHub (automatic on every push to `main`)

### Step 5.1 – Create the repository and push

```
git init -b main
git add .
git commit -m "Orders event-driven application"
git remote add origin https://github.com/<your-user>/<your-repo>.git
git push -u origin main
```

Never commit AWS keys (`.gitignore` already excludes `.aws-sam/` and `node_modules/`).

### Step 5.2 – Create deployment credentials in AWS

IAM → Users → *Create user* `github-sam-deployer` → attach `AdministratorAccess` (lab; tighten later) → *Security credentials* → *Create access key* (use case: *Application running outside AWS*).

### Step 5.3 – Store them as GitHub repository secrets

Repository → *Settings → Secrets and variables → Actions*:

| Type | Name | Value |
|---|---|---|
| **Secret** | `AWS_ACCESS_KEY_ID` | access key id |
| **Secret** | `AWS_SECRET_ACCESS_KEY` | secret access key |
| Variable (optional) | `PRIMARY_REGION` | default `eu-central-1` |
| Variable (optional) | `SECONDARY_REGION` | default `eu-west-1` |
| Variable (optional) | `MULTI_REGION` | `true` (default) or `false` = single Region, steps 1-8 only |
| Variable (optional) | `NOTIFICATION_EMAIL` | address for the SNS subscription |

### Step 5.4 – What the workflow does

`.github/workflows/deploy.yml` runs on every push to `main` (and manually: *Actions → Run workflow*):

1. check out, set up Node 22 and SAM CLI, load the AWS credentials from the secrets
2. `sam validate --lint`, then `sam build` (packages every function with its npm dependencies)
3. `sam deploy` **secondary → primary → secondary (again, only if the endpoint id changed)** – the same order as §4.4
4. prints the outputs of both stacks

Pushing a change to a Lambda file re-packages and updates that function; pushing a template change updates the stacks. An unchanged push ends green (`--no-fail-on-empty-changeset`). The first run takes a few minutes longer (endpoint creation).

> Hardening for later: replace long-lived access keys with GitHub OIDC and an IAM role; pin actions to commit SHAs.

---

## 6. Tear down and stop all costs (keep the configuration)

Your **configuration is code** and stays untouched by everything below: `template.yaml`, `src/`, `samconfig.toml`, the workflow, and the GitHub secrets/variables. Redeploying later recreates the whole thing.

### 6.1 What can cost money while idle

An idle stack has almost no cost (queues, SNS, event buses, Lambda are pay-per-use). The only standing items are the Route 53 health check (about $0.50/month, possibly inside its free allowance), the DynamoDB provisioned capacity (inside the free 25/25 units), CloudWatch logs/dashboard (tiny) and S3 storage of the packaged code (cents). There is no "pause" button, so for a **guaranteed $0**, delete the stacks.

### 6.2 Delete everything in AWS

1. **Stop automatic deployments**, so a push cannot recreate the stacks: *Actions → Deploy SAM application → ⋯ → Disable workflow* (or `gh workflow disable deploy.yml`).
2. **(Optional) back up the data** (the tables are deleted with the stacks):
   ```
   aws dynamodb scan --table-name orders-app-orders --region eu-central-1 --output json > orders-backup.json
   aws dynamodb scan --table-name orders-app-fulfilment --region eu-central-1 --output json > fulfilment-backup.json
   ```
3. **Delete the primary stack first** (this removes the global endpoint, Route 53 health check and alarm), **then the secondary**:
   ```
   sam delete --stack-name orders-app --region eu-central-1
   sam delete --stack-name orders-app --region eu-west-1
   ```
   Answer *yes* to also delete the uploaded code in S3. If a delete ends in `DELETE_FAILED`, open CloudFormation → the stack → *Events*, fix what it names (usually a leftover manual resource), and delete again.
4. **Remove SAM's artifact buckets** if no other SAM project uses them (one per Region): S3 → bucket starting with `aws-sam-cli-managed-default-samclisourcebucket-` → *Empty* → then CloudFormation → delete the stack `aws-sam-cli-managed-default` in that Region.
5. **Neutralise the credentials but keep them on file:** IAM → user `github-sam-deployer` → access key → *Make inactive*. (The GitHub secrets can stay as they are.)
6. **Confirm nothing is left** (repeat for both Regions):
   ```
   aws cloudformation list-stacks --region eu-central-1 --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE --query "StackSummaries[].StackName"
   aws events list-endpoints --region eu-central-1
   aws sqs list-queues --region eu-central-1
   aws dynamodb list-tables --region eu-central-1
   aws logs describe-log-groups --log-group-name-prefix /aws/lambda/orders-app --region eu-central-1
   ```
   Also look at Billing → *Bills* and *Free Tier usage* the next day. Route 53 → *Health checks* must be empty.

### 6.3 Redeploy later

Re-activate the IAM access key, re-enable the workflow, then push or run it manually – or deploy by hand with §4.4. Stack names, Regions and parameters come from `samconfig.toml`, the workflow and the GitHub variables.

---

## 7. Design notes and known limits

- **Not run end-to-end** (see the status note at the top). Parts I could not verify against a live account:
  - **Native CRT library.** `PutEvents` through a global endpoint is signed with SigV4A, which the JavaScript SDK does through `aws-crt` (native code). I expect the npm package to ship Linux prebuilt binaries, and the Lambda is set to `x86_64` to match the GitHub runner, but I could not install it here. If CreateOrder fails at cold start with a CRT / `signature-v4-crt` error, that is the place to look (build with `sam build --use-container`).
  - **Replication role permissions** (`events:PutEvents` on both buses, trust `events.amazonaws.com`) are my reading of the docs, which did not list the policy. Adjust if endpoint creation reports an access error.
  - **Dashboard dimensions** for `MatchedEvents` (`EventBusName` + `RuleName`). If a widget stays empty, open the metric in CloudWatch and copy its dimensions.
  - `package.json` uses version ranges (`^3.700.0`); run `npm install` once in `src/create-order/` and commit the generated `package-lock.json` for reproducible builds.
- **Idempotency is regional.** `orders` is a normal regional table. The same `orderId` sent to *both* Regions can be created once per Region (DynamoDB global tables use last-writer-wins, so a conditional write cannot guarantee uniqueness across Regions). Clients should retry against the same Region unless it is down.
- **At-least-once, not exactly-once.** If CreateOrder crashes after publishing but before marking the order `COMPLETED`, the retry publishes the event again. Every event carries `idempotencyKey = orderId`, and FulfilOrder ignores duplicates the same way. A stricter variant would use the transactional-outbox pattern (DynamoDB Streams → EventBridge Pipes).
- **Global endpoints give automatic *ingestion* failover plus replication; they do not route customer traffic.** For real active-active client traffic add a Route 53 latency/weighted record in front of the two API URLs (needs a domain). Event processing, however, is active in both Regions because of replication.
- **Failover/recovery time** documented by AWS for the recommended alarm settings is about 6 minutes (360 s, max 420 s). Producers can see `PutEvents` errors in that window: use retries with backoff.
- **Health-check alarm and quiet traffic.** `HealthCheckTreatMissingData=missing` (default) keeps the last known state when no events flow, so a quiet lab does not fail over. Set `breaching` only for steady production traffic.
- Order messages go through a *standard* SQS queue (at-least-once, unordered); that is why the consumer is idempotent.
- The earlier DAX / API-cache design of this project is not part of these nine steps and was removed (DAX bills per hour and needs a VPC). The previous version is still in `orders-app.zip`.
