# Project 4 – Event-Driven Orders Application (AWS SAM)

A serverless orders backend built with the AWS Serverless Application Model (SAM), deployed from your machine
and automatically from GitHub Actions on every push to `main`.

---

## 1. Project scope

| What | Detail |
|---|---|
| **Goal** | Receive orders over HTTPS, store them in DynamoDB, read them back fast, and observe everything on a dashboard. |
| **Write path** | `POST /orders` → API Gateway → Lambda → DynamoDB (`Orders` table, one row per order item, written atomically). |
| **Read path 1** | `GET /orders/{orderId}` → API Gateway → **DynamoDB directly** (no Lambda; request/response VTL mapping templates). |
| **Read path 2** | `GET /orders/{orderId}/items/{itemId}` → API Gateway → Lambda → **DAX** (falls back to plain DynamoDB when DAX is off). |
| **Performance** | API Gateway stage cache (TTL 300 s) on the GET methods; DAX in front of DynamoDB for Lambda reads. |
| **Observability** | CloudWatch dashboard (API Count, 4XXError, Lambda Invocations, Errors) + X-Ray active tracing on the Lambda. |
| **Delivery** | GitHub Actions: validate → build → deploy on every push to `main`, using AWS credentials stored as repository secrets. |

> **Assumption to confirm:** your step 8 says "configure the existing Lambda to use DAX for read operations", but the Lambda
> from step 2 only writes. I therefore gave the *same* Lambda a read route (`GET /orders/{orderId}/items/{itemId}`) that
> uses DAX. Step 7's direct DynamoDB integration (`GET /orders/{orderId}`) bypasses Lambda, so it cannot use DAX.

### AWS services used (short résumé)

| Service | Role in this project | Free-tier position (see §2) |
|---|---|---|
| **AWS SAM / CloudFormation** | Defines and deploys the whole stack from `template.yaml`. | Free (you pay for what it creates). |
| **Amazon DynamoDB** | `Orders` table, partition key `orderId`, sort key `itemId`, on-demand capacity. | Storage free up to 25 GB; **on-demand requests have no free tier** (pennies at lab volume). |
| **AWS Lambda** (Node.js 22, arm64) | Creates orders; reads items (DAX). | Always-free monthly allowance. |
| **Amazon API Gateway** (REST) | Public API, direct DynamoDB integration, stage cache. | Free-tier eligible; **the stage cache is billed hourly, no free tier**. |
| **Amazon CloudWatch** | Dashboard, Lambda logs (7-day retention), metrics. | Small free allowance. |
| **AWS X-Ray** | Active tracing on the Lambda. | Small free monthly allowance. |
| **Amazon DAX** | In-memory read cache for DynamoDB. | **No free tier – billed per node-hour.** |
| **Amazon VPC** (+ DynamoDB gateway endpoint) | Needed only because DAX lives in a VPC. No NAT gateway, no internet gateway. | Free as built here. |
| **IAM** | Least-privilege roles for Lambda, API Gateway → DynamoDB, DAX → DynamoDB. | Free. |
| **Amazon S3** | SAM stores the packaged Lambda code (managed bucket). | Pennies. |

---

## 2. AWS free tier – what you need to know

**Rule of this template:** everything runs on free-tier-friendly services **except** DAX and the API Gateway stage cache.
Those two are controlled by parameters that default to `false`, so the default deployment (steps 1-7) costs about nothing.

| Restriction / cost | What happens | How the template handles it |
|---|---|---|
| **DAX has no free tier** | Billed per node-hour (a `dax.t3.small` was about $0.04/hour in US East when I checked, roughly $29/month if left running), and it is not serverless. | `EnableDax=false` by default; single node; tear-down chapter removes it. |
| **API Gateway stage cache has no free tier** | The smallest cache (0.5 GB) bills hourly while it exists (from my memory of AWS pricing, about $0.02/hour – check the pricing page). | `EnableApiCache=false` by default. |
| **DynamoDB on-demand** | The always-free 25 RCU/WCU applies to *provisioned* tables only. On-demand requests are billed per request. | You asked for on-demand, so it is kept; at lab volume this is cents. Storage (25 GB) stays free. |
| **DAX forces a VPC** | The Lambda must run inside a VPC. A NAT gateway would cost money, so none is created; the Lambda reaches DynamoDB over a free gateway endpoint. | Built into the `EnableDax` condition. |
| **DAX build time** | Creating the cluster takes roughly 15-25 minutes. | Workflow timeout is 45 min. |
| **API cache staleness** | A new order may not show on cached GETs for up to 300 s. | Expected behaviour of the requirement. |

**Which AWS plan is your account on?** (Billing console → *Account plan*)

- **Accounts created on/after 15 July 2025** choose a *Free plan* (credit based, up to $200, ends after 6 months or when credits run out) or a *Paid plan*.
  On the Free plan, AWS describes services such as API Gateway as "Free Tier eligible", meaning usage is deducted from your credits, and some services are restricted.
  Enabling a service the Free plan does not allow can require upgrading to the Paid plan. I could **not** confirm whether DAX or the API cache are on the Free plan's allowed list, so
  check the *AWS Free Tier* page. If stack creation fails on `EnableDax=true` or `EnableApiCache=true` with a plan/permission message, that is the cause.
  Upgrading keeps your remaining credits, but charges continue after they run out.
- **Accounts created before that date** are on the legacy Free Tier (always-free services plus 12-month offers, which have probably expired).

Set a **budget alert** before you start: Billing → *Budgets* → *Create budget* → *Zero spend budget* (or $5).

---

## 3. Prerequisites

- An AWS account and an IAM user for deployments (dedicated user, access keys; AdministratorAccess is simplest for a lab).
- [AWS CLI v2](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) and
  [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)
- Node.js 22 and npm
- Git and a GitHub account

Project layout:

```
.
├── template.yaml                    # all AWS resources (steps 1-4, 7, 8)
├── samconfig.toml                   # default stack name / region for sam deploy
├── src/
│   ├── index.js                     # Lambda function (step 2, optimised in step 6)
│   └── package.json                 # esbuild (dev) + DAX client
├── .github/workflows/deploy.yml     # CI/CD (step 5)
└── README.md
```

| Step | Where it lives in the template |
|---|---|
| 1 DynamoDB `Orders` table | `OrdersTable` |
| 2 Lambda + IAM | `OrdersFunction` (inline least-privilege policies) |
| 3 REST API `POST /orders` | `OrdersApi` → `paths./orders.post` |
| 4 Dashboard + X-Ray | `OrdersDashboard`, `Globals.Function.Tracing: Active` |
| 5 GitHub Actions | `.github/workflows/deploy.yml` |
| 6 Optimisation | `OrdersFunction.Metadata` (esbuild), clients created outside the handler in `src/index.js` |
| 7 API → DynamoDB without Lambda | `ApiDynamoDbReadRole`, `paths./orders/{orderId}.get` (VTL templates) |
| 8 DAX + API cache | `DaxCluster` & VPC resources (`EnableDax`), `CacheClusterEnabled` / `MethodSettings` (`EnableApiCache`) |

---

## 4. Deploy procedure – AWS (from your computer)

### Step 4.1 – Configure the AWS CLI

```bash
aws configure                      # access key, secret key, region (e.g. eu-central-1), output json
aws sts get-caller-identity        # confirm which account you are using
```

### Step 4.2 – Install esbuild

`sam build` bundles the function with esbuild, which must be available on your `PATH`
(SAM installs the function's own dependencies, such as the DAX client, by itself):

```bash
npm install -g esbuild
esbuild --version
```

### Step 4.3 – Validate and build

```bash
sam validate --lint
sam build
```

`sam build` bundles `src/index.js` with esbuild (minified, SDK v3 left out because the Lambda runtime provides it).
Check the result size: `du -sh .aws-sam/build/OrdersFunction`.

### Step 4.4 – Deploy the free-tier-friendly stack (steps 1-7)

```bash
sam deploy --guided
```

Answer: stack name `orders-app`, your region, *confirm changes* **Y**, *allow IAM role creation* **Y**,
*save arguments to samconfig.toml* **Y**. Leave `EnableDax` and `EnableApiCache` at `false`.

When it finishes, read the outputs:

```bash
aws cloudformation describe-stacks --stack-name orders-app \
  --query "Stacks[0].Outputs[].[OutputKey,OutputValue]" --output table
export API_URL="<ApiUrl output>"
```

### Step 4.5 – Test

```bash
# create an order (Lambda -> DynamoDB)
curl -s -X POST "$API_URL/orders" -H 'Content-Type: application/json' \
  -d '{"customerId":"c-100","items":[{"itemId":"sku-1","quantity":2,"price":9.99},{"itemId":"sku-2","quantity":1,"price":24.5}]}'
# -> {"orderId":"<uuid>","itemCount":2,"createdAt":"..."}

export ORDER_ID="<orderId from above>"

# read all items of the order (API Gateway -> DynamoDB directly, no Lambda)
curl -s "$API_URL/orders/$ORDER_ID"

# read one item (Lambda; DAX when enabled)
curl -s "$API_URL/orders/$ORDER_ID/items/sku-1"
```

Then open the **DashboardUrl** output (CloudWatch → Dashboards) and *Lambda → Monitor → Traces* for X-Ray.
Direct integration check: the Lambda **Invocations** widget must not increase when you call `GET /orders/{orderId}`.

### Step 4.6 – (Optional, costs money) Enable DAX and the API cache – step 8

Read §2 first. Then:

```bash
sam deploy --parameter-overrides EnableDax=true EnableApiCache=true
```

DAX takes roughly 15-25 minutes to create. Afterwards:

```bash
curl -s "$API_URL/orders/$ORDER_ID/items/sku-1"     # response contains "source":"dax"
```

Cache check: call the same GET twice within 300 s; the second call is served from the stage cache (check `CacheHitCount` /
`CacheMissCount` for the API in CloudWatch). **Switch both back to `false` (see §6) as soon as you finish testing.**

---

## 5. Deploy procedure – GitHub (automatic deployment on every push to `main`)

### Step 5.1 – Create the repository and push

```bash
git init -b main
git add .
git commit -m "Initial commit: orders SAM application"
git remote add origin https://github.com/<your-user>/<your-repo>.git
git push -u origin main
```

(Do not commit AWS keys. The `.gitignore` already excludes `.aws-sam/` and `node_modules/`.)

### Step 5.2 – Create deployment credentials in AWS

IAM → Users → *Create user* `github-sam-deployer` → attach a policy (AdministratorAccess for a lab; tighten later) →
*Security credentials* → *Create access key* (use case: *Application running outside AWS*).

### Step 5.3 – Store them as GitHub repository secrets

Repository → *Settings → Secrets and variables → Actions → Secrets → New repository secret*:

| Secret | Value |
|---|---|
| `AWS_ACCESS_KEY_ID` | access key id |
| `AWS_SECRET_ACCESS_KEY` | secret access key |
| `AWS_REGION` | e.g. `eu-central-1` (same region as your first deployment) |

Optional *Variables* tab (not secrets): `ENABLE_DAX` and `ENABLE_API_CACHE`, `true` or `false`. If unset they default to `false`.

### Step 5.4 – What the workflow does

`.github/workflows/deploy.yml` runs on every push to `main` (and manually via *Actions → Run workflow*):

1. check out → set up Node 22 and SAM CLI → configure AWS credentials from the secrets
2. `npm install -g esbuild` → `sam validate --lint` → `sam build` (esbuild)
3. `sam deploy` – uploads the package to S3 and updates the CloudFormation stack **and** the Lambda function code
4. prints the stack outputs

Push a change and watch *Actions*. A deployment with no changes ends successfully (`--no-fail-on-empty-changeset`).

> Hardening for later: replace long-lived access keys with GitHub OIDC and an IAM role; pin actions to commit SHAs.

---

## 6. Tear down and stop all costs (keep the configuration)

Your **configuration is the code**: `template.yaml`, `src/`, `samconfig.toml`, the workflow and the GitHub secrets. None of the options below touch them, so you can redeploy at any time.

### Option A – Stop only the paid parts (stack and data stay)

Removes DAX, the VPC pieces and the API cache; the table, Lambda, API and dashboard remain (all free or cents).

```bash
sam deploy --parameter-overrides EnableDax=false EnableApiCache=false
```

Or set the GitHub variables `ENABLE_DAX` / `ENABLE_API_CACHE` to `false` and re-run the workflow.

### Option B – Delete everything in AWS (nothing left that can bill)

1. **Pause automatic deployments** so a push does not recreate the stack:
   `gh workflow disable deploy.yml` (or *Actions → Deploy SAM application → ⋯ → Disable workflow*).
2. **(Optional) back up the data** – the table is deleted with the stack:
   `aws dynamodb scan --table-name Orders --output json > orders-backup.json`
3. **Delete the stack** (API, Lambda, table, log group, dashboard, DAX, VPC – all of it):
   ```bash
   sam delete --stack-name orders-app --region <your-region>
   ```
   Answer *yes* to also delete the uploaded code in S3.
4. **Remove the SAM artifact bucket** if no other SAM project uses it: S3 console → bucket starting with
   `aws-sam-cli-managed-default-samclisourcebucket-` → *Empty* (versioned bucket, so empty it in the console) → then CloudFormation →
   delete the stack `aws-sam-cli-managed-default`.
5. **Neutralise the credentials but keep them on file:** IAM → user `github-sam-deployer` → access key → *Make inactive*.
   (Delete the key if you will not come back; the GitHub secrets can stay as they are.)
6. **Confirm nothing is running:**
   ```bash
   aws cloudformation list-stacks --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE \
     --query "StackSummaries[].StackName"
   aws dax describe-clusters --query "Clusters[].ClusterName"          # must be empty
   aws logs describe-log-groups --log-group-name-prefix /aws/lambda/orders-app
   ```
   Also look at Billing → *Bills* and *Free Tier usage* the next day; DAX and cache usage is what you want to see stop.

### Redeploy later

```bash
sam build && sam deploy            # or re-enable the workflow and push / run it manually
```

Re-activate the IAM access key first if you made it inactive. Stack name, region and parameters are remembered in `samconfig.toml` and the GitHub secrets/variables.

---

## 7. Notes and known limits

- **Not run end-to-end.** The template, workflow and function were validated for structure and logic offline, but they have not been deployed. In particular the DAX client package (`amazon-dax-client`, built on AWS SDK v2) and its endpoint format were not verified against a live cluster. If the first `EnableDax=true` run fails, check the Lambda logs first.
- **Bundle size.** The DAX client pulls in AWS SDK v2, which makes the bundle noticeably larger than a plain SDK v3 function. It is only loaded at cold start when `DAX_ENDPOINT` is set. Measure with `du -sh .aws-sam/build/OrdersFunction`.
- DynamoDB table name is fixed to `Orders` (as requested); a second copy of the stack in the same region would clash. The DAX cluster name `orders-dax` is fixed for the same reason.
- DAX is configured with one node for cost. Use three nodes across Availability Zones for production.
- Writes always go straight to DynamoDB; DAX serves reads only.
- Only `GET` responses are cached by API Gateway (300 s TTL). Cache keys are the path parameters, so each order/item is cached separately.
