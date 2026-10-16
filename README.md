# Solo Traveler Data Engineering Project

## Table of contents

- [Overview](#overview)
- [Tech Stack](#tech-stack)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Data Sources](#data-sources)
- [Getting Started](#getting-started)
- [Setup](#setup)
- [CI/CD Pipeline](#cicd-pipeline)
- [Usage](#usage)

## Overview

An AWS data pipeline that finds the cheapest last-minute vacations for a solo traveler flying out of Phoenix. It pulls travel data from four sources (flights, hotels, destination activities and exchange rates), stores the raw data, transforms it and loads it into a database where it can be queried by dashboards.

Queryable datasets are grouped by region based on each destination's latitude and longitude. Destinations in the United States and Canada are grouped into West or East regions, while destinations in Mexico are grouped into North or South regions.

### How it works

- **Flights:** Starting from Phoenix, the pipeline finds the 30 cheapest flight destinations in the US, Canada and Mexico.
- **Hotels:** For each destination extracted in the raw flight data, it pulls room rates for 18 hotel properties.
- **Destination activities:** For each destination, the Geoapify API finds activities within a set radius of the city, such as artwork, museums, viewpoints, cultural entertainment, heritage sites, outdoor activities, beaches, national parks and skiing. These activities are used to categorize each destination by vacation type, so travelers can see at a glance whether it's an adventure, beach, restaurant (food) or other kind of trip.
- **Exchange rates:** The pipeline pulls the latest USD → Mexican peso and USD → Canadian dollar rates on the day the data is extracted. Rates are only attached to international destinations.

### What you get

The transformed data is loaded into the gold layer in Amazon Aurora Serverless and queried with Athena. For each trip, it shows:

- Which vacations depart in 7 days.
- Which destinations are cheapest, based on a round-trip flight from Phoenix plus a week-long hotel stay.
- The activities available at each destination and its vacation type.
- The exchange rate, for international destinations.

## Tech Stack

- Amazon EventBridge
- AWS Step Functions
- Amazon API Gateway
- AWS Lambda
- AWS Glue
- AWS Glue Data Catalog
- AWS Glue Data Quality
- Amazon S3 (bronze and silver layer)
- Amazon Aurora Serverless (gold layer)
- Amazon Athena
- Amazon Quick Sight
- GitHub Actions (CI/CD)

## Architecture

The pipeline follows the medallion architecture: data moves through 3 layers, each one cleaner and more query-ready than the last.

| Layer      | What it holds                                                                                          | Format  |
| ---------- | ------------------------------------------------------------------------------------------------------ | ------- |
| **Bronze** | Raw API responses, exactly as returned (one S3 bucket per source)                                      | JSON    |
| **Silver** | Each source cleaned & standardized on its own (one S3 bucket per source + one combined S3 data bucket) | Parquet |
| **Gold**   | Sources are grouped by region into four analytics-ready datasets (four Aurora Serverless clusters)     | Parquet |

Every layer is partitioned by `extract_date=YYYY-MM-DD/`.

```
             EventBridge (daily schedule)    API Gateway
                         │                        │
                         └───────────┬────────────┘
                                     │
                              Step Functions
                                     │
 ── EXTRACT ─────────────────────────┼─────────────────────────────────────────────
                                     │
          ┌──────────────────────────┴───────────────┐
          │                                          │
 Lambda: extract exchange rates           Lambda: extract airplane ticket rates
          │                                          │
          │                               Lambda: extract hotel room rates
          │                                          │
          │                               Lambda: extract destination activities
          │                                          │
          ▼                                          ▼
 ┌──────────────────────────────────────────────────────────────────────────────┐
 │   BRONZE     S3 raw buckets (JSON)                                           │
 │   exchange_rates/   airplane_ticket_rates/   hotel_room_rates/               │
 │   destination_activities/                                                    │
 └──────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │
          ┌────────────────┬──────────┴──────────┬──────────────────────┐
          │                │                     │                      │
 ── TRANSFORM ─────────────┼─────────────────────┼──────────────────────┼─────────
          │                │                     │                      │
 Lambda: transform  Lambda: transform     Lambda: transform     Lambda: transform
 exchange rates     airplane ticket rates hotel room rates      destination activities
          │                │                     │                      │
          ▼                ▼                     ▼                      ▼
 ┌──────────────────────────────────────────────────────────────────────────────┐
 │ SILVER       S3 transformed buckets (Parquet), one per source                │
 │   exchange_rates/   airplane_ticket_rates/   hotel_room_rates/               │
 │   destination_activities/                                                    │
 │        │                │                     │                      │       │
 │        └────────────────┴─────────┬───────────┴──────────────────────┘       │
 │                                   │                                          │
 │─── CATALOG ───────────────────────┼──────────────────────────────────────────│
 │                                   │                                          │
 │             Glue Crawler: crawl the silver transformed buckets               │
 │                                   │                                          │
 │                                   ▼                                          │
 │──────────────────────────────────────────────────────────────────────────────│
 │ GLUE DATA CATALOG     one table per silver source                            │
 │                schemas + extract_date partitions registered                  │
 │──────────────────────────────────────────────────────────────────────────────│
 │                                   │                                          │
 │                                   │                                          │
 │                                   │                                          │
 │             Glue: transform combine (all four datasets)                      │
 │                                   │                                          │
 │                                   ▼                                          │
 │                     S3 combined bucket (Parquet)                             │
 └──────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     │
  ── LOAD ───────────────────────────┼───────────────────────────────────────────
                                     │
            Lambda: Load transformed data into Amazon Aurora Serverless
                                     │
                                     ▼
 ┌──────────────────────────────────────────────────────────────────────────────┐
 │   GOLD       Amazon Aurora Serverless combined cluster (Parquet)             │
 └──────────────────────────────────────────────────────────────────────────────┘
                                     │
 ── ANALYTICS ───────────────────────┼───────────────────────────────────────────
                                     │
                     Athena (via the Glue Data Catalog)
                                     │
                             Amazon Quick Sight
```

1. **EventBridge** runs a daily schedule that starts the Step Functions workflow, so the full ETL pipeline runs every day with no manual steps.
2. **API Gateway** exposes REST endpoints to start the pipeline on demand (for example, to backfill or test).
3. **Step Functions** orchestrates every step below.
4. **Extract → Bronze** The extract Lambdas call each API and write the raw JSON responses to the bronze S3 buckets.
   - Exchange rates run on their own.
   - Airplane ticket rates, hotel room rates and destination activities run in a chain: the hotels Lambda searches the destinations from the flight deals, and the activities Lambda searches the destinations from the hotels.
5. **Transform → Silver** One transform Lambda per source reads its bronze JSON, keeps only the fields the pipeline needs, cleans them (for example, filling missing values and standardizing types), and writes the result as Parquet to that source's silver bucket.
   - Exchange rates
   - Airplane ticket rates
   - Hotel room rates
   - Destination activities: also categorizes each destination by vacation type (adventure, beach, restaurant, etc.) based on the places and activities found there.
6. **Catalog** Once extraction finishes, a Glue Crawler crawls the bronze S3 buckets, infers each source's schema and registers it as a table (with its `extract_date` partitions) in the Glue Data Catalog, so the raw data can be discovered and queried before it is transformed.
7. **Final Transform → Silver** A final AWS Glue script joins the four silver datasets into one combined Parquet dataset, converting all prices with the exchange rates. Instead of pointing at the S3 buckets and inferring schemas from the files on every run, the script reads each dataset through its table in the Glue Data Catalog. The catalog already stores each table’s schema, S3 location and extract_date partitions, so the job works from that metadata to read only the partitions and columns it needs. This reduces the data scanned and processed, which lowers both run time and cost.
8. **Load → Gold** A load Lambda reads the combined Parquet dataset from the S3 combined bucket and writes it into the gold layer. The gold layer consists of four Amazon Aurora Serverless clusters. Each cluster corresponds to a specific region and datasets are grouped by region based on each destinations latitude and longitude. Destinations in the United States and Canada are grouped into West or East regions, while destinations in Mexico are grouped into North or South regions.
9. **Athena** queries the gold Parquet files in Amazon Aurora Serverless through the Glue Data Catalog.
10. **Amazon Quick Sight** visualizes the results.

## Project Structure

```
├── .github/
│   └── workflows/
│       ├── deploy-lambda.yaml                               # CI/CD for the Lambdas
│       └── deploy-glue.yaml                                 # CI/CD for the Glue job
├── src/
│   ├── extract/                                             # API → bronze (raw JSON)
│   │   ├── extract_exchange_rates/
│   │   ├── extract_airplane_ticket_rates/
│   │   ├── extract_hotel_room_rates/
│   │   └── extract_destination_activities/
│   ├── transformation/                                      # bronze → silver (Parquet)
│   │   ├── transform_exchange_rates/                        # (planned)
│   │   ├── transform_airplane_ticket_rates/
│   │   ├── transform_hotel_room_rates/
│   │   ├── transform_destination_activities/
│   │   └── transform_combine_airplane_hotel_destination_datasets/
│   └── load/
│       └── load_combined_dataset_aurora                     # silver → gold (Parquet)
├── tests/
│   ├── extract/                                             # one test file per extract Lambda
│   ├── transformation/                                      # one test file per transform Lambda
│   └── load/                                                # one test file per load Lambda
├── .gitignore
└── README.md
```

Each Lambda folder contains:

- `handler.py`: the Lambda code (entry point `lambda_handler`)
- `requirements.txt`: its Python dependencies
- `__init__.py`

## Data Sources

| Data                    | Source                                                                                  |
| ----------------------- | --------------------------------------------------------------------------------------- |
| Currency exchange rates | [Free Currency API](https://freecurrencyapi.com/docs/)                                  |
| Flight costs            | [SerpApi – Google Flights API](https://serpapi.com/google-flights-api)                  |
| Hotel room costs        | [SerpApi – Google Hotels API](https://serpapi.com/google-hotels-api)                    |
| Destination activities  | [Geoapify – Travel & Tourism APIs](https://www.geoapify.com/industries/travel-tourism/) |

## Getting Started

### Prerequisites

To run the code and tests on your computer:

- [Git](https://git-scm.com/downloads)
- [Python 3.14](https://www.python.org/downloads/) (the same version the CI/CD pipeline uses) and `pip`

To deploy and run the pipeline in AWS, you also need:

- An AWS account with permission to create Lambda, S3, Step Functions, EventBridge, API Gateway, Glue, Athena, Aurora Serverless, Secrets Manager and IAM resources.
- API keys listed in [setup](#setup).
- A GitHub fork of this repository, so GitHub Actions can deploy the Lambdas.

### Local setup

1. **Clone the repository.**

   ```bash
   git clone git@github.com:melgp7423/data-engineering-solo-traveler-portfolio-project.git
   cd data-engineering-solo-traveler-portfolio-project
   ```

2. **Create and activate a virtual environment.**

   ```bash
   python3.14 -m venv .venv
   source .venv/bin/activate
   ```

3. **Install the dependencies.** Each Lambda has its own `requirements.txt`. Install `pytest` plus the requirements of every Lambda:

   ```bash
   pip install pytest
   for req in src/*/*/requirements.txt; do pip install -r "$req"; done
   ```

4. **Run the tests.** The tests import the Lambdas from `src/`, so set `PYTHONPATH`:

   ```bash
   PYTHONPATH=src pytest tests
   PYTHONPATH=src pytest tests/extract/test_extract_exchange_rates.py
   ```

5. **Create a `.env` file** in the project root.

## Setup

### CI/CD Pipeline key

| Category                   | Stored in                           | Repository secret     |
| -------------------------- | ----------------------------------- | --------------------- |
| AWS IAM Role OIDC Provider | `github actions repository secrets` | `AWS_DEPLOY_ROLE_ARN` |

### API keys

The extraction Lambdas need three API keys. The keys are stored in AWS Secrets Manager. Each Lambda has an environment variable that holds the name of its secret and the Lambda reads the key from Secrets Manager when it runs.

| API            | Used by                                                     | Lambda environment variable |
| -------------- | ----------------------------------------------------------- | --------------------------- |
| SerpApi        | `extract_airplane_ticket_rates`, `extract_hotel_room_rates` | `API_KEY_SECRET`            |
| Exchange Rates | `extract_exchange_rates`                                    | `ACCESS_KEY_SECRET`         |
| Geoapify       | `extract_destination_activities`                            | `API_KEY_SECRET`            |

### AWS ARN keys

The extraction, transformation and load Lambda functions use environment variables to reference the ARNs of the AWS S3 buckets and the Aurora cluster they read from and write to. These values are kept in a local .env file and set on each Lambda function's configuration in AWS, so account-specific resource identifiers aren't exposed.

| ENV Type           | Used by                                                                                                                                                          | Environment variable                                                 |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| Lambda ARN         | `deploy-lambda.yaml`                                                                                                                                             | `AWS_EXTRACT_EXCHANGE_RATES_ARN`                                     |
| Lambda ARN         | `deploy-lambda.yaml`                                                                                                                                             | `AWS_EXTRACT_AIRPLANE_TICKET_RATES_ARN`                              |
| Lambda ARN         | `deploy-lambda.yaml`                                                                                                                                             | `AWS_EXTRACT_HOTEL_ROOM_RATES_ARN`                                   |
| Lambda ARN         | `deploy-lambda.yaml`                                                                                                                                             | `AWS_EXTRACT_DESTINATION_ACTIVITIES_ARN`                             |
| Lambda ARN         | `deploy-lambda.yaml`                                                                                                                                             | `AWS_TRANSFORM_EXCHANGE_RATES_ARN`                                   |
| Lambda ARN         | `deploy-lambda.yaml`                                                                                                                                             | `AWS_TRANSFORM_AIRPLANE_TICKET_RATES_ARN`                            |
| Lambda ARN         | `deploy-lambda.yaml`                                                                                                                                             | `AWS_TRANSFORM_HOTEL_ROOM_RATES_ARN`                                 |
| Lambda ARN         | `deploy-lambda.yaml`                                                                                                                                             | `AWS_TRANSFORM_DESTINATION_ACTIVITIES_ARN`                           |
| Glue job name      | `deploy-glue.yaml`                                                                                                                                               | `AWS_TRANSFORM_COMBINE_AIRPLANE_HOTEL_DESTINATION_DATASETS_GLUE_JOB` |
| Lambda ARN         | `deploy-lambda.yaml`                                                                                                                                             | `AWS_LOAD_COMBINED_DATASET_AURORA_ARN`                               |
| S3 BUCKET ARN      | `extract_exchange_rates`,<br>`transform_exchange_rates`,<br>`transform_combine_airplane_hotel_destination_datasets`                                              | `AWS_EXCHANGE_RATES_S3_BUCKET`                                       |
| S3 BUCKET ARN      | `extract_airplane_ticket_rates`,<br>`extract_hotel_room_rates`,<br>`transform_airplane_ticket_rates`,<br>`transform_combine_airplane_hotel_destination_datasets` | `AWS_AIRPLANE_TICKET_RATES_S3_BUCKET`                                |
| S3 BUCKET ARN      | `extract_hotel_room_rates`,<br>`extract_destination_activities`,<br>`transform_hotel_room_rates`,<br>`transform_combine_airplane_hotel_destination_datasets`     | `AWS_HOTEL_ROOM_RATES_S3_BUCKET`                                     |
| S3 BUCKET ARN      | `extract_destination_activities`,<br>`transform_destination_activities`,<br>`transform_combine_airplane_hotel_destination_datasets`                              | `AWS_DESTINATION_ACTIVITIES_S3_BUCKET`                               |
| S3 BUCKET ARN      | `transform_combine_airplane_hotel_destination_datasets`,<br>`load_gold_layer`                                                                                    | `AWS_COMBINE_AIRPLANE_HOTEL_DESTINATION_TRANSFORMED_DATA_S3_BUCKET`  |
| AURORA CLUSTER ARN | `extract_airplane_ticket_rates`,<br>`extract_hotel_room_rates` REVIEW                                                                                            | `AWS_GOLD_LAYER_AURORA_SERVERLESS_CLUSTER`                           |

Each source keeps its raw (bronze) and transformed (silver) data in the same bucket, separated by folder prefixes set on each Lambda:

| Environment variable  | Used by                                    | Purpose                                                                               |
| --------------------- | ------------------------------------------ | ------------------------------------------------------------------------------------- |
| `RAW_PREFIX`          | every `extract_*` and `transform_*` Lambda | Folder above `extract_date=` for that source's raw JSON (`""` = bucket root)          |
| `TRANSFORMED_PREFIX`  | every `transform_*` Lambda                 | Folder above `extract_date=` for that source's Parquet output                         |
| `FLIGHT_DEALS_PREFIX` | `extract_hotel_room_rates`                 | Raw prefix of the airplane ticket rates data in `AWS_AIRPLANE_TICKET_RATES_S3_BUCKET` |
| `HOTEL_PREFIX`        | `extract_destination_activities`           | Raw prefix of the hotel room rates data in `AWS_HOTEL_ROOM_RATES_S3_BUCKET`           |

## CI/CD Pipeline

The Lambda functions and the Glue job are tested and deployed automatically with GitHub Actions, through two separate workflows. Each one only runs for its own code, so a change to a Lambda never redeploys the Glue job, and the other way around.

| Workflow                                                     | Deploys                                                              | Packaged as                   |
| ------------------------------------------------------------ | -------------------------------------------------------------------- | ----------------------------- |
| [`deploy-lambda.yaml`](.github/workflows/deploy-lambda.yaml) | Every `extract_*` and `transform_*` Lambda function                  | `.zip` uploaded to AWS Lambda |
| [`deploy-glue.yaml`](.github/workflows/deploy-glue.yaml)     | The `transform_combine_airplane_hotel_destination_datasets` Glue job | Python script uploaded to S3  |

Both workflows sign in to AWS with OIDC by assuming the IAM role in the `AWS_DEPLOY_ROLE_ARN` secret.

### Lambda pipeline (`deploy-lambda.yaml`)

The Lambda workflow only tests and redeploys the Lambdas whose code or tests changed, so a change to one function never touches the others.

#### When it runs

| Trigger                          | What happens                            |
| -------------------------------- | --------------------------------------- |
| Pull request to `main`           | Tests the changed Lambdas (no deploy)   |
| Push or merge to `main`          | Tests, then deploys the changed Lambdas |
| Manual run (`workflow_dispatch`) | Tests and deploys every Lambda          |

#### How it works

```
      Pull request / push to main / manual run
                        │
                        ▼
 ┌──────────────────────────────────────────────┐
 │ 1. DETECT CHANGES                            │
 │    Which Lambda folders or tests changed?    │
 └──────────────────────────────────────────────┘
                        │
                        ▼
 ┌──────────────────────────────────────────────┐
 │ 2. TEST (CI)        one parallel job per     │
 │    pytest           changed Lambda           │
 └──────────────────────────────────────────────┘
                        │  tests pass (main only)
                        ▼
 ┌──────────────────────────────────────────────┐
 │ 3. DEPLOY (CD)      one parallel job per     │
 │    zip → AWS        changed Lambda           │
 └──────────────────────────────────────────────┘
                        │
                        ▼
        AWS Lambda functions updated with new code
```

1. **Detect changes.** [`dorny/paths-filter`](https://github.com/dorny/paths-filter) checks each Lambda's `src/` folder and its test file to build the list of Lambdas to run. Editing `deploy-lambda.yaml` itself marks every Lambda as changed. A manual run selects all of them.
2. **Test (CI).** For each changed Lambda, a matrix job sets up Python 3.14, installs `pytest` and the Lambda's `requirements.txt`, then runs its test file in `tests/`. Lambdas that don't have a test file yet are skipped instead of failing.
3. **Deploy (CD).** Runs only after the tests pass and never on pull requests. For each changed Lambda it:
   - Builds a `.zip` deployment package with the dependencies installed next to `handler.py`.
   - Signs in to AWS with OIDC by assuming the IAM role in the `AWS_DEPLOY_ROLE_ARN` secret.
   - Looks up the function's ARN from its repository secret, runs `aws lambda update-function-code` and waits until the update finishes.

Jobs use `fail-fast: false`, so one Lambda failing its tests or deploy doesn't cancel the others.

#### Adding a new Lambda to the pipeline

1. Create the folder in `src/<stage>/<lambda_folder_name>` and create the files `handler.py`, `requirements.txt` and `__init__.py`. The lambdas folder name must start with `extract_`,`transform_` or `load_`, since the workflow uses the prefix to find the folder.
2. Add its test file at `tests/<stage>/test_<lambda_name>.py`.
3. Add a repository secret with the function's ARN, for example `AWS_TRANSFORM_EXCHANGE_RATES_ARN`.
4. In `deploy-lambda.yaml`, add the Lambda to `FUNCTION_NAMES`, to the `paths-filter` filters and to the manual-run list in the `detect-changes` job.

### Glue job pipeline (`deploy-glue.yaml`)

The `transform_combine_airplane_hotel_destination_datasets` step runs as an AWS Glue job instead of a Lambda, so it has its own workflow. The Glue job itself (IAM role, workers, Glue version and job parameters) is created and configured in AWS. The pipeline only replaces the job's script.

#### When it runs

Pushes and pull requests only trigger the workflow when they change the job's `src/` folder, its test file or `deploy-glue.yaml`.

| Trigger                          | What happens                     |
| -------------------------------- | -------------------------------- |
| Pull request to `main`           | Tests the Glue job (no deploy)   |
| Push or merge to `main`          | Tests, then deploys the Glue job |
| Manual run (`workflow_dispatch`) | Tests and deploys the Glue job   |

#### How it works

```
      Pull request / push to main / manual run
                        │
                        ▼
 ┌──────────────────────────────────────────────┐
 │ 1. TEST (CI)                                 │
 │    pytest on Python 3.11 (Glue 5.0)          │
 └──────────────────────────────────────────────┘
                        │  tests pass (main only)
                        ▼
 ┌──────────────────────────────────────────────┐
 │ 2. DEPLOY (CD)                               │
 │    script → S3 location of the Glue job      │
 └──────────────────────────────────────────────┘
                        │
                        ▼
        AWS Glue job runs the new script next time
```

1. **Test (CI).** Sets up Python 3.11 (the version Glue 5.0 runs), installs `pytest` and the job's `requirements.txt`, then runs its test file in `tests/transformation/`. A missing or empty test file is skipped instead of failing.
2. **Deploy (CD).** Runs only after the tests pass and never on pull requests. It:
   - Signs in to AWS with OIDC by assuming the IAM role in the `AWS_DEPLOY_ROLE_ARN` secret.
   - Reads the job name from the `AWS_TRANSFORM_COMBINE_AIRPLANE_HOTEL_DESTINATION_DATASETS_GLUE_JOB` secret and runs `aws glue get-job` to find the S3 path the job runs its script from.
   - Saves a copy of the script under `history/<commit-sha>/` next to the live script, so a deploy can be rolled back.
   - Uploads `handler.py` over the live script.

The job definition is never changed by the pipeline. `aws glue update-job` is avoided on purpose, because it resets any setting that isn't passed to it.

#### IAM permissions for the Glue pipeline

The role in `AWS_DEPLOY_ROLE_ARN` needs an inline policy that lets it read the job and upload the script:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadGlueJobScriptLocation",
      "Effect": "Allow",
      "Action": "glue:GetJob",
      "Resource": "arn:aws:glue:us-east-2:<ACCOUNT_ID>:job/<GLUE_JOB_NAME>"
    },
    {
      "Sid": "UploadGlueJobScript",
      "Effect": "Allow",
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::<SCRIPT_BUCKET>/<SCRIPT_FOLDER>/*"
    }
  ]
}
```

## Usage

### Running the pipeline

The pipeline runs automatically. An EventBridge cron job starts the Step Functions workflow every morning at 7:00 AM. Each run extracts the latest exchange rates, flights, hotel room rates and destination activities, then transforms and loads them through the bronze, silver and gold layers. No manual steps are needed.

To run the pipeline outside the daily schedule (for example, to backfill or test), start it through the API Gateway endpoint.

### Viewing the results

When a run finishes, the new data is in the gold bucket. Athena queries it through the Glue Data Catalog and Amazon QuickSight shows the results in dashboards. Open the QuickSight dashboard to see the latest data.
