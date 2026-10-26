# Solo Traveler Data Engineering Project

## Table of Contents

- [Overview](#overview)
- [QuickSight Results](#quicksight-results)
- [Tech Stack](#tech-stack)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Data Sources](#data-sources)
- [Getting Started](#getting-started)
- [Setup](#setup)
- [CI/CD Pipeline](#cicd-pipeline)
- [Athena/Aurora DSQL Connector](#athenaaurora-dsql-connector)

## Overview

An AWS data pipeline that finds the cheapest last-minute vacations for a solo traveler flying out of Phoenix. It pulls travel data from four sources (flights, hotels, destination activities and exchange rates), stores the raw data, transforms it and loads it into a database where it can be queried by dashboards.

Queryable datasets are grouped by region based on each destination's latitude and longitude. Destinations in the United States and Canada are grouped into West or East regions, while destinations in Mexico are grouped into North or South regions.

### How it works

- **Flights:** Starting from Phoenix, the pipeline finds the 30 cheapest flight destinations in the US, Canada and Mexico.
- **Hotels:** For each destination extracted in the raw flight data, it pulls room rates for 18 hotel properties.
- **Destination activities:** For each destination, the Geoapify API finds activities within a set radius of the city, such as artwork, museums, viewpoints, cultural entertainment, heritage sites, outdoor activities, beaches, national parks and skiing.
- **Exchange rates:** The pipeline pulls the latest USD → Mexican peso and USD → Canadian dollar rates on the day the data is extracted. Rates are only attached to international destinations.

### What you get

The transformed data is loaded into the gold layer in Amazon Aurora Serverless and queried with Athena. For each trip, it shows:

- Which vacations depart in 7 days.
- Which destinations are cheapest, based on a round-trip flight from Phoenix plus a week-long hotel stay.
- The activities available at each destination.
- The exchange rate, for international destinations.

## QuickSight Results

![QuickSight Results](docs/images/Results_Cheapest_Vacations_Analysis.png)

### Dashboard Overview

This Amazon QuickSight dashboard presents travel cost data extracted on October 23, 2026.

Flight and hotel rates reflect a departure/check-in date of October 30, 2026 and a return/check-out date of November 6, 2026 with Phoenix set as the origin.

### Map Visualizations

Each destination is plotted as a point sized by its base vacation cost which includes a round-trip coach flight plus a week long stay at a 3, 4 or 5-star hotel. Larger markers indicate higher costs. Results are segmented by region so users can easily filter by personal preference, such as avoiding winter conditions in the eastern U.S.

### Summary Tables

The summary tables below the maps rank destinations by total cost in ascending order, identify the lowest-priced hotel that meets the selection criteria and list suggested activities for each location.

## Tech Stack

- Amazon EventBridge
- AWS Step Functions
- Amazon API Gateway
- AWS Lambda
- AWS Glue PySpark Scripts
- AWS Glue Data Catalog
- AWS Glue Data Quality
- Amazon S3 (bronze and silver layer)
- Amazon Aurora Serverless (gold layer)
- Amazon Athena
- Amazon Quick Sight
- GitHub Actions (CI/CD)

## Architecture

The pipeline follows the medallion architecture.
Data moves through 3 layers, each one cleaner and more query-ready than the last.

| Layer      | What it holds                                                                                          | Format  |
| ---------- | ------------------------------------------------------------------------------------------------------ | ------- |
| **Bronze** | Raw API responses, exactly as returned (one S3 bucket per source)                                      | JSON    |
| **Silver** | Each source cleaned & standardized on its own (one S3 bucket per source + one combined S3 data bucket) | Parquet |
| **Gold**   | Sources are grouped by region into four analytics-ready datasets (four Aurora Serverless clusters)     | Parquet |

### Partitions

Every layer is partitioned by `extract_date=YYYY-MM-DD/`.

![Data Engineering Architecture Diagram](docs/images/Data_Engineering_Diagram.svg)

1. **EventBridge** runs a daily schedule that starts the Step Functions workflow allowing the full ETL pipeline to run every morning.
2. **API Gateway** exposes REST API endpoints to start the pipeline.
3. **Step Functions** orchestrates the ETL workflow.
4. **Extract → Bronze** The extract Lambdas call each API and write the raw JSON responses to the bronze S3 buckets. The lambda functions are coordinated to run in parallel.
   - Exchange rates run on their own.
   - Airplane ticket rates, hotel room rates and destination activities run in a chain. The hotels Lambda searches the destinations from the flight deals data and the destination activities Lambda searches the destinations data from the hotels.
5. **Transform → Silver** One transform Lambda per source reads its bronze JSON, keeps only the fields the pipeline needs, cleans all the data and writes the result as Parquet to that source's silver bucket.
   - Exchange rates
   - Airplane ticket rates
   - Hotel room rates
   - Destination activities
6. **Catalog** Once extraction finishes, a Glue Crawler crawls the bronze S3 buckets, infers each source's schema and registers it as a table (with its `extract_date` partitions) in the Glue Data Catalog, so the raw data can be discovered and queried before it is transformed.
7. **Data Quality Checks** Once each crawler has finished cataloging its data in the four silver S3 buckets (Exchange Rates, Airplane Ticket Rates, Hotel Room Rates and Destination Activities), a dedicated data quality check runs against each dataset before the final transformation script executes.

   These checks are orchestrated through AWS Step Functions, which decouples orchestration from the processing logic. This keeps the pipeline easy to scale and maintain as new sources or rules are added.

   Each dataset has its own response to a failed ruleset based on how much its data affects the final analysis.

| Crawler                                    | On Failure                                         | Rationale                                                                                                                                                                                                   |
| ------------------------------------------ | -------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `transform-exchange-rates-crawler`         | Log and continue                                   | The exchange rate API returns a very small dataset, so a failure has minimal impact downstream.                                                                                                             |
| `transform-airplane-ticket-rates-crawler`  | Alert and continue if more than 5% of records fail | Ticket prices directly drive the trip cost calculations, so significant data loss needs visibility.                                                                                                         |
| `transform-hotel-room-rates-crawler`       | Alert and continue if more than 5% of records fail | Hotel prices are the other direct input to the trip cost calculations.                                                                                                                                      |
| `transform-destination-activities-crawler` | Log and continue                                   | This dataset is large but inherently messy. Because so many activities are extracted, a small number of bad records won't meaningfully affect the analysis and plenty of valid activities remain available. |

<hr />

8. **Final Transform → Silver** A final AWS Glue job script joins the four silver datasets into one combined Parquet dataset. Instead of pointing at the S3 buckets and inferring schemas from the files on every run, the script reads each dataset through its table in the Glue Data Catalog.
9. **Catalog** Once the final transform Glue job script joins the datasets and sets it into it's S3 bucket within the silver layer, a Glue Crawler crawls the final S3 bucket to infer its schema and register it as a table in the Glue Data Catalog.

10. **Final Data Quality Check** This data quality check runs against the combined dataset in the final silver-layer S3 bucket using a fail fast rule. Fail fast is used here because the combined dataset feeds directly into the Glue job that loads data into the gold layer which is organized into four clusters. A critical schema issue or data anomaly at this stage could corrupt the downstream analysis in the gold tier or cause records to be categorized into the wrong gold-layer cluster. These errors would propagate silently and be difficult to unwind once in the gold layer so stopping the pipeline is safer than continuing with bad data.

11. **Load → Gold** A load Glue job script reads the combined Parquet dataset metadata from the Glue Data Catalog and writes it into the gold layer. The gold layer consists of four Amazon Aurora Serverless clusters. Each cluster corresponds to a specific region and datasets are grouped by region based on each destinations latitude and longitude. Destinations in the United States and Canada are grouped into West or East regions, while destinations in Mexico are grouped into North or South regions.
12. **Athena** queries the gold Parquet files in Amazon Aurora Serverless through the Glue Data Catalog.
13. **Amazon Quick Sight** visualizes the results.

## Project Structure

```
├── .github/
│   └── workflows/
│       ├── deploy-lambda.yaml                               # CI/CD for the Lambdas
│       └── deploy-glue.yaml                                 # CI/CD for the Glue job
├── docs/
│   └── images/
│       ├── Data_Engineering_Diagram.svg                     # README.md ETL Architecture Diagram
│       ├── Athena_Aurora_Connector_Diagram.svg              # README.md Athena Aurora Federated Query Connector Diagram
│       └── Results_Cheapest_Vacations_Analysis.png          # README.md QuickSight Visualization
├── src/
│   ├── extract/                                             # API → bronze (raw JSON)
│   │   ├── extract_exchange_rates/
│   │   ├── extract_airplane_ticket_rates/
│   │   ├── extract_hotel_room_rates/
│   │   └── extract_destination_activities/
│   ├── transformation/                                      # bronze → silver (Parquet)
│   │   ├── transform_exchange_rates/
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
- [Python 3.14](https://www.python.org/downloads/) and `pip`

To deploy and run the pipeline in AWS, you also need:

- An AWS account with permission to create Lambda, S3, Step Functions, EventBridge, API Gateway, Glue, Athena, Aurora Serverless, Secrets Manager and IAM resources.
- API keys listed in [setup](#setup).
- A GitHub fork of this repository.

### Local setup

1. **Clone the repository**

   ```bash
   git clone git@github.com:melgp7423/data-engineering-solo-traveler-portfolio-project.git
   cd data-engineering-solo-traveler-portfolio-project
   ```

2. **Create and activate a virtual environment**

   ```bash
   python3.14 -m venv .venv
   source .venv/bin/activate
   ```

3. **Install the dependencies** Each Lambda has its own `requirements.txt`. Install `pytest` plus the requirements of every Lambda:

   ```bash
   pip install pytest
   for req in src/*/*/requirements.txt; do pip install -r "$req"; done
   ```

4. **Run the tests** The tests import the Lambdas from `src/`, so set `PYTHONPATH`:

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

Account-specific identifiers are never hard-coded. The deploy workflows read them from GitHub Actions repository secrets, the Lambdas read them from environment variables and the Glue jobs read them from job parameters.

#### Deploy targets

| Type       | Used by              | Repository secret                                                    |
| ---------- | -------------------- | -------------------------------------------------------------------- |
| Lambda ARN | `deploy-lambda.yaml` | `AWS_EXTRACT_EXCHANGE_RATES_ARN`                                     |
| Lambda ARN | `deploy-lambda.yaml` | `AWS_EXTRACT_AIRPLANE_TICKET_RATES_ARN`                              |
| Lambda ARN | `deploy-lambda.yaml` | `AWS_EXTRACT_HOTEL_ROOM_RATES_ARN`                                   |
| Lambda ARN | `deploy-lambda.yaml` | `AWS_EXTRACT_DESTINATION_ACTIVITIES_ARN`                             |
| Lambda ARN | `deploy-lambda.yaml` | `AWS_TRANSFORM_EXCHANGE_RATES_ARN`                                   |
| Lambda ARN | `deploy-lambda.yaml` | `AWS_TRANSFORM_AIRPLANE_TICKET_RATES_ARN`                            |
| Lambda ARN | `deploy-lambda.yaml` | `AWS_TRANSFORM_HOTEL_ROOM_RATES_ARN`                                 |
| Lambda ARN | `deploy-lambda.yaml` | `AWS_TRANSFORM_DESTINATION_ACTIVITIES_ARN`                           |
| S3 bucket  | `deploy-lambda.yaml` | `AWS_LAMBDA_DEPLOY_ARTIFACTS_S3_BUCKET` (bucket name or ARN)         |
| Glue job   | `deploy-glue.yaml`   | `AWS_TRANSFORM_COMBINE_AIRPLANE_HOTEL_DESTINATION_DATASETS_GLUE_JOB` |
| Glue job   | `deploy-glue.yaml`   | `AWS_LOAD_GOLD_LAYER_GLUE_JOB`                                       |

#### S3 buckets

| Used by                                                                                              | Environment variable                   |
| ---------------------------------------------------------------------------------------------------- | -------------------------------------- |
| `extract_exchange_rates`,<br>`transform_exchange_rates`                                              | `AWS_EXCHANGE_RATES_S3_BUCKET`         |
| `extract_airplane_ticket_rates`,<br>`extract_hotel_room_rates`,<br>`transform_airplane_ticket_rates` | `AWS_AIRPLANE_TICKET_RATES_S3_BUCKET`  |
| `extract_hotel_room_rates`,<br>`extract_destination_activities`,<br>`transform_hotel_room_rates`     | `AWS_HOTEL_ROOM_RATES_S3_BUCKET`       |
| `extract_destination_activities`,<br>`transform_destination_activities`                              | `AWS_DESTINATION_ACTIVITIES_S3_BUCKET` |

#### Bucket Prefixes

| Environment variable  | Used by                                    | Purpose                                                                               |
| --------------------- | ------------------------------------------ | ------------------------------------------------------------------------------------- |
| `RAW_PREFIX`          | every `extract_*` and `transform_*` Lambda | Folder above `extract_date=` for that source's raw JSON (`""` = bucket root)          |
| `TRANSFORMED_PREFIX`  | every `transform_*` Lambda                 | Folder above `extract_date=` for that source's Parquet output                         |
| `FLIGHT_DEALS_PREFIX` | `extract_hotel_room_rates`                 | Raw prefix of the airplane ticket rates data in `AWS_AIRPLANE_TICKET_RATES_S3_BUCKET` |
| `HOTEL_PREFIX`        | `extract_destination_activities`           | Raw prefix of the hotel room rates data in `AWS_HOTEL_ROOM_RATES_S3_BUCKET`           |

## CI/CD Pipeline

The Lambda functions and the Glue job are tested and deployed automatically with GitHub Actions through two separate workflows.

| Workflow                                                     | Deploys                                                              | Packaged as                   |
| ------------------------------------------------------------ | -------------------------------------------------------------------- | ----------------------------- |
| [`deploy-lambda.yaml`](.github/workflows/deploy-lambda.yaml) | Every `extract_*` and `transform_*` Lambda function                  | `.zip` uploaded to AWS Lambda |
| [`deploy-glue.yaml`](.github/workflows/deploy-glue.yaml)     | The `transform_combine_airplane_hotel_destination_datasets` Glue job | Python script uploaded to S3  |
| [`deploy-glue.yaml`](.github/workflows/deploy-glue.yaml)     | The `load_gold_layer` Glue job                                       | Python script uploaded to S3  |

### Lambda pipeline (`deploy-lambda.yaml`)

The Lambda workflow only tests and redeploys the Lambdas whose code or tests changed, so a change to one function never touches the others.

### Glue job pipeline (`deploy-glue.yaml`)

The `transform_combine_airplane_hotel_destination_datasets` and the `load_gold_layer` steps run as AWS Glue jobs so it has its own workflow.

## Athena/Aurora DSQL Connector

Athena queries the four Aurora DSQL clusters live through a federated query connector. Everything is created by `setup-athena-dsql.sh` and is ran from AWS CloudShell. It deploys a CloudFormation stack called `athena-dsql` in a private mode with VPC endpoints and security groups.

The connector runs inside the VPC and uses only VPC endpoints via PrivateLink so traffic never crosses the internet and a cluster policy can reject any connection from outside the VPC.

### Architecture (private mode)

![Athena Aurora Connector Architecture Diagram](docs/images/Athena_Aurora_Connector_Diagram.svg)

### Components

| Component                                                                                                        | What it does                                                                                                                                                                                                                                                                                                                                                                |
| ---------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Athena data sources** `dsql_west`, `dsql_east`, `dsql_north`, `dsql_south`                                     | Catalogs of type `LAMBDA` that point Athena at the connector. Pick one in the query editor's Data source dropdown.                                                                                                                                                                                                                                                          |
| **Connector Lambda** (`athena-dsql-ConnectorFunction-…`)                                                         | AWS's open-source Athena PostgreSQL connector (`PostGreSqlMuxCompositeHandler`, Java 17). DSQL speaks the PostgreSQL wire protocol, so the standard connector works. One function serves both catalogs via `dsql_west_connection_string` / `dsql_east_connection_string`. Needs `JAVA_TOOL_OPTIONS=--add-opens=java.base/java.nio=ALL-UNNAMED` for Apache Arrow on Java 17. |
| **Secrets** `athena-dsql/dsql_west`, `athena-dsql/dsql_east`, `athena-dsql/dsql_north`, `athena-dsql/dsql_south` | Hold `{"username":"admin","password":"<IAM token>"}` per cluster. The connector reads them at connect time (cached about 60 s).                                                                                                                                                                                                                                             |
| **Token refresher Lambda** + **EventBridge rule**                                                                | DSQL has no static passwords, only short-lived IAM auth tokens. Every 5 minutes this function generates a fresh `DbConnectAdmin` token that is valid 1 hour for each cluster's hostname and writes it into the secret.                                                                                                                                                      |
| **Spill bucket**                                                                                                 | Private, encrypted S3 bucket for result sets too large to return directly. Objects expire after 1 day.                                                                                                                                                                                                                                                                      |
| **Code bucket** `athena-dsql-code-<account>-<region>`                                                            | Holds the connector jar that Lambda loads.                                                                                                                                                                                                                                                                                                                                  |

### Private networking

| Resource                                               | Purpose                                                                                                                                                                                       |
| ------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Security group** `athena-dsql-connector`             | Attached to the connector Lambda. Outbound only; no inbound rules.                                                                                                                            |
| **Security group** `athena-dsql-endpoints`             | Attached to endpoints the script creates. Allows TCP 443 and 5432 **only from** `athena-dsql-connector`.                                                                                      |
| **DSQL interface endpoint**                            | Private connection to the clusters. The connector uses the PrivateLink hostname `<cluster-id>.<service-id>.<region>.on.aws`. The script adds the connector to this endpoint's security group. |
| **Secrets Manager** and **Athena** interface endpoints | Let the connector read secrets and check query status privately.                                                                                                                              |
| **S3 gateway endpoint**                                | Lets the connector write to the spill bucket privately.                                                                                                                                       |

There's no NAT gateway or internet route. Every interface endpoint must have private DNS enabled; the script checks this and stops if it's off.

### Request flow

1. A query runs in Athena against `dsql_west`, `dsql_east`, `dsql_north`, `dsql_south`.
2. Athena invokes the connector Lambda with the catalog name.
3. The connector reads that catalog's secret (through the Secrets Manager endpoint) and connects to the cluster's PrivateLink hostname on port 5432 (database `postgres`, `sslmode=require`), using the current token as the password.
4. DSQL checks the token's IAM signature and the cluster resource policy (connection must come from the VPC).
5. The connector reads metadata and rows, pushes filters down to DSQL where it can, and returns results to Athena, through the spill bucket for large results.
