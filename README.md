# Solo Traveler Data Engineering Project

## Project Summary

An AWS data pipeline that finds the cheapest last-minute vacations for a solo traveler flying out of Phoenix. It pulls travel data from four sources (flights, hotels, destination activities and exchange rates), stores the raw data, transforms it and makes it queryable for dashboards.

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

## Data Sources

| Data                    | Source                                                                                  |
| ----------------------- | --------------------------------------------------------------------------------------- |
| Currency exchange rates | [Exchange Rates API](https://exchangeratesapi.io/)                                      |
| Flight costs            | [SerpApi – Google Flights API](https://serpapi.com/google-flights-api)                  |
| Hotel room costs        | [SerpApi – Google Hotels API](https://serpapi.com/google-hotels-api)                    |
| Destination activities  | [Geoapify – Travel & Tourism APIs](https://www.geoapify.com/industries/travel-tourism/) |

## Architecture

The pipeline follows the **medallion architecture**: data moves through 3 layers, each one cleaner and more query-ready than the last.

| Layer      | What it holds                                                                                          | Format  |
| ---------- | ------------------------------------------------------------------------------------------------------ | ------- |
| **Bronze** | Raw API responses, exactly as returned (one S3 bucket per source)                                      | JSON    |
| **Silver** | Each source cleaned & standardized on its own (one S3 bucket per source + one combined S3 data bucket) | Parquet |
| **Gold**   | All sources combined into one analytics-ready dataset (a single Aurora Serverless cluster)             | Parquet |

Every layer is partitioned by `ingest_date=YYYY-MM-DD/`.

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
 │ BRONZE   S3 raw buckets (JSON)                                               │
 │   exchange_rates/   airplane_ticket_rates/   hotel_room_rates/               │
 │   destination_activities/                                                    │
 └──────────────────────────────────────────────────────────────────────────────┘
          │                │                     │                      │
 ── TRANSFORM ─────────────┼─────────────────────┼──────────────────────┼─────────
          │                │                     │                      │
 Lambda: transform  Lambda: transform     Lambda: transform     Lambda: transform
 exchange rates     airplane ticket rates hotel room rates      destination activities
          │                │                     │                      │
          ▼                ▼                     ▼                      ▼
 ┌──────────────────────────────────────────────────────────────────────────────┐
 │ SILVER   S3 transformed buckets (Parquet), one per source                    │
 │   exchange_rates/   airplane_ticket_rates/   hotel_room_rates/               │
 │   destination_activities/                                                    │
 │        │                │                     │                      │       │
 │        └────────────────┴─────────┬───────────┴──────────────────────┘       │
 │                                   │                                          │
 │             Lambda: transform combine (all four datasets)                    │
 │                                   │                                          │
 │                                   ▼                                          │
 │                     S3 combined bucket (Parquet)                             │
 └──────────────────────────────────────────────────────────────────────────────┘
                                     │
            Lambda: Load transformed data into Amazon Aurora Serverless
                                     │
                                     ▼
 ┌──────────────────────────────────────────────────────────────────────────────┐
 │ GOLD     Amazon Aurora Serverless combined cluster (Parquet)                 │
 └──────────────────────────────────────────────────────────────────────────────┘
                                     │
 ── ANALYTICS ───────────────────────┼─────────────────────────────────────────────
                                     │
                     Athena (via the Glue Data Catalog)
                                     │
                             Amazon Quick Suite
```

1. **EventBridge** runs a daily schedule that starts the Step Functions workflow, so the full ETL pipeline runs every day with no manual steps.
2. **API Gateway** exposes REST endpoints to start the pipeline on demand (for example, to backfill or test).
3. **Step Functions** orchestrates every step below.
4. **Extract → Bronze.** The extract Lambdas call each API and write the raw JSON responses to the bronze S3 buckets.
   - Exchange rates run on their own.
   - Airplane ticket rates, hotel room rates and destination activities run in a chain: the hotels Lambda searches the destinations from the flight deals, and the activities Lambda searches the destinations from the hotels.
5. **Transform → Silver.** One transform Lambda per source reads its bronze JSON, keeps only the fields the pipeline needs, cleans them (for example, filling missing values and standardizing types), and writes the result as **Parquet** to that source's silver bucket.
   - Exchange rates
   - Airplane ticket rates
   - Hotel room rates
   - Destination activities: also categorizes each destination by vacation type (adventure, beach, restaurant, etc.) based on the places and activities found there.

   A final transform Lambda joins the four silver datasets (converting prices with the exchange rates) into one combined **Parquet** dataset.

6. **Load → Gold.** A load Lambda reads the combined **Parquet** dataset from the S3 combined bucket and writes it into the gold layer, an **Amazon Aurora Serverless** cluster. This gives downstream analytics one curated, query-ready table of flight, hotel, destination activities and currency-converted price data for each travel location. Aurora Serverless scales capacity up and down with the daily load.
7. **Athena** queries the gold Parquet files in Amazon Aurora Serverless through the Glue Data Catalog.
8. **Amazon Quick Suite** visualizes the results.

## Tech Stack

- Amazon EventBridge
- AWS Lambda (Python, with `pyarrow` for Parquet)
- AWS Step Functions
- Amazon API Gateway
- Amazon S3 (bronze and silver layer)
- Amazon Aurora Serverless (gold layer)
- AWS Glue Data Catalog
- Amazon Athena
- Amazon Quick Suite

## Project Structure

```
├── .github/
│   └── workflows/
│       └── deploy.yaml                                      # CI/CD
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

## Setup

### CI/CD Pipeline key

| Category                   | Stored in                           | Repository secret |
| -------------------------- | ----------------------------------- | ----------------- |
| AWS IAM Role OIDC Provider | `github actions repository secrets` | `AWS_DEPLOY_ARN`  |

### API keys

The extraction Lambdas need three API keys. The keys are stored in **AWS Secrets Manager**. Each Lambda has an environment variable that holds the name of its secret and the Lambda reads the key from Secrets Manager when it runs.

| API            | Used by                                                     | Lambda environment variable |
| -------------- | ----------------------------------------------------------- | --------------------------- |
| SerpApi        | `extract_airplane_ticket_rates`, `extract_hotel_room_rates` | `API_KEY_SECRET`            |
| Exchange Rates | `extract_exchange_rates`                                    | `ACCESS_KEY_SECRET`         |
| Geoapify       | `extract_destination_activities`                            | `API_KEY_SECRET`            |

### AWS ARN keys

The extraction, transformation and load Lambda functions use environment variables to reference the ARNs of the AWS S3 buckets and the Aurora cluster they read from and write to. These values are kept in a local .env file and set on each Lambda function's configuration in AWS, so account-specific resource identifiers aren't exposed.

| ENV Type           | Used by                                                                                              | Environment variable                                                |
| ------------------ | ---------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| Lambda ARN         | `deploy.yaml`                                                                                        | `AWS_EXTRACT_EXCHANGE_RATES_ARN`                                    |
| Lambda ARN         | `deploy.yaml`                                                                                        | `AWS_EXTRACT_AIRPLANE_TICKET_RATES_ARN`                             |
| Lambda ARN         | `deploy.yaml`                                                                                        | `AWS_EXTRACT_HOTEL_ROOM_RATES_ARN`                                  |
| Lambda ARN         | `deploy.yaml`                                                                                        | `AWS_EXTRACT_DESTINATION_ACTIVITIES_ARN`                            |
| Lambda ARN         | `deploy.yaml`                                                                                        | `AWS_TRANSFORM_EXCHANGE_RATES_ARN`                                  |
| Lambda ARN         | `deploy.yaml`                                                                                        | `AWS_TRANSFORM_AIRPLANE_TICKET_RATES_ARN`                           |
| Lambda ARN         | `deploy.yaml`                                                                                        | `AWS_TRANSFORM_HOTEL_ROOM_RATES_ARN`                                |
| Lambda ARN         | `deploy.yaml`                                                                                        | `AWS_TRANSFORM_DESTINATION_ACTIVITIES_ARN`                          |
| Lambda ARN         | `deploy.yaml`                                                                                        | `AWS_TRANSFORM_COMBINE_AIRPLANE_HOTEL_DESTINATION_DATASETS_ARN`     |
| Lambda ARN         | `deploy.yaml`                                                                                        | `AWS_LOAD_COMBINED_DATASET_AURORA_ARN`                              |
| S3 BUCKET ARN      | `extract_exchange_rates`,<br>`transform_exchange_rates`                                              | `AWS_EXCHANGE_RATES_RAW_DATA_S3_BUCKET`                             |
| S3 BUCKET ARN      | `extract_airplane_ticket_rates`,<br>`extract_hotel_room_rates`,<br>`transform_airplane_ticket_rates` | `AWS_AIRPLANE_TICKET_RAW_DATA_S3_BUCKET`                            |
| S3 BUCKET ARN      | `extract_hotel_room_rates`,<br>`extract_destination_activities`,<br>`transform_hotel_room_rates`     | `AWS_HOTEL_ROOM_RATES_RAW_DATA_S3_BUCKET`                           |
| S3 BUCKET ARN      | `extract_destination_activities`,<br>`transform_destination_activities`                              | `AWS_DESTINATION_ACTIVITIES_RAW_DATA_S3_BUCKET`                     |
| S3 BUCKET ARN      | `transform_exchange_rates`,<br>`transform_combine_airplane_hotel_destination_datasets`               | `AWS_EXCHANGE_RATES_TRANSFORMED_DATA_S3_BUCKET`                     |
| S3 BUCKET ARN      | `transform_airplane_ticket_rates`,<br>`transform_combine_airplane_hotel_destination_datasets`        | `AWS_AIRPLANE_TICKET_TRANSFORMED_DATA_S3_BUCKET`                    |
| S3 BUCKET ARN      | `transform_hotel_room_rates`,<br>`transform_combine_airplane_hotel_destination_datasets`             | `AWS_HOTEL_ROOM_RATES_TRANSFORMED_DATA_S3_BUCKET`                   |
| S3 BUCKET ARN      | `transform_destination_activities`,<br>`transform_combine_airplane_hotel_destination_datasets`       | `AWS_DESTINATION_ACTIVIES_TRANSFORMED_DATA_S3_BUCKET`               |
| S3 BUCKET ARN      | `transform_combine_airplane_hotel_destination_datasets`,<br>`load_gold_layer`                        | `AWS_COMBINE_AIRPLANE_HOTEL_DESTINATION_TRANSFORMED_DATA_S3_BUCKET` |
| AURORA CLUSTER ARN | `extract_airplane_ticket_rates`,<br>`extract_hotel_room_rates` REVIEW                                | `AWS_GOLD_LAYER_AURORA_SERVERLESS_CLUSTER`                          |

## Usage

### Running the pipeline

The pipeline runs automatically. An EventBridge cron job starts the Step Functions workflow every morning at 7:00 AM. Each run extracts the latest exchange rates, flights, hotel room rates and destination activities, then transforms and loads them through the bronze, silver and gold layers. No manual steps are needed.

To run the pipeline outside the daily schedule (for example, to backfill or test), start it through the API Gateway endpoint.

### Viewing the results

When a run finishes, the new data is in the gold bucket. Athena queries it through the Glue Data Catalog and Amazon QuickSight shows the results in dashboards. Open the QuickSight dashboard to see the latest data.
