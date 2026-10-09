# Solo Traveler Data Engineering Project

A simple AWS data pipeline that pulls travel data from four sources: exchange rates, flights, hotels and destination activities. It stores the raw data, transforms it, and makes it queryable for dashboards.

The activities data is used to categorize each vacation destination by the main types of activities it offers, so travelers can see at a glance whether it's an adventure, beach, restaurant (food) or other kind of vacation.

## Data Sources

| Data                    | Source                                                                                  |
| ----------------------- | --------------------------------------------------------------------------------------- |
| Currency exchange rates | [Exchange Rates API](https://exchangeratesapi.io/)                                      |
| Flight costs            | [SerpApi – Google Flights API](https://serpapi.com/google-flights-api)                  |
| Hotel room costs        | [SerpApi – Google Hotels API](https://serpapi.com/google-hotels-api)                    |
| Destination activities  | [Geoapify – Travel & Tourism APIs](https://www.geoapify.com/industries/travel-tourism/) |

## Architecture

```
EventBridge (daily schedule)    API Gateway
            │                        │
            └───────────┬────────────┘
                        │
Step Functions ──┬── Lambda: exchange rates ──┐
                 ├── Lambda: flights ─────────┤
                 ├── Lambda: hotels ──────────┼──► S3 (raw)
                 └── Lambda: activities ──────┘
                                                    │
                                          Lambda: transform
                                                    │
                                                DynamoDB
                                                    │
                                                 Athena
                                                    │
                                            Amazon Quick Suite
```

1. **EventBridge** runs a daily schedule that starts the Step Functions workflow, so the full ETL pipeline (extract, transform, load) runs every day with no manual steps.
2. **API Gateway** exposes REST endpoints to start ingestion on demand (for example, to backfill or test).
3. **Step Functions** runs the four ingestion Lambda functions in parallel.
4. **Lambda (ingest)** calls each API and writes the raw responses to **S3**.
5. **Lambda (transform)** cleans and standardizes the raw data, and categorizes each destination by vacation type (adventure, beach, restaurant, etc.) based on the places and activities found there.
6. **DynamoDB** stores the transformed data.
7. **Athena** queries the data (via the Athena DynamoDB connector).
8. **Amazon Quick Suite** visualizes the results.

## Tech Stack

- Amazon EventBridge
- AWS Lambda
- AWS Step Functions
- Amazon API Gateway
- Amazon S3
- Amazon DynamoDB
- Amazon Athena
- Amazon Quick Suite

## Project Structure

```
.
├── .github/     # CI/CD workflows
├── src/         # Lambda function code
├── tests/       # Tests
└── README.md
```

## Setup

### API keys

The ingestion Lambdas need three API keys. The keys are stored in **AWS Secrets Manager**. Each Lambda has an environment variable that holds the name of its secret and the Lambda reads the key from Secrets Manager when it runs.

| API            | Used by                                                   | Lambda environment variable |
| -------------- | --------------------------------------------------------- | --------------------------- |
| SerpApi        | `ingest_airplane_ticket_rates`, `ingest_hotel_room_rates` | `API_KEY_SECRET`            |
| Exchange Rates | `ingest_exchange_rates`                                   | `ACCESS_KEY_SECRET`         |
| Geoapify       | `ingest_destination_activities`                           | `API_KEY_SECRET`            |

## Usage

_TODO: how to trigger the pipeline and view results._
