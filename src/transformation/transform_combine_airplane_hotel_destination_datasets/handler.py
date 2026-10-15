import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.context import SparkContext
from datetime import datetime
from zoneinfo import ZoneInfo
from itertools import chain


def MyTransform(glueContext, dfc) -> DynamicFrameCollection:

    # CONFIG

    FLIGHTS_PRICE_CURRENCY = "USD"

    DESTINATION_COUNTRY_SUFFIXES = {
        "united_states_of_america": "US",
        "united_states": "US",
        "usa": "US",
        "canada": "CA",
        "mexico": "MX",
    }

    CITY_ALIASES = {
        "washington dc": "washington",
        "saint george": "st george",
        "new york city": "new york",
        "nyc": "new york",
    }

    KNOWN_CITY_COUNTRY = {
        # Canada
        "vancouver": "CA",
        "toronto": "CA",
        "montreal": "CA",
        "calgary": "CA",
        "edmonton": "CA",
        "ottawa": "CA",
        "quebec city": "CA",
        "victoria": "CA",
        "winnipeg": "CA",
        "halifax": "CA",
        "kelowna": "CA",
        # Mexico
        "cancun": "MX",
        "mexico city": "MX",
        "guadalajara": "MX",
        "monterrey": "MX",
        "puerto vallarta": "MX",
        "cabo san lucas": "MX",
        "san jose del cabo": "MX",
        "los cabos": "MX",
        "mazatlan": "MX",
        "tijuana": "MX",
        "hermosillo": "MX",
        "oaxaca": "MX",
        "merida": "MX",
        "puerto penasco": "MX",
        "tulum": "MX",
        "cozumel": "MX",
        "la paz": "MX",
        "guaymas": "MX",
        "manzanillo": "MX",
    }
    DEFAULT_COUNTRY = "US"

    COUNTRY_NAME_TO_ISO = {
        "united states": "US",
        "united states of america": "US",
        "usa": "US",
        "us": "US",
        "canada": "CA",
        "ca": "CA",
        "mexico": "MX",
        "mx": "MX",
    }
    ISO_TO_CURRENCY = {"US": "USD", "CA": "CAD", "MX": "MXN"}

    ACCENTS_FROM = "áàâäãéèêëíìîïóòôöõúùûüñçÁÀÂÄÃÉÈÊËÍÌÎÏÓÒÔÖÕÚÙÛÜÑÇ"
    ACCENTS_TO = "aaaaaeeeeiiiiooooouuuuncAAAAAEEEEIIIIOOOOOUUUUNC"

    NODE_KEYWORDS = {
        "fx": ["exchange"],
        "flights": ["airplane", "ticket", "flight"],
        "hotels": ["hotel"],
        "activities": ["activit"],
    }

    def optional_arg(name):
        if f"--{name}" in sys.argv:
            return getResolvedOptions(sys.argv, [name])[name]
        return None

    city_filter = optional_arg("CITY_FILTER")
    cities = (
        [c.strip().lower() for c in city_filter.split(",") if c.strip()]
        if city_filter
        else None
    )
    extract_date = optional_arg("EXTRACT_DATE")

    keys = list(dfc.keys())
    print(f"Custom transform inputs: {keys}")

    def pick(dataset):
        for k in keys:
            if any(word in k.lower() for word in NODE_KEYWORDS[dataset]):
                return dfc.select(k).toDF()
        raise ValueError(f"No input node matched '{dataset}'. Inputs were: {keys}")

    def keep_extract(df, name):
        """Keep one extract: the one passed in, or the table's most recent partition."""
        if "extract_date" not in df.columns:
            return df
        target = extract_date or df.agg(F.max("extract_date")).first()[0]
        print(f"[{name}] using extract_date={target}")
        return df.filter(F.col("extract_date") == target)

    alias_map = F.create_map([F.lit(x) for x in chain(*CITY_ALIASES.items())])
    country_name_map = F.create_map(
        [F.lit(x) for x in chain(*COUNTRY_NAME_TO_ISO.items())]
    )
    known_city_map = F.create_map(
        [F.lit(x) for x in chain(*KNOWN_CITY_COUNTRY.items())]
    )
    currency_map = F.create_map([F.lit(x) for x in chain(*ISO_TO_CURRENCY.items())])

    def normalize(col):
        """Lowercase, strip accents, '_'/'-' -> space, drop . and ', collapse spaces."""
        c = F.lower(F.translate(col, ACCENTS_FROM, ACCENTS_TO))
        c = F.regexp_replace(c, r"[_\-]", " ")
        c = F.regexp_replace(c, r"[.'’]", "")
        return F.trim(F.regexp_replace(c, r"\s+", " "))

    def city_key(col):
        """'Vancouver, Canada' -> 'vancouver'; 'St. George' -> 'st george';
        'Washington, D.C.' -> 'washington'"""
        key = normalize(F.split(col, ",").getItem(0))
        return F.coalesce(alias_map[key], key)

    suffix_regex = (
        r"_("
        + "|".join(sorted(DESTINATION_COUNTRY_SUFFIXES, key=len, reverse=True))
        + r")$"
    )
    suffix_map = F.create_map(
        [F.lit(x) for x in chain(*DESTINATION_COUNTRY_SUFFIXES.items())]
    )

    def destination_city(col):
        key = normalize(F.regexp_replace(F.lower(col), suffix_regex, ""))
        return F.coalesce(alias_map[key], key)

    def destination_country(col):
        return suffix_map[F.regexp_extract(F.lower(col), suffix_regex, 1)]

    if cities:
        import re, unicodedata

        def py_key(s):
            s = (
                unicodedata.normalize("NFKD", s)
                .encode("ascii", "ignore")
                .decode()
                .lower()
            )
            s = s.split(",")[0]
            s = re.sub(r"[_\-]", " ", s)
            s = re.sub(r"[.'’]", "", s)
            s = re.sub(r"\s+", " ", s).strip()
            return CITY_ALIASES.get(s, s)

        cities = [py_key(c) for c in cities]
        print(f"City filter: {cities}")

    def apply_city_filter(df):
        return df.filter(F.col("city").isin(cities)) if cities else df

    fx = (
        pick("fx")
        .select(
            F.upper(F.trim("base_currency")).alias("base"),
            F.upper(F.trim("currency")).alias("quote"),
            F.col("rate").cast("double").alias("rate"),
            F.col("rate_date").cast("date").alias("rate_date"),
        )
        .filter(F.col("rate") > 0)
    )

    local_per_usd = (
        fx.filter("base = 'USD'")
        .select(
            F.col("quote").alias("currency"),
            F.col("rate").alias("local_per_usd"),
            "rate_date",
        )
        .unionByName(
            fx.filter("quote = 'USD' AND base <> 'USD'").select(
                F.col("base").alias("currency"),
                (F.lit(1.0) / F.col("rate")).alias("local_per_usd"),
                "rate_date",
            )
        )
    )
    latest_fx = F.broadcast(
        local_per_usd.withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("currency").orderBy(F.col("rate_date").desc())
            ),
        )
        .filter("rn = 1")
        .select(
            "currency",
            "local_per_usd",
            (F.lit(1.0) / F.col("local_per_usd")).alias("usd_per_unit"),
            F.col("rate_date").alias("fx_rate_date"),
        )
    )

    def to_usd(df, name):
        """Expects price_local + currency; adds price_usd. Prices already in USD pass through."""
        out = (
            df.join(
                latest_fx.select("currency", "usd_per_unit"), on="currency", how="left"
            )
            .withColumn(
                "price_usd",
                F.when(F.col("currency") == "USD", F.col("price_local")).otherwise(
                    F.col("price_local") * F.col("usd_per_unit")
                ),
            )
            .drop("usd_per_unit")
        )
        missing = (
            out.filter(F.col("price_usd").isNull() & F.col("price_local").isNotNull())
            .select("currency")
            .distinct()
            .collect()
        )
        if missing:
            print(
                f"[{name}] no exchange rate for currencies: {[r.currency for r in missing]}"
            )
        return out

    activities_raw = keep_extract(pick("activities"), "activities")

    if "destination" in activities_raw.columns:
        destination_col = F.col("destination")
        print("[activities] grouping by the `destination` partition column")
    else:
        destination_col = F.regexp_extract(
            F.input_file_name(), r"destination=([^/]+)", 1
        )
        print(
            "[activities] no `destination` column; parsing destination=... from file paths"
        )

    STREET_SUFFIX_REGEX = (
        r"(?i)\b(street|st|avenue|ave|road|rd|boulevard|blvd|drive|dr|lane|ln|way|"
        r"highway|hwy|parkway|pkwy|court|ct|place|pl|terrace|ter|circle|cir|trail)\.?$"
    )

    activities = apply_city_filter(
        activities_raw.select(
            destination_city(destination_col).alias("city"),
            F.coalesce(
                destination_country(destination_col),
                F.upper(F.trim("country_code")),
            ).alias("activity_country"),
            F.trim(F.col("name")).alias("name"),
            F.trim(F.split(F.col("formatted"), ",").getItem(0)).alias(
                "formatted_first"
            ),
            # First category, skipping the "access" / "access.yes" flags:
            # ["tourism","tourism.attraction",...] -> "tourism"
            F.expr(
                "CASE WHEN size(filter(coalesce(categories, array()), c -> NOT startswith(c, 'access'))) > 0 "
                "THEN filter(categories, c -> NOT startswith(c, 'access'))[0] END"
            ).alias("category"),
            "categories",
            F.col("longitude").cast("double").alias("longitude"),
            F.col("latitude").cast("double").alias("latitude"),
        )
    ).filter(F.col("city").isNotNull() & (F.col("city") != ""))

    is_real_name = (
        F.col("name").isNotNull()
        & (F.length("name") >= 2)
        & ~F.col("name").rlike(r"^[0-9]")  # "1285", "1285 Elmwood Avenue"
        & ~F.col("name").rlike(r"^[0-9\s#\-.,/]+$")  # only digits / punctuation
        & ~F.col("name").contains(",")  # "Holbrook Street, Detroit, MI"
        & ~(  # bare street used as the name
            F.col("name").rlike(STREET_SUFFIX_REGEX)
            & (F.lower("name") == F.lower("formatted_first"))
        )
    )
    activities = activities.filter(is_real_name).drop("formatted_first").cache()

    print(
        "[activities] destinations found: "
        f"{sorted(r.city for r in activities.select('city').distinct().collect())}"
    )

    activity_items = activities.groupBy("city", "name").agg(
        F.first("category", ignorenulls=True).alias("category"),
        F.first("longitude", ignorenulls=True).alias("longitude"),
        F.first("latitude", ignorenulls=True).alias("latitude"),
    )

    activities_summary = (
        activity_items.groupBy("city")
        .agg(
            F.count("*").alias("activity_count"),
            # [{name: "Light Matrix", category: "tourism", longitude: -80.12, latitude: 25.79}, ...]
            # sorted by name, max 10
            F.slice(
                F.array_sort(
                    F.collect_list(
                        F.struct("name", "category", "longitude", "latitude")
                    )
                ),
                1,
                10,
            ).alias("activity_examples"),
        )
        .join(
            activities.groupBy("city").agg(
                F.array_sort(F.array_distinct(F.collect_list("category"))).alias(
                    "activity_categories"
                )
            ),
            on="city",
            how="left",
        )
    )

    hotels = apply_city_filter(
        keep_extract(pick("hotels"), "hotels").select(
            city_key(F.col("search_parameters_q")).alias("city"),
            country_name_map[
                normalize(F.element_at(F.split("search_parameters_q", ","), -1))
            ].alias("hotel_country"),
            F.upper(
                F.coalesce(F.col("search_parameters_currency"), F.lit("USD"))
            ).alias("currency"),
            F.col("total_rate_extracted_lowest").cast("double").alias("price_local"),
            F.datediff(
                F.to_date("search_parameters_check_out_date"),
                F.to_date("search_parameters_check_in_date"),
            ).alias("nights"),
            F.to_date("search_parameters_check_in_date").alias("check_in_date"),
            F.to_date("search_parameters_check_out_date").alias("check_out_date"),
            F.col("overall_rating").cast("double").alias("overall_rating"),
            F.col("extracted_hotel_class").cast("double").alias("hotel_class"),
            F.col("name").alias("hotel_name"),
        )
    )
    hotels = (
        to_usd(hotels, "hotels")
        .withColumn(
            "nightly_usd",
            F.when(F.col("nights") > 0, F.col("price_usd") / F.col("nights")),
        )
        .cache()
    )

    hotels_summary = hotels.groupBy("city").agg(
        F.first("hotel_country", ignorenulls=True).alias("hotel_country"),
        F.count("*").alias("hotel_options"),
        F.count("price_usd").alias("hotel_priced_options"),
        F.min("check_in_date").alias("hotel_check_in"),
        F.max("check_out_date").alias("hotel_check_out"),
        F.max("nights").alias("hotel_nights"),
        F.round(F.min("price_usd"), 2).alias("hotel_min_stay_usd"),
        F.round(F.avg("price_usd"), 2).alias("hotel_avg_stay_usd"),
        F.round(F.min("nightly_usd"), 2).alias("hotel_min_nightly_usd"),
        F.round(F.avg("nightly_usd"), 2).alias("hotel_avg_nightly_usd"),
        F.expr("min_by(hotel_name, price_usd)").alias("cheapest_hotel"),
    )

    flights = apply_city_filter(
        keep_extract(pick("flights"), "flights").select(
            city_key(F.col("name")).alias("city"),
            F.col("name").alias("flight_destination"),
            F.lit(FLIGHTS_PRICE_CURRENCY).alias("currency"),
            F.col("price").cast("double").alias("price_local"),
            F.col("flight_duration").cast("double").alias("flight_duration_min"),
            F.to_date("outbound_date").alias("outbound_date"),
            F.to_date("return_date").alias("return_date"),
        )
    )
    flights = to_usd(flights, "flights").cache()

    flights_summary = flights.groupBy("city").agg(
        F.first("flight_destination").alias("destination_name"),
        F.count("*").alias("flight_options"),
        F.round(F.min("price_usd"), 2).alias("flight_min_usd"),
        F.round(F.avg("price_usd"), 2).alias("flight_avg_usd"),
        F.expr("min_by(outbound_date, price_usd)").alias("flight_outbound_date"),
        F.expr("min_by(return_date, price_usd)").alias("flight_return_date"),
        F.expr("min_by(flight_duration_min, price_usd)").alias("flight_duration_min"),
    )

    activity_country = activities.groupBy("city").agg(
        F.first("activity_country", ignorenulls=True).alias("activity_country")
    )

    base_cities = flights_summary.join(hotels_summary, on="city", how="full_outer")

    unmatched = (
        activities_summary.join(base_cities.select("city"), on="city", how="left_anti")
        .select("city")
        .collect()
    )
    if unmatched:
        print(
            f"[activities] destinations with no flight/hotel match (add to CITY_ALIASES?): "
            f"{sorted(r.city for r in unmatched)}"
        )

    combined = (
        base_cities.join(activities_summary, on="city", how="left")
        .join(activity_country, on="city", how="left")
        .withColumn("known_country", known_city_map[F.col("city")])
        .withColumn(
            "country_source",
            F.when(F.col("hotel_country").isNotNull(), "hotel_search")
            .when(F.col("activity_country").isNotNull(), "activities")
            .when(F.col("known_country").isNotNull(), "known_city_list")
            .otherwise("default"),
        )
        .withColumn(
            "country",
            F.coalesce(
                "hotel_country",
                "activity_country",
                "known_country",
                F.lit(DEFAULT_COUNTRY),
            ),
        )
        .drop("hotel_country", "activity_country", "known_country")
        .withColumn(
            "local_currency", F.coalesce(currency_map[F.col("country")], F.lit("USD"))
        )
        .join(
            latest_fx.select(
                F.col("currency").alias("local_currency"),
                "local_per_usd",
                "fx_rate_date",
            ),
            on="local_currency",
            how="left",
        )
        .withColumn(
            "local_per_usd",
            F.when(F.col("local_currency") == "USD", F.lit(1.0)).otherwise(
                F.col("local_per_usd")
            ),
        )
        # Cheapest flight + cheapest priced stay; null if either is missing.
        .withColumn(
            "est_min_trip_cost_usd",
            F.round(F.col("flight_min_usd") + F.col("hotel_min_stay_usd"), 2),
        )
        .withColumn(
            "est_avg_trip_cost_usd",
            F.round(F.col("flight_avg_usd") + F.col("hotel_avg_stay_usd"), 2),
        )
        # Same figures in the destination's currency (CAD / MXN; equal to USD for US cities)
        .withColumn(
            "flight_min_local",
            F.round(F.col("flight_min_usd") * F.col("local_per_usd"), 2),
        )
        .withColumn(
            "hotel_min_stay_local",
            F.round(F.col("hotel_min_stay_usd") * F.col("local_per_usd"), 2),
        )
        .withColumn(
            "est_min_trip_cost_local",
            F.round(F.col("est_min_trip_cost_usd") * F.col("local_per_usd"), 2),
        )
        .withColumn("has_flights", F.col("flight_options").isNotNull())
        .withColumn("has_hotels", F.col("hotel_options").isNotNull())
        .withColumn("has_activities", F.col("activity_count").isNotNull())
        .withColumn("processed_at", F.current_timestamp())
    )

    ordered = [
        "city",
        "destination_name",
        "country",
        "country_source",
        "local_currency",
        "local_per_usd",
        "fx_rate_date",
        "has_flights",
        "has_hotels",
        "has_activities",
        "est_min_trip_cost_usd",
        "est_avg_trip_cost_usd",
        "est_min_trip_cost_local",
        "flight_options",
        "flight_min_usd",
        "flight_avg_usd",
        "flight_min_local",
        "flight_outbound_date",
        "flight_return_date",
        "flight_duration_min",
        "hotel_options",
        "hotel_priced_options",
        "hotel_check_in",
        "hotel_check_out",
        "hotel_nights",
        "hotel_min_stay_usd",
        "hotel_avg_stay_usd",
        "hotel_min_stay_local",
        "hotel_min_nightly_usd",
        "hotel_avg_nightly_usd",
        "cheapest_hotel",
        "activity_count",
        "activity_examples",
        "activity_categories",
        "processed_at",
    ]
    combined = combined.select(*ordered)

    return DynamicFrameCollection(
        {"combined": DynamicFrame.fromDF(combined, glueContext, "combined")},
        glueContext,
    )


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

HotelRoomRates_node1791925973881 = glueContext.create_dynamic_frame.from_catalog(
    database="transformed-data-catalog",
    table_name="hotel-room-rates-transformed",
    transformation_ctx="HotelRoomRates_node1791925973881",
)

ExchangeRatesCatalog_node1791925915942 = glueContext.create_dynamic_frame.from_catalog(
    database="transformed-data-catalog",
    table_name="exchange-rates-transformed",
    transformation_ctx="ExchangeRatesCatalog_node1791925915942",
)

AirplaneTicketRates_node1791925972897 = glueContext.create_dynamic_frame.from_catalog(
    database="transformed-data-catalog",
    table_name="airplane-ticket-rates-transformed",
    transformation_ctx="AirplaneTicketRates_node1791925972897",
)


def read_with_catalog_schema(database, table_name):
    """Read a parquet catalog table using the Data Catalog's column list instead of
    the schema Glue infers from the files, so columns missing from older partitions
    (e.g. longitude/latitude) come through as nulls rather than disappearing."""
    table = boto3.client("glue").get_table(DatabaseName=database, Name=table_name)[
        "Table"
    ]
    columns = table["StorageDescriptor"]["Columns"] + table.get("PartitionKeys", [])
    schema = ", ".join(f"`{c['Name']}` {c['Type']}" for c in columns)
    location = table["StorageDescriptor"]["Location"].rstrip("/") + "/"
    print(f"[{table_name}] reading {location} with catalog schema: {schema}")
    df = spark.read.schema(schema).option("basePath", location).parquet(location)
    return DynamicFrame.fromDF(df, glueContext, table_name)


DestinationActivities_node1791925974614 = read_with_catalog_schema(
    "transformed-data-catalog", "destination-activities-transformed"
)

CombineDatasets_node1791924321376 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {
            "ExchangeRatesCatalog_node1791925915942": ExchangeRatesCatalog_node1791925915942,
            "AirplaneTicketRates_node1791925972897": AirplaneTicketRates_node1791925972897,
            "HotelRoomRates_node1791925973881": HotelRoomRates_node1791925973881,
            "DestinationActivities_node1791925974614": DestinationActivities_node1791925974614,
        },
        glueContext,
    ),
)

SelectCombined_node = SelectFromCollection.apply(
    dfc=CombineDatasets_node1791924321376,
    key="combined",
    transformation_ctx="SelectCombined_node",
)

OUTPUT_PATH = "s3://combine-datasets-transformation-075996947402-us-east-2-an/"
if "--OUTPUT_PATH" in sys.argv:
    OUTPUT_PATH = getResolvedOptions(sys.argv, ["OUTPUT_PATH"])["OUTPUT_PATH"]

combined_date = datetime.now(ZoneInfo("America/Phoenix")).strftime("%Y-%m-%d")
print(f"Writing output to {OUTPUT_PATH}combined_date={combined_date}/")

output_df = (
    SelectCombined_node.toDF()
    .withColumn("combined_date", F.lit(combined_date))
    .coalesce(1)
)

# Script generated for node Write to S3
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(output_df, glueContext, "output_df"),
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": OUTPUT_PATH, "partitionKeys": ["combined_date"]},
    format_options={"compression": "snappy"},
    transformation_ctx="WriteToS3_node",
)

job.commit()
