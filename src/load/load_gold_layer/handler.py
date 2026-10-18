import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.context import SparkContext

CATALOG_PARAMS = ["CATALOG_DATABASE", "CATALOG_TABLE"]

CLUSTER_PARAMS = {
    "us_ca_west": "US_CA_WEST_CLUSTER_ENDPOINT",
    "us_ca_east": "US_CA_EAST_CLUSTER_ENDPOINT",
    "mx_north": "MX_NORTH_CLUSTER_ENDPOINT",
    "mx_south": "MX_SOUTH_CLUSTER_ENDPOINT",
}
TARGET_TABLE = "destinations"

DSQL_PARAMS = ["DSQL_DRIVER", "DSQL_DATABASE", "DSQL_USER"]

WEST_EAST_LONGITUDE = -100.0
TROPIC_OF_CANCER_LATITUDE = 23.4362

JSON_COLUMNS = ["activity_examples", "activity_categories"]


def optional_arg(name):
    if f"--{name}" in sys.argv:
        return getResolvedOptions(sys.argv, [name])[name]
    return None


def route_destinations(df):
    first_activity = F.when(
        F.size("activity_examples") > 0, F.col("activity_examples").getItem(0)
    )
    country = F.upper(F.trim("country"))
    lon = F.col("longitude")
    lat = F.col("latitude")
    us_ca = country.isin("US", "CA")
    mx = country == "MX"

    return (
        df.withColumn("longitude", first_activity.getField("longitude"))
        .withColumn("latitude", first_activity.getField("latitude"))
        .withColumn(
            "cluster",
            F.when(us_ca & (lon < WEST_EAST_LONGITUDE), "us_ca_west")
            .when(us_ca & (lon >= WEST_EAST_LONGITUDE), "us_ca_east")
            .when(mx & (lat >= TROPIC_OF_CANCER_LATITUDE), "mx_north")
            .when(mx & (lat < TROPIC_OF_CANCER_LATITUDE), "mx_south"),
        )
    )


def to_jdbc_rows(df):
    for c in JSON_COLUMNS:
        df = df.withColumn(c, F.to_json(c))
    return df


def read_latest_partition(spark, database, table_name, combined_date=None):

    glue = boto3.client("glue")
    table = glue.get_table(DatabaseName=database, Name=table_name)["Table"]
    columns = table["StorageDescriptor"]["Columns"]
    schema = ", ".join(f"`{c['Name']}` {c['Type']}" for c in columns)

    partitions = [
        p
        for page in glue.get_paginator("get_partitions").paginate(
            DatabaseName=database, TableName=table_name
        )
        for p in page["Partitions"]
    ]
    if not partitions:
        raise ValueError(f"{database}.{table_name} has no partitions")
    by_date = {p["Values"][0]: p for p in partitions}
    target = combined_date or max(by_date)
    if target not in by_date:
        raise ValueError(
            f"No combined_date={target} partition. Available: {sorted(by_date)}"
        )

    location = by_date[target]["StorageDescriptor"]["Location"]
    print(f"[{table_name}] reading combined_date={target} from {location}")
    return (
        spark.read.schema(schema)
        .parquet(location)
        .withColumn("combined_date", F.lit(target))
    )


def write_cluster(df, endpoint, driver, database, user):
    (
        df.write.format("jdbc")
        .option("url", f"jdbc:aws-dsql:postgresql://{endpoint}/{database}")
        .option("driver", driver)
        .option("dbtable", TARGET_TABLE)
        .option("user", user)
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )


def main():
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", *CATALOG_PARAMS, *DSQL_PARAMS, *CLUSTER_PARAMS.values()],
    )
    endpoints = {cluster: args[param] for cluster, param in CLUSTER_PARAMS.items()}
    print(f"Cluster endpoints: {endpoints}")
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    combined = read_latest_partition(
        spark,
        args["CATALOG_DATABASE"],
        args["CATALOG_TABLE"],
        optional_arg("COMBINED_DATE"),
    )
    routed = route_destinations(combined).cache()

    unrouted = (
        routed.filter(F.col("cluster").isNull())
        .select("city", "country", "longitude", "latitude")
        .collect()
    )
    if unrouted:
        print(
            "[routing] dropping destinations with no cluster (unknown country or no coordinates): "
            f"{sorted((r.city, r.country, r.longitude, r.latitude) for r in unrouted)}"
        )

    for cluster, endpoint in endpoints.items():
        rows = to_jdbc_rows(routed.filter(F.col("cluster") == cluster).drop("cluster"))
        count = rows.count()
        if count == 0:
            print(f"[{cluster}] no destinations, skipping write")
            continue
        print(
            f"[{cluster}] writing {count} destinations to {TARGET_TABLE} via {endpoint}"
        )
        write_cluster(
            rows,
            endpoint,
            args["DSQL_DRIVER"],
            args["DSQL_DATABASE"],
            args["DSQL_USER"],
        )

    job.commit()


if __name__ == "__main__":
    main()
