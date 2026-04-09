"""
bigquery/schema.py

Defines BigQuery table schema for surge analytics.
Creates table with partitioning and clustering if it doesn't exist.

Partitioned by recorded_at (date) — queries filter by date range.
Clustered by location — queries almost always filter by zone.
Together these reduce query cost by 80-90% vs unpartitioned table.
"""

from google.cloud import bigquery
from config.settings import settings
from utils.logger import get_logger

logger = get_logger(__name__)

# BigQuery dataset and table names
DATASET_ID = "dynamic_pricing"
SURGE_TABLE_ID = "surge_analytics"
EVENTS_TABLE_ID = "raw_events"


def get_client() -> bigquery.Client:
    """Returns a BigQuery client for the configured project."""
    return bigquery.Client(project=settings.gcp_project_id)


def create_dataset_if_not_exists(client: bigquery.Client) -> None:
    """Creates the dataset if it doesn't already exist."""
    dataset_ref = bigquery.Dataset(
        f"{settings.gcp_project_id}.{DATASET_ID}"
    )
    dataset_ref.location = "US"

    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset {DATASET_ID} already exists")
    except Exception:
        client.create_dataset(dataset_ref)
        logger.info(f"Created dataset {DATASET_ID}")


def create_surge_table_if_not_exists(client: bigquery.Client) -> None:
    """
    Creates surge_analytics table with partitioning and clustering.
    This is the main table for Looker Studio dashboard queries.
    """
    table_ref = f"{settings.gcp_project_id}.{DATASET_ID}.{SURGE_TABLE_ID}"

    schema = [
        bigquery.SchemaField("location", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("surge_multiplier", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("rider_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("driver_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("hour_of_day", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("is_peak_hour", "BOOLEAN", mode="REQUIRED"),
        bigquery.SchemaField("recorded_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("window_start", "TIMESTAMP", mode="REQUIRED"),
    ]

    table = bigquery.Table(table_ref, schema=schema)

    # partition by day — queries filter by date range
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="recorded_at"
    )

    # cluster by location — queries almost always filter by zone
    table.clustering_fields = ["location"]

    try:
        client.get_table(table_ref)
        logger.info(f"Table {SURGE_TABLE_ID} already exists")
    except Exception:
        client.create_table(table)
        logger.info(f"Created table {SURGE_TABLE_ID}")


def create_raw_events_table_if_not_exists(client: bigquery.Client) -> None:
    """
    Creates external table pointing to GCS Parquet files.
    No data copying — BigQuery queries GCS directly.
    Used for raw event exploration and debugging.
    """
    table_ref = f"{settings.gcp_project_id}.{DATASET_ID}.{EVENTS_TABLE_ID}"

    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = [
        f"gs://{settings.gcs_bucket}/events/*"
    ]
    external_config.autodetect = True

    table = bigquery.Table(table_ref)
    table.external_data_configuration = external_config

    try:
        client.get_table(table_ref)
        logger.info(f"Table {EVENTS_TABLE_ID} already exists")
    except Exception:
        client.create_table(table)
        logger.info(f"Created external table {EVENTS_TABLE_ID}")


def setup_bigquery() -> None:
    """
    One-time setup — creates dataset and both tables.
    Safe to run multiple times — skips existing resources.
    """
    client = get_client()
    create_dataset_if_not_exists(client)
    create_surge_table_if_not_exists(client)
    create_raw_events_table_if_not_exists(client)
    logger.info("BigQuery setup complete")


if __name__ == "__main__":
    setup_bigquery()