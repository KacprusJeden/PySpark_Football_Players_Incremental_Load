from datetime import date
from pyspark.sql.functions import col
import pipeline.spark_incremental_data_load as sidl


def test_insert_new_records(spark):
    today = str(date.today())

    stg_data = [
        ("1", "Messi", "Inter Miami", "FW", "Argentina", "2025-10-01")
    ]

    dim_data = []

    stg_schema = "player_id string, player_name string, team_name string, position string, country string, load_date string"
    dim_schema = "player_id string, player_name string, team_name string, position string, country string, effective_date string, end_date string, is_current string, created_at string"

    stg_df = spark.createDataFrame(stg_data, stg_schema)
    dim_df = spark.createDataFrame(dim_data, dim_schema)

    delta_df, dim_next = sidl.scd2_incremental_load(stg_df, dim_df)

    assert delta_df.count() == 1
    assert dim_next.count() == 1

    row = dim_next.collect()[0]
    assert row.player_id == "1"
    assert row.is_current == "True"
    assert row.end_date is None


def test_update_existing_record(spark):
    today = str(date.today())

    stg_data = [
        ("10", "Messi", "Inter Miami", "FW", "Argentina", "2025-10-01")
    ]

    dim_data = [
        ("10", "Messi", "PSG", "FW", "Argentina", "2025-08-01", None, "True", today)
    ]

    stg_schema = "player_id string, player_name string, team_name string, position string, country string, load_date string"
    dim_schema = "player_id string, player_name string, team_name string, position string, country string, effective_date string, end_date string, is_current string, created_at string"

    stg_df = spark.createDataFrame(stg_data, stg_schema)
    dim_df = spark.createDataFrame(dim_data, dim_schema)

    delta_df, dim_next = sidl.scd2_incremental_load(stg_df, dim_df)

    assert delta_df.count() == 2

    closed = dim_next.filter(col("is_current") == "False").collect()
    opened = dim_next.filter(col("is_current") == "True").collect()

    assert len(closed) == 1
    assert len(opened) == 1

    assert closed[0].end_date == today
    assert opened[0].team_name == "Inter Miami"


def test_delete_record(spark):
    today = str(date.today())

    stg_data = []

    dim_data = [
        ("99", "Old Player", "Old Team", "FW", "PL", "2025-01-01", None, "True", today)
    ]

    stg_schema = "player_id string, player_name string, team_name string, position string, country string, load_date string"
    dim_schema = "player_id string, player_name string, team_name string, position string, country string, effective_date string, end_date string, is_current string, created_at string"

    stg_df = spark.createDataFrame(stg_data, stg_schema)
    dim_df = spark.createDataFrame(dim_data, dim_schema)

    delta_df, dim_next = sidl.scd2_incremental_load(stg_df, dim_df)

    assert delta_df.count() == 1
    assert dim_next.filter(col("is_current") == "True").count() == 0
    assert dim_next.filter(col("is_current") == "False").count() == 1
