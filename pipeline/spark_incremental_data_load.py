from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from datetime import date
import os
import traceback
import sys

# ------------------------------------------------------------------
# ENV
# ------------------------------------------------------------------
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


# ------------------------------------------------------------------
# SPARK SESSION
# ------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("SCD2_CDC_Incremental_Load")
    .master("local[1]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("\n########## SCD2 + CDC + INCREMENTAL LOAD (PySpark) ##########\n")

# ------------------------------------------------------------------
# FUNCTION: SCD2 MERGE LOGIC
# ------------------------------------------------------------------
def scd2_incremental_load(stg_df, dim_df):
    """
    Implements SCD Type 2 with CDC (insert/update/delete)
    """

    dim_current = dim_df.filter(col("is_current") == "True")
    dim_history = dim_df.filter(col("is_current") == "False")

    current_date = str(date.today())

    joined = (
        stg_df.alias("s")
        .join(dim_current.alias("d"), col("s.player_id") == col("d.player_id"), "full_outer")
    )

    # ----------------------------------------
    # RECORDS TO CLOSE (UPDATE / DELETE)
    # ----------------------------------------
    to_close = (
        joined
        .filter(
            (col("d.player_id").isNotNull()) & (
                col("s.player_id").isNull() | (
                    (col("s.player_name") != col("d.player_name")) |
                    (col("s.team_name") != col("d.team_name")) |
                    (col("s.position") != col("d.position")) |
                    (col("s.country") != col("d.country"))
                )
            )
        )
        .select(
            col("d.player_id"),
            col("d.player_name"),
            col("d.team_name"),
            col("d.position"),
            col("d.country"),
            col("d.effective_date"),
            lit(current_date).alias("end_date"),
            lit("False").alias("is_current"),
            col("d.created_at")
        )
    )

    print('### ROWS TO CLOSE ###')
    to_close.show()

    # ----------------------------------------
    # RECORDS TO INSERT (NEW / UPDATED)
    # ----------------------------------------
    to_open = (
        joined
        .filter(
            col("s.player_id").isNotNull() & (
                col("d.player_id").isNull() | (
                    (col("s.player_name") != col("d.player_name")) |
                    (col("s.team_name") != col("d.team_name")) |
                    (col("s.position") != col("d.position")) |
                    (col("s.country") != col("d.country"))
                )
            )
        )
        .select(
            col("s.player_id"),
            col("s.player_name"),
            col("s.team_name"),
            col("s.position"),
            col("s.country"),
            lit(current_date).alias("effective_date"),
            lit(None).alias("end_date"),
            lit("True").alias("is_current"),
            lit(current_date).alias("created_at")
        )
    )

    print('### ROWS TO INSERT ###')
    to_open.show()

    # ----------------------------------------
    # UNCHANGED RECORDS
    # ----------------------------------------
    unchanged_current = (dim_current.alias("dc").join(
            to_close.alias("tc"),
            (col("dc.player_id") == col("tc.player_id")),
            "left_outer"
        )
    ).select("dc.*").filter(
        (col("dc.is_current") == col("tc.is_current"))
        | (col("tc.is_current").isNull())
    )

    print('### UNCHANGED ROWS ###')
    unchanged_current.show()

    # ----------------------------------
    # FINAL DIMENSION
    # ----------------------------------

    dim_history = dim_history.repartition(1)
    unchanged_current = unchanged_current.repartition(1)
    to_close = to_close.repartition(1)
    to_open = to_open.repartition(1)

    dim_next = (
        dim_history
        .unionByName(unchanged_current)
        .unionByName(to_close)
        .unionByName(to_open)
    )

    delta_df = to_close.unionByName(to_open)

    return delta_df, dim_next

# ------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------
try:
    today = str(date.today())

    # -----------------------------
    # STAGING DATA
    # -----------------------------
    stg_data = [
        ("10", "Lionel Messi", "Inter Miami", "Forward", "Argentina", "2025-10-01"),
        ("7", "Cristiano Ronaldo", "Al Nassr", "Forward", "Portugal", "2025-10-01"),
        ("11", "Neymar Jr", "Al Hilal", "Forward", "Brazil", "2025-10-01"),
        ("19", "Lamine Yamal", "FC Barcelona", "Forward", "Spain", "2025-10-01"),
        ("9", "Endrick", "Real Madrid", "Forward", "Brazil", "2025-10-01")
    ]

    stg_schema = "player_id string, player_name string, team_name string, "\
            "position string, country string, load_date string"

    stg_df = spark.createDataFrame(
        data=stg_data,
        schema=stg_schema
    )

    print("### STAGING ###")
    stg_df.show()

    # -----------------------------
    # DIMENSION (INITIAL STATE)
    # -----------------------------
    dim_data = [
        ("10", "Lionel Messi", "PSG", "Forward", "Argentina", "2025-08-01", None, "True", today),
        ("7", "Cristiano Ronaldo", "Manchester United", "Forward", "Portugal", "2025-08-01", None, "True", today),
        ("11", "Neymar Jr", "PSG", "Forward", "Brazil", "2025-08-01", None, "True", today)
    ]

    dim_schema = "player_id string, player_name string, team_name string," \
            " position string, country string, effective_date string," \
            " end_date string, is_current string, created_at string"

    dim_df = spark.createDataFrame(
        data=dim_data,
        schema=dim_schema   
    )

    print("### DIM BEFORE ###")
    dim_df.show()

    # -----------------------------
    # FIRST INCREMENTAL LOAD
    # -----------------------------
    delta_df, dim_df = scd2_incremental_load(stg_df, dim_df)

    print("### DELTA (INSERT / UPDATE / DELETE) ###")
    delta_df.show()

    print("### DIM AFTER LOAD ###")
    dim_df.show()

    # -----------------------------
    # SIMULATE DELETE (player 19)
    # -----------------------------
    stg_df = stg_df.filter(col("player_id") != "19")

    print("### STAGING AFTER DELETE ###")
    stg_df.show()

    delta_df, dim_df = scd2_incremental_load(stg_df, dim_df)

    print("### DELTA AFTER DELETE ###")
    delta_df.show()

    print("### DIM AFTER DELETE ###")
    dim_df.show()

except Exception as e:
    print("❌ ERROR during Spark job")
    print(str(e))
    traceback.print_exc()
    sys.exit(1)

finally:
    spark.stop()
    print('Spark session stoped')
