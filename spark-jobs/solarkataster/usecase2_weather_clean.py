import os
import sys
import argparse
import urllib.parse
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


# -----------------------------
# Config (SAFE Defaults)
# -----------------------------
CLICKHOUSE_HOST = os.environ.get("CH_HOST", "172.25.0.2")
CLICKHOUSE_HTTP_PORT = int(os.environ.get("CH_HTTP_PORT", "8123"))
CLICKHOUSE_DB = os.environ.get("CH_DB", "default")
CLICKHOUSE_USER = os.environ.get("CH_USER", "default")
CLICKHOUSE_PASS = os.environ.get("CH_PASS", "")

# JDBC URL (works with clickhouse-jdbc 0.6.x + HTTP)
JDBC_URL = os.environ.get(
    "CH_JDBC_URL",
    f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/{CLICKHOUSE_DB}"
    "?protocol=http&ssl=false&compress=0"
)

CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"

SOURCE_TABLE = os.environ.get("UC2_SOURCE", f"{CLICKHOUSE_DB}.weather_data")
TARGET_TABLE = os.environ.get("UC2_TARGET", f"{CLICKHOUSE_DB}.weather_data_clean_uc2")


# -----------------------------
# ClickHouse HTTP EXEC (DDL only)
# -----------------------------
def ch_http_exec(sql: str) -> str:
    if not CLICKHOUSE_PASS:
        raise RuntimeError("CH_PASS ist leer. Bitte export CH_PASS='...'.")
    params = {
        "user": CLICKHOUSE_USER,
        "password": CLICKHOUSE_PASS,
        "query": sql,
    }
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", errors="ignore")


# -----------------------------
# Schema mapping: Spark -> ClickHouse
# -----------------------------
def spark_type_to_ch(dt: T.DataType) -> str:
    if isinstance(dt, T.StringType):
        return "String"
    if isinstance(dt, T.BooleanType):
        return "UInt8"
    if isinstance(dt, T.ByteType):
        return "Int8"
    if isinstance(dt, T.ShortType):
        return "Int16"
    if isinstance(dt, T.IntegerType):
        return "Int32"
    if isinstance(dt, T.LongType):
        return "Int64"
    if isinstance(dt, T.FloatType):
        return "Float32"
    if isinstance(dt, T.DoubleType):
        return "Float64"
    if isinstance(dt, T.DateType):
        return "Date"
    if isinstance(dt, T.TimestampType):
        return "DateTime"
    if isinstance(dt, T.DecimalType):
        return f"Decimal({dt.precision},{dt.scale})"
    return "String"


def build_create_table_ddl(df, target_table: str) -> str:
    cols = []
    for f in df.schema.fields:
        ch_type = spark_type_to_ch(f.dataType)
        if f.nullable and not ch_type.startswith("Nullable("):
            ch_type = f"Nullable({ch_type})"
        cols.append(f"`{f.name}` {ch_type}")

    cols_sql = ",\n  ".join(cols)

    ddl = f"""
CREATE TABLE IF NOT EXISTS {target_table} (
  {cols_sql}
)
ENGINE = MergeTree
ORDER BY tuple()
"""
    return ddl.strip()


# -----------------------------
# Read from ClickHouse via JDBC
# -----------------------------
def read_ch(spark: SparkSession, table_or_query: str):
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("driver", CLICKHOUSE_DRIVER)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASS)
        .option("dbtable", table_or_query)
        .option("fetchsize", "10000")
        .option("numPartitions", "1")
        .load()
    )


# -----------------------------
# Cleaning logic (matches YOUR schema)
# -----------------------------
def clean_weather(df):
    # 1) Drop duplicates by business key: station_id + timestamp
    # If multiple rows exist for same key, keep the first (arbitrary but stable enough for UC)
    if "station_id" in df.columns and "timestamp" in df.columns:
        df2 = df.dropDuplicates(["station_id", "timestamp"])
    else:
        df2 = df.dropDuplicates()

    # 2) Basic null checks (only what exists)
    if "station_id" in df2.columns:
        df2 = df2.filter(F.col("station_id").isNotNull())
    if "timestamp" in df2.columns:
        df2 = df2.filter(F.col("timestamp").isNotNull())

    # 3) Outlier handling for your columns
    # temperatur in Celsius plausible range
    if "temperatur" in df2.columns:
        df2 = df2.withColumn(
            "temperatur",
            F.when((F.col("temperatur") < F.lit(-80)) | (F.col("temperatur") > F.lit(60)), F.lit(None))
             .otherwise(F.col("temperatur"))
        )

    # wind (unknown unit, but negative is invalid; cap very high values)
    if "wind" in df2.columns:
        df2 = df2.withColumn(
            "wind",
            F.when((F.col("wind") < F.lit(0)) | (F.col("wind") > F.lit(200)), F.lit(None))
             .otherwise(F.col("wind"))
        )

    # 4) Add processed_at
    df2 = df2.withColumn("processed_at", F.current_timestamp())

    return df2


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=10000, help="Max rows to read (SAFE default 10000)")
    ap.add_argument("--write", action="store_true", help="Actually write to ClickHouse (default: dry-run)")
    ap.add_argument("--max-write-rows", type=int, default=200000, help="Safety guard: refuse writing above this")
    args = ap.parse_args()

    if not CLICKHOUSE_PASS:
        print("❌ CH_PASS ist leer. Bitte: export CH_PASS='...'", file=sys.stderr)
        sys.exit(2)

    spark = SparkSession.builder.appName("UC2_Weather_SAFE_ETL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("==> UC2 Start (SAFE)")
    print(f"    JDBC_URL={JDBC_URL}")
    print(f"    SOURCE={SOURCE_TABLE}")
    print(f"    TARGET={TARGET_TABLE}")
    print(f"    LIMIT={args.limit}")
    print(f"    WRITE={'YES' if args.write else 'NO (dry-run)'}")

    # Ensure driver can be loaded
    spark._jvm.java.lang.Class.forName(CLICKHOUSE_DRIVER)
    print(f"✅ JDBC Driver geladen: {CLICKHOUSE_DRIVER}")

    # SAFE read with LIMIT
    src_query = f"(SELECT * FROM {SOURCE_TABLE} LIMIT {int(args.limit)}) t"
    df = read_ch(spark, src_query)

    print("==> Source schema:")
    df.printSchema()
    print("==> Source sample:")
    df.show(5, truncate=False)

    df_clean = clean_weather(df)

    print("==> Clean sample:")
    df_clean.show(10, truncate=False)

    out_count = df_clean.count()
    print(f"==> Rows after cleaning: {out_count}")

    if not args.write:
        print("✅ UC2 fertig (DRY-RUN). Kein Schreiben nach ClickHouse durchgeführt.")
        spark.stop()
        return

    if out_count > args.max_write_rows:
        print(f"❌ Safety-Stop: out_count={out_count} > max_write_rows={args.max_write_rows}.")
        spark.stop()
        sys.exit(3)

    ddl = build_create_table_ddl(df_clean, TARGET_TABLE)
    print("==> Creating target table (IF NOT EXISTS)...")
    ch_http_exec(ddl)
    print("✅ Target table bereit.")

    print("==> Writing to ClickHouse (APPEND, no overwrite)...")
    (
        df_clean.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("driver", CLICKHOUSE_DRIVER)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASS)
        .option("dbtable", TARGET_TABLE)
        .option("batchsize", "5000")
        .mode("append")
        .save()
    )

    print(f"✅ UC2 fertig. Geschrieben: {out_count} Zeilen nach {TARGET_TABLE} (APPEND).")
    spark.stop()


if __name__ == "__main__":
    main()


