import os
import sys
import argparse
import urllib.parse
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


# -----------------------------
# Config
# -----------------------------
CLICKHOUSE_HOST = os.environ.get("CH_HOST", "172.25.0.2")
CLICKHOUSE_HTTP_PORT = int(os.environ.get("CH_HTTP_PORT", "8123"))
CLICKHOUSE_DB = os.environ.get("CH_DB", "default")
CLICKHOUSE_USER = os.environ.get("CH_USER", "default")
CLICKHOUSE_PASS = os.environ.get("CH_PASS", "")

JDBC_URL = os.environ.get(
    "CH_JDBC_URL",
    f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/{CLICKHOUSE_DB}"
    "?protocol=http&ssl=false&compress=0"
)

CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"

SOURCE_TABLE = os.environ.get("UC4_SOURCE", f"{CLICKHOUSE_DB}.solarkataster_uc1_clean")
TARGET_TABLE = os.environ.get("UC4_TARGET", f"{CLICKHOUSE_DB}.uc4_potential_by_gemeinde")


# -----------------------------
# ClickHouse HTTP EXEC (DDL)
# -----------------------------
def ch_http_exec(sql: str) -> str:
    if not CLICKHOUSE_PASS:
        raise RuntimeError("CH_PASS ist leer. Bitte: export CH_PASS='...'")
    params = {
        "user": CLICKHOUSE_USER,
        "password": CLICKHOUSE_PASS,
        "query": sql
    }
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", errors="ignore")


# -----------------------------
# Build CREATE TABLE for UC4 result
# -----------------------------
def build_target_table_ddl(target_table: str) -> str:
    ddl = f"""
CREATE TABLE IF NOT EXISTS {target_table} (
  gemeinde String,
  records UInt64,
  sum_modulflaeche Float64,
  sum_leistung_kwp Float64,
  sum_strom_kwh Float64,
  processed_at DateTime
)
ENGINE = MergeTree
ORDER BY (gemeinde)
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
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=20, help="Top N Gemeinden (default 20)")
    ap.add_argument("--limit", type=int, default=0, help="Optional LIMIT beim Lesen (0 = kein Limit)")
    ap.add_argument("--write", action="store_true", help="Write Ergebnis nach ClickHouse (default: nur anzeigen)")
    args = ap.parse_args()

    if not CLICKHOUSE_PASS:
        print("❌ CH_PASS ist leer. Bitte: export CH_PASS='...'", file=sys.stderr)
        sys.exit(2)

    spark = (
        SparkSession.builder
        .appName("UC4_Potential_By_Gemeinde")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("==> UC4 Start (Compute Aggregation)")
    print(f"    JDBC_URL={JDBC_URL}")
    print(f"    SOURCE={SOURCE_TABLE}")
    print(f"    TARGET={TARGET_TABLE}")
    print(f"    TOP={args.top}")
    print(f"    LIMIT={args.limit if args.limit > 0 else 'NONE'}")
    print(f"    WRITE={'YES' if args.write else 'NO (dry-run)'}")

    # Load Driver
    spark._jvm.java.lang.Class.forName(CLICKHOUSE_DRIVER)
    print(f"✅ JDBC Driver geladen: {CLICKHOUSE_DRIVER}")

    # Read source
    if args.limit and args.limit > 0:
        src_query = f"(SELECT * FROM {SOURCE_TABLE} LIMIT {int(args.limit)}) t"
    else:
        src_query = SOURCE_TABLE

    df = read_ch(spark, src_query)

    print("==> Source schema:")
    df.printSchema()

    print("==> Source sample:")
    df.show(5, truncate=False)

    # Compute Aggregation
    # Wichtig: mögliche Nulls safe behandeln
    df_clean = (
        df.filter(F.col("gemeinde").isNotNull())
        .withColumn("modulflaeche", F.coalesce(F.col("modulflaeche"), F.lit(0.0)))
        .withColumn("leistung_kwp", F.coalesce(F.col("leistung_kwp"), F.lit(0.0)))
        .withColumn("strom_kwh", F.coalesce(F.col("strom_kwh"), F.lit(0.0)))
    )

    df_result = (
        df_clean.groupBy("gemeinde")
        .agg(
            F.count("*").alias("records"),
            F.sum("modulflaeche").alias("sum_modulflaeche"),
            F.sum("leistung_kwp").alias("sum_leistung_kwp"),
            F.sum("strom_kwh").alias("sum_strom_kwh"),
        )
        .withColumn("processed_at", F.current_timestamp())
        .orderBy(F.desc("sum_leistung_kwp"))
    )

    print(f"==> TOP {args.top} Gemeinden nach sum_leistung_kwp:")
    df_result.show(args.top, truncate=False)

    if not args.write:
        print("✅ UC4 fertig (DRY-RUN). Kein Schreiben nach ClickHouse durchgeführt.")
        spark.stop()
        return

    # Create table in CH
    print("==> Creating target table (IF NOT EXISTS)...")
    ddl = build_target_table_ddl(TARGET_TABLE)
    ch_http_exec(ddl)
    print("✅ Target table bereit.")

    # Write result
    print("==> Writing UC4 result to ClickHouse (APPEND)...")
    (
        df_result.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("driver", CLICKHOUSE_DRIVER)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASS)
        .option("dbtable", TARGET_TABLE)
        .option("batchsize", "5000")
        .mode("append")
        .save()
    )

    print(f"✅ UC4 fertig. Ergebnis gespeichert in {TARGET_TABLE}")
    print("   Tipp: In ClickHouse prüfen mit:")
    print(f"   SELECT * FROM {TARGET_TABLE} ORDER BY sum_leistung_kwp DESC LIMIT 20;")

    spark.stop()


if __name__ == "__main__":
    main()
