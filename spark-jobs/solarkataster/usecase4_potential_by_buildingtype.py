import os
import sys
import argparse
import urllib.parse
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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

SOURCE_TABLE = os.environ.get("UC4_SOURCE", f"{CLICKHOUSE_DB}.solarkataster_mv")  # ORIGINAL
TARGET_TABLE = os.environ.get("UC4_TARGET", f"{CLICKHOUSE_DB}.uc4_potential_by_buildingtype")


def ch_http_exec(sql: str) -> str:
    if not CLICKHOUSE_PASS:
        raise RuntimeError("CH_PASS ist leer. Bitte: export CH_PASS='...'.")
    params = {"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASS, "query": sql}
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", errors="ignore")


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


def create_target_table():
    ddl = f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
  gebaeudefunktion String,
  records UInt64,
  sum_area Float64,
  sum_modulflaeche Float64,
  sum_leistung_kwp Float64,
  sum_strom_kwh Float64,
  avg_leistung_kwp Float64,
  avg_strom_kwh Float64,
  processed_at DateTime
)
ENGINE = MergeTree
ORDER BY (sum_leistung_kwp, gebaeudefunktion)
"""
    ch_http_exec(ddl)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=20, help="Top N Zeilen anzeigen")
    ap.add_argument("--limit", type=int, default=0, help="Optional: Quelle begrenzen (0 = alles)")
    ap.add_argument("--write", action="store_true", help="Schreibe Ergebnis nach ClickHouse")
    ap.add_argument("--overwrite", action="store_true", help="Überschreibe die Zieltabelle (statt append)")
    args = ap.parse_args()

    if not CLICKHOUSE_PASS:
        print("❌ CH_PASS ist leer. Bitte: export CH_PASS='...'", file=sys.stderr)
        sys.exit(2)

    spark = SparkSession.builder.appName("UC4_Potential_By_BuildingType").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("==> UC4 Start (Compute)")
    print(f"    SOURCE={SOURCE_TABLE}")
    print(f"    TARGET={TARGET_TABLE}")
    print(f"    TOP={args.top}")
    print(f"    LIMIT={args.limit}")
    print(f"    WRITE={'YES' if args.write else 'NO (dry-run)'}")
    print(f"    MODE={'OVERWRITE' if args.overwrite else 'APPEND'}")

    # Driver load test
    spark._jvm.java.lang.Class.forName(CLICKHOUSE_DRIVER)
    print(f"✅ JDBC Driver geladen: {CLICKHOUSE_DRIVER}")

    # Read source (optional LIMIT)
    if args.limit and args.limit > 0:
        src = f"(SELECT * FROM {SOURCE_TABLE} LIMIT {int(args.limit)}) t"
    else:
        src = SOURCE_TABLE

    df = read_ch(spark, src)

    # Compute aggregation
    df_agg = (
        df.groupBy("gebaeudefunktion")
        .agg(
            F.count("*").alias("records"),
            F.sum(F.col("area")).alias("sum_area"),
            F.sum(F.col("modulflaeche")).alias("sum_modulflaeche"),
            F.sum(F.col("leistung_kwp")).alias("sum_leistung_kwp"),
            F.sum(F.col("strom_kwh")).alias("sum_strom_kwh"),
            F.avg(F.col("leistung_kwp")).alias("avg_leistung_kwp"),
            F.avg(F.col("strom_kwh")).alias("avg_strom_kwh"),
        )
        .withColumn("processed_at", F.current_timestamp())
    )

    # Show top
    print(f"==> TOP {args.top} Gebäudefunktionen nach sum_leistung_kwp:")
    (
        df_agg.orderBy(F.col("sum_leistung_kwp").desc_nulls_last())
        .show(args.top, truncate=False)
    )

    if not args.write:
        print("✅ UC4 fertig (DRY-RUN). Kein Schreiben nach ClickHouse durchgeführt.")
        spark.stop()
        return

    print("==> Creating target table (IF NOT EXISTS)...")
    create_target_table()
    print("✅ Target table bereit.")

    mode = "overwrite" if args.overwrite else "append"
    print(f"==> Writing to ClickHouse ({mode.upper()}) ...")
    (
        df_agg.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("driver", CLICKHOUSE_DRIVER)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASS)
        .option("dbtable", TARGET_TABLE)
        .option("batchsize", "5000")
        .mode(mode)
        .save()
    )

    print(f"✅ UC4 fertig. Ergebnis geschrieben nach {TARGET_TABLE}.")
    spark.stop()


if __name__ == "__main__":
    main()
