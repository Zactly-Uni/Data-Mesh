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

JDBC_URL = os.environ.get(
    "CH_JDBC_URL",
    f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/{CLICKHOUSE_DB}"
    "?protocol=http&ssl=false&compress=0"
)

CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"

SOURCE_TABLE = os.environ.get("UC1_SOURCE", f"{CLICKHOUSE_DB}.solarkataster_uc1_clean")
TARGET_TABLE = os.environ.get("UC1_TARGET", f"{CLICKHOUSE_DB}.uc1_potential_by_gemeinde")


# -----------------------------
# ClickHouse HTTP EXEC (DDL)
# -----------------------------
def ch_http_exec(sql: str) -> str:
    if not CLICKHOUSE_PASS:
        raise RuntimeError("CH_PASS ist leer. Bitte: export CH_PASS='...'.")
    params = {"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASS, "query": sql}
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, method="POST")
    with urllib.request.urlopen(req, timeout=20) as resp:
        return resp.read().decode("utf-8", errors="ignore")


# -----------------------------
# Read via JDBC
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
        .option("numPartitions", "1")  # SAFE
        .load()
    )


# -----------------------------
# Create target table DDL (fixed schema, safe)
# -----------------------------
def create_target_table_if_needed():
    ddl = f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
  gemeinde Nullable(String),
  anzahl_gebaeude UInt64,
  total_leistung_kwp Float64,
  total_strom_kwh Float64,
  total_modulflaeche_m2 Float64,
  avg_leistung_kwp Float64,
  avg_strom_kwh Float64,
  processed_at DateTime
)
ENGINE = MergeTree
ORDER BY tuple()
"""
    ch_http_exec(ddl.strip())


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0,
                    help="Optional: LIMIT beim Lesen (0 = kein Limit). Für Tests z.B. 50000.")
    ap.add_argument("--write", action="store_true",
                    help="Schreibt Ergebnis nach ClickHouse (default: dry-run).")
    ap.add_argument("--max-write-rows", type=int, default=50000,
                    help="Safety-Limit für Anzahl Gemeinden (Default 50k).")
    ap.add_argument("--top", type=int, default=20,
                    help="Wie viele Top-Gemeinden anzeigen (Default 20).")
    args = ap.parse_args()

    if not CLICKHOUSE_PASS:
        print("❌ CH_PASS ist leer. Bitte: export CH_PASS='...'", file=sys.stderr)
        sys.exit(2)

    spark = SparkSession.builder.appName("UC1_Compute_Potential_By_Gemeinde_SAFE").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("==> UC1 Compute Start (SAFE)")
    print(f"    JDBC_URL={JDBC_URL}")
    print(f"    SOURCE={SOURCE_TABLE}")
    print(f"    TARGET={TARGET_TABLE}")
    print(f"    LIMIT={args.limit if args.limit else 'NONE'}")
    print(f"    WRITE={'YES' if args.write else 'NO (dry-run)'}")

    # ensure driver
    spark._jvm.java.lang.Class.forName(CLICKHOUSE_DRIVER)
    print(f"✅ JDBC Driver geladen: {CLICKHOUSE_DRIVER}")

    # read source (optional LIMIT)
    if args.limit and args.limit > 0:
        src = f"(SELECT * FROM {SOURCE_TABLE} LIMIT {int(args.limit)}) t"
    else:
        src = SOURCE_TABLE

    df = read_ch(spark, src)

    # minimal sanity
    required = {"gemeinde", "leistung_kwp", "strom_kwh", "modulflaeche"}
    missing = required - set(df.columns)
    if missing:
        print(f"❌ Quelle hat nicht alle benötigten Spalten. Fehlen: {sorted(missing)}", file=sys.stderr)
        spark.stop()
        sys.exit(3)

    print("==> Source schema:")
    df.printSchema()
    print("==> Source sample:")
    df.select("gemeinde", "leistung_kwp", "strom_kwh", "modulflaeche").show(10, truncate=False)

    # aggregation
    agg = (
        df.groupBy("gemeinde")
        .agg(
            F.count(F.lit(1)).alias("anzahl_gebaeude"),
            F.sum(F.col("leistung_kwp")).cast("double").alias("total_leistung_kwp"),
            F.sum(F.col("strom_kwh")).cast("double").alias("total_strom_kwh"),
            F.sum(F.col("modulflaeche")).cast("double").alias("total_modulflaeche_m2"),
            F.avg(F.col("leistung_kwp")).cast("double").alias("avg_leistung_kwp"),
            F.avg(F.col("strom_kwh")).cast("double").alias("avg_strom_kwh"),
        )
        .withColumn("processed_at", F.current_timestamp())
    )

    # show top
    print(f"==> Top {args.top} Gemeinden nach total_leistung_kwp:")
    (
        agg.orderBy(F.col("total_leistung_kwp").desc_nulls_last())
        .select(
            "gemeinde",
            "anzahl_gebaeude",
            F.round("total_leistung_kwp", 2).alias("total_leistung_kwp"),
            F.round("total_strom_kwh", 2).alias("total_strom_kwh"),
            F.round("total_modulflaeche_m2", 2).alias("total_modulflaeche_m2"),
            F.round("avg_leistung_kwp", 2).alias("avg_leistung_kwp"),
        )
        .show(args.top, truncate=False)
    )

    out_count = agg.count()
    print(f"==> Ergebnis-Zeilen (Anzahl Gemeinden): {out_count}")

    if not args.write:
        print("✅ DRY-RUN fertig. Kein Schreiben nach ClickHouse.")
        spark.stop()
        return

    # safety guard
    if out_count > args.max_write_rows:
        print(f"❌ Safety-Stop: out_count={out_count} > max_write_rows={args.max_write_rows}")
        print("   Erhöhe max-write-rows nur wenn du sicher bist.")
        spark.stop()
        sys.exit(4)

    print("==> Creating target table (IF NOT EXISTS)...")
    create_target_table_if_needed()
    print("✅ Target table bereit.")

    print("==> Writing to ClickHouse (APPEND, no overwrite)...")
    (
        agg.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("driver", CLICKHOUSE_DRIVER)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASS)
        .option("dbtable", TARGET_TABLE)
        .option("batchsize", "5000")
        .mode("append")
        .save()
    )

    print(f"✅ Fertig. Ergebnis appended nach {TARGET_TABLE}.")
    print("   Tipp zum Prüfen in ClickHouse:")
    print(f"   SELECT count(*) FROM {TARGET_TABLE};")
    print(f"   SELECT * FROM {TARGET_TABLE} ORDER BY total_leistung_kwp DESC LIMIT 20;")

    spark.stop()


if __name__ == "__main__":
    main()
