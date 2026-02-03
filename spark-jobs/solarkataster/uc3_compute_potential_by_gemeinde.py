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


# -----------------------------
# ClickHouse HTTP EXEC (nur DDL)
# -----------------------------
def ch_http_exec(sql: str) -> str:
    if not CLICKHOUSE_PASS:
        raise RuntimeError("CH_PASS ist leer. Bitte export CH_PASS='...'.")
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
# Read ClickHouse via JDBC
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
        .option("numPartitions", "1")  # SAFE: nicht viele parallele Connections
        .load()
    )


# -----------------------------
# Create target table for UC3 result
# -----------------------------
def create_uc3_target_table(target_table: str):
    ddl = f"""
CREATE TABLE IF NOT EXISTS {target_table} (
  gemeinde String,
  buildings UInt64,
  sum_area Float64,
  sum_modulflaeche Float64,
  sum_leistung_kwp Float64,
  sum_strom_kwh Float64,
  avg_leistung_kwp Float64,
  avg_strom_kwh Float64,
  processed_at DateTime
)
ENGINE = MergeTree
ORDER BY (gemeinde)
"""
    ch_http_exec(ddl.strip())


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default=f"{CLICKHOUSE_DB}.solarkataster_uc1_clean",
                    help="Quelle (z.B. default.solarkataster_uc1_clean oder default.solarkataster_mv)")
    ap.add_argument("--top", type=int, default=20, help="Top N Gemeinden (Default: 20)")
    ap.add_argument("--limit", type=int, default=0,
                    help="SAFE-Limit beim Einlesen (0 = kein Limit). Für Tests z.B. 50000")
    ap.add_argument("--write", action="store_true",
                    help="Ergebnis nach ClickHouse schreiben (Default: nur anzeigen)")
    ap.add_argument("--target", default=f"{CLICKHOUSE_DB}.uc3_potential_by_gemeinde",
                    help="Zieltabelle für UC3 (nur wenn --write)")
    ap.add_argument("--max-source-rows", type=int, default=500000,
                    help="Safety: verweigere Run, wenn Quelle mehr als diese Anzahl Zeilen hat (nur wenn limit=0)")
    args = ap.parse_args()

    if not CLICKHOUSE_PASS:
        print("❌ CH_PASS ist leer. Bitte: export CH_PASS='...'", file=sys.stderr)
        sys.exit(2)

    spark = (
        SparkSession.builder
        .appName("UC3_Compute_Potential_by_Gemeinde_SAFE")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("==> UC3 Start (SAFE)")
    print(f"    JDBC_URL={JDBC_URL}")
    print(f"    SOURCE={args.source}")
    print(f"    TOP={args.top}")
    print(f"    LIMIT={args.limit if args.limit > 0 else 'NONE'}")
    print(f"    WRITE={'YES' if args.write else 'NO'}")
    print(f"    TARGET={args.target if args.write else '-'}")

    # Driver load check
    spark._jvm.java.lang.Class.forName(CLICKHOUSE_DRIVER)
    print(f"✅ JDBC Driver geladen: {CLICKHOUSE_DRIVER}")

    # Build query safely
    if args.limit and args.limit > 0:
        src_query = f"(SELECT * FROM {args.source} LIMIT {int(args.limit)}) t"
    else:
        # Safety check: count source (kann kurz dauern)
        count_query = f"(SELECT count(*) AS c FROM {args.source}) t"
        cdf = read_ch(spark, count_query)
        total = cdf.collect()[0]["c"]
        print(f"==> Source rowcount: {total}")

        if total > args.max_source_rows:
            print(f"❌ Safety-Stop: Quelle hat {total} Zeilen > max-source-rows={args.max_source_rows}.")
            print("   Lösung: starte mit --limit 50000 oder erhöhe max-source-rows nur wenn du sicher bist.")
            spark.stop()
            sys.exit(3)

        src_query = f"(SELECT * FROM {args.source}) t"

    df = read_ch(spark, src_query)

    print("==> Source schema:")
    df.printSchema()
    print("==> Source sample:")
    df.show(5, truncate=False)

    # Minimal required columns checks
    needed = ["gemeinde"]
    for col in needed:
        if col not in df.columns:
            print(f"❌ Spalte '{col}' fehlt in Quelle. Prüfe ob du richtige Tabelle nutzt.", file=sys.stderr)
            spark.stop()
            sys.exit(4)

    # Fill numeric cols if missing
    def safe_num_col(name, dtype="double"):
        if name in df.columns:
            return F.col(name).cast(dtype)
        return F.lit(0.0).cast(dtype)

    df2 = (
        df
        .filter(F.col("gemeinde").isNotNull() & (F.trim(F.col("gemeinde")) != ""))
        .withColumn("area_num", safe_num_col("area"))
        .withColumn("modulflaeche_num", safe_num_col("modulflaeche"))
        .withColumn("leistung_kwp_num", safe_num_col("leistung_kwp"))
        .withColumn("strom_kwh_num", safe_num_col("strom_kwh"))
    )

    # Aggregation = "Potential pro Gemeinde"
    agg = (
        df2.groupBy("gemeinde")
        .agg(
            F.count(F.lit(1)).cast("bigint").alias("buildings"),
            F.sum("area_num").alias("sum_area"),
            F.sum("modulflaeche_num").alias("sum_modulflaeche"),
            F.sum("leistung_kwp_num").alias("sum_leistung_kwp"),
            F.sum("strom_kwh_num").alias("sum_strom_kwh"),
            F.avg("leistung_kwp_num").alias("avg_leistung_kwp"),
            F.avg("strom_kwh_num").alias("avg_strom_kwh"),
        )
        .withColumn("processed_at", F.current_timestamp())
    )

    # Sort: meistens ist "sum_leistung_kwp" oder "sum_strom_kwh" das "Potential"
    result = agg.orderBy(F.col("sum_leistung_kwp").desc()).limit(int(args.top))

    print(f"==> Top {args.top} Gemeinden nach sum_leistung_kwp:")
    result.show(200, truncate=False)

    # Warum manchmal weniger als Top 20?
    # -> Wenn es in den eingelesenen Daten weniger als 20 unterschiedliche Gemeinden gibt,
    #    oder viele rows NULL/leer waren und rausgefiltert wurden.

    if not args.write:
        print("✅ UC3 fertig (nur Anzeige). Kein Schreiben nach ClickHouse.")
        spark.stop()
        return

    # Create target table + append
    print("==> Creating target table (IF NOT EXISTS)...")
    create_uc3_target_table(args.target)
    print("✅ Target table bereit.")

    print("==> Writing to ClickHouse (APPEND, no overwrite)...")
    (
        result.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("driver", CLICKHOUSE_DRIVER)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASS)
        .option("dbtable", args.target)
        .option("batchsize", "5000")
        .mode("append")
        .save()
    )

    print(f"✅ UC3 fertig. Ergebnis appended nach {args.target}.")
    spark.stop()


if __name__ == "__main__":
    main()
