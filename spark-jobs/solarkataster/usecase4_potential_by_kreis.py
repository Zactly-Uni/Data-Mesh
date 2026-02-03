
import os
import sys
import argparse
import urllib.parse
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# -----------------------------
# ClickHouse Config
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

# Für UC4 bewusst ORIGINAL als Default (wie du wolltest)
SOURCE_TABLE = os.environ.get("UC4_SOURCE", f"{CLICKHOUSE_DB}.solarkataster_mv")
TARGET_TABLE = os.environ.get("UC4_TARGET", f"{CLICKHOUSE_DB}.uc4_potential_by_kreis")

# -----------------------------
# ClickHouse HTTP Exec (DDL)
# -----------------------------
def ch_http_exec(sql: str) -> str:
    if not CLICKHOUSE_PASS:
        raise RuntimeError("CH_PASS ist leer. Bitte export CH_PASS='...'.")
    params = {"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASS, "query": sql}
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, method="POST")
    with urllib.request.urlopen(req, timeout=20) as resp:
        return resp.read().decode("utf-8", errors="ignore")

# -----------------------------
# Minimaler CREATE TABLE
# -----------------------------
def create_target_table_if_needed():
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
      regbez String,
      kreis String,
      anzahl_gebaeude UInt64,
      sum_leistung_kwp Float64,
      sum_strom_kwh Float64,
      avg_leistung_kwp Float64,
      avg_strom_kwh Float64,
      processed_at DateTime
    )
    ENGINE = MergeTree
    ORDER BY (regbez, kreis)
    """
    ch_http_exec(ddl)

# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=20, help="Top-N Kreise")
    ap.add_argument("--limit", type=int, default=0, help="Optional LIMIT auf Quelle (0 = kein Limit)")
    ap.add_argument("--write", action="store_true", help="Ergebnis nach ClickHouse schreiben")
    ap.add_argument("--overwrite", action="store_true", help="Beim Schreiben die Zieltabelle überschreiben")
    args = ap.parse_args()

    if not CLICKHOUSE_PASS:
        print("❌ CH_PASS ist leer. Bitte: export CH_PASS='...'", file=sys.stderr)
        sys.exit(2)

    spark = SparkSession.builder.appName("UC4_Potential_By_Kreis").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # JDBC Driver laden
    spark._jvm.java.lang.Class.forName(CLICKHOUSE_DRIVER)

    print("==> UC4 Start")
    print(f"    SOURCE={SOURCE_TABLE}")
    print(f"    TARGET={TARGET_TABLE}")
    print(f"    TOP={args.top}")
    print(f"    LIMIT={args.limit if args.limit and args.limit>0 else 'NONE'}")
    print(f"    WRITE={'YES' if args.write else 'NO'}")

    # Quelle lesen (optional mit LIMIT via Subquery)
    if args.limit and args.limit > 0:
        src = f"(SELECT * FROM {SOURCE_TABLE} LIMIT {int(args.limit)}) t"
    else:
        src = SOURCE_TABLE

    df = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("driver", CLICKHOUSE_DRIVER)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASS)
        .option("dbtable", src)
        .option("fetchsize", "10000")
        .option("numPartitions", "1")
        .load()
    )

    # Erwartete Spalten: regbez, kreis, leistung_kwp, strom_kwh
    needed = {"regbez", "kreis", "leistung_kwp", "strom_kwh"}
    missing = [c for c in needed if c not in df.columns]
    if missing:
        print(f"❌ Fehlende Spalten in Quelle: {missing}")
        print(f"   Vorhanden: {df.columns}")
        spark.stop()
        sys.exit(3)

    # Plausible Compute-Aggregation (skalierter UC3 → UC4 auf Kreis)
    df2 = (
        df.filter(F.col("kreis").isNotNull())
          .withColumn("leistung_kwp", F.col("leistung_kwp").cast("double"))
          .withColumn("strom_kwh", F.col("strom_kwh").cast("double"))
    )

    agg = (
        df2.groupBy("regbez", "kreis")
           .agg(
               F.count(F.lit(1)).alias("anzahl_gebaeude"),
               F.sum("leistung_kwp").alias("sum_leistung_kwp"),
               F.sum("strom_kwh").alias("sum_strom_kwh"),
               F.avg("leistung_kwp").alias("avg_leistung_kwp"),
               F.avg("strom_kwh").alias("avg_strom_kwh"),
           )
           .withColumn("processed_at", F.current_timestamp())
    )

    # Top-N nach Summe kWp (kannst du auch nach kWh sortieren)
    topn = (
        agg.orderBy(F.col("sum_leistung_kwp").desc_nulls_last())
           .limit(int(args.top))
    )

    print("==> UC4 Ergebnis (Top):")
    topn.show(int(args.top), truncate=False)

    if not args.write:
        print("✅ UC4 fertig (DRY-RUN).")
        spark.stop()
        return

    # Schreiben nach ClickHouse
    print("==> Creating target table (IF NOT EXISTS)...")
    create_target_table_if_needed()
    print("✅ Target table bereit.")

    mode = "overwrite" if args.overwrite else "append"
    print(f"==> Writing to ClickHouse (mode={mode})...")

    (
        topn.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("driver", CLICKHOUSE_DRIVER)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASS)
        .option("dbtable", TARGET_TABLE)
        .option("batchsize", "5000")
        .mode(mode)
        .save()
    )

    print(f"✅ UC4 fertig. Ergebnis geschrieben nach: {TARGET_TABLE}")
    spark.stop()

if __name__ == "__main__":
    main()
