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
CH_HOST = os.environ.get("CH_HOST", "172.25.0.2")
CH_HTTP_PORT = int(os.environ.get("CH_HTTP_PORT", "8123"))
CH_DB = os.environ.get("CH_DB", "default")
CH_USER = os.environ.get("CH_USER", "default")
CH_PASS = os.environ.get("CH_PASS", "")

# Original Source (wichtig!)
SOURCE_TABLE = os.environ.get("UC3_SOURCE", f"{CH_DB}.solarkataster_mv")  # oder default.solarkataster

TARGET_TABLE = os.environ.get("UC3_TARGET", f"{CH_DB}.uc3_potential_by_gemeinde_original")

JDBC_URL = os.environ.get(
    "CH_JDBC_URL",
    f"jdbc:clickhouse://{CH_HOST}:{CH_HTTP_PORT}/{CH_DB}?protocol=http&ssl=false&compress=0"
)

CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"

def ch_http_exec(sql: str) -> str:
    if not CH_PASS:
        raise RuntimeError("CH_PASS ist leer. Bitte export CH_PASS='...'.")
    params = {"user": CH_USER, "password": CH_PASS, "query": sql}
    url = f"http://{CH_HOST}:{CH_HTTP_PORT}/?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, method="POST")
    with urllib.request.urlopen(req, timeout=20) as resp:
        return resp.read().decode("utf-8", errors="ignore")

def spark_type_to_ch(dt: T.DataType) -> str:
    if isinstance(dt, T.StringType): return "String"
    if isinstance(dt, T.BooleanType): return "UInt8"
    if isinstance(dt, T.ByteType): return "Int8"
    if isinstance(dt, T.ShortType): return "Int16"
    if isinstance(dt, T.IntegerType): return "Int32"
    if isinstance(dt, T.LongType): return "Int64"
    if isinstance(dt, T.FloatType): return "Float32"
    if isinstance(dt, T.DoubleType): return "Float64"
    if isinstance(dt, T.DateType): return "Date"
    if isinstance(dt, T.TimestampType): return "DateTime"
    if isinstance(dt, T.DecimalType): return f"Decimal({dt.precision},{dt.scale})"
    return "String"

def build_create_table_ddl(df, target_table: str) -> str:
    cols = []
    for f in df.schema.fields:
        ch_t = spark_type_to_ch(f.dataType)
        if f.nullable and not ch_t.startswith("Nullable("):
            ch_t = f"Nullable({ch_t})"
        cols.append(f"`{f.name}` {ch_t}")
    cols_sql = ",\n  ".join(cols)
    ddl = f"""
CREATE TABLE IF NOT EXISTS {target_table} (
  {cols_sql}
)
ENGINE = MergeTree
ORDER BY tuple()
"""
    return ddl.strip()

def read_ch(spark: SparkSession, table_or_query: str):
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("driver", CLICKHOUSE_DRIVER)
        .option("user", CH_USER)
        .option("password", CH_PASS)
        .option("dbtable", table_or_query)
        .option("fetchsize", "10000")
        .option("numPartitions", "1")
        .load()
    )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=20)
    ap.add_argument("--limit", type=int, default=0, help="0 = kein LIMIT (vorsichtig!)")
    ap.add_argument("--write", action="store_true", help="Schreibe Ergebnis nach ClickHouse (default: dry-run)")
    ap.add_argument("--max-write-rows", type=int, default=200000)
    args = ap.parse_args()

    if not CH_PASS:
        print("❌ CH_PASS ist leer. Bitte: export CH_PASS='...'", file=sys.stderr)
        sys.exit(2)

    spark = SparkSession.builder.appName("UC3_Potential_By_Gemeinde_Original_SAFE").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("==> UC3 Start (SAFE)")
    print(f"    SOURCE={SOURCE_TABLE}")
    print(f"    TARGET={TARGET_TABLE}")
    print(f"    TOP={args.top}")
    print(f"    LIMIT={args.limit}")
    print(f"    WRITE={'YES' if args.write else 'NO (dry-run)'}")

    spark._jvm.java.lang.Class.forName(CLICKHOUSE_DRIVER)
    print(f"✅ JDBC Driver geladen: {CLICKHOUSE_DRIVER}")

    # optional LIMIT
    if args.limit and args.limit > 0:
        src = f"(SELECT * FROM {SOURCE_TABLE} LIMIT {int(args.limit)}) t"
    else:
        src = f"(SELECT * FROM {SOURCE_TABLE}) t"

    df = read_ch(spark, src)

    # sehr wichtig: nur Gemeinden mit Wert
    df2 = df.filter(F.col("gemeinde").isNotNull() & (F.trim(F.col("gemeinde")) != ""))

    # compute
    out = (
        df2.groupBy("gemeinde")
        .agg(
            F.count(F.lit(1)).alias("records"),
            F.sum(F.col("area")).alias("sum_area"),
            F.sum(F.col("modulflaeche")).alias("sum_modulflaeche"),
            F.sum(F.col("leistung_kwp")).alias("sum_leistung_kwp"),
            F.sum(F.col("strom_kwh")).alias("sum_strom_kwh"),
        )
        .withColumn("processed_at", F.current_timestamp())
        .orderBy(F.col("sum_leistung_kwp").desc_nulls_last())
    )

    print(f"==> TOP {args.top} Gemeinden nach sum_leistung_kwp:")
    out.show(args.top, truncate=False)

    out_count = out.count()
    print(f"==> Ergebnis-Zeilen (Anzahl Gemeinden): {out_count}")

    if not args.write:
        print("✅ UC3 fertig (DRY-RUN). Kein Schreiben nach ClickHouse durchgeführt.")
        spark.stop()
        return

    if out_count > args.max_write_rows:
        print(f"❌ Safety-Stop: out_count={out_count} > max_write_rows={args.max_write_rows}")
        spark.stop()
        sys.exit(3)

    print("==> Creating target table (IF NOT EXISTS)...")
    ddl = build_create_table_ddl(out, TARGET_TABLE)
    ch_http_exec(ddl)
    print("✅ Target table bereit.")

    print("==> Writing to ClickHouse (APPEND, no overwrite)...")
    (
        out.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("driver", CLICKHOUSE_DRIVER)
        .option("user", CH_USER)
        .option("password", CH_PASS)
        .option("dbtable", TARGET_TABLE)
        .option("batchsize", "5000")
        .mode("append")
        .save()
    )

    print(f"✅ UC3 fertig. Ergebnis appended nach {TARGET_TABLE}")
    print("   Tipp zum Prüfen in ClickHouse:")
    print(f"   SELECT count() FROM {TARGET_TABLE};")
    print(f"   SELECT * FROM {TARGET_TABLE} ORDER BY sum_leistung_kwp DESC LIMIT 20;")
    spark.stop()

if __name__ == "__main__":
    main()
