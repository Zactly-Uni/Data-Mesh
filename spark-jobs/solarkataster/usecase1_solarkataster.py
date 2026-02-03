#!/usr/bin/env python3
import os
import sys
import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


# --- Default SAFE Settings ---
DEFAULT_JDBC_URL = "jdbc:clickhouse://172.25.0.2:8123/default?protocol=http&ssl=false&compress=0"
CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"

DEFAULT_SOURCE_TABLE = "default.solarkataster_mv"
DEFAULT_TARGET_TABLE = "default.solarkataster_uc1_clean"

# ClickHouse JDBC options for Spark read/write
def jdbc_options(jdbc_url: str, user: str, password: str, driver: str):
    return {
        "url": jdbc_url,
        "user": user,
        "password": password,
        "driver": driver,
        # Safety-ish / stability options:
        "socket_timeout": "300000",
        "connect_timeout": "10000",
        # Spark write batching (safe small default)
        "batchsize": "5000",
        "isolationLevel": "NONE",
    }


def require_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"ENV {name} ist nicht gesetzt. Beispiel: export {name}='...'.")
    return v


def load_driver_or_fail(spark: SparkSession, driver_class: str):
    # Ensures driver is on classpath (you already fixed this via spark-submit flags)
    spark._jvm.java.lang.Class.forName(driver_class)
    print(f"✅ JDBC Driver geladen: {driver_class}")


def ch_execute_ddl(spark: SparkSession, jdbc_url: str, user: str, password: str, driver: str, ddl: str):
    """
    Executes a DDL statement via JDBC using JVM DriverManager.
    (Spark JDBC read/write cannot reliably run DDL.)
    """
    jvm = spark._jvm
    jvm.java.lang.Class.forName(driver)

    conn = None
    stmt = None
    try:
        conn = jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        stmt = conn.createStatement()
        stmt.execute(ddl)
    finally:
        try:
            if stmt is not None:
                stmt.close()
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def read_clickhouse_table(spark: SparkSession, opts: dict, table: str):
    return (
        spark.read.format("jdbc")
        .options(**opts)
        .option("dbtable", table)
        .load()
    )


def read_clickhouse_query(spark: SparkSession, opts: dict, query: str):
    # Spark JDBC expects the query wrapped as a subquery with alias
    q = f"({query}) AS q"
    return (
        spark.read.format("jdbc")
        .options(**opts)
        .option("dbtable", q)
        .load()
    )


def transform_solarkataster(df):
    """
    Simple, nachvollziehbare Transformationen:
    - Strings trimmen
    - Zahlen casten
    - negative/unsinnige Werte zu NULL (optional)
    - processed_at hinzufügen
    """
    # Ensure columns exist (best effort)
    cols = df.columns

    def safe_trim(c):
        return F.when(F.col(c).isNull(), F.lit(None)).otherwise(F.trim(F.col(c)))

    out = df

    for c in ["id", "source", "regbez", "kreis", "gemeinde", "gebaeudefunktion", "geometry"]:
        if c in cols:
            out = out.withColumn(c, safe_trim(c))

    # Cast numeric columns to double safely
    for c in ["area", "modulflaeche", "leistung_kwp", "strom_kwh"]:
        if c in cols:
            out = out.withColumn(c, F.col(c).cast("double"))

    # Simple cleaning rules (optional but sensible)
    # area/modulflaeche: should not be negative
    if "area" in cols:
        out = out.withColumn("area", F.when(F.col("area") < 0, F.lit(None)).otherwise(F.col("area")))
    if "modulflaeche" in cols:
        out = out.withColumn("modulflaeche", F.when(F.col("modulflaeche") < 0, F.lit(None)).otherwise(F.col("modulflaeche")))
    if "leistung_kwp" in cols:
        out = out.withColumn("leistung_kwp", F.when(F.col("leistung_kwp") < 0, F.lit(None)).otherwise(F.col("leistung_kwp")))
    if "strom_kwh" in cols:
        out = out.withColumn("strom_kwh", F.when(F.col("strom_kwh") < 0, F.lit(None)).otherwise(F.col("strom_kwh")))

    # Add processed_at
    out = out.withColumn("processed_at", F.current_timestamp())

    # Optional: drop duplicates by id if you want
    if "id" in cols:
        out = out.dropDuplicates(["id"])

    return out


def create_target_table_if_needed(spark, jdbc_url, user, password, driver, target_table):
    """
    Creates a safe target table. Will NOT touch the source table.
    """
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
        id String,
        source Nullable(String),
        regbez Nullable(String),
        kreis Nullable(String),
        gemeinde Nullable(String),
        gebaeudefunktion Nullable(String),
        area Nullable(Float64),
        modulflaeche Nullable(Float64),
        leistung_kwp Nullable(Float64),
        strom_kwh Nullable(Float64),
        geometry Nullable(String),
        processed_at DateTime
    )
    ENGINE = MergeTree
    ORDER BY id
    """
    ch_execute_ddl(spark, jdbc_url, user, password, driver, ddl)
    print(f"✅ Zieltabelle vorhanden/erstellt: {target_table}")


def write_to_clickhouse(df, opts: dict, target_table: str):
    """
    SAFE write: append only (no overwrite).
    """
    (
        df.write.format("jdbc")
        .options(**opts)
        .option("dbtable", target_table)
        .mode("append")
        .save()
    )


def main():
    parser = argparse.ArgumentParser(description="UC1 Solarkataster: Read -> Transform -> (optional) Write to ClickHouse safely.")
    parser.add_argument("--jdbc-url", default=DEFAULT_JDBC_URL)
    parser.add_argument("--user", default="default")
    parser.add_argument("--source-table", default=DEFAULT_SOURCE_TABLE)
    parser.add_argument("--target-table", default=DEFAULT_TARGET_TABLE)
    parser.add_argument("--sample-n", type=int, default=10, help="How many rows to show as sample.")
    parser.add_argument("--write", action="store_true", help="Actually write into target table (append). Without this flag = READ ONLY.")
    parser.add_argument("--max-write-rows", type=int, default=200000, help="Safety guard. Abort if df.count() exceeds this number.")
    args = parser.parse_args()

    ch_pass = require_env("CH_PASS")

    print("==> Start UC1 (SAFE)")
    print(f"    JDBC_URL={args.jdbc_url}")
    print(f"    USER={args.user}")
    print(f"    SOURCE={args.source_table}")
    print(f"    TARGET={args.target_table}")
    print(f"    WRITE={'YES' if args.write else 'NO (READ ONLY)'}")
    print(f"    MAX_WRITE_ROWS={args.max_write_rows}")

    spark = (
        SparkSession.builder
        .appName("UC1_Solarkataster_SAFE_ETL")
        .getOrCreate()
    )

    try:
        # Ensure driver is present
        load_driver_or_fail(spark, CLICKHOUSE_DRIVER)

        opts = jdbc_options(args.jdbc_url, args.user, ch_pass, CLICKHOUSE_DRIVER)

        # Quick smoke test that doesn't rely on SELECT 1 failing quirks:
        print("==> Test: system.one lesen (muss klappen)")
        df_test = read_clickhouse_query(spark, opts, "SELECT 1 AS x")
        df_test.show()

        # Read sample from source
        print(f"==> Lese Solarkataster Sample aus: {args.source_table} (LIMIT {args.sample_n})")
        df_sample = read_clickhouse_query(
            spark, opts, f"SELECT * FROM {args.source_table} LIMIT {args.sample_n}"
        )
        print("==> Schema:")
        df_sample.printSchema()
        print("==> Sample rows:")
        df_sample.show(args.sample_n, truncate=False)

        # Read full source (careful)
        print(f"==> Lese komplette Quelle: {args.source_table}")
        df_src = read_clickhouse_table(spark, opts, args.source_table)

        # Transform
        print("==> Transformiere Daten ...")
        df_out = transform_solarkataster(df_src)

        print("==> Transformiertes Sample (Anzeige):")
        df_out.limit(args.sample_n).show(args.sample_n, truncate=False)

        # If not writing, finish here
        if not args.write:
            print("\n✅ UC1 fertig (READ ONLY). Kein Schreiben nach ClickHouse durchgeführt.")
            return

        # Safety guard before any write
        print("==> Safety Check: row count (kann kurz dauern) ...")
        cnt = df_out.count()
        print(f"==> Rows nach Transformation: {cnt}")

        if cnt > args.max_write_rows:
            raise RuntimeError(
                f"ABORT: {cnt} rows > max-write-rows={args.max_write_rows}. "
                f"Erhöhe max-write-rows nur wenn du sicher bist."
            )

        # Create target table safely
        print("==> Erstelle Zieltabelle (falls nicht existiert) ...")
        create_target_table_if_needed(spark, args.jdbc_url, args.user, ch_pass, CLICKHOUSE_DRIVER, args.target_table)

        # Write (append only)
        print("==> Schreibe nach ClickHouse (APPEND ONLY) ...")
        # Reduce partitions to avoid too many concurrent JDBC connections (safer)
        df_to_write = df_out.repartition(1)

        write_to_clickhouse(df_to_write, opts, args.target_table)

        print(f"\n✅ UC1 fertig: Daten wurden in {args.target_table} appended.")
        print("   Tipp: In ClickHouse prüfen mit:")
        print(f"   SELECT count(*) FROM {args.target_table};")
        print(f"   SELECT * FROM {args.target_table} LIMIT 10;")

    except Exception as e:
        print("❌ Fehler im UC1 Script.")
        print("===== PYTHON EXCEPTION =====")
        print(str(e))
        raise
    finally:
        try:
            spark.stop()
        except Exception:
            pass


if __name__ == "__main__":
    main()

