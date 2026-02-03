import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

CH_HOST = os.getenv("CH_HOST", "172.25.0.2")
CH_PORT = os.getenv("CH_HTTP_PORT", "8123")
CH_DB   = os.getenv("CH_DB", "default")
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASS", "")

JDBC_URL = f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/{CH_DB}"

CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"

def read_query(spark, query: str):
    # Spark erwartet dbtable als Subquery in Klammern mit Alias
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("driver", CLICKHOUSE_DRIVER)
        .option("user", CH_USER)
        .option("password", CH_PASS)
        .option("dbtable", f"({query}) AS t")
        .load()
    )

def main():
    spark = (
        SparkSession.builder
        .appName("UC1_Solarkataster_SAFE_READONLY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # WICHTIG: Treiber explizit laden, sonst "No suitable driver"
    spark._jvm.java.lang.Class.forName(CLICKHOUSE_DRIVER)

    print("==> UC1 Start (SAFE: nur lesen/anzeigen)")
    print("JDBC_URL=", JDBC_URL)
    print("USER=", CH_USER)
    print("PASS_LEN=", len(CH_PASS))

    print("\n==> Test: SELECT 1")
    df1 = read_query(spark, "SELECT 1 AS x")
    df1.show()

    # Beispiel: Solarkataster - nur kleine Sample-Abfrage (LIMIT!)
    # WICHTIG: wir machen NICHT "SELECT *" ohne Limit.
    print("\n==> Lade Sample aus solarkataster (LIMIT 50)")
    df = read_query(spark, "SELECT * FROM solarkataster LIMIT 50")

    print("Schema:")
    df.printSchema()

    print("\n==> Kleine Transformation (Beispiel): neue Spalte quality_flag")
    # Beispiel-Logik: falls eine Spalte nicht existiert, ist das egal -> du passt sie gleich an
    # Wir machen eine generische Transformation: Nulls markieren
    df2 = df
    for c in df.columns[:5]:  # nur erste 5 Spalten anschauen (SAFE)
        df2 = df2.withColumn(
            f"{c}_isnull",
            when(col(c).isNull(), 1).otherwise(0)
        )

    df2.show(20, truncate=False)

    print("\n✅ UC1 fertig. (Noch nichts in ClickHouse geschrieben)")
    spark.stop()

if __name__ == "__main__":
    main()

