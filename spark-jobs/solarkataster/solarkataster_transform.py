from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SolarkatasterTransform").getOrCreate()

CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "zVNQxQbCK7nwUYYAiGsuxhf3RU0cO4gK"   # genau das Passwort, das du eben im clickhouse-client eingegeben hast
jdbc_url = "jdbc:clickhouse://localhost:18123/solar_data_mesh?ssl=false"


df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "(SELECT 1 AS x) t")
    .option("user", CLICKHOUSE_USER)
    .option("password", CLICKHOUSE_PASSWORD)
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    .load()
)

df.show()

print("Rows:", df.count())

# Use-case: Daten bereinigen
df_clean = df \
    .withColumn("leistung_kwp", when(col("leistung_kwp") < 0, None).otherwise(col("leistung_kwp"))) \
    .withColumn("strom_kwh", when(col("strom_kwh") < 0, None).otherwise(col("strom_kwh"))) \
    .withColumn("effizienz",
        when((col("leistung_kwp").isNotNull()) & (col("leistung_kwp") > 0),
             col("strom_kwh") / col("leistung_kwp")
        ).otherwise(None)
    )

# Optional: Filter auf sinnvolle Werte
df_clean = df_clean.filter(col("leistung_kwp").isNotNull())

# Ergebnis als CSV auf Server speichern
out_path = "file:///spark-jobs/solarkataster/output/solarkataster_clean"
df_clean.coalesce(1).write.mode("overwrite").option("header","true").csv(out_path)

print("✅ Export done:", out_path)

spark.stop()

