from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("WeatherData_Cleaning").getOrCreate()

# --- ClickHouse Connection ---
CLICKHOUSE_HOST = "clickhouse-server"
CLICKHOUSE_DB = "default"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASS = "zVNQxQbCK7nwUYYAiGsuxhf3RU0cO4gK"

jdbc_url = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:8123/{CLICKHOUSE_DB}?ssl=false"

# --- Read table weather_data ---
df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "weather_data")
    .option("user", CLICKHOUSE_USER)
    .option("password", CLICKHOUSE_PASS)
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    .load()
)

print("Rows (raw):", df.count())
df.show(5)

# --- Use-Case 2: Daten bereinigen ---
# Beispiel-Regeln:
# - Temperatur < -50 oder > 60 => NULL
# - NULLs in temp/humidity entfernen
# - Neue Spalte: temp_ok (True/False)

df_clean = df

# Falls deine Spaltennamen anders heißen: temp/humidity anpassen!
if "temp" in df.columns:
    df_clean = df_clean.withColumn(
        "temp",
        when((col("temp") < -50) | (col("temp") > 60), None).otherwise(col("temp"))
    )

if "humidity" in df.columns:
    df_clean = df_clean.withColumn(
        "humidity",
        when((col("humidity") < 0) | (col("humidity") > 100), None).otherwise(col("humidity"))
    )

# Neue Spalte (Beispiel)
if "temp" in df_clean.columns:
    df_clean = df_clean.withColumn("temp_ok", col("temp").isNotNull())

# Nur sinnvolle Zeilen behalten (Beispiel)
cols_to_check = [c for c in ["temp", "humidity"] if c in df_clean.columns]
for c in cols_to_check:
    df_clean = df_clean.filter(col(c).isNotNull())

print("Rows (clean):", df_clean.count())
df_clean.show(5)

# --- Export als CSV (in Spark Container) ---
out_path = "file:/spark-jobs/weather/output/weather_clean"
(
    df_clean.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(out_path)
)

print("✅ Export done:", out_path)

spark.stop()
