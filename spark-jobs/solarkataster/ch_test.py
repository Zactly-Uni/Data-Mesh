#!/usr/bin/env python3
from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError

CH_HOST = "172.25.0.2"
CH_HTTP_PORT = 8123
CH_DB = "default"
CH_USER = "default"
CH_PASSWORD = "zVNQxQbCK7nwUYYAiGsuxhf3RU0cO4gK"

# Wichtig: Spark macht Schema-Checks; Subquery + Alias ist am robustesten
DBTABLE = "(SELECT 1 AS x) AS t"

# ClickHouse JDBC (HTTP)
# compress=0 ist manchmal stabiler, timeouts hochsetzen ist harmlos
JDBC_URL = (
    f"jdbc:clickhouse://{CH_HOST}:{CH_HTTP_PORT}/{CH_DB}"
    f"?compress=0&socket_timeout=300000&connection_timeout=300000"
)

spark = SparkSession.builder.appName("CH_Test").getOrCreate()

try:
    df = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("user", CH_USER)
        .option("password", CH_PASSWORD)
        .option("dbtable", DBTABLE)
        .load()
    )

    df.show(truncate=False)

except Py4JJavaError as e:
    print("\n=== PY4J ERROR ===")
    print(str(e))

    try:
        je = e.java_exception
        print("\n=== JAVA EXCEPTION CLASS ===")
        print(je.getClass().getName())

        print("\n=== JAVA MESSAGE ===")
        print(je.getMessage())

        # Wichtig: viele JDBC-Fehler stecken hier drin
        try:
            ne = je.getNextException()
            if ne is not None:
                print("\n=== JAVA nextException ===")
                print(ne.getClass().getName())
                print(ne.getMessage())
        except Exception:
            pass

        # Ursache (Cause) ausgeben
        try:
            cause = je.getCause()
            if cause is not None:
                print("\n=== JAVA CAUSE ===")
                print(cause.getClass().getName())
                print(cause.getMessage())
        except Exception:
            pass

        # Voller Stacktrace (geht nach stderr, ist aber Gold wert)
        try:
            print("\n=== JAVA STACKTRACE ===")
            je.printStackTrace()
        except Exception:
            pass

    except Exception:
        pass

    raise
finally:
    spark.stop()

