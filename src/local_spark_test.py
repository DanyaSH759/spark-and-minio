from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def main():
    spark = (
        SparkSession.builder
        .appName("local-spark-test")
        .master("local[*]")
        .getOrCreate()
    )

    # Простой DataFrame
    df = spark.createDataFrame(
        [
            (1, "click"),
            (1, "view"),
            (2, "click"),
            (1, "click"),
        ],
        ["user_id", "event_type"]
    )

    # Агрегация
    result = (
        df.groupBy("user_id", "event_type")
        .agg(count("*").alias("cnt"))
        .orderBy(col("cnt").desc())
    )

    result.show()

    # Запись в локальную файловую систему
    output_path = "output/local_test_parquet"
    (
        result.write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(f"Written to {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()