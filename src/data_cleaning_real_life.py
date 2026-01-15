cat > src/data_cleaning_real_life.py <<'PY'
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, when, to_timestamp, coalesce, lit
)

def main():
    spark = (
        SparkSession.builder
        .appName("data-cleaning-real-life")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Симулируем "грязные" данные
    raw = spark.createDataFrame(
        [
            (" 1 ", " CLICK ", "2026-01-15 10:00:00", "10"),
            ("1",   "click",  "2026-01-15 10:00:00", "10"),   # дубликат (после нормализации)
            ("2",   None,     "2026-01-15 11:00:00", "5"),
            ("3",   "View",   "bad_ts",              "7"),
            (None,  "click",  "2026-01-15 12:00:00", None),
        ],
        ["user_id_raw", "event_type_raw", "event_time_raw", "amount_raw"]
    )

    print("\n=== RAW ===")
    raw.show(truncate=False)

    # 1) Нормализация строк + типы
    cleaned = (
        raw
        .withColumn("user_id", trim(col("user_id_raw")).cast("int"))
        .withColumn("event_type", lower(trim(col("event_type_raw"))))
        .withColumn("event_ts", to_timestamp(col("event_time_raw"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("amount", col("amount_raw").cast("int"))
        .drop("user_id_raw", "event_type_raw", "event_time_raw", "amount_raw")
    )

    print("\n=== CLEANED (types + normalization) ===")
    cleaned.show(truncate=False)

    # 2) Правила качества данных (пример)
    # - event_type должен быть не null
    # - event_ts должен быть распарсен
    # - user_id должен быть не null
    # - amount если null -> 0 (пример бизнес-правила)
    with_rules = (
        cleaned
        .withColumn("amount", coalesce(col("amount"), lit(0)))
        .withColumn(
            "is_valid",
            when(col("user_id").isNull(), lit(False))
            .when(col("event_type").isNull(), lit(False))
            .when(col("event_ts").isNull(), lit(False))
            .otherwise(lit(True))
        )
    )

    print("\n=== With quality flag ===")
    with_rules.show(truncate=False)

    # 3) Разделяем valid и quarantine (как в проде)
    valid = with_rules.filter(col("is_valid") == True).drop("is_valid")
    quarantine = with_rules.filter(col("is_valid") == False)

    print("\n=== VALID ===")
    valid.show(truncate=False)

    print("\n=== QUARANTINE (bad records) ===")
    quarantine.show(truncate=False)

    # 4) Дедупликация (после нормализации!)
    deduped = valid.dropDuplicates(["user_id", "event_type", "event_ts", "amount"])

    print("\n=== DEDUPED ===")
    deduped.show(truncate=False)

    # 5) Запись локально (для проверки)
    out_good = "output/cleaning/valid_parquet"
    out_bad = "output/cleaning/quarantine_parquet"
    deduped.write.mode("overwrite").parquet(out_good)
    quarantine.write.mode("overwrite").parquet(out_bad)

    print(f"\nWritten VALID to: {out_good}")
    print(f"Written QUARANTINE to: {out_bad}")

    spark.stop()

if __name__ == "__main__":
    main()
PY
