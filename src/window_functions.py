cat > src/window_functions.py <<'PY'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, sum as fsum, row_number, dense_rank
from pyspark.sql.window import Window

def main():
    spark = (
        SparkSession.builder
        .appName("window-functions")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Пример: транзакции (user_id, ts, amount)
    df = spark.createDataFrame(
        [
            (1, "2026-01-15 10:00:00", 10),
            (1, "2026-01-15 10:10:00", 5),
            (1, "2026-01-15 11:00:00", 7),
            (2, "2026-01-15 09:00:00", 3),
            (2, "2026-01-15 12:00:00", 20),
            (3, "2026-01-15 08:30:00", 1),
        ],
        ["user_id", "ts", "amount"]
    ).withColumn("ts", to_timestamp(col("ts"), "yyyy-MM-dd HH:mm:ss"))

    print("\n=== Base data ===")
    df.orderBy("user_id", "ts").show(truncate=False)

    # 1) running sum: накопительная сумма по пользователю
    w_user_time = Window.partitionBy("user_id").orderBy(col("ts")).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    running = df.withColumn("running_amount", fsum(col("amount")).over(w_user_time))

    print("\n=== 1) Running sum per user ===")
    running.orderBy("user_id", "ts").show(truncate=False)

    # 2) row_number: нумерация событий
    w_user_order = Window.partitionBy("user_id").orderBy(col("ts"))
    numbered = df.withColumn("row_number", row_number().over(w_user_order))

    print("\n=== 2) row_number per user (order by ts) ===")
    numbered.orderBy("user_id", "ts").show(truncate=False)

    # 3) dense_rank: топ-N транзакций по сумме (внутри пользователя)
    w_rank = Window.partitionBy("user_id").orderBy(col("amount").desc())
    ranked = df.withColumn("dense_rank_by_amount", dense_rank().over(w_rank))

    print("\n=== 3) dense_rank by amount (per user) ===")
    ranked.orderBy("user_id", col("amount").desc()).show(truncate=False)

    print("\n=== 4) Take top-1 transaction per user by amount (dense_rank=1) ===")
    top1 = ranked.filter(col("dense_rank_by_amount") == 1)
    top1.orderBy("user_id").show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
PY
