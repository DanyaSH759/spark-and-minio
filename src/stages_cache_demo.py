import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def ms(t0: float) -> int:
    return int((time.time() - t0) * 1000)

def main():
    spark = (
        SparkSession.builder
        .appName("stages-cache-demo")
        .master("local[*]")
        # сделаем шифл нагляднее и чуть тяжелее
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("AQE enabled:", spark.conf.get("spark.sql.adaptive.enabled"))
    print("shuffle.partitions:", spark.conf.get("spark.sql.shuffle.partitions"))

    # --- 1) Делаем "факт" и "справочник" ---
    # большой факт: 2 млн строк
    n = 2_000_000
    events = (
        spark.range(n)
        .select(
            (col("id") % 200000).alias("user_id"),          # много разных ключей
            (col("id") % 20).alias("event_type")            # 20 типов событий
        )
    )

    # маленький справочник: 200k пользователей
    users = (
        spark.range(200_000)
        .select(
            col("id").alias("user_id"),
            (col("id") % 50).alias("country_id")            # 50 "стран"
        )
    )

    # --- 2) Пайплайн, который гарантированно создаёт shuffle ---
    # join часто вызывает shuffle; groupBy тоже вызывает shuffle
    pipeline = (
        events.join(users, on="user_id", how="inner")
              .groupBy("country_id", "event_type")
              .agg(count("*").alias("cnt"))
    )

    print("\n=== EXPLAIN (pipeline) ===")
    pipeline.explain(True)
    print("Partitions (pipeline):", pipeline.rdd.getNumPartitions())

    # --- 3) Две actions БЕЗ cache: Spark пересчитает пайплайн ---
    print("\n=== WITHOUT CACHE: first action (count) ===")
    t0 = time.time()
    c1 = pipeline.count()
    print("result rows:", c1, "| time_ms:", ms(t0))

    print("\n=== WITHOUT CACHE: second action (take(5)) ===")
    t0 = time.time()
    top5 = pipeline.orderBy(col("cnt").desc()).take(5)
    print("top5:", top5, "| time_ms:", ms(t0))

    # --- 4) С cache: материализуем один раз, дальше читаем из памяти ---
    print("\n=== WITH CACHE: caching pipeline ===")
    cached = pipeline.cache()  # помечаем кэшировать (пока не исполняется)

    print("Is cached before action?:", cached.is_cached)

    print("\nMaterializing cache with count() ...")
    t0 = time.time()
    c2 = cached.count()        # action -> вычислит и заполнит cache
    print("result rows:", c2, "| time_ms:", ms(t0))

    print("\n=== EXPLAIN (cached after materialization) ===")
    cached.explain(True)

    print("\n=== WITH CACHE: second action (take(5)) should be faster ===")
    t0 = time.time()
    top5_cached = cached.orderBy(col("cnt").desc()).take(5)
    print("top5:", top5_cached, "| time_ms:", ms(t0))

    # чистим кэш (хорошая практика)
    cached.unpersist()

    spark.stop()

if __name__ == "__main__":
    main()