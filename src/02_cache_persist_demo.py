
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark import StorageLevel

def ms(t0): return int((time.time() - t0) * 1000)

def main():
    spark = (
        SparkSession.builder
        .appName("02-cache-persist-demo")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    base = spark.range(0, 1_000_000).select(
        (col("id") % 50000).alias("user_id"),
        (col("id") % 20).alias("event_type")
    )

    pipeline = base.groupBy("user_id", "event_type").agg(count("*").alias("cnt"))

    print("\n=== WITHOUT CACHE: action #1 (count) ===")
    t0 = time.time()
    a = pipeline.count()
    print("rows:", a, "time_ms:", ms(t0))

    print("\n=== WITHOUT CACHE: action #2 (take 5) ===")
    t0 = time.time()
    b = pipeline.orderBy(col("cnt").desc()).take(5)
    print("top5:", b, "time_ms:", ms(t0))

    print("\n=== WITH CACHE (MEMORY_ONLY): materialize once ===")
    cached = pipeline.cache()
    t0 = time.time()
    c = cached.count()
    print("rows:", c, "time_ms:", ms(t0))

    print("\n=== EXPLAIN cached (look for InMemoryTableScan) ===")
    cached.explain(True)

    print("\n=== WITH CACHE: action #2 (take 5) should be faster ===")
    t0 = time.time()
    d = cached.orderBy(col("cnt").desc()).take(5)
    print("top5:", d, "time_ms:", ms(t0))

    cached.unpersist()

    print("\n=== WITH PERSIST (MEMORY_AND_DISK): example ===")
    persisted = pipeline.persist(StorageLevel.MEMORY_AND_DISK)
    t0 = time.time()
    e = persisted.count()
    print("rows:", e, "time_ms:", ms(t0))
    persisted.unpersist()

    spark.stop()

if __name__ == "__main__":
    main()

