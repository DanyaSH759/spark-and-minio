
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def build_pipeline(spark):
    # 1) источник
    df = spark.range(0, 2_000_00).select(  # 200k
        (col("id") % 10000).alias("user_id"),
        (col("id") % 50).alias("event_type")
    )
    # 2) wide transformation -> shuffle
    agg = df.groupBy("user_id", "event_type").agg(count("*").alias("cnt"))
    return df, agg

def main():
    spark = (
        SparkSession.builder
        .appName("01-dag-stages-shuffle-aqe")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("AQE enabled:", spark.conf.get("spark.sql.adaptive.enabled"))
    print("shuffle.partitions:", spark.conf.get("spark.sql.shuffle.partitions"))

    df, agg = build_pipeline(spark)

    print("\n=== Partitions before groupBy ===")
    print("df partitions:", df.rdd.getNumPartitions())

    print("\n=== EXPLAIN agg (look for Exchange / ShuffleQueryStage) ===")
    agg.explain(True)

    print("\n=== Trigger action (agg.count()) ===")
    n = agg.count()
    print("agg rows:", n)

    print("\n=== Partitions after groupBy (physical) ===")
    print("agg partitions:", agg.rdd.getNumPartitions())

    # Попробуем отключить AQE и сравнить план
    print("\n=== Disable AQE and re-explain ===")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    _, agg2 = build_pipeline(spark)
    agg2.explain(True)
    print("AQE enabled now:", spark.conf.get("spark.sql.adaptive.enabled"))

    spark.stop()

if __name__ == "__main__":
    main()

