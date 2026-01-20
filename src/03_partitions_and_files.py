
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = (
        SparkSession.builder
        .appName("03-partitions-and-files")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.range(0, 100_000).select((col("id") % 10).alias("k"), col("id"))

    print("Initial partitions:", df.rdd.getNumPartitions())

    r4 = df.repartition(4, "k")
    print("After repartition(4,'k'):", r4.rdd.getNumPartitions())

    c2 = r4.coalesce(2)
    print("After coalesce(2):", c2.rdd.getNumPartitions())

    # clean output dirs
    shutil.rmtree("output/p3_r4", ignore_errors=True)
    shutil.rmtree("output/p3_c2", ignore_errors=True)

    r4.write.mode("overwrite").parquet("output/p3_r4")
    c2.write.mode("overwrite").parquet("output/p3_c2")

    print("\nWrote output/p3_r4 and output/p3_c2")
    print("Expect ~4 part files in p3_r4, ~2 part files in p3_c2")

    spark.stop()

if __name__ == "__main__":
    main()
