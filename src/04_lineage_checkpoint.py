
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = (
        SparkSession.builder
        .appName("04-lineage-checkpoint")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Для checkpoint нужно указать директорию
    spark.sparkContext.setCheckpointDir("output/checkpoints")

    df = spark.range(0, 200_000).select(col("id"))

    # Строим "длинную" lineage цепочку (искусственно)
    x = df
    for i in range(10):
        x = x.withColumn(f"c{i}", (col("id") + i) % 7)

    print("\n=== EXPLAIN before checkpoint (long lineage) ===")
    x.explain(True)

    # checkpoint: обрывает lineage, материализуя датафрейм
    # локальный checkpoint (eager=True) — выполнит сразу
    xc = x.localCheckpoint(eager=True)

    print("\n=== EXPLAIN after localCheckpoint (lineage truncated) ===")
    xc.explain(True)

    # action
    print("\ncount:", xc.count())

    spark.stop()

if __name__ == "__main__":
    main()

