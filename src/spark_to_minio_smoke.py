from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# кредиты для коннекта к бакету
MINIO_ENDPOINT = "http://127.0.0.1:9000"
ACCESS_KEY = "THE_BEST_LOGIN"
SECRET_KEY = "THE_BEST_PASSWORD"
BUCKET = "The best bucket"

def make_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("spark-to-minio-smoke")
        .master("local[*]")
        # Подтягиваем зависимости для S3A
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            ])
        )
        # Настройки S3A под MinIO
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Явно указываем, какой класс файловой системы использовать для s3a://
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Уменьшаем шум на маленьких данных
        .config("spark.sql.shuffle.partitions", "4")
        # создаётся спарк сессия и запускается context
        .getOrCreate()
    )

def main():
    spark = make_spark()

    df = spark.createDataFrame(
        [(1, "click"), (1, "view"), (2, "click"), (1, "click")],
        ["user_id", "event_type"]
    )

    out = df.groupBy("event_type").agg(count("*").alias("cnt"))
    out.show()

    # Запись в бакет
    path = f"s3a://{BUCKET}/tmp/smoke_parquet"
    out.write.mode("overwrite").parquet(path)

    print("Wrote to:", path)
    spark.stop()

if __name__ == "__main__":
    main()
