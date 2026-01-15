from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col

def main():

    # запуск сессии
    spark = (
        SparkSession.builder
        .appName("joins-broadcast")
        # запуск на все ресурсы пк
        .master("local[*]")
        # для наглядности (меньше шума) будет использоваться всего 4 партиции
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # "Факт" (условно события) - много строк
    events = spark.createDataFrame(
        [
            (1, "click"), (1, "view"), (2, "click"), (3, "view"),
            (4, "click"), (5, "view"), (6, "click"), (7, "click"),
        ],
        ["user_id", "event_type"]
    )

    # "Справочник" (маленькая таблица) - идеально для broadcast
    users = spark.createDataFrame(
        [
            (1, "RU"), (2, "KZ"), (3, "RU"), (4, "DE")
        ],
        ["user_id", "country"]
    )

    print("\n=== 1) Обычный left join (Spark сам решает стратегию) ===")
    j1 = events.join(users, on="user_id", how="left")
    j1.show()
    j1.explain(True)

    print("\n=== 2) Принудительный broadcast join (обычно быстрее для маленького справочника) ===")
    j2 = events.join(broadcast(users), on="user_id", how="left")
    j2.show()
    j2.explain(True)

    print("\n=== 3) Пример anti join: найти события без пользователя в справочнике ===")
    missing = events.join(users, on="user_id", how="left_anti")
    missing.show()

    # Небольшая демонстрация: фильтр + join
    print("\n=== 4) Фильтр по стране после join ===")
    ru = j2.filter(col("country") == "RU")
    ru.show()

    spark.stop()

if __name__ == "__main__":
    main()

