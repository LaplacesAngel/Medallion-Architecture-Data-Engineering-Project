from pyspark.sql import SparkSession

columns = ["language", "user_count"]
data = [("java", "1000"), ("python", "1000000"), ("scala", "10")]

spark = SparkSession \
        .builder \
        .appName("dataframecreationpractice") \
        .master("local[1]") \
        .getOrCreate()

rdd = spark.sparkContext.parallelize(data)

df = rdd.toDF(columns)

df.printSchema()
df.show()

df.createOrReplaceTempView("languages")

df2 = spark.sql("SELECT language as most_popular_language \
                FROM languages \
                order by user_count desc \
                limit 1")
df2.show()


spark.stop()