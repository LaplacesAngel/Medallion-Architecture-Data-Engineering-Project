from pyspark.sql import SparkSession

### Setup: Create a SparkSession
spark = SparkSession.builder \
    .appName("Pauls query app") \
    .master("local[1]") \
    .getOrCreate()

# For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager").setLevel(
    logger.Level.OFF
)
logger.LogManager.getLogger("org.apache.spark.SparkEnv").setLevel(logger.Level.ERROR)

### Questions

# Question 1: Read the tab separated file named "resources/reviews.tsv.gz" into a dataframe. Call it "reviews".
reviews = spark.read.csv("resources/reviews.tsv.gz", sep="\t", header=True, inferSchema=True)
# datatypes = reviews.dtypes
# for x,y in datatypes:
#     print(f"{x} : {y}")
reviews.show(n=1)


# Question 2: Create a virtual view on top of the reviews dataframe, so that we can query it with Spark SQL.
reviews.createOrReplaceTempView("reviews")
contents = spark.sql("select * from reviews")
contents.show(n=2)

# Question 3: Add a column to the dataframe named "review_timestamp", representing the current time on your computer.
timestamp = spark.sql("select product_title, date_format(current_timestamp(), 'dd/MM/yyyy hh:mm a') as review_timestamp from reviews limit 2")
timestamp.show(truncate=False)

# Question 4: How many records are in the reviews dataframe? 

records = spark.sql("select count(*) as record_count from reviews")
records.show()

# Question 5: Print the first 5 rows of the dataframe.
# Some of the columns are long - print the entire record, regardless of length.
first5 = spark.sql("select * from reviews limit 5")
first5.show(truncate=False)

# Question 6: Create a new dataframe based on "reviews" with exactly 1 column: the value of the product category field.
# Look at the first 50 rows of that dataframe.
# Which value appears to be the most common?
product_cat = spark.sql("select product_category from reviews")
product_cat.show(50)

# Question 7: Find the most helpful review in the dataframe - the one with the highest number of helpful votes.
# What is the product title for that review? How many helpful votes did it have?
helpful_reviews = spark.sql("select product_title, helpful_votes from reviews order by cast(helpful_votes as int) desc")
helpful_reviews.show(1, truncate=False)

helpful_reviews_grouped = spark.sql("select product_title, \
                                    count(cast(helpful_votes as int)) as vote_count \
                                    from reviews \
                                    group by product_title \
                                    order by vote_count desc")
helpful_reviews_grouped.show()

reviews10 = spark.sql("select * from reviews order by cast(helpful_votes as int) desc limit 10")
reviews10.write.mode('overwrite').json("resources/count_rows_json")

#do a group by and a count to spot check the data in case of data type being inferred badly
#also save out ten rows and just count them, write them out as json and just count them up

# Question 8: How many reviews exist in the dataframe with a 5 star rating?
five_star = spark.sql("select count(star_rating) from reviews where star_rating = 5")
five_star.show()

# Question 9: Currently every field in the data file is interpreted as a string, but there are 3 that should really be numbers.
# Create a new dataframe with just those 3 columns, except cast them as "int"s.
# Look at 10 rows from this dataframe.
columns = reviews.dtypes
for x, y in columns:
    print (f"{x}: {y}")

ints = spark.sql("SELECT cast(star_rating as int), cast(helpful_votes as int), cast(total_votes as int)  from reviews")
ints.show(10)

# Question 10: Find the date with the most purchases.
# Print the date and total count of the date which had the most purchases.

purchases = spark.sql("select cast(purchase_date as date), \
                      count(purchase_date) as `times_purchased` \
                      from reviews \
                      group by purchase_date \
                      order by `times_purchased` \
                      desc \
                      limit 1")
purchases.show()


##Question 11: Write the dataframe from Question 3 to your drive in JSON format.
##Feel free to pick any directory on your computer.
##Use overwrite mode.

timestamp.write.mode("overwrite").json("resources/time_stamp_data")


### Teardown
# Stop the SparkSession
spark.stop()