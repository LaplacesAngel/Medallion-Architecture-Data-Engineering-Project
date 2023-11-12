from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp

spark = SparkSession.builder \
    .appName("Pauls cool demo") \
    .master("local[1]") \
    .getOrCreate()

version = spark.version
vid_game_list = spark.read.csv("resources/video_game_scores.tsv", sep=",", header=True)

print(f"My Spark version is: {version}")
vid_game_list.printSchema()
vid_game_list.show(n=4,truncate=False)
vid_game_list.write.json("resources/video_game_scores_json", mode="overwrite")

# column_datatypes = vid_game_list.dtypes
# print(f"{column_datatypes}")
# for column, datatype in column_datatypes:
#     print(f"{column}: {datatype}")

vid_game_list.createOrReplaceTempView("scores")
total_scores = spark.sql("select * from scores where player = 'Jack' or player = 'Ivan'")
total_scores.show()

players = vid_game_list.select("player")
players.show()
games = vid_game_list.select(vid_game_list.game)
games.show()
scores = vid_game_list.select(vid_game_list['score'])
scores.show()

newExpression = vid_game_list.selectExpr("CONCAT('Player name ', player) AS player"
                                         ,'game'
                                         ,"score")
newExpression.show(truncate=False)


justAlice = vid_game_list.filter("player == 'Alice'")
justAlice.show()

justBob = vid_game_list.filter(vid_game_list.player == 'Bob')
justBob.show()

justCharlie = vid_game_list.filter(vid_game_list['player']=='Charlie')
justCharlie.show()

vid_game_list.withColumn('date', current_date()) \
    .withColumn('time', current_timestamp()) \
    .withColumn("scores_plus_100", vid_game_list.score+100) \
    .show(truncate=False)


vid_game_list.selectExpr("player as losers_to_winners").orderBy(vid_game_list.score.asc()).show()
spark.stop()
