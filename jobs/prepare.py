
from pyspark.sql import SparkSession, functions as F, Window

CSV = "/work/data/archive/csv"
OUT = "hdfs://namenode:9000/nba/pbp"

spark = SparkSession.builder.appName("nba-prepare").getOrCreate()

pbp_raw = spark.read.option("header", "true").csv(f"{CSV}/play_by_play.csv")
game = spark.read.option("header", "true").csv(f"{CSV}/game.csv")

w = Window.partitionBy("game_id").orderBy("eventnum").rowsBetween(Window.unboundedPreceding, 0)

pbp = (pbp_raw.select("game_id", "eventnum", "eventmsgtype", "period",
                      "pctimestring", "score",
                      F.col("homedescription").alias("hd"), F.col("visitordescription").alias("vd"),
                      "player1_name", "player1_team_abbreviation",
                      F.col("player2_name").alias("passeur"))
    .withColumn("eventnum", F.col("eventnum").cast("int"))
    .withColumn("eventmsgtype", F.col("eventmsgtype").cast("int"))
    .withColumn("period", F.col("period").cast("int"))
    .withColumn("rest", F.split("pctimestring", ":").getItem(0).cast("int") * 60
                      + F.split("pctimestring", ":").getItem(1).cast("int"))
    .withColumn("sec", F.when(F.col("period") <= 4, (F.col("period") - 1) * 720 + 720 - F.col("rest"))
                       .otherwise(2880 + (F.col("period") - 5) * 300 + 300 - F.col("rest")))
    .withColumn("side", F.when(F.col("hd").isNotNull(), "H")
                        .when(F.col("vd").isNotNull(), "A").otherwise("N"))
    .withColumn("descr", F.coalesce("hd", "vd"))
    .withColumn("pts", F.when(F.col("eventmsgtype") == 1,
                              F.when(F.col("descr").contains("3PT"), 3).otherwise(2))
                       .when((F.col("eventmsgtype") == 3) & (~F.col("descr").contains("MISS")), 1)
                       .otherwise(0))
    .withColumn("tir", F.when(F.col("descr").contains("3PT"), "3 points")
                       .when(F.col("descr").rlike("Dunk|Layup|Tip|Alley Oop|Finger Roll"), "proche cercle")
                       .when(F.col("eventmsgtype").isin(1, 2), "mi-distance"))
    .withColumn("score_f", F.last(F.when(F.length("score") > 0, F.col("score")), True).over(w))
    .withColumn("marge_dom", F.split("score_f", " - ").getItem(1).cast("int")
                           - F.split("score_f", " - ").getItem(0).cast("int")))

g = (game.select("game_id", "season_id", "game_date", "season_type", "wl_home",
                 "team_abbreviation_home", "team_abbreviation_away")
        .dropDuplicates(["game_id"])
        .withColumn("annee", F.col("season_id").cast("int") - 20000))

pbp = pbp.join(g, "game_id")
pbp.write.mode("overwrite").parquet(OUT)

n = spark.read.parquet(OUT).count()
print(f"evenements ecrits dans {OUT} : {n}")
spark.stop()
