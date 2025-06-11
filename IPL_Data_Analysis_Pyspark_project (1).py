# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,BooleanType,StringType,DateType,DecimalType
from pyspark.sql.functions import col,when,sum,avg,row_number
from pyspark.sql.window import Window

# COMMAND ----------

#create spark session
from pyspark.sql import SparkSession

spark=SparkSession.builder \
      .appName("IPL_Data_Analysis") \
      .getOrCreate()

# COMMAND ----------

#fixing data types of ball_by_ball table

ball_by_ball_Schema=StructType([
    StructField("match_id", IntegerType(), True),
    StructField("over_id", IntegerType(), True),
    StructField("ball_id", IntegerType(), True),
    StructField("innings_no", IntegerType(), True),
    StructField("team_batting", StringType(), True),
    StructField("team_bowling", StringType(), True),
    StructField("striker_batting_position", IntegerType(), True),
    StructField("extra_type", StringType(), True),
    StructField("runs_scored", IntegerType(), True),
    StructField("extra_runs", IntegerType(), True),
    StructField("wides", IntegerType(), True),
    StructField("legbyes", IntegerType(), True),
    StructField("byes", IntegerType(), True),
    StructField("noballs", IntegerType(), True),
    StructField("penalty", IntegerType(), True),
    StructField("bowler_extras", IntegerType(), True),
    StructField("out_type", StringType(), True),
    StructField("caught", BooleanType(), True),
    StructField("bowled", BooleanType(), True),
    StructField("run_out", BooleanType(), True),
    StructField("lbw", BooleanType(), True),
    StructField("retired_hurt", BooleanType(), True),
    StructField("stumped", BooleanType(), True),
    StructField("caught_and_bowled", BooleanType(), True),
    StructField("hit_wicket", BooleanType(), True),
    StructField("obstructingfeild", BooleanType(), True),
    StructField("bowler_wicket", BooleanType(), True),
    StructField("match_date", DateType(), True),
    StructField("season", IntegerType(), True),
    StructField("striker", IntegerType(), True),
    StructField("non_striker", IntegerType(), True),
    StructField("bowler", IntegerType(), True),
    StructField("player_out", IntegerType(), True),
    StructField("fielders", IntegerType(), True),
    StructField("striker_match_sk", IntegerType(), True),
    StructField("strikersk", IntegerType(), True),
    StructField("nonstriker_match_sk", IntegerType(), True),
    StructField("nonstriker_sk", IntegerType(), True),
    StructField("fielder_match_sk", IntegerType(), True),
    StructField("fielder_sk", IntegerType(), True),
    StructField("bowler_match_sk", IntegerType(), True),
    StructField("bowler_sk", IntegerType(), True),
    StructField("playerout_match_sk", IntegerType(), True),
    StructField("battingteam_sk", IntegerType(), True),
    StructField("bowlingteam_sk", IntegerType(), True),
    StructField("keeper_catch", BooleanType(), True),
    StructField("player_out_sk", IntegerType(), True),
    StructField("matchdatesk", DateType(), True)
])

# COMMAND ----------

#Readig first table Data ball by ball

ball_by_ball_df=spark.read.schema(ball_by_ball_Schema) \
                    .format("csv") \
                    .option("header","True") \
                    .load("/FileStore/tables/Ball_By_Ball.csv")

# COMMAND ----------

#fixing datatypes of match table

Match_Schema=StructType([
        StructField("match_sk", IntegerType(), True),
        StructField("match_id", IntegerType(), True),
        StructField("team1", StringType(), True),
        StructField("team2", StringType(), True),
        StructField("match_date", DateType(), True),
        StructField("season_year", IntegerType(), True), # Changed from 'year' to IntegerType
        StructField("venue_name", StringType(), True),
        StructField("city_name", StringType(), True),
        StructField("country_name", StringType(), True),
        StructField("toss_winner", StringType(), True),
        StructField("match_winner", StringType(), True),
        StructField("toss_name", StringType(), True),
        StructField("win_type", StringType(), True),
        StructField("outcome_type", StringType(), True),
        StructField("manofmach", StringType(), True),
        StructField("win_margin", IntegerType(), True),
        StructField("country_id", IntegerType(), True)   
])

# COMMAND ----------

#reading Match table 

match_df=spark.read \
         .schema(Match_Schema) \
         .format("csv") \
         .option("header","True") \
         .load("/FileStore/tables/Match.csv")



# COMMAND ----------

Player_Schema=StructType([
        StructField("player_sk", IntegerType(), True),
        StructField("player_id", IntegerType(), True),
        StructField("player_name", StringType(), True),
        StructField("dob", DateType(), True),
        StructField("batting_hand", StringType(), True),
        StructField("bowling_skill", StringType(), True),
        StructField("country_name", StringType(), True),
])

# COMMAND ----------

player_df=spark.read \
         .schema(Player_Schema) \
         .format("csv") \
         .option("header","True") \
         .load("/FileStore/tables/Player.csv")

# COMMAND ----------

Player_Match_Schema = StructType(
    [
        StructField("player_match_sk", IntegerType(), True),
        StructField("playermatch_key", DecimalType(), True), # Using DecimalType for 'decimal'
        StructField("match_id", IntegerType(), True),
        StructField("player_id", IntegerType(), True),
        StructField("player_name", StringType(), True),
        StructField("dob", DateType(), True),
        StructField("batting_hand", StringType(), True),
        StructField("bowling_skill", StringType(), True),
        StructField("country_name", StringType(), True),
        StructField("role_desc", StringType(), True),
        StructField("player_team", StringType(), True),
        StructField("opposit_team", StringType(), True),
        StructField("season_year", IntegerType(), True), # Changed from 'year' to IntegerType
        StructField("is_manofthematch", BooleanType(), True),
        StructField("age_as_on_match", IntegerType(), True),
        StructField("isplayers_team_won", BooleanType(), True),
        StructField("batting_status", StringType(), True),
        StructField("bowling_status", StringType(), True),
        StructField("player_captain", StringType(), True),
        StructField("opposit_captain", StringType(), True),
        StructField("player_keeper", StringType(), True),
        StructField("opposit_keeper", StringType(), True),
    ]
)

# COMMAND ----------

player_match_df=spark.read \
         .schema(Player_Match_Schema) \
         .format("csv") \
         .option("header","True") \
         .load("/FileStore/tables/Player_match.csv")

# COMMAND ----------

Team_Schema = StructType(
    [
        StructField("team_sk", IntegerType(), True),
        StructField("team_id", IntegerType(), True),
        StructField("team_name", StringType(), True),
    ]
)

# COMMAND ----------

team_df=spark.read \
         .schema(Team_Schema) \
         .format("csv") \
         .option("header","True") \
         .load("/FileStore/tables/Team.csv")

# COMMAND ----------

#filter to include only valid deliveries (excluding extra wides and no balls)
ball_by_ball_df=ball_by_ball_df.filter((col("wides") == 0) & (col("noBalls") == 0))

#Gregation:calculate the total and average runs scored in each match and each innings
total_and_avg_runs=ball_by_ball_df.select("*") \
                         .groupby("match_id","innings_no") \
                         .agg(sum("runs_scored").alias("total_runs_scored"),
                              avg("runs_scored").alias("average_runs_scored"))
                         
                      

# COMMAND ----------

#Window function: Calculate runing total of runs in each match for each over 
windowSpec=Window.partitionBy("match_id","innings_no").orderBy("over_id")

ball_by_ball_df=ball_by_ball_df.withColumn("running_total_runs",sum("runs_scored").over(windowSpec))

# COMMAND ----------

#Conditional column:flag for high imapact balls(either wicket or mor than 6 runs including extras)

ball_by_ball_df=ball_by_ball_df.withColumn("high_impact",when((col("runs_scored") + col("extra_runs")>6) | (col("bowler_wicket")==True),True).otherwise(False))



# COMMAND ----------

ball_by_ball_df.show(5)

# COMMAND ----------

#noe doing for second table 
#extracting day,month and year from match data for more detailed time based analysis.
from pyspark.sql.functions import year,month,dayofmonth

match_df=match_df.withColumn("year",year(col("match_date")))
match_df=match_df.withColumn("month",month(col("match_date")))
match_df=match_df.withColumn("day",dayofmonth(col("match_date")))


#High margin win: catogerizing win margin into high,medium and low
match_df=match_df.withColumn("win_margin_state",when(col("win_margin")>=100,"High")
                 .when((col("win_margin")<100) & (col("win_margin")>=50),"medium")
                 .otherwise("low"))   



#analays the impact of the toss: who win the toss and match

match_df=match_df.withColumn("impact_match_winner",when(col("toss_winner")==col("match_winner"),"Yes").otherwise("Yes"))

#show the enhanced match report
match_df.show(5)


# COMMAND ----------

from pyspark.sql.functions import lower,regexp_replace

# normalize and clean player names player
player_df=player_df.withColumn("player_name",lower(regexp_replace(col("player_name"),"[^a-zA-Z0-9]","")))

#handle missing value in 'batting hand' and bowling skill with default unknown
player_df=player_df.na.fill({"batting_hand":"unknown","bowling_skill":"unknown"})

#categorzing player based on batting hand
player_df=player_df.withColumn("batting_style",when(col("batting_hand").contains("Left"),"Left-Handed").otherwise("Right-Handed"))

#show
player_df.show(2)

# COMMAND ----------

from pyspark.sql.functions import current_date,expr

#add vetaran status column based on player age
player_match_df=player_match_df.withColumn("veteran_status",when(col("age_as_on_match")>=35,"Veteran").otherwise("Non-Veteran"))

#Dynamic column to calculate year since debut
player_match_df=player_match_df.withColumn("year_since_debut",(year(current_date())-col("season_year")))

#show
player_match_df.show(2)


# COMMAND ----------

#now find some business inside from tables
#coverting them to global view

ball_by_ball_df.createOrReplaceTempView("ball_by_ball")
match_df.createOrReplaceTempView("match")
player_df.createOrReplaceTempView("player")
player_match_df.createOrReplaceTempView("player_match")
team_df.createOrReplaceTempView("team")

# COMMAND ----------

top_scoring_batsman_per_season=spark.sql("""select p.player_name,m.season_year,sum(b.runs_scored) as total_runs from ball_by_ball as b
                                         join match as m on b.match_id=m.match_id
                                         join player_match as pm on m.match_id=pm.match_id
                                         join player as p on pm.player_id=p.player_id AND b.striker=pm.player_id
                                         group by p.player_name,m.season_year
                                         order by m.season_year,total_runs desc
                                         """)

# COMMAND ----------

top_scoring_batsman_per_season.show(5)

# COMMAND ----------

economical_bowlers_powerplay = spark.sql("""
SELECT 
p.player_name, 
AVG(b.runs_scored) AS avg_runs_per_ball, 
COUNT(b.bowler_wicket) AS total_wickets
FROM ball_by_ball b
JOIN player_match pm ON b.match_id = pm.match_id AND b.bowler = pm.player_id
JOIN player p ON pm.player_id = p.player_id
WHERE b.over_id <= 6
GROUP BY p.player_name
HAVING COUNT(*) >= 1
ORDER BY avg_runs_per_ball, total_wickets DESC
""")

# COMMAND ----------

toss_impact_individual_matches = spark.sql("""
SELECT m.match_id, m.toss_winner, m.toss_name, m.match_winner,
       CASE WHEN m.toss_winner = m.match_winner THEN 'Won' ELSE 'Lost' END AS match_outcome
FROM match m
WHERE m.toss_name IS NOT NULL
ORDER BY m.match_id
""")
toss_impact_individual_matches.show()


# COMMAND ----------

average_runs_in_wins = spark.sql("""
SELECT p.player_name, AVG(b.runs_scored) AS avg_runs_in_wins, COUNT(*) AS innings_played
FROM ball_by_ball b
JOIN player_match pm ON b.match_id = pm.match_id AND b.striker = pm.player_id
JOIN player p ON pm.player_id = p.player_id
JOIN match m ON pm.match_id = m.match_id
WHERE m.match_winner = pm.player_team
GROUP BY p.player_name
ORDER BY avg_runs_in_wins ASC
""")
average_runs_in_wins.show()


# COMMAND ----------

import matplotlib.pyplot as plt
     

# Assuming 'economical_bowlers_powerplay' is already executed and available as a Spark DataFrame
economical_bowlers_pd = economical_bowlers_powerplay.toPandas()

# Visualizing using Matplotlib
plt.figure(figsize=(12, 8))
# Limiting to top 10 for clarity in the plot
top_economical_bowlers = economical_bowlers_pd.nsmallest(10, 'avg_runs_per_ball')
plt.bar(top_economical_bowlers['player_name'], top_economical_bowlers['avg_runs_per_ball'], color='skyblue')
plt.xlabel('Bowler Name')
plt.ylabel('Average Runs per Ball')
plt.title('Most Economical Bowlers in Powerplay Overs (Top 10)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

average_runs_pd = average_runs_in_wins.toPandas()

# Using seaborn to plot average runs in winning matches
plt.figure(figsize=(12, 8))
top_scorers = average_runs_pd.nlargest(10, 'avg_runs_in_wins')
sns.barplot(x='player_name', y='avg_runs_in_wins', data=top_scorers)
plt.title('Average Runs Scored by Batsmen in Winning Matches (Top 10 Scorers)')
plt.xlabel('Player Name')
plt.ylabel('Average Runs in Wins')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

