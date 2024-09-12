# **IPL Data Analysis Using PySpark - Data Engineering Project**

## **Introduction**

The Indian Premier League (IPL) is one of the most popular cricket leagues globally, featuring a large dataset with detailed match and player statistics. In this project, I leveraged Apache Spark’s PySpark library to analyze IPL data, focusing on player performances, match outcomes, and the effects of toss results. My goal was to process and analyze large datasets efficiently to uncover insights that can drive strategic decisions and enhance understanding of game dynamics.

## **Objectives**

1. **Understand player performance**: Identify top-performing batsmen and bowlers in various scenarios.
2. **Analyze match outcomes**: Examine how toss results impact match outcomes.
3. **Evaluate venue performances**: Assess average scores and highest scores at different venues.
4. **Explore dismissal types**: Determine the frequency of different types of dismissals.

## **Data Ingestion and Schema Definition**

### **1. Data Sources**

I ingested the following datasets from an S3 bucket into PySpark DataFrames:

- **Ball by Ball Data**: Detailed delivery-by-delivery information including runs scored, extras, and wickets.
- **Match Data**: Includes match-level information such as teams, venue, and results.
- **Player Data**: Information about players, including their names, batting hand, and bowling skill.
- **Player Match Data**: Player-specific performance metrics for each match.
- **Team Data**: Team information for all participating teams.

### **2. Schema Definition**

To ensure accurate data loading, I defined schemas for each dataset using `StructType` and `StructField`. This included specifying data types and handling null values. For instance:

```python
ball_by_ball_schema = StructType([
    StructField("match_id", IntegerType(), True),
    StructField("over_id", IntegerType(), True),
    StructField("ball_id", IntegerType(), True),
    StructField("innings_no", IntegerType(), True),
    StructField("team_batting", StringType(), True),
    StructField("team_bowling", StringType(), True),
    StructField("striker_batting_position", IntegerType(), True),
    StructField("extra_type", StringType(), True),
    StructField("runs_scored", IntegerType(), True),
    # Additional fields...
])
```

## **Data Transformation**

### **1. Filtering and Aggregation**

I filtered out deliveries with extras (wides and no-balls) to focus on valid deliveries. Subsequently, I aggregated data to calculate total and average runs scored per match and innings:

```python
total_and_avg_runs = ball_by_ball_df.groupBy("match_id", "innings_no").agg(
    sum("runs_scored").alias("total_runs"),
    avg("runs_scored").alias("average_runs")
)
```

### **2. Running Total Calculation**

I used window functions to calculate the running total of runs scored for each over within a match:

```python
windowSpec = Window.partitionBy("match_id", "innings_no").orderBy("over_id")
ball_by_ball_df = ball_by_ball_df.withColumn(
    "running_total_runs",
    sum("runs_scored").over(windowSpec)
)
```

### **3. High-Impact Balls Identification**

I flagged high-impact balls where the combined runs scored plus extras exceeded 6 or a wicket was taken:

```python
ball_by_ball_df = ball_by_ball_df.withColumn(
    "high_impact",
    when((col("runs_scored") + col("extra_runs") > 6) | (col("bowler_wicket") == True), True).otherwise(False)
)
```

### **4. Enriching Match Data**

I extracted year, month, and day from match dates and categorized win margins:

```python
match_df = match_df.withColumn("year", year("match_date"))
match_df = match_df.withColumn("win_margin_category",
    when(col("win_margin") >= 100, "High")
    .when((col("win_margin") >= 50) & (col("win_margin") < 100), "Medium")
    .otherwise("Low")
)
```

### **5. Cleaning and Enriching Player Data**

I normalized player names and handled missing values in batting hand and bowling skill:

```python
player_df = player_df.withColumn("player_name", lower(regexp_replace("player_name", "[^a-zA-Z0-9 ]", "")))
player_df = player_df.na.fill({"batting_hand": "unknown", "bowling_skill": "unknown"})
player_df = player_df.withColumn(
    "batting_style",
    when(col("batting_hand").contains("left"), "Left-Handed").otherwise("Right-Handed")
)
```

### **6. Veteran Status and Years Since Debut**

I added columns for veteran status and calculated years since debut:

```python
player_match_df = player_match_df.withColumn(
    "veteran_status",
    when(col("age_as_on_match") >= 35, "Veteran").otherwise("Non-Veteran")
)
player_match_df = player_match_df.withColumn(
    "years_since_debut",
    (year(current_date()) - col("season_year"))
)
```

## **Data Analysis**

I performed several analyses using SQL queries to extract meaningful insights:

### **1. Top Scoring Batsmen per Season**

Identified the top-performing batsmen based on total runs scored each season:

```sql
SELECT 
p.player_name,
m.season_year,
SUM(b.runs_scored) AS total_runs 
FROM ball_by_ball b
JOIN match m ON b.match_id = m.match_id   
JOIN player_match pm ON m.match_id = pm.match_id AND b.striker = pm.player_id     
JOIN player p ON p.player_id = pm.player_id
GROUP BY p.player_name, m.season_year
ORDER BY m.season_year, total_runs DESC
```

### **2. Economical Bowlers in Powerplay**

Analyzed bowlers’ performance during powerplay overs based on average runs per ball and total wickets:

```sql
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
```

### **3. Impact of Toss on Match Outcomes**

Examined the correlation between winning the toss and winning the match:

```sql
SELECT m.match_id, m.toss_winner, m.toss_name, m.match_winner,
       CASE WHEN m.toss_winner = m.match_winner THEN 'Won' ELSE 'Lost' END AS match_outcome
FROM match m
WHERE m.toss_name IS NOT NULL
ORDER BY m.match_id
```

### **4. Average Runs in Winning Matches**

Calculated average runs scored by players in matches their team won:

```sql
SELECT p.player_name, AVG(b.runs_scored) AS avg_runs_in_wins, COUNT(*) AS innings_played
FROM ball_by_ball b
JOIN player_match pm ON b.match_id = pm.match_id AND b.striker = pm.player_id
JOIN player p ON pm.player_id = p.player_id
JOIN match m ON pm.match_id = m.match_id
WHERE m.match_winner = pm.player_team
GROUP BY p.player_name
ORDER BY avg_runs_in_wins ASC
```

### **5. Scores by Venue**

Analyzed average and highest scores at different venues:

```sql
SELECT venue_name, AVG(total_runs) AS average_score, MAX(total_runs) AS highest_score
FROM (
    SELECT ball_by_ball.match_id, match.venue_name, SUM(runs_scored) AS total_runs
    FROM ball_by_ball
    JOIN match ON ball_by_ball.match_id = match.match_id
    GROUP BY ball_by_ball.match_id, match.venue_name
)
GROUP BY venue_name
ORDER BY average_score DESC
```

### **6. Dismissal Types**

Counted the frequency of various dismissal types:

```sql
SELECT out_type, COUNT(*) AS frequency
FROM ball_by_ball
WHERE out_type IS NOT NULL
GROUP BY out_type
ORDER BY frequency DESC
```

## **Visualizations**

### **1. Economical Bowlers in Powerplay**

A bar plot visualizing the most economical bowlers during powerplay overs was created using `Matplotlib`:

```python
plt.figure(figsize=(12, 8))
top_economical_bowlers = economical_bowlers_pd.nsmallest(10, 'avg_runs_per_ball')
plt.bar(top_economical_bowlers['player_name'], top_economical_bowlers['avg_runs_per_ball'], color='skyblue')
plt.xlabel('Bowler Name')
plt.ylabel('Average Runs per Ball')
plt.title('Most Economical Bowlers in Powerplay Overs (Top 10)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
```

### **2. Impact of Winning Toss on Match Outcomes**

A countplot showing the impact of winning the toss on match outcomes:

```python
plt.figure(figsize=(10, 6))
sns.countplot(x='toss_winner', hue='match_outcome', data=toss_impact_pd)
plt.title('Impact of Winning Toss on Match Outcomes')
plt.xlabel('Toss Winner')
plt.ylabel('Number of Matches')
plt.legend(title='Match Outcome')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
```

### **3.

 Average Runs in Winning Matches**

A bar plot displaying the top batsmen based on average runs scored in winning matches:

```python
plt.figure(figsize=(12, 8))
top_batsmen = avg_runs_in_wins_pd.nlargest(10, 'avg_runs_in_wins')
plt.bar(top_batsmen['player_name'], top_batsmen['avg_runs_in_wins'], color='coral')
plt.xlabel('Batsman Name')
plt.ylabel('Average Runs in Winning Matches')
plt.title('Top Batsmen Based on Average Runs in Winning Matches (Top 10)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
```

### **4. Scores by Venue**

A bar plot displaying average scores at different venues:

```python
plt.figure(figsize=(14, 7))
sns.barplot(x='venue_name', y='average_score', data=scores_by_venue_pd, palette='viridis')
plt.title('Average Scores by Venue')
plt.xlabel('Venue Name')
plt.ylabel('Average Score')
plt.xticks(rotation=90)
plt.tight_layout()
plt.show()
```

### **5. Dismissal Types**

A bar plot showing the frequency of different types of dismissals:

```python
plt.figure(figsize=(10, 6))
sns.barplot(x='out_type', y='frequency', data=dismissal_types_pd, palette='pastel')
plt.title('Frequency of Dismissal Types')
plt.xlabel('Dismissal Type')
plt.ylabel('Frequency')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
```

## **Conclusion**

In this project, I successfully utilized PySpark to analyze IPL data, uncovering insights into player performances, match outcomes, and venue-specific scoring patterns. Key findings include:

- **Top Performers**: Identified leading batsmen and bowlers based on various metrics.
- **Toss Impact**: Explored the correlation between toss results and match outcomes.
- **Venue Analysis**: Analyzed average and highest scores at different venues.
- **Dismissal Insights**: Gained insights into the frequency of various dismissal types.

This project demonstrated my capability to handle large-scale data processing and analysis using PySpark, SQL, and data visualization tools. The insights gained can inform strategic decisions for teams and players, and the skills developed here are transferable to various data engineering and analytics scenarios.

### **Thank You!** 😀
