# Databricks notebook source
# MAGIC %md
# MAGIC # Instruction :
# MAGIC
# MAGIC You are a data scientist working for a data analytics firm.  Your firm has explored a multitude of data sources and is tasked with providing key insights that your clients can make actionable. Your manager has asked you to provide some data analytics guidance for one of the firm’s clients.
# MAGIC
# MAGIC In a typical scenario, you would iteratively work with your client to understand the data wanting to be analyzed.  Having a solid understanding of the data and any underlying assumptions present is crucial to the success of a data analysis project.  However, in this case, you will need to do a little more of the “heavy lifting”.
# MAGIC
# MAGIC To begin, you will prepare a project proposal detailing: 
# MAGIC
# MAGIC - The questions we are wanting to answer, 
# MAGIC
# MAGIC - initial hypothesis about the data relationships, and 
# MAGIC
# MAGIC - the approach you will take to get your answers.  
# MAGIC
# MAGIC **NOTE:** The proposal is just a plan for how we will travel.  It’s there to help keep you on your path by keeping the end goal in mind.  You will then will execute your plan and in the end present your findings in a month to your management.

# COMMAND ----------

# MAGIC %md
# MAGIC # Milestone 1: Project Proposal and Data Selection/Preparation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 : Preparing for Your Proposal

# COMMAND ----------

# MAGIC %md
# MAGIC You will document your preparation in developing the project proposal. This includes:
# MAGIC
# MAGIC 1) Which client/dataset did you select and why?
# MAGIC
# MAGIC 2) Describe the steps you took to import and clean the data.
# MAGIC
# MAGIC 3) Perform initial exploration of data and provide some screenshots or display some stats of the data you are looking at.
# MAGIC
# MAGIC 4) Create an ERD or proposed ERD to show the relationships of the data you are exploring. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1) Which client/dataset did you select and why?

# COMMAND ----------

# MAGIC %md
# MAGIC The client/dataset I have selected for this milestone is SportsStats. My rationale for this selection is twofold. Firstly, I have a personal affinity for sports, which I consider not only a favorite pastime but also an area where I possess substantial foundational knowledge. Secondly, the nature of the data provided by SportsStats aligns well with my interests and expertise.
# MAGIC
# MAGIC To provide some context / information about the client :
# MAGIC SportsStats is a sports analysis firm partnering with local news and elite personal trainers to provide “interesting” insights to help their partners. 
# MAGIC Insights could be patterns/trends highlighting certain groups/events/countries, etc. for the purpose of developing a news story or discovering key health insights.
# MAGIC
# MAGIC - I believe working with the SportsStats dataset will yield meaningful and engaging results.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2) Describe the steps you took to import and clean the data.

# COMMAND ----------

# MAGIC %md
# MAGIC The initial approach I prefer to employ for this dataset involves utilizing Python packages, specifically Data Wrangler and ProfileReport. These tools facilitate a comprehensive understanding of the dataset and streamline the data cleaning process. Below are the steps I intend to take to accomplish this.
# MAGIC - However, it’s important to note that the final deliverable for this milestone is SQL-based, and as such, the final implementation will be conducted using MySQL.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python:

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import

# COMMAND ----------

# MAGIC %md
# MAGIC Import the packages and the dataset

# COMMAND ----------

#packages
import pandas as pd 
import matplotlib
import numpy as np
matplotlib.use('module://ipykernel.pylab.backend_inline')
from ydata_profiling import ProfileReport
from sqlalchemy import create_engine as ce
from sqlalchemy import inspect

# Set the display option
pd.set_option('display.max_colwidth', None)

# COMMAND ----------

# Define the data types
data_types = {
    'ID': 'Int64',
    'Name': str,
    'Sex': str,
    'Age': 'Int64',
    'Height': 'Int64',
    'Weight': float,
    'Team': str,
    'NOC': str,
    'Games': str,
    'Year': 'Int64',
    'Season': str,
    'City': str,
    'Sport': str,
    'Event': str,
    'Medal': str
}

# Read the CSV file
athlete_events = pd.read_csv('athlete_events.csv', dtype=data_types, quotechar='"', delimiter=',')
noc_regions = pd.read_csv("noc_regions.csv", quotechar='"', delimiter=',')

# COMMAND ----------

# MAGIC %md
# MAGIC Merge the two datasets and reorder the columns.

# COMMAND ----------

# merge the data
df1 = pd.merge(athlete_events, noc_regions, on='NOC', how='left')

# re order the columns
df1 = df1[['ID', 'Name', 'Sex', 'Age', 'Height', 'Weight', 'Team', 'NOC', 'region', 'notes', 'Games', 'Year', 'Season', 'City', 'Sport', 'Event', 'Medal']]

# rename the 'region' and 'notes' columns to 'Region' and 'Notes'
df1 = df1.rename(columns={'Name': 'Name_max','region': 'Region', 'notes': 'Notes'})


# COMMAND ----------

#create a copy of the df1 
df = df1.copy()

# COMMAND ----------

# dl the new data created to use it in sql 
df.to_csv('athlete_events_region.csv', index=False,header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cleaning

# COMMAND ----------

df.head()

# COMMAND ----------

df.info()

# COMMAND ----------

# MAGIC %md
# MAGIC It appears that we have various \<NA\> and NaN values present in the DataFrame. These will be replaced with ‘null’ values to facilitate easier data cleaning in subsequent steps.

# COMMAND ----------

# replace 
# df = df.replace({np.nan: None})

# COMMAND ----------

# MAGIC %md
# MAGIC Upon initial examination of the DataFrame, it appears that the ‘Game’ column is a concatenation of the ‘Year’ and ‘Season’ columns. If this is indeed the case, the ‘Game’ column will be deemed redundant and subsequently removed. A thorough check will be conducted to confirm this observation.

# COMMAND ----------

# split the Game column
df[['Year2', 'Season2']] = df['Games'].str.split(' ', expand=True)

# COMMAND ----------

#convert the Year2 column 
df = df.astype({"Year2": 'Int64'})

# COMMAND ----------

#check if this 2 columns are the same
df['Year'].equals(df['Year2'])

# COMMAND ----------

#check if this 2 columns are the same
df['Season'].equals(df['Season2'])

# COMMAND ----------

# MAGIC %md
# MAGIC As confirmed, the 'Game' column is indeed redundant as it is a concatenation of the 'Year' and 'Season' columns. Therefore, it will be appropriately removed to maintain the efficiency and relevance of the dataset.

# COMMAND ----------

#drop the columns
df = df.drop(columns=['Games', 'Year2', 'Season2'])

# COMMAND ----------

# MAGIC %md
# MAGIC The names of some athletes are too long, so they will be trimmed to only Two.

# COMMAND ----------

# check the Name length
df['Name_length'] = df['Name_max'].str.len()

# COMMAND ----------

# print the lenght of name to have a better view
df['Name_length'].unique()

# COMMAND ----------

df[df['Name_length'] > 100]

# COMMAND ----------

# create a new columns and store only max 2 name of each athletes
df['Name'] = df['Name_max'].str.split().str[:2].str.join(' ')

# COMMAND ----------

# MAGIC %md
# MAGIC check the len of the Event column

# COMMAND ----------

# check the Name length
df['event_max'] = df['Event'].str.len()

# print the lenght of name to have a better view
df['event_max'].unique()

# COMMAND ----------

# MAGIC %md
# MAGIC The length is a bit long, but it is okay.

# COMMAND ----------

# MAGIC %md
# MAGIC Drop the column that we added to understand the data. We don’t need it anymore. 

# COMMAND ----------

#drop the columns
df = df.drop(columns=['event_max', 'Name_max', 'Name_length'])

# COMMAND ----------

# MAGIC %md
# MAGIC Check duplicate and missing values

# COMMAND ----------

# Check for duplicates
print('1 ) -Shape of dataframe:', df.shape)
print('2 ) -Total Duplicates:', df.duplicated().sum())
print('There are many duplicate values in the data, but it’s unclear whether they are truly duplicates.')

# Check for missing values in dataframe
print('4 ) -Total count of missing values:', df.isna().sum().sum())
print('')
# Display missing values per column in dataframe
print('5 ) -Missing values per column:')
df.isna().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC There are many duplicate values in the data, but it’s unclear whether they are truly duplicates. This is because certain sports have numerous events that occur consecutively. Sports such as ‘Art Competitions’, ‘Cycling’, ‘Sailing’, and ‘Equestrianism’ are examples of this.

# COMMAND ----------

# MAGIC %md
# MAGIC It appears that there are numerous missing values in your DataFrame. At this stage of the project, it’s unclear whether these missing values will impact the results. Therefore, we will retain them for now. If we find that they significantly affect our analysis, we will address these missing values accordingly.

# COMMAND ----------

# MAGIC %md
# MAGIC Pivot Table of the Sport and sex by age

# COMMAND ----------

#pivot_table = df.pivot_table(values='Age', index='Sex', columns='Sport', aggfunc='median')
#pivot_table

# COMMAND ----------

# MAGIC %md
# MAGIC Fill the missing values with the Median
# MAGIC - change the code to do the median for each column this code fill with the age in the height and weight

# COMMAND ----------

#cols_to_fill = ['Age', 'Height', 'Weight']

# Convert columns to numeric
#for col in cols_to_fill:
#    df[col] = pd.to_numeric(df[col], errors='coerce')

# Now you can fill NA values with the mean
#mean_values = df.groupby(['Sex', 'Sport'])[cols_to_fill].transform('median')
#df[cols_to_fill] = df[cols_to_fill].fillna(mean_values)


# COMMAND ----------

# Define the columns to be filled
#cols_to_fill = ['Age', 'Height', 'Weight']

# Calculate the mean for each column grouped by 'Sex' and 'Sport'
#mean_values = df.groupby(['Sex', 'Sport'])[cols_to_fill].transform('mean')

# Fill NA values in df with the calculated mean values
#df[cols_to_fill] = df[cols_to_fill].fillna(mean_values)


# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL:

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Delta Lake
# MAGIC * Create medallion architecture (bronze, silver, gold) with [Delta Lake](http://delta.io/)
# MAGIC
# MAGIC ![](https://files.training.databricks.com/images/davis/delta_multihop.png)

# COMMAND ----------

# MAGIC %md
# MAGIC The files were uploaded using Databricks' "Create Table" feature.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE default;

# COMMAND ----------

# MAGIC %fs head  /FileStore/tables/athlete_events.csv

# COMMAND ----------

# MAGIC %fs ls  /FileStore/tables/athlete_events.csv

# COMMAND ----------

# MAGIC %fs ls  /FileStore/tables/noc_regions.csv

# COMMAND ----------

athlete_events_df = spark.sql("SELECT * FROM athlete_events_csv")
athlete_events_df.createOrReplaceTempView('athlete_events')

# COMMAND ----------

noc_regions_df = spark.sql("SELECT * FROM noc_regions_csv")
noc_regions_df.createOrReplaceTempView('noc_regions')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM athlete_events LIMIT 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM noc_regions LIMIT 2

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h2> Write raw data into Delta Bronze</h2>
# MAGIC
# MAGIC <img width="75px" src="https://files.training.databricks.com/images/davis/images_bronze.png">

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS Databricks;
# MAGIC USE Databricks;
# MAGIC CREATE OR REPLACE TABLE athlete_events_Bronze
# MAGIC USING DELTA
# MAGIC AS 
# MAGIC   SELECT * FROM athlete_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE Databricks;
# MAGIC CREATE OR REPLACE TABLE noc_regions_Bronze
# MAGIC USING DELTA
# MAGIC AS 
# MAGIC   SELECT * FROM noc_regions;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Refine bronze tables, write to Delta Silver
# MAGIC
# MAGIC <img width="75px" src="https://files.training.databricks.com/images/davis/images_silver.png">
# MAGIC
# MAGIC Clean and Filter unnecessary columns and nulls.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE athlete_events_Silver 
# MAGIC USING DELTA
# MAGIC AS 
# MAGIC   SELECT 
# MAGIC     CAST(ID AS int) AS ID,
# MAGIC     CAST(Name AS string) AS Name,
# MAGIC     CAST(Sex AS string) AS Sex,
# MAGIC     CAST(Age AS int) AS Age,
# MAGIC     CAST(Height AS int) AS Height,
# MAGIC     CAST(Weight AS float) AS Weight,
# MAGIC     CAST(Team AS string) AS Team,
# MAGIC     CAST(NOC AS string) AS NOC,
# MAGIC     CAST(Games AS string) AS Games,
# MAGIC     CAST(Year AS int) AS Year,
# MAGIC     CAST(Season AS string) AS Season,
# MAGIC     CAST(City AS string) AS City,
# MAGIC     CAST(Sport AS string) AS Sport,
# MAGIC     CAST(Event AS string) AS Event,
# MAGIC     CAST(Medal AS string) AS Medal
# MAGIC   FROM athlete_events_Bronze;
# MAGIC   
# MAGIC SELECT * FROM athlete_events_Silver LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE noc_regions_Silver 
# MAGIC USING DELTA
# MAGIC AS 
# MAGIC   SELECT NOC,
# MAGIC          region AS Region,
# MAGIC          notes AS Note
# MAGIC   FROM noc_regions_Bronze;
# MAGIC   
# MAGIC SELECT * FROM noc_regions_Silver LIMIT 10;

# COMMAND ----------

# MAGIC %sql DESCRIBE athlete_events_Silver;

# COMMAND ----------

# MAGIC %sql DESCRIBE noc_regions_Silver;

# COMMAND ----------

# MAGIC %md
# MAGIC Upon initial examination of the DataFrame, it appears that the ‘Game’ column is a concatenation of the ‘Year’ and ‘Season’ columns. If this is indeed the case, the ‘Game’ column will be deemed redundant and subsequently removed. A thorough check will be conducted to confirm this observation.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_Games_concat
# MAGIC AS
# MAGIC SELECT  Year, 
# MAGIC         CAST(SUBSTRING_INDEX(Games, ' ', 1) AS int) AS Games_Year, 
# MAGIC         SUBSTRING_INDEX(Games, ' ', -1) AS Games_Season,
# MAGIC         Season
# MAGIC FROM athlete_events_Silver;
# MAGIC
# MAGIC SELECT * FROM temp_Games_concat LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC An easy way to determine if two columns are identical is to use the <> operator to compare them. If the columns are the same, there will be no result. If they are not the same, there will be an output.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM temp_Games_concat
# MAGIC WHERE Year <> Games_Year OR Season <> Games_Season
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC As confirmed, the 'Game' column is indeed redundant as it is a concatenation of the 'Year' and 'Season' columns. Therefore, it will be appropriately removed to maintain the efficiency and relevance of the dataset.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop the temporary view we created it is useless now
# MAGIC DROP VIEW temp_Games_concat

# COMMAND ----------

# MAGIC %md
# MAGIC  Delta table doesn’t support the DROP COLUMN operation because Column Mapping is not enabled. You can enable Column Mapping on your Delta table with mapping mode ‘name’ 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- This command alters the properties of the 'athlete_events_Silver' table.
# MAGIC ALTER TABLE athlete_events_Silver SET TBLPROPERTIES (
# MAGIC    -- Enables column mapping with 'name' mode. This allows operations like adding, dropping, and renaming columns.
# MAGIC    'delta.columnMapping.mode' = 'name',
# MAGIC    
# MAGIC    -- Sets the minimum reader version to '2'. This means that the table can only be read by Delta Lake version 0.2.0 and above.
# MAGIC    'delta.minReaderVersion' = '2',
# MAGIC    
# MAGIC    -- Sets the minimum writer version to '5'. This means that the table can only be written by Delta Lake version 0.5.0 and above.
# MAGIC    'delta.minWriterVersion' = '5'
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delete the 'Games' column
# MAGIC ALTER TABLE athlete_events_Silver DROP COLUMNS (Games)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM athlete_events_Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1) Get the number of rows in the dataframe
# MAGIC SELECT COUNT(*) AS NumberOfRows
# MAGIC FROM athlete_events_Silver;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- 2) Get the number of duplicate rows
# MAGIC SELECT COUNT(*) - COUNT(DISTINCT *) AS NumberOfDuplicates
# MAGIC FROM athlete_events_Silver;

# COMMAND ----------

# MAGIC %md
# MAGIC There are many duplicate values in the data, but it’s unclear whether they are truly duplicates. This is because certain sports have numerous events that occur consecutively. Sports such as ‘Art Competitions’, ‘Cycling’, ‘Sailing’, and ‘Equestrianism’ are examples of this.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- missing values for each columns
# MAGIC SELECT 
# MAGIC   COUNT(*) - COUNT(ID) as ID,
# MAGIC   COUNT(*) - COUNT(Name) as Name,
# MAGIC   COUNT(*) - COUNT(Sex) as Sex,
# MAGIC   COUNT(*) - COUNT(Age) as Age,
# MAGIC   COUNT(*) - COUNT(Height) as Height,
# MAGIC   COUNT(*) - COUNT(Weight) as Weight,
# MAGIC   COUNT(*) - COUNT(Team) as Team,
# MAGIC   COUNT(*) - COUNT(NOC) as NOC,
# MAGIC   COUNT(*) - COUNT(Year) as Year,
# MAGIC   COUNT(*) - COUNT(Season) as Season,
# MAGIC   COUNT(*) - COUNT(City) as City,
# MAGIC   COUNT(*) - COUNT(Sport) as Sport,
# MAGIC   COUNT(*) - COUNT(Event) as Event,
# MAGIC   COUNT(*) - COUNT(Medal) as Medal
# MAGIC FROM 
# MAGIC   athlete_events_silver

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY athlete_events_Silver

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Refine, Enrich and Aggregate Data, write to Delta Gold
# MAGIC
# MAGIC <img width="75px" src="https://files.training.databricks.com/images/davis/images_gold.png">
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE athlete_events_Gold
# MAGIC USING DELTA
# MAGIC AS 
# MAGIC   SELECT *
# MAGIC   FROM athlete_events_Silver;
# MAGIC   
# MAGIC SELECT * FROM athlete_events_Gold LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE noc_regions_Gold
# MAGIC USING DELTA
# MAGIC AS 
# MAGIC   SELECT *
# MAGIC   FROM noc_regions_Silver;
# MAGIC   
# MAGIC SELECT * FROM noc_regions_Gold LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3) Perform initial exploration of data and provide some screenshots or display some stats of the data you are looking at.

# COMMAND ----------

# MAGIC %md
# MAGIC -  Note: We will switch from using SQL Delta tables to utilizing the Python package `pandas_profiling.ProfileReport` to gain a comprehensive understanding of the dataframe.
# MAGIC
# MAGIC brief explanation of the package : 
# MAGIC The ydata-profiling package is a powerful tool that provides a quick and detailed exploratory data analysis (EDA) with just a single line of code. It’s designed to help data scientists understand their dataset better by providing insights such as:
# MAGIC
# MAGIC - Type Inference: Automatically detects the types of columns in your DataFrame.
# MAGIC - Descriptive Statistics: Provides summary statistics for each column in your DataFrame.
# MAGIC - Correlations: Identifies relationships between different variables in your DataFrame.
# MAGIC - Missing Values: Highlights columns with missing values and their impact on other columns.
# MAGIC - Warnings: Alerts you to potential issues with your data, such as high cardinality.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python:

# COMMAND ----------

# MAGIC %md
# MAGIC join the two Tables and reorder the columns.

# COMMAND ----------


df1 = sql(""" SELECT at.ID, at.Name, at.Sex, at.Age, at.Height, at.Weight, at.Team, at.NOC, nr.Region, nr.Note, at.Year, at.Season, at.City, at.Sport, at.Event, at.Medal
FROM athlete_events_Gold at
LEFT JOIN noc_regions_Gold nr ON at.NOC = nr.NOC""").toPandas()
df = df1.replace('NA', None)

# COMMAND ----------

df.head()

# COMMAND ----------

 from ydata_profiling import ProfileReport
 #ProfileReport
 ProfileReport(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Descriptive Statistics of the DataFrame

# COMMAND ----------

df.describe(include="all")

# COMMAND ----------

df['Medal'].value_counts()

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC Top 10 Region with the most Medal

# COMMAND ----------

# Create the pivot table
pivot_table = df.pivot_table(values='ID', index='Region', columns='Medal', aggfunc='count')

# Calculate the total medals for each region
total_medals = pivot_table.sum(axis=1).sort_values(ascending=False)

# Reorder the rows of the pivot table based on total_medals
pivot_table = pivot_table.reindex(total_medals.index)

# top 10 of the region with the most medal
pivot_table.head(10)

# COMMAND ----------

# MAGIC %md
# MAGIC Based on the data, it appears that the United States leads in terms of medal count, followed closely by Russia and Germany.

# COMMAND ----------

# MAGIC %md
# MAGIC Top 10 Most Medaled Athletes

# COMMAND ----------

# Create the pivot table
pivot_table = df.pivot_table(values='ID', index='Name', columns='Medal', aggfunc='count')

# Calculate the total medals for each region
total_medals = pivot_table.sum(axis=1).sort_values(ascending=False)

# Reorder the rows of the pivot table based on total_medals
pivot_table = pivot_table.reindex(total_medals.index)

# top 10 of the region with the most medal
pivot_table.head(20)

# COMMAND ----------

# MAGIC %md
# MAGIC Upon conducting research on Michael Phelps, it is evident that he stands as the most successful and decorated Olympian of all time, boasting a total of 28 medals. Phelps holds the all-time records for Olympic gold medals, Olympic gold medals in individual events, and Olympic medals in individual events. 
# MAGIC
# MAGIC During the 2004 Summer Olympics in Athens, Phelps matched the record of eight medals of any color at a single Games, previously held by gymnast Alexander Dityatin, by securing six gold and two bronze medals.
# MAGIC
# MAGIC In a remarkable feat four years later, Phelps won eight gold medals at the 2008 Beijing Games, surpassing fellow American swimmer Mark Spitz's 1972 record of seven first-place finishes at any single Olympic Games.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Utilizing the data at our disposal, we will verify the information we have discovered

# COMMAND ----------

# Create the pivot table
pivot_table = df.pivot_table(values='ID', index=['Name', 'Year'], columns='Medal', aggfunc='count')

# Calculate the total medals for each athlete and year
total_medals = pivot_table.sum(axis=1).sort_values(ascending=False)

# Reorder the rows of the pivot table based on total_medals
pivot_table = pivot_table.reindex(total_medals.index)

# Display the top 20 athletes and years with the most medals
pivot_table.head(20)


# COMMAND ----------

# Filter the dataframe for only 'Gold' medals
df_gold = df[df['Medal'] == 'Gold']

# Create the pivot table
pivot_table_gold = df_gold.pivot_table(values='ID', index=['Name', 'Year'], aggfunc='count')

# Calculate the total gold medals for each athlete and year
total_gold_medals = pivot_table_gold.sum(axis=1).sort_values(ascending=False)

# Reorder the rows of the pivot table based on total_gold_medals
pivot_table_gold = pivot_table_gold.reindex(total_gold_medals.index)

# Display the top 20 athletes and years with the most gold medals
pivot_table_gold = pivot_table_gold.rename(columns={'ID': 'Gold'})
pivot_table_gold.head(20)


# COMMAND ----------

# MAGIC %md
# MAGIC The information seems to be correct. Michael Phelps came close to matching the record set by Mark Andrew Spitz in 1972 of seven gold medals during the 2004 Olympics, having secured six gold medals himself. However, in the 2008 Olympics, Phelps exceeded this record and established himself as the most decorated athlete in Olympic history with eight gold medals. Furthermore, with a total of 28 medals, he is the most decorated Olympian of all time.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4) Create an ERD or proposed ERD to show the relationships of the data you are exploring. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python

# COMMAND ----------

# MAGIC %md
# MAGIC Split the columns to create my ERD

# COMMAND ----------

df.head()

# COMMAND ----------

# Fact Table: Competitions
df_competitions = df1[['Year', 'Season', 'City', 'Sport', 'Event', 'Medal']].copy()
df_competitions.insert(0, 'Competition_ID', range(1, 1 + len(df_competitions)))

# Dimension Tables
df_athletes = df1[['ID', 'Name', 'Sex', 'Age', 'Height', 'Weight']]
df_teams = df1[['ID', 'Team', 'NOC', 'Region', 'Note']]

# Add Athlete_ID to Competitions table
df_competitions['Athlete_ID'] = df1['ID']


# COMMAND ----------

# MAGIC %md
# MAGIC Save the result as csv

# COMMAND ----------

# Save the Competitions dataframe as a CSV file
display(df_competitions)

# COMMAND ----------

# Save the Athletes dataframe as a CSV file
display(df_athletes)

# COMMAND ----------

# Save the Teams dataframe as a CSV file
display(df_teams)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![ERDs](/files/tables/ERDs-1.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 : Develop Project Proposal

# COMMAND ----------

# MAGIC %md
# MAGIC In this step, you will need to include the following:
# MAGIC
# MAGIC **1) Description**
# MAGIC Write a 5-6 sentence paragraph describing your project; include who might be interested to learn about your findings. Who might be your audience?
# MAGIC
# MAGIC **2) Questions**
# MAGIC Create 2-3 questions that you want to answer with the data:
# MAGIC
# MAGIC - This will be easier to answer once you've had an opportunity to look at the data and do some initial exploration.
# MAGIC
# MAGIC - Don't get carried away on the analysis piece at this stage as there will be more analysis later.
# MAGIC
# MAGIC - Do focus on key data elements that are present. For instance: What are they, when are they, who are they about? Do they connect? How do they connect? Jot down ideas as you brainstorm.
# MAGIC
# MAGIC **3) Hypothesis**
# MAGIC What are your initial hypotheses about the data?
# MAGIC
# MAGIC Write 2-3 assumptions about the data that you'll want to go back to prove or disprove. You will want to keep them in front of you as you look at the data to keep them or change them. You may see relationships that you want to explore and will develop a "belief" about the data.  
# MAGIC
# MAGIC - Start documenting what you think you can tell from the data. 
# MAGIC
# MAGIC - What pops up as interesting to you? Most likely it will be interesting to others as well.
# MAGIC
# MAGIC - Use the discussion boards to discuss with others about your client and the data to brainstorm together.
# MAGIC
# MAGIC **4) Approach**
# MAGIC Describe in 5-6 sentences what approach you are going to take in order to prove (or disprove) your hypotheses. Think about the following in your answer:  
# MAGIC
# MAGIC - What features (fields/columns) are you going to look at first?
# MAGIC
# MAGIC - Is there a relationship that exists that you want to explore?
# MAGIC
# MAGIC - What metric/ evaluation measure will you use?

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Description**
# MAGIC Write a 5-6 sentence paragraph describing your project; include who might be interested to learn about your findings. Who might be your audience?
# MAGIC
# MAGIC This project aims to analyze a dataset from SportsStats, a sports analysis firm that partners with local news outlets and elite personal trainers to provide insightful patterns and trends. The dataset contains detailed information about various sports competitions, including athletes' details, teams, and competition results. The findings from this project could be of interest to sports enthusiasts, news outlets looking for unique sports stories, personal trainers seeking health insights, and even sports analysts looking for patterns and trends in sports competitions.

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Questions**
# MAGIC Create 2-3 questions that you want to answer with the data:
# MAGIC
# MAGIC - Which are the top 10 regions with the most medals?
# MAGIC
# MAGIC - Who are the top 10 athletes with the most medals?
# MAGIC
# MAGIC - Conduct a detailed analysis on Michael Phelps and his all-time records for the Olympics. Is he really the athlete with the most gold medals?

# COMMAND ----------

# MAGIC %md
# MAGIC **3) Hypothesis**
# MAGIC What are your initial hypotheses about the data?
# MAGIC
# MAGIC Write 2-3 assumptions about the data that you'll want to go back to prove or disprove. You will want to keep them in front of you as you look at the data to keep them or change them. You may see relationships that you want to explore and will develop a "belief" about the data.  
# MAGIC
# MAGIC - Regional Dominance: Certain regions might have a higher medal count due to factors such as better training facilities, more funding, or a larger pool of athletes.
# MAGIC
# MAGIC - Athlete Excellence: Some athletes might have significantly more medals than others. This could be due to their exceptional skills, physical attributes, or the nature of the sport they participate in.
# MAGIC
# MAGIC - Michael Phelps’ Record: Michael Phelps might indeed be the athlete with the most gold medals in the history of the Olympics. His exceptional performance could be attributed to factors such as his training regimen, physical attributes, or mental strength.

# COMMAND ----------

# MAGIC %md
# MAGIC **4) Approach**
# MAGIC Describe in 5-6 sentences what approach you are going to take in order to prove (or disprove) your hypotheses. 
# MAGIC
# MAGIC The best approach is to use a pivot table.
# MAGIC
# MAGIC - Regional Medal Count: To identify the top 10 regions with the most medals, I will first focus on the ‘Region’ and ‘Medal’ columns. I will count the number of medals for each region and then sort the regions based on this count.
# MAGIC
# MAGIC - Top Athletes: To find the top 10 athletes with the most medals, I will look at the ‘Name’ and ‘Medal’ columns. Similar to the regional medal count, I will count the number of medals for each athlete and then sort the athletes based on this count.
# MAGIC
# MAGIC - Michael Phelps Analysis: For a detailed analysis on Michael Phelps, I will create a pivot table similar to the ones used in previous questions. The pivot table will be based on the ‘Name’, ‘Year’, and ‘Medal’ columns. I will filter the data to include only the top 10 athletes based on the number of ‘Gold’ medals won. This will allow me to analyze and compare the performances of Michael Phelps and other top athletes over the years in terms of gold medals won.
# MAGIC
# MAGIC Throughout the analysis, I will use visualizations to better understand the data and to present the findings. Depending on the nature of the relationships discovered, appropriate statistical tests may be applied later to confirm the findings.
