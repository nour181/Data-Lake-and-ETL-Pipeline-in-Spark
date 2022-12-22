# Data-Lake-and-ETL-Pipeline-in-Spark
## Project Overview:

Sparkify (music streaming startup) wants to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
In this project, we will build a data lake and an ETL Pipeline in Spark that loads data from S3, processes the data into analytics tables and loads them back into S3

## Project Details:

### 1)Read data from S3

Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data
The script reads song_data and load_data from S3.

### 2) Process data using spark:

Transforms them to create five different tables listed below :

Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong

songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
Dimension Tables
users - users in the app Fields - user_id, first_name, last_name, gender, level

songs - songs in music database Fields - song_id, title, artist_id, year, duration

artists - artists in music database Fields - artist_id, name, location, lattitude, longitude

time - timestamps of records in songplays broken down into specific units Fields - start_time, hour, day, week, month, year, weekday

### 3) Load it back to S3

Writes them to partitioned parquet files in table directories on S3.
