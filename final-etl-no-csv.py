# modules
import mysql.connector
import pandas as pd
from google.cloud import bigquery

# variables
proj = 'etl-python-388514'
dataset = 'sample_dataset'
target_table = 'annual_movie_summary_df'
table_id = f'{proj}.{dataset}.{target_table}'

# data connections
conn = mysql.connector.connect(read_default_file='./.my.cnf')
client = bigquery.Client(project=proj)

# create SQL extract query
sql = """ SELECT year, 
                count(imdb_title_id) as movie_count, 
                avg(duration) as avg_movie_duration, 
                avg(avg_vote) as avg_rating 
                FROM oscarval_sql_course.imdb_movies 
                GROUP BY 1"""

# extract data
df = pd.read_sql(sql, conn)

# transform data


def year_rating(r):
    if r <= 5.65:
        return 'ban movie year'
    elif r <= 5.9:
        return 'okay movie year'
    elif r <= 10:
        return 'good movie year'
    else:
        return 'no rated'


df['year_rating'] = df['avg_rating'].apply(year_rating)

# job config
job_config = bigquery.LoadJobConfig(
    autodetect=True,
    write_disposition='WRITE_TRUNCATE'
)

# load data
load_job = client.load_table_from_dataframe(
    df, table_id, job_config=job_config)
load_job.result()

# check how many records where loaded
dest_table = client.get_table(table_id)
print(f'You have {dest_table.num_rows} in your table')
