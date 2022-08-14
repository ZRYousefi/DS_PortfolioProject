
# pip install psycopg2 
import pandas as pd
import psycopg2 as ps

# upload csv files 
df = pd.read.csv('youtube_vids_1st_pull.csv', index_col=0)
df.head()

def connect_to_db(host_name, db_name, port, user_name, password):

    try: 
        conn = ps.connect(host=host_name, database=db_name, port=port, user=user_name, password=password)
    except ps.OperationalError as e:
        raise e 
    else:
        print('Successfully connected!')
        return conn


def create_table(curr):
    
    create_table_command = ("""CREATE TABLE IF NOT EXISTS videos (
                video_id VARCHAR(255) PRIMARY KEY,
                video_title TEXT NOT NULL,
                upload_date DATE NOT NULL DEFAULT CURRENT_DATE,
                view_count INTEGER NOT NULL,
                like_count INTEGER NOT NULL,
                favorite_count INTEGER NOT NULL,
                comment_count INTEGER NOT NULL
                )""")
    curr.execute(create_table_command)

    # import mysql.connector
    # mydb = mysql.connector.connect(
    #   host="localhost",
    #   user="yourusername",
    #   password="yourpassword",
    #   database="mydatabase"
    # )
    # mycursor = mydb.cursor()
    # mycursor.execute("CREATE TABLE customers (name VARCHAR(255), address VARCHAR(255))")

def update_row(curr, video_id, video_title, view_count, like_count, favorite_count, comment_count):
    query = ("""UPDATE videos SET 
            video_title = %s,
            view_count = %s, 
            like_count = %s, 
            favorite_count = %s,
            comment_count = %s
            WHERE video_id = %s;""")
    vars_to_update = (video_title, view_count, like_count, favorite_count, comment_count, video_id)
    curr.execute(query, vars_to_update)


def check_if_video_exists(curr, video_id):
    query = ("""SELECT video_id FROM videos WHERE video_id = %s;""")
    curr.execute(query, (video_id,))
    return curr.fetchone is not None 

def truncate_table(curr):
    truncate_table = ("""TRUNCATE TABLE videos""")
    curr.execute(truncate_table)

def insert_into_table(curr, video_id, video_title, upload_date, view_count, like_count, favorite_count, comment_count):
    insert_into_videos = ("""INSERT INTO videos (video_id, video_title, upload_date,
                        view_count, like_count, favorite_count, comment_count)
    VALUES(%s,%s,%s,%s,%s,%s,%s);""")
    row_to_insert = (video_id, video_title, upload_date, view_count, like_count, favorite_count, comment_count)
    curr.execute(insert_into_videos, row_to_insert)

def append_from_df_to_db(curr,df):
    for i, row in df.iterrows():
        insert_into_table(curr, row['video_id'], row['video_title'], row['upload_date'], row['view_count']
                          , row['like_count'], row['favorite_count'], row['comment_count'])


def update_db(curr, df):

    temp_df = pd.DataFrame(columns=['video_id', 'video_title', 'upload_date', 'view_count',
                                   'like_count', 'favorite_count', 'comment_count'])
    for i, row in df.iterrows():
        # If video already exists, then we will update
        if check_if_video_exists(curr, row['video_id']): 
            update_row(curr, row['video_id'],row['video_title'],row['view_count'],row['like_count'],row['favorite_count'],row['comment_count'])
        # If video doesn't exist, we will add it to a temp df and append it using append_from_df_to_db
        else: 
            tmp_df = tmp_df.append(row)


#Main

#database credentials
host_name="localhost" # 'xxxxxxxxx.rds.amazonaws.com'
db_name="mydatabase"
port="port"  # '5432'
user_name="yourusername"
password="yourpassword"
conn = None

# establish a connection to db 
conn = connect_to_db(host_name, db_name, port, user_name, password)
curr = conn.cursor()


#create table
create_table(curr)

#update data for existing videos
new_vid_df = update_db(curr, df)
conn.commit()

