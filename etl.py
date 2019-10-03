import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    The Function accepts files in JSON format, reads song details and artist details; 
    finally inserts records to songs and artists tables.

    Args:
    cur: Database cursor.
    filepath: JSON file's location.

    Returns:
    None

    """
    df =pd.read_json(filepath,lines=True)
    song_data = df[['song_id','title','artist_id','year','duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
        
    artist_data =df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']]. \
                     drop_duplicates(subset='artist_id', keep="first").values[0].tolist()
    cur.execute(artist_table_insert, artist_data)

    
    
def process_log_file(cur, filepath):
    """
    The Function accepts files in JSON format, reads time, user, songplay details; 
    finally inserts records to songplays, users and time tables.

    Args:
    cur: Database cursor.
    filepath: JSON file's location.

    Returns:
    None

    """
    df = pd.read_json(filepath,lines=True)

    df = df.loc[df['page'] == 'NextSong']  

    time_df=pd.DataFrame()
    time_df['start_time'] =pd.to_datetime(df['ts'], unit='ms').drop_duplicates(keep='first')
    time_data=["hour","day","week","month","year","weekday"]
    for t in time_data:
        time_df[t]=getattr(time_df['start_time'].dt,t)
        
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    user_df = df[['userId','firstName','lastName','gender','level']].drop_duplicates(subset='userId',keep='first')

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_data =  ((pd.to_datetime(row.ts, unit = 'ms'),#convert time to timestamp so it is consistent with time table \
                           row.userId, row.level, songid , artistid, row.sessionId, row.location, row.userAgent ))
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    The Function accepts a filepath and walk the tree top-down to get all the absolution paths of JSON files, 
    then print the total number of files found in filepath and iterate over files and refer to functions to 
    process and print each file is processed status info

    Args:
    cur: Database cursor.
    conn: Database connection.
    filepath: JSON file's location.
    func: data process function.

    Returns:
    None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    The Function sets up connection, cursor, read and process data, then close the connection

    Args:
    None

    Returns:
    Populates those five tables.
    """ 
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()