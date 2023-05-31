import sqlite3
from sqlalchemy import create_engine
import sqlalchemy
from mongo_op import pd

#connecting to database
def connect_engine():
    engine = create_engine("sqlite:///my_yt_database.db")
    return engine
def send_to_sql(engine,ch_df,pl_df,vd_df,com_df):
    engine=connect_engine()
    ch_df.to_sql('channel_details', engine, if_exists='append', index=False)
    pl_df.to_sql('playlist_details', engine, if_exists='append', index=False)
    vd_df.to_sql('video_details', engine, if_exists='append', index=False)
    com_df.to_sql('com_details', engine, if_exists='append', index=False)

#querying

#videos and channel names
def videos_and_names():
    engine = connect_engine()
    x=pd.read_sql_query("select title, channel_name from video_details,playlist_details,Channel_details where video_details.video_id=playlist_details.video_id and playlist_details.playlist_id=Channel_details.playlist_id",engine)
    return x
# channels with most videos
def channel_video_count():
    engine = connect_engine()
    x = pd.read_sql_query(
        "select channel_name, count(video_details.video_id) as videos from video_details,playlist_details,Channel_details where video_details.video_id=playlist_details.video_id and playlist_details.playlist_id=Channel_details.playlist_id group by channel_name order by videos desc",
        engine)
    return x

# videos and channel names ordered my most views
def video_channels_views():
    engine = connect_engine()
    data= pd.read_sql_query("select title, channel_name,viewCount from video_details,playlist_details,Channel_details where video_details.video_id=playlist_details.video_id and playlist_details.playlist_id=Channel_details.playlist_id ORDER BY viewCount DESC LIMIT 10",engine)
    return data

#video name and comment count
def video_commentcount():
    engine = connect_engine()
    v_comm=pd.read_sql_query("select title, commentCount from video_details order by commentCount desc",engine)
    v_comm['commentCount'].fillna(0)
    return v_comm
#most likes and video names
def most_liked_vids():
    engine = connect_engine()
    mostliked = pd.read_sql_query(
        "select title, channel_name,likeCount from video_details,playlist_details,Channel_details where video_details.video_id=playlist_details.video_id and playlist_details.playlist_id=Channel_details.playlist_id ORDER BY likeCount DESC LIMIT 10",
        engine)
    return mostliked
#channel names and their views
def most_viewed_channels():
    engine = connect_engine()
    ch_tot_views=pd.read_sql_query("select channel_name,view_count from Channel_details order by view_count desc",engine)
    return ch_tot_views

#videos in 2022
def videos_in_2022():
    engine = connect_engine()
    x=pd.read_sql_query(
        "select channel_name,count(video_details.video_id) as number_of_videos from video_details,playlist_details,Channel_details where video_details.video_id=playlist_details.video_id and playlist_details.playlist_id=Channel_details.playlist_id and publishedAt like '2022%' group by channel_name order by number_of_videos desc",
        engine)

    return x
#comments and channels
def channel_vids_cmts():
    engine = connect_engine()
    x=pd.read_sql_query(
        "select channel_name,title,commentCount from Channel_details,video_details,playlist_details where video_details.video_id=playlist_details.video_id and playlist_details.playlist_id=Channel_details.playlist_id order by commentCount desc limit 10",
        engine)
    return x
def avg_duration():
    engine = connect_engine()
    x=pd.read_sql_query("select channel_name, (avg(duration)/60) as duration_in_minutes from Channel_details,video_details,playlist_details where video_details.video_id=playlist_details.video_id and playlist_details.playlist_id=Channel_details.playlist_id group by channel_name",engine)
    return x
