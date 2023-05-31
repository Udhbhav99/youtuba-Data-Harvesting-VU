import pandas as pd
import streamlit as st
from YTdata import *
from mongo_op import *
from sql_engine import *
def get_ytdata():
    ch_id=st.sidebar.text_input('Enter the required channel id')
    get=st.sidebar.button('get_data')
    if ch_id and get:
        chdata=get_all_details(ch_id)
        display=st.json(chdata)
        try:
            if unq_channel(ch_id):
                st.sidebar.write('Exists in Database')
            else:
                upload(ch_id)
        except:
            st.write('Exists in Database')
def migrate_tosql():
    ch_name=st.sidebar.selectbox("select channel",db_channel_list())
    if ch_name!=ch_details(ch_name):
        data_display=ch_details(ch_name)
        cd= pd.DataFrame(data_display['Channel_details'],index=[0])
        pld=pd.DataFrame(data_display['playlist_details'])
        vd=pd.DataFrame(data_display['video_details'])
        cmtd=pd.DataFrame(data_display['comments'])
        st.title('Channel_details')
        st.dataframe(cd)
        st.title('playlist_details')
        st.dataframe(pld)
        st.title('video_details')
        st.dataframe(vd)
        st.title('comments')
        st.dataframe(cmtd)
    engine = connect_engine()
    insert = st.sidebar.button('insert')
    if insert:
        if ch_name in pd.read_sql_query('select * from channel_details',engine)['channel_name'].to_list():
            st.sidebar.write('already exist')
        else:
            data_to_insert=ch_details(ch_name)
            ch_df=convert_ch(data_to_insert)
            vd_df=convert_vd(data_to_insert)
            pl_df=convert_playlist(data_to_insert)
            com_df=convert_com(data_to_insert)
            send_to_sql(engine, ch_df, pl_df, vd_df, com_df)
def query_data():

    query= st.sidebar.selectbox('select option',['Channels and Videos','Channels with most videos','most viewed videos','Videos and Comments',
    'videos, channels and highest likes','channels and total views','Channels with videos in 2022','videos, channels and Highest comments','avg duration of videos'])
    button = st.sidebar.button("Query")
    if button:
        if query=='Channels and Videos':
            st.title('Videos and Channels')
            st.dataframe(videos_and_names())
        elif query=='Channels with most videos':
            st.title('Channels with most videos')
            st.dataframe(channel_video_count())
        elif query=='most viewed videos':
            st.title('MOST VIEWS')
            st.dataframe(video_channels_views())
        elif query == 'Videos and Comments':
            st.title('comments on each video')
            st.dataframe(video_commentcount())
        elif query == 'videos, channels and highest likes':
            st.title('highest likes')
            st.dataframe(most_liked_vids())
        elif query == 'channels and total views':
            st.title('views and channels')
            st.dataframe(most_viewed_channels())
        elif query == 'Channels with videos in 2022':
            st.title('videos in 2022')
            x=videos_in_2022()
            st.dataframe(x)
        elif query == 'videos, channels and Highest comments':
            st.title('videos with highest comments')
            st.dataframe(channel_vids_cmts())
        elif query=='avg duration of videos':
            st.title('avg duration of videos')
            st.dataframe(avg_duration())


operation=st.sidebar.selectbox('select option',['get data','migrate data','view stats'])
if operation=='get data':
    st.title("yt data harvesting")
    get_ytdata()
elif operation=='migrate data':
    migrate_tosql()
elif operation=='view stats':
    query_data()



