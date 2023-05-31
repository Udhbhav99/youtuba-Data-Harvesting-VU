from pymongo import MongoClient
from YTdata import *

#CONNECTING TO MONGODB
def connect_mongo():
    conn = 'mongodb+srv://Udhbhav_Guvi:Vaddadi12@cluster0.t7i4vxz.mongodb.net/?retryWrites=true&w=majority'
    client = MongoClient(conn)
    db = client['yt_data']
    coll = db['channel_info']
    return coll
#getting list of channel ids already present in mongo
def ch_ids_in_mongo():
    coll=connect_mongo()
    mongo_ch_ids=[]
    for i in list(coll.find()):
        mongo_ch_ids.append(i['Channel_details']['channel_id'])
    return mongo_ch_ids
#getting channel details by name of channel
def ch_details(name):
    coll=connect_mongo()
    ch_d=[]
    for i in list(coll.find()):
        if i['Channel_details']['channel_name']==name:
            ch_d.append(i)
    return ch_d[0]
#getting all details of a channel in required format
def channel_all_details(c_id):
    coll = connect_mongo()
    for i in list(coll.find()):
        if i['Channel_details']['channel_id']==c_id:
            data={'Channel_details':i['Channel_details'],
                  'playlist_details':i['playlist_details'],
                  'video_details':i['video_details'],
                  'comments':i['comments']
                  }
            return data

#uploading to mongodb as a document
def upload(ch_id):
    coll=connect_mongo()
    data_to_enter = get_all_details(ch_id)
    coll.insert_one(data_to_enter)

#list of names of channels present in mongo
def db_channel_list():
    coll=connect_mongo()
    ch_list=[]
    for i in list(coll.find()):
        ch_list.append(i['Channel_details']['channel_name'])
    return ch_list
#checking to see if channel id is unique or if its already present in db
def unq_channel(c_id):
    coll = connect_mongo()
    ch_id_list = []
    for i in list(coll.find()):
        ch_id_list.append(i['Channel_details']['channel_id'])
    if c_id in ch_id_list:
        return True
    else:
        return False
#converting the data to required format before entering to sql database
def convert_ch(data):
  ch_df=pd.DataFrame(data['Channel_details'],index=[0]).astype(str)
  ch_df['view_count']=ch_df['view_count'].astype(int)
  return ch_df

def convert_vd(data):
  vd_df=pd.DataFrame(data['video_details'])
  vd_df['publishedAt']=pd.to_datetime(vd_df['publishedAt'])
  vd_df['duration']=vd_df['duration'].str.replace('PT','')
  vd_df['duration']=pd.to_timedelta(vd_df['duration']).dt.total_seconds()
  vd_df[['viewCount','likeCount','commentCount','favouriteCount']]=vd_df[['viewCount','likeCount','commentCount','favouriteCount']].apply(pd.to_numeric)
  vd_df['favouriteCount']=vd_df['favouriteCount'].fillna(0)
  vd_df['tags'] = vd_df['tags'].astype(str)
  return vd_df

def convert_com(data):
  com_df=pd.DataFrame(data['comments'])
  com_df['comment_published_at']=pd.to_datetime(com_df['comment_published_at'])
  return com_df

def convert_playlist(data):
    pl_df = pd.DataFrame(data['playlist_details']).astype(str)
    return pl_df






