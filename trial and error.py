from pymongo import MongoClient

conn = 'mongodb+srv://Udhbhav_Guvi:Vaddadi12@cluster0.t7i4vxz.mongodb.net/?retryWrites=true&w=majority'
client = MongoClient(conn)
db = client['yt_data']
coll = db['channel_info']

docs