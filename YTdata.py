import pandas as pd
from googleapiclient.discovery import build
from pymongo import MongoClient

#connecting api using key
def yt_conn():
    #key = 'AIzaSyCewDILW-OUkA9wrEFVrXUo9ZhZN3QUxxU'
    #original
    #key='AIzaSyAYB0Kd3RJOE1OkAUcSAYKfqtmIZFWUDGI'
    #bonsai id
    key='AIzaSyD34xkxF55w4B1haAuu3xQzuDjhqus4hbE'
    yt = build('youtube', 'v3', developerKey=key)
    return yt

#channel details
def get_channels(yt,ch_id):
    request = yt.channels().list(part='snippet,contentDetails,statistics', id=ch_id)
    response = request.execute()
    for i in response['items']:
        data = {'channel_name': i['snippet']['title'],
                'channel_id': i['id'],
                'Subscription_Count': i['statistics']['subscriberCount'],
                'view_count': i['statistics']['viewCount'],
                'channel_description': i['snippet']['description'],
                'Playlist_Id': i['contentDetails']['relatedPlaylists']['uploads']
                }
    return (data)

#all video ids of channels
def get_video_id(yt, p_id):
    request = yt.playlistItems().list(part='contentDetails', playlistId=p_id, maxResults=50)
    response = request.execute()
    video_ids =[]
    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])
    next_page = response.get('nextPageToken')

    while next_page is not None:
        request = yt.playlistItems().list(part='contentDetails', playlistId=p_id, maxResults=50, pageToken=next_page)
        response = request.execute()

        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['contentDetails']['videoId'])

        next_page = response.get('nextPageToken')
    return video_ids

#video info of each video
def get_video_info(yt, v_id):
    request = yt.videos().list(
        part="snippet,contentDetails,statistics",
        id=v_id)
    response = request.execute()

    for video in response['items']:
        stats_to_keep = {'snippet': ['title', 'description', 'tags', 'publishedAt', ],
                         'statistics': ['viewCount', 'likeCount', 'favouriteCount', 'commentCount'],
                         'contentDetails': ['duration', 'caption']}
        video_info = {}
        video_info['video_id'] = video['id']

        for k in stats_to_keep.keys():
            for v in stats_to_keep[k]:
                try:
                    video_info[v] = video[k][v]
                except:
                    video_info[v] = None
            video_info['Thumbnail'] = video['snippet']['thumbnails']['default']['url']
    return video_info

#all comments of a channel
def get_comments(yt, v_ids):
    all_comments = []
    for i in v_ids:
        try:
            request = yt.commentThreads().list(part="snippet,replies",
                                               videoId=i, maxResults=100)
            response = request.execute()
            while response:
                for x in response['items']:
                    comment = x['snippet']['topLevelComment']['snippet']['textOriginal']
                    author = x['snippet']['topLevelComment']['snippet']['authorDisplayName']
                    date = x['snippet']['topLevelComment']['snippet']['publishedAt']
                    comment_id = x['snippet']['topLevelComment']['id']
                    data = {'video_id': i, 'comment_id': comment_id, 'comments': comment, 'Comment_Author': author,
                            'comment_published_at': date}
                    all_comments.append(data)
                if 'nextPageToken' in response:
                    request = yt.commentThreads().list(part="snippet,replies", videoId=i,
                                                       maxResults=100, pageToken=response['nextPageToken'])
                    response = request.execute()

                else:
                    break
        except BaseException as e:
            pass
    return all_comments

#playlist details
def playlist_det(v_ids, p_id):
    playlist_vids = []
    for i in v_ids:
        playlist_vids.append({'video_id': i, 'Playlist_Id': p_id})
    return playlist_vids

#all details in dict or json format to enter into db
def get_all_details(ch_id):
    yt = yt_conn()
    channel_details = get_channels(yt, ch_id)
    p_id = channel_details['Playlist_Id']
    v_ids = get_video_id(yt, p_id)
    playlist_details = playlist_det(v_ids, p_id)
    all_video_info = []
    for i in v_ids:
        all_video_info.append(get_video_info(yt, i))
    channel_comments = get_comments(yt, v_ids)
    all_details = {'Channel_details': channel_details,
                   'playlist_details': playlist_details,
                   'video_details': all_video_info,
                   'comments': channel_comments
                   }
    return all_details


