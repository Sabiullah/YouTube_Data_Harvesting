#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from googleapiclient.discovery import build

# Replace with your YouTube API key
api_key = 'AIzaSyBaEWl1tZDJTbl6i1WhTHrFQQge5PytGR8'

# List of channel IDs
channel_ids = ['UCWjJ0lSAXKaGd9EeWQLYVtQ',  # SkillsDirection
               'UCc28LDlk4N2h37zWZWj-t1A',  # LearnEnglish
               'UCWNuH1-JGn4nZ7uRuURhG6g',  # EnglishAnika
               'UCOvsCixXw19GHxe34mqXw3A',  # Nileg
               'UCTn-HaKV8vju5G2j6wRTzwQ', # BodyBrain
               'UCpNUYWW0kiqyh0j5Qy3aU7w', # Misra Turp
               'UCG8nFCXDN907eYU43JMck-g', # Learn English with Matta
               'UCvy_5_4s3jAyGjX62CB1qcA', # Starr Factory
               'UCTqmTIDAauPm7_MY6UuqYzg', # Umang tambi
               'UC-S2cf2krYdMP8cJmOAdLZA', # Tigerman Root
               ]

# Initialize the YouTube API client
youtube = build('youtube', 'v3', developerKey=api_key)

def get_channel_data(youtube, channel_ids):
    all_data = []

    for channel_id in channel_ids:
        channel_data = {}

        # Get channel details
        channel_request = youtube.channels().list(
            part='snippet, statistics',
            id=channel_id
        )
        channel_response = channel_request.execute()
        channel_data["Channel_ID"] = channel_id
        channel_data["Channel_Name"] = channel_response['items'][0]['snippet']['title']
        channel_data["Subscribers"] = channel_response['items'][0]['statistics']['subscriberCount']
        channel_data["Views"] = channel_response['items'][0]['statistics']['viewCount']
        channel_data["Total_Videos"] = channel_response['items'][0]['statistics']['videoCount']

        # Get playlist details for the channel
        playlist_request = youtube.channels().list(
            part='contentDetails',
            id=channel_id
        )
        playlist_response = playlist_request.execute()
        uploads_playlist_id = playlist_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        # Get video details for the playlist
        video_ids = get_video_ids(youtube, uploads_playlist_id)
        video_details = get_video_details(youtube, video_ids, channel_data)
        
        all_data.extend(video_details)

    return all_data

# Function to get video details for a list of video IDs
def get_video_details(youtube, video_ids, channel_data):
    all_video_stats = []

    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part='snippet, statistics',
            id=','.join(video_ids[i:i+50])
        )
        response = request.execute()

        for video in response['items']:
            video_stats = {
                "Title": video['snippet']['title'],
                "Publish_Dt": video['snippet']['publishedAt'],
                "Views": video['statistics']['viewCount'],
                "Likes": video['statistics'].get('likeCount', 0),
                "Dislikes": video['statistics'].get('dislikeCount', 0),
                "Comments": video['statistics'].get('commentCount', 0),
                "Channel_Name": channel_data["Channel_Name"],
                "Channel_ID": channel_data["Channel_ID"],                
                "Subscribers": channel_data["Subscribers"],
                "Total_Videos": channel_data["Total_Videos"],
            }
            all_video_stats.append(video_stats)

    return all_video_stats

# Function to get video IDs from a playlist
def get_video_ids(youtube, playlist_id):
    video_ids = []
    next_page_token = None

    while True:
        request = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])

        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break

    return video_ids

# Get channel, playlist, and video details
data = get_channel_data(youtube, channel_ids)

# Create a DataFrame from the data
video_data = pd.DataFrame(data)

# Display the DataFrame
print(video_data)


# In[2]:


get_ipython().system('pip install pymongo')


# In[3]:


from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://Safy:1234@cluster0.4w8kary.mongodb.net/?retryWrites=true&w=majority"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)


# In[4]:


import pandas as pd
import pymongo

# MongoDB connection parameters
mongodb_uri = "mongodb+srv://Safy:1234@cluster0.4w8kary.mongodb.net/?retryWrites=true&w=majority"  # Replace with your MongoDB Atlas URI
database_name = "YouTube"  # Replace with your database name
collection_name = "VideoData10"  # Replace with your collection name

# Create a MongoClient to connect to your MongoDB Atlas cluster
client = pymongo.MongoClient(mongodb_uri)

# Access the desired database and collection
db = client[database_name]
collection = db[collection_name]

# Convert the DataFrame to a list of dictionaries (one dictionary per row)
data_dict_list = video_data.to_dict(orient='records')

# Insert the data into MongoDB
collection.insert_many(data_dict_list)

# Close the MongoDB connection
client.close()

print("Data has been successfully inserted into MongoDB Atlas.")


# In[ ]:




