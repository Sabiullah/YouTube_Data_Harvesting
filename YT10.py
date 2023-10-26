
import pymongo

mongodb_uri = "mongodb+srv://Safy:1234@cluster0.4w8kary.mongodb.net/"  # Replace with your MongoDB Atlas URI

client = pymongo.MongoClient(mongodb_uri)

# Access the database and collection
db = client['YouTube']
collection = db['VideoData10']

import pandas as pd

# Query data from MongoDB and convert to DataFrame
data = list(collection.find())
df = pd.DataFrame(data)
Names_all_Videos = df[["Title", "Channel_Name", "Comments"]]

import streamlit as st

# Load data from MongoDB
# @st.cache_data
def load_data():
    # Your code to retrieve data from MongoDB here
    return Names_all_Videos

# Create a Streamlit app
st.title('YouTube Data Harvesting and Warehousing ')
st.markdown("---")
st.header('What are the names of all the videos and their correspondig channels?')
All_videos = load_data()
st.write(All_videos)

st.header("Which channels have the most number of videos, and how many videos do they have?")
channel_counts = df['Channel_Name'].value_counts()
max_channel = channel_counts.idxmax()
max_videos_count = channel_counts.max()
st.write(f"The channel '{max_channel}' has the maximum number of videos: {max_videos_count}")

st.header('Top 10 Most Viewed Videos and Their Respective Channels')
df['Views'] = pd.to_numeric(df['Views'], errors='coerce')
df = df.dropna(subset=['Views'])
df = df.drop_duplicates(subset=['Title', 'Channel_Name'])
df_sorted = df.sort_values(by='Views', ascending=False)
top_10_videos = df_sorted.head(10)
st.write(top_10_videos[['Title', 'Channel_Name', 'Views']])

st.header('Number of Comments for Each Video and Their Respective Titles')
df['Comments'] = pd.to_numeric(df['Comments'], errors='coerce')
df = df.dropna(subset=['Comments'])
comments_per_video = df.groupby('Title')['Comments'].sum().reset_index()
st.write(comments_per_video)

st.header('Videos with the Highest Number of Likes and Their Respective Channel Names')
df['Likes'] = pd.to_numeric(df['Likes'], errors='coerce')
df = df.dropna(subset=['Likes'])
df = df.drop_duplicates(subset=['Title', 'Channel_Name'])
df_sorted_likes = df.sort_values(by='Likes', ascending=False)
top_videos_likes = df_sorted_likes.head(10)
st.write(top_videos_likes[['Title', 'Channel_Name', 'Likes']])

st.header('Total Likes and Dislikes for Each Video and Their Respective Titles')
df['Likes'] = pd.to_numeric(df['Likes'], errors='coerce')
df['Dislikes'] = pd.to_numeric(df['Dislikes'], errors='coerce')
df = df.dropna(subset=['Likes', 'Dislikes'])
likes_dislikes_per_video = df.groupby('Title')[['Likes', 'Dislikes']].sum().reset_index()
st.write(likes_dislikes_per_video)

st.header('Total number of views for each Channel and Their Corresponding Channel Names')
df['Views'] = pd.to_numeric(df['Views'], errors='coerce')
df = df.dropna(subset=['Views'])
views_per_channel = df.groupby('Channel_Name')['Views'].sum().reset_index()
st.write(views_per_channel[['Channel_Name', 'Views']])


st.header('Channels that Published Videos in the Year 2022')
df['Publish_Dt'] = pd.to_datetime(df['Publish_Dt'], errors='coerce')
# df = df.dropna(subset=['Publish_Dt'])
channels_2022 = df[df['Publish_Dt'].dt.year == 2022]
unique_channels_2022 = channels_2022['Channel_Name'].unique()
st.write(unique_channels_2022)

st.header('Videos that have highest number of comments and the corresponding Channels')
Names_all_Videos["Comments"] = pd.to_numeric(Names_all_Videos["Comments"], errors='coerce')
sorted_df = Names_all_Videos.sort_values(by="Comments", ascending=False)
top_videos = sorted_df.head()
st.table(top_videos[["Title", "Channel_Name", "Comments"]])

