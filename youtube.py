api_key='AIzaSyBK8TB3loqKboyA1BUlEgXDMqzO2l3NL4k'

import re
import psycopg2
import pandas as pd
import streamlit as st
from googleapiclient.discovery import build


# Define API service name and version for YouTube Data API
api_service_name = "youtube"
api_version = "v3"

# Build the YouTube API service object using the developer key
youtube = build(api_service_name, api_version, developerKey=api_key)

# Database connection details
host = 'localhost'
port = '5432'
database = 'youtube_final'  # The name of the PostgreSQL database
username = 'postgres'      # PostgreSQL username
password = 'malathi03'      # PostgreSQL password

# Connect to PostgreSQL database using psycopg2
eta = psycopg2.connect(host=host, port=port, database=database, user=username, password=password)
cursor = eta.cursor()





d = {
    'Astronomic': 'UCmXkiw-1x9ZhNOPz0X73tTA',
    'Aravind_SA': 'UCrJNwpevlqZLVO1LW2Mo-Ag',
    'BrainCraft': 'UCt_t6FwNsqr3WWoL6dFqG9w',
    'CodeParade': 'UCrv269YwJzuZL3dH5PCgxUw',
    'Garden_up': 'UC0nChSOqQbA6tAi8_K7pD_A',
    'Jordindian': 'UCYLS9TSah19IsB8yyUpiDzg',
    'Dr_Riya': 'UCfzzu2GRpjpKkoGQzx6nl5Q',
    'Lisa_Koshy': 'UCxSz6JVYmzVhtkraHWZC7HQ',
    'minutephysics': 'UCUHW94eEFW7hkUMVaZz4eDg',
    'Pentatronix': 'UCmv1CLT6ZcFdTJMHxaR9XeA'
}





# Function to retrieve channel details using channel ID
def get_channel_details(youtube, channel_id):
    # Request channel details from YouTube API
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",  # Specify parts to retrieve
        id=channel_id
    )
    
    # Execute the API request
    response = request.execute()

    # Extract and store relevant channel information
    for item in response['items']:
        data = {
            'channelName': item['snippet']['title'],  # Channel name
            'channelId': item['id'],  # Channel ID
            'subscribers': item['statistics']['subscriberCount'],  # Subscriber count
            'views': item['statistics']['viewCount'],  # Total view count
            'totalVideos': item['statistics']['videoCount'],  # Total video count
            'playlistId': item['contentDetails']['relatedPlaylists']['uploads'],  # Uploads playlist ID
            'channel_description': item['snippet']['description']  # Channel description
        }
    
    # Return the collected data
    return data


# This function collects all playlists created by the channel using its channel ID
def get_playlists_details(youtube, channel_id):
    # Request playlist details from YouTube API
    request = youtube.playlists().list(
        part="snippet,contentDetails",  # Specify parts to retrieve
        channelId=channel_id,  # Channel ID
        maxResults=25  # Max 25 playlists per request
    )
    
    response = request.execute()
    All_data = []

    # Loop through the playlists in the response
    for item in response['items']:
        data = {
            'PlaylistId': item['id'],  # Playlist ID
            'Title': item['snippet']['title'],  # Playlist title
            'ChannelId': item['snippet']['channelId'],  # Channel ID
            'ChannelName': item['snippet']['channelTitle'],  # Channel name
            'PublishedAt': item['snippet']['publishedAt'],  # Playlist publish date
            'VideoCount': item['contentDetails']['itemCount']  # Number of videos in the playlist
        }
        All_data.append(data)

    # Get next page token to retrieve more playlists, if available
    next_page_token = response.get('nextPageToken')

    # Continue fetching playlists until no more pages
    while next_page_token is not None:
        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=25
        )
        response = request.execute()

        # Add new playlists to the data list
        for item in response['items']:
            data = {
                'PlaylistId': item['id'],
                'Title': item['snippet']['title'],
                'ChannelId': item['snippet']['channelId'],
                'ChannelName': item['snippet']['channelTitle'],
                'PublishedAt': item['snippet']['publishedAt'],
                'VideoCount': item['contentDetails']['itemCount']
            }
            All_data.append(data)

        # Update next page token for further requests
        next_page_token = response.get('nextPageToken')

    return All_data  # Return all collected playlists


# This function retrieves all video IDs from a given playlist (usually the 'uploads' playlist for a channel)
def get_video_ids(youtube, playlist_id):
    # Initial request to get video IDs from the playlist
    request = youtube.playlistItems().list(
        part='contentDetails',  # Requesting content details (video IDs)
        playlistId=playlist_id,  # Playlist ID (usually uploads playlist)
        maxResults=50  # Maximum of 50 results per request
    )
    
    response = request.execute()

    # Initialize a list to store video IDs
    video_ids = []

    # Extract video IDs from the response and store them in the list
    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])

    # Get next page token to check if there are more pages of video results
    next_page_token = response.get('nextPageToken')
    more_pages = True

    # Loop to get video IDs from all pages until no more pages are left
    while more_pages:
        if next_page_token is None:  # If no more pages, exit the loop
            more_pages = False
        else:
            # Request the next page of results
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token  # Use the token to request the next page
            )
            response = request.execute()

            # Extract and append video IDs from the new page
            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])

            # Update the next page token for further requests
            next_page_token = response.get('nextPageToken')

    # Return the list of all video IDs collected from the playlist
    return video_ids


# This function retrieves details of a specific video using its video ID
def get_video_info(youtube, video_id):

    # Request to get details of the video from YouTube API
    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",  # Specify parts to retrieve
        id=video_id  # The unique video ID
    )
    response = request.execute()

    # Initialize an empty dictionary to store video information
    for video in response['items']:
        stats_to_keep = {
            'snippet': ['channelTitle', 'title', 'description', 'tags', 'publishedAt', 'channelId'],
            'statistics': ['viewCount', 'likeCount', 'favoriteCount', 'commentCount'],
            'contentDetails': ['duration', 'definition', 'caption']
        }
        
        video_info = {}
        video_info['video_id'] = video['id']  # Add video ID to the information

        # Extract and store specific details from the video response
        for key in stats_to_keep.keys():
            for value in stats_to_keep[key]:
                try:
                    video_info[value] = video[key][value]  # Get the value if it exists
                except KeyError:
                    video_info[value] = None  # Set to None if the key is not found

    # Return the collected video information
    return video_info
    

# This function retrieves comments for a given video using its video ID
def get_comments_info(youtube, video_id):
    all_comments = []  # List to store all comments

    try:
        # Request to get comments from the YouTube API
        request = youtube.commentThreads().list(
            part="snippet,replies",  # Specify parts to retrieve
            videoId=video_id  # The unique video ID
        )
        response = request.execute()

        # Loop through the comments in the response
        for item in response['items']:
            data = {
                'comment_id': item['snippet']['topLevelComment']['id'],  # Comment ID
                'comment_txt': item['snippet']['topLevelComment']['snippet']['textOriginal'],  # Comment text
                'videoId': item['snippet']['topLevelComment']['snippet']['videoId'],  # Video ID
                'author_name': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],  # Comment author's name
                'published_at': item['snippet']['topLevelComment']['snippet']['publishedAt']  # Comment publish date
            }
            all_comments.append(data)  # Add the comment data to the list

    except:
        # Return an error message if comments could not be retrieved
        return 'Could not get comments for video '  # Comments may be disabled for some videos

    # Return the list of all comments retrieved
    return all_comments




# Integrating all the functions and creating DataFrames for each collection
def channel_Details(channel_name):
    # Get the channel ID from the dictionary using the channel name
    channel_id = d[channel_name]
    
    # Retrieve channel details and create a DataFrame
    det = get_channel_details(youtube, channel_id)
    channel_df = pd.DataFrame([det])  # Create a DataFrame for channel details
    
    # Retrieve all playlists for the channel and create a DataFrame
    playlist = get_playlists_details(youtube, channel_id)
    playlist_df = pd.DataFrame(playlist)  # Create a DataFrame for playlists
    
    # Get the playlist ID for the uploads playlist
    Playlist = det.get('playlistId')
    
    # Retrieve video IDs from the playlist
    videos = get_video_ids(youtube, Playlist)
    
    # Create an empty list to store video information
    video_data = []
    comment_data = []
    
    # For each video ID, retrieve video details and comments, and store them in lists
    for i in videos:
        v = get_video_info(youtube, i)
        video_data.append(v)  # Collect video information for each video
        
        c = get_comments_info(youtube, i)
        if c != 'Could not get comments for video ':
            comment_data.extend(c)  # Collect comments for each video if available
    
    # Create DataFrames for videos and comments
    video_df = pd.DataFrame(video_data)  # Create a DataFrame for videos
    comment_df = pd.DataFrame(comment_data)  # Create a DataFrame for comments
    
    # Return all DataFrames
    print(video_df)
    return "Data collection and storage for the channel is complete.",channel_df,playlist_df,video_df,comment_df





# Creating the channels table in PostgreSQL
def channels_table(channel_df): 

    try:
        # Create the table if it does not exist
        cursor.execute('''CREATE TABLE IF NOT EXISTS channels(
                            channelName VARCHAR(50),
                            channelId VARCHAR(80) PRIMARY KEY,
                            subscribers BIGINT, 
                            views BIGINT,
                            totalVideos INT,
                            playlistId VARCHAR(80),
                            channel_description TEXT
                        )'''
                       )
        eta.commit()
    except Exception as e:
        eta.rollback()
        print(f"Error creating table: {e}")

    # Assuming `channel_df` is the DataFrame containing channel data
    try:
        for _, row in channel_df.iterrows():
            # Define the insert query
            insert_query = '''
                INSERT INTO channels (channelName, channelId, subscribers, views, totalVideos, playlistId, channel_description)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (channelId) DO NOTHING  -- To avoid duplicates
            '''
            # Extract row values for insertion
            values = (
                row['channelName'],
                row['channelId'],
                row['subscribers'],
                row['views'],
                row['totalVideos'],
                row['playlistId'],
                row['channel_description']
            )
            try:
                cursor.execute(insert_query, values)  # Insert the row into the table
                eta.commit()  # Commit the transaction
            except Exception as e:
                eta.rollback()  # Rollback in case of an error
                print(f"Error inserting row {row['channelId']}: {e}")

    except Exception as e:
        print(f"Error processing DataFrame: {e}")


# Creating the playlists table in PostgreSQL
def playlists_table(playlists_df): 
    try:
        # Create the 'playlists' table if it doesn't already exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS playlists (
                PlaylistId VARCHAR(100) PRIMARY KEY,
                Title TEXT,
                ChannelId VARCHAR(80),
                ChannelName VARCHAR(50),
                PublishedAt TIMESTAMP,
                VideoCount INT
            )
        ''')
        eta.commit()  # Commit the transaction to save the changes
    except Exception as e:
        eta.rollback()  # Rollback in case of an error
        print(f"Error creating table: {e}")

    # Assuming `playlists_df` is the DataFrame containing playlist data
    try:
        # Iterate over each row in the DataFrame
        for _, row in playlists_df.iterrows():
            # Define the SQL insert query
            insert_query = '''
                INSERT INTO playlists (
                    PlaylistId, Title, ChannelId, ChannelName, PublishedAt, VideoCount
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (PlaylistId) DO NOTHING  -- Avoid duplicates
            '''
            # Prepare the values for the insert query
            values = (
                row['PlaylistId'],
                row['Title'],
                row['ChannelId'],
                row['ChannelName'],
                row['PublishedAt'],
                row['VideoCount']
            )
            try:
                # Execute the insert query
                cursor.execute(insert_query, values)
                eta.commit()  # Commit the transaction to save the changes
            except Exception as e:
                eta.rollback()  # Rollback in case of an error during insertion
                print(f"Error inserting row {row['PlaylistId']}: {e}")

    except Exception as e:
        print(f"Error processing DataFrame: {e}")


# Creating the videos table in PostgreSQL
def videos_table(videos_df):  
    try:
        # Create the 'videos' table if it doesn't already exist
        cursor.execute('''  
            CREATE TABLE IF NOT EXISTS videos (
                video_id TEXT PRIMARY KEY, 
                channelTitle TEXT, 
                title TEXT, 
                description TEXT, 
                tags TEXT, 
                publishedAt TEXT, 
                viewCount TEXT, 
                likeCount TEXT,
                favoriteCount TEXT, 
                commentCount TEXT, 
                duration TEXT, 
                definition TEXT, 
                caption TEXT, 
                channelId TEXT
            )
        ''')
        eta.commit()  # Commit the transaction to save the changes
    except Exception as e:
        # Rollback in case of an error during table creation
        eta.rollback()
        print(f"Error creating videos table: {e}")

    try:
        # Iterate over each row in the DataFrame
        for _, row in videos_df.iterrows():
            # Define the SQL insert query
            insert_query = '''
                INSERT INTO videos (
                    video_id, channelTitle, title, description, tags, publishedAt, 
                    viewCount, likeCount, favoriteCount, commentCount, duration, 
                    definition, caption, channelId
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (video_id) DO NOTHING  -- Avoid duplicates
            '''
            # Prepare the values for the insert query
            values = (
                row['video_id'],
                row['channelTitle'],
                row['title'],
                row['description'],
                row['tags'],
                row['publishedAt'],
                row['viewCount'],
                row['likeCount'],
                row['favoriteCount'],
                row['commentCount'],
                row['duration'],
                row['definition'],
                row['caption'],
                row['channelId']
            )
            try:
                # Execute the insert query
                cursor.execute(insert_query, values)
                eta.commit()  # Commit the transaction to save the changes
            except Exception as e:
                # Rollback in case of an error during insertion
                eta.rollback()
                print(f"Error inserting video {row['video_id']}: {e}")

    except Exception as e:
        print(f"Error processing videos DataFrame: {e}")


# Creating the comments table in PostgreSQL
def comments_table(comments_df): 
    try:
        # Create the 'comments' table if it doesn't already exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS comments (
                comment_id VARCHAR(100) PRIMARY KEY, 
                comment_txt TEXT, 
                videoId VARCHAR(80), 
                author_name VARCHAR(150), 
                published_at TIMESTAMP
            )
        ''')
        eta.commit()  # Commit the transaction to save the changes
    except Exception as e:
        # Rollback in case of an error during table creation
        eta.rollback()
        print(f"Error creating comments table: {e}")

    # Assuming `comments_df` is the DataFrame containing the comments data
    try:
        # Iterate over each row in the DataFrame
        for _, row in comments_df.iterrows():
            # Define the SQL insert query
            insert_query = '''
                INSERT INTO comments (
                    comment_id, comment_txt, videoId, author_name, published_at
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (comment_id) DO NOTHING  -- Avoid duplicates
            '''
            # Prepare the values for the insert query
            values = (
                row['comment_id'],
                row['comment_txt'],
                row['videoId'],
                row['author_name'],
                row['published_at']
            )
            try:
                # Execute the insert query
                cursor.execute(insert_query, values)
                eta.commit()  # Commit the transaction to save the changes
            except Exception as e:
                # Rollback in case of an error during insertion
                eta.rollback()
                print(f"Error inserting comment {row['comment_id']}: {e}")

    except Exception as e:
        print(f"Error processing comments DataFrame: {e}")


def tables(c,p,v,com):
    print(v)
    # Create and populate the 'channels' table
    channels_table(c)
    
    # Create and populate the 'playlists' table
    playlists_table(p)
    
    # Create and populate the 'videos' table
    videos_table(v)
    
    # Create and populate the 'comments' table
    comments_table(com)
    
    return ("Done")






def display_channels():  # Function to display channel details on the web page
    try:
        # Execute SQL query to fetch all records from the channels table
        cursor.execute("SELECT * FROM channels;")
        tableofchannels = cursor.fetchall()  # Retrieve all records
        tableofchannels = pd.DataFrame(tableofchannels, columns=[desc[0] for desc in cursor.description])  # Convert to DataFrame with column names
        # Display the DataFrame as a table in the web app
        tableofchannels = st.dataframe(tableofchannels)
        return tableofchannels
    except Exception as e:
        # Rollback in case of error and log the exception
        eta.rollback()
        st.error(f"An error occurred: {e}")
        try:
            # Retry fetching and displaying data
            cursor.execute("SELECT * FROM channels;")
            tableofchannels = cursor.fetchall()
            tableofchannels = pd.DataFrame(tableofchannels, columns=[desc[0] for desc in cursor.description])  # Convert to DataFrame with column names
            tableofchannels = st.dataframe(tableofchannels)
            return tableofchannels
        except Exception as e:
            st.error(f"An error occurred on retry: {e}")
            return None


def display_videos():  # Function to display video details on the web page
    try:
        # Execute SQL query to fetch all records from the videos table
        cursor.execute("SELECT * FROM videos;")
        tableofvideos = cursor.fetchall()  # Retrieve all records
        # Convert to DataFrame with column names
        tableofvideos = pd.DataFrame(tableofvideos, columns=[desc[0] for desc in cursor.description])
        # Display the DataFrame as a table in the web app
        tableofvideos = st.dataframe(tableofvideos)
        return tableofvideos
    except Exception as e:
        # Rollback in case of error and log the exception
        eta.rollback()
        st.error(f"An error occurred: {e}")
        try:
            # Retry fetching and displaying data
            cursor.execute("SELECT * FROM videos;")
            tableofvideos = cursor.fetchall()
            tableofvideos = pd.DataFrame(tableofvideos, columns=[desc[0] for desc in cursor.description])  # Convert to DataFrame with column names
            tableofvideos = st.dataframe(tableofvideos)
            return tableofvideos
        except Exception as e:
            st.error(f"An error occurred on retry: {e}")
            return None


def display_playlists():  # Function to display playlist details on the web page
    try:
        # Execute SQL query to fetch all records from the playlists table
        cursor.execute("SELECT * FROM playlists;")
        tableofplaylists = cursor.fetchall()  # Retrieve all records
        # Convert to DataFrame with column names
        tableofplaylists = pd.DataFrame(tableofplaylists, columns=[desc[0] for desc in cursor.description])
        # Display the DataFrame as a table in the web app
        tableofplaylists = st.dataframe(tableofplaylists)
        return tableofplaylists
    except Exception as e:
        # Rollback in case of error and log the exception
        eta.rollback()
        st.error(f"An error occurred: {e}")
        try:
            # Retry fetching and displaying data
            cursor.execute("SELECT * FROM playlists;")
            tableofplaylists = cursor.fetchall()
            tableofplaylists = pd.DataFrame(tableofplaylists, columns=[desc[0] for desc in cursor.description])  # Convert to DataFrame with column names
            tableofplaylists = st.dataframe(tableofplaylists)
            return tableofplaylists
        except Exception as e:
            st.error(f"An error occurred on retry: {e}")
            return None


def display_comments():  # Function to display comment details on the web page
    try:
        # Execute SQL query to fetch all records from the comments table
        cursor.execute("SELECT * FROM comments;")
        tableofcomments = cursor.fetchall()  # Retrieve all records
        # Convert to DataFrame with column names
        tableofcomments = pd.DataFrame(tableofcomments, columns=[desc[0] for desc in cursor.description])
        # Display the DataFrame as a table in the web app
        tableofcomments = st.dataframe(tableofcomments)
        return tableofcomments
    except Exception as e:
        # Rollback in case of error and log the exception
        eta.rollback()
        st.error(f"An error occurred: {e}")
        try:
            # Retry fetching and displaying data
            cursor.execute("SELECT * FROM comments;")
            tableofcomments = cursor.fetchall()
            tableofcomments = pd.DataFrame(tableofcomments, columns=[desc[0] for desc in cursor.description])  # Convert to DataFrame with column names
            tableofcomments = st.dataframe(tableofcomments)
            return tableofcomments
        except Exception as e:
            st.error(f"An error occurred on retry: {e}")
            return None






def one():
    try:
        cursor.execute("SELECT title AS videos, channelTitle AS channel_name FROM videos;")
        eta.commit()
        t1 = cursor.fetchall()
        
        st.write("### Videos and Their Channels Name")

        df = pd.DataFrame(t1, columns=['Video Title', 'Channel Name'])
        st.dataframe(df)
        
        return df
    except Exception as e:
        eta.rollback()
        st.write(f"An error occurred: {e}")
        

def two():
    try:
        cursor.execute("SELECT channelName AS ChannelName, totalVideos AS No_Videos FROM channels ORDER BY totalVideos DESC LIMIT 1;")
        eta.commit()
        t2 = cursor.fetchall()
        
        st.write("### Channel Have Most Number of Videos")

        df = pd.DataFrame(t2, columns=['Channel Name', 'No. of Videos'])
        st.dataframe(df)
        
        return df
    except Exception as e:
        eta.rollback()
        st.write(f"An error occurred: {e}")
        

def three():
    try:
        # Execute the query to fetch data from the database
        cursor.execute('''SELECT viewCount AS views, channelTitle AS "Channel Name", title AS "Video Title"
                          FROM videos
                          WHERE viewCount IS NOT NULL
                          ORDER BY viewCount DESC
                          LIMIT 10;''')
        eta.commit()
        t3 = cursor.fetchall()
        
        # Create a DataFrame with the fetched data
        df = pd.DataFrame(t3, columns=['Views', 'Channel Name', 'Video Title'])
        
        # Ensure 'Views' is treated as a numeric type for sorting
        df['Views'] = pd.to_numeric(df['Views'], errors='coerce')
        
        # Sort the DataFrame in descending order by 'Views'
        df = df.sort_values(by='Views', ascending=False)
        
        # Display the formatted DataFrame in Streamlit
        st.write("### Top 10 Most Viewed Videos and Their Channels")
        st.dataframe(df.style.format({'Views': '{:,.0f}'}), use_container_width=True)
        
        return df
    except Exception as e:
        # Rollback in case of an error and print it
        eta.rollback()
        st.write(f"An error occurred: {e}")

        

def four():
    try:
        # Execute the query to fetch data from the database
        cursor.execute("SELECT commentCount AS No_comments, title AS Name FROM videos WHERE commentCount IS NOT NULL;")
        eta.commit()
        t4 = cursor.fetchall()
        
        # Create a DataFrame with the fetched data
        df = pd.DataFrame(t4, columns=['No_comments', 'Name'])
        
        # Ensure 'No_comments' is treated as a numeric type
        df['No_comments'] = pd.to_numeric(df['No_comments'], errors='coerce')
        
        # Display the formatted DataFrame in Streamlit
        st.write("### Number of Comments for Each Video")
        
        # Use a lambda function to format the 'No_comments' column
        df['No_comments'] = df['No_comments'].apply(lambda x: f'{x:,.0f}')
        st.dataframe(df, use_container_width=True)
        
        return df
    except Exception as e:
        # Rollback in case of an error and print it
        eta.rollback()
        st.write(f"An error occurred: {e}")
        

def five():
    try:
        # Execute the query to fetch data from the database
        cursor.execute('''SELECT title AS Video, channelTitle AS ChannelName, likeCount AS Likes
                          FROM videos 
                          WHERE likeCount IS NOT NULL 
                          ORDER BY likeCount DESC;''')
        eta.commit()
        t5 = cursor.fetchall()
        
        # Create a DataFrame with the fetched data
        df = pd.DataFrame(t5, columns=['Video', 'ChannelName', 'Likes'])
        
        # Ensure 'Likes' is treated as a numeric type
        df['Likes'] = pd.to_numeric(df['Likes'], errors='coerce')
        
        # Sort the DataFrame in descending order by 'Likes'
        df = df.sort_values(by='Likes', ascending=False)
        
        # Display the formatted DataFrame in Streamlit
        st.write("### Videos with Highest Likes")
        
        # Format the 'Likes' column manually
        df['Likes'] = df['Likes'].apply(lambda x: f'{x:,.0f}')
        st.dataframe(df, use_container_width=True)
        
        return df
    except Exception as e:
        # Rollback in case of an error and print it
        eta.rollback()
        st.write(f"An error occurred: {e}")
        

def six():
    try:
        # Execute the query to fetch likes, video names, and channel names from the database
        cursor.execute('''SELECT likeCount AS Likes, title AS VideoName, channelTitle AS ChannelName
                          FROM videos;''')
        eta.commit()
        t6 = cursor.fetchall()
        
        # Create a DataFrame with the fetched data
        df = pd.DataFrame(t6, columns=['Likes', 'Video Name', 'Channel Name'])
        
        # Ensure 'Likes' is treated as a numeric type
        df['Likes'] = pd.to_numeric(df['Likes'], errors='coerce')
        
        # Display the formatted DataFrame in Streamlit
        st.write("### Total Likes for Each Video")
        
        # Format the 'Likes' column manually
        df['Likes'] = df['Likes'].apply(lambda x: f'{x:,.0f}')
        st.dataframe(df, use_container_width=True)
        
        return df
    except Exception as e:
        # Rollback in case of an error and print it
        eta.rollback()
        st.write(f"An error occurred: {e}")
        

def seven():
    try:
        cursor.execute("SELECT channelName AS ChannelName, views AS ChannelViews FROM channels;")
        eta.commit()
        t7 = cursor.fetchall()
        
        df = pd.DataFrame(t7, columns=['Channel Name', 'Channel Views'])
        st.write("### Views of Each Channel")
        st.dataframe(df.style.format({'Channel Views': '{:,}'}), use_container_width=True)
        
        return df
    except Exception as e:
        eta.rollback()
        st.write(f"An error occurred: {e}")
        

def eight():
    try:
        # Execute the query to fetch video names, release dates, and channel names for the year 2022
        cursor.execute('''SELECT title AS VideoName, publishedAt AS VideoRelease, channelTitle AS ChannelName
                          FROM videos
                          WHERE EXTRACT(YEAR FROM publishedAt::date) = 2022;''')
        eta.commit()
        t8 = cursor.fetchall()
        
        if not t8:
            st.write("### No videos were published in the year 2022.")
            return None
        
        # Create a DataFrame with the fetched data
        df = pd.DataFrame(t8, columns=['Video Name', 'Video Release', 'Channel Name'])
        
        # Format the 'Video Release' column as a date
        df['Video Release'] = pd.to_datetime(df['Video Release']).dt.strftime('%Y-%m-%d')
        
        # Display the DataFrame in Streamlit
        st.write("### Videos Published in 2022")
        st.dataframe(df, use_container_width=True)
        
        return df
    except Exception as e:
        # Rollback in case of an error and print it
        eta.rollback()
        st.write(f"An error occurred: {e}")

        



def iso8601_duration_to_seconds(duration):
    """Convert ISO 8601 duration string to total seconds."""
    match = re.match(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$', duration)
    if not match:
        return 0
    hours, minutes, seconds = match.groups(default='0')
    return int(hours) * 3600 + int(minutes) * 60 + int(seconds)

def nine():
    try:
        # Fetch the data from the database
        cursor.execute('''SELECT title AS VideoName, channelTitle AS ChannelName, duration
                          FROM videos;''')
        eta.commit()
        t9 = cursor.fetchall()
        
        if not t9:
            st.write("### No video duration data available.")
            return None
        
        # Convert the fetched data into a DataFrame
        df = pd.DataFrame(t9, columns=['Video Name', 'Channel Name', 'Duration'])
        
        # Convert the ISO 8601 duration strings to total seconds
        df['Duration'] = df['Duration'].apply(iso8601_duration_to_seconds)
        
        # Calculate the average duration per channel
        avg_duration = df.groupby(['Channel Name', 'Video Name'])['Duration'].mean().reset_index()
        avg_duration.columns = ['Channel Name', 'Video Name', 'Average Duration (seconds)']
        
        # Format the 'Average Duration (seconds)' column
        avg_duration['Average Duration (seconds)'] = avg_duration['Average Duration (seconds)'].apply(lambda x: f"{x:,.2f}")
        
        # Display the DataFrame in Streamlit
        st.write("### Average Video Duration for Each Channel")
        st.dataframe(avg_duration, use_container_width=True)
        
        return avg_duration
    except Exception as e:
        # Rollback in case of an error and print it
        eta.rollback()
        st.write(f"An error occurred: {e}")
      

def ten():
    try:
        # Execute the query to fetch video names, channel names, and comment counts
        cursor.execute('''SELECT title AS VideoName, channelTitle AS ChannelName, commentCount AS Comments
                          FROM videos
                          WHERE commentCount IS NOT NULL;''')
        eta.commit()
        t10 = cursor.fetchall()
        
        if not t10:
            st.write("### No videos have comments recorded.")
            return None
        
        # Create a DataFrame with the fetched data
        df = pd.DataFrame(t10, columns=['Video Name', 'Channel Name', 'Comments'])
        
        # Convert 'Comments' to integer for sorting
        df['Comments'] = pd.to_numeric(df['Comments'], errors='coerce')
        
        # Sort the DataFrame by 'Comments' in descending order
        df = df.sort_values(by='Comments', ascending=False)
        
        # Format the 'Comments' column with commas
        df['Comments'] = df['Comments'].apply(lambda x: f"{int(x):,}")
        
        # Display the DataFrame in Streamlit
        st.write("### Videos with Highest Number of Comments")
        st.dataframe(df, use_container_width=True)
        
        return df
    except Exception as e:
        # Rollback in case of an error and print it
        eta.rollback()
        st.write(f"An error occurred: {e}")







st.markdown("<h1 style='text-align: center;'>YouTube Data Harvesting and Warehousing using SQL and Streamlit</h1>", unsafe_allow_html=True)


st.markdown("<h5 style='text-align: left; color: black;'>Select the channel here</h5>", unsafe_allow_html=True)

options = st.multiselect(
    '',
    ['Astronomic', 'Aravind_SA', 'BrainCraft', 'CodeParade', 'Garden_up', 'Jordindian', 'Dr_Riya', 'Lisa_Koshy', 'minutephysics', 'Pentatronix'],
    []
)


if options:
    st.write('You Selected:')
    for option in options:
        st.write(f"- {option}")
else:
    st.markdown(
    "<p style='text-align: right; color: lightcoral; font-size: 12px;'>You have not selected any options</p>",
    unsafe_allow_html=True
)


# Collect and Store Data for Multiple Channels
if st.button("Collect and Store Data"):
    if 'channel_data' not in st.session_state:
        st.session_state['channel_data'] = {}  # Initialize a dictionary to hold data for each channel
    
    for i in options:
        output, c, p, v, com = channel_Details(i)
        st.code(output)
        
        # Store the data for each channel using the channel name as the key
        st.session_state['channel_data'][i] = {
            'c': c,
            'p': p,
            'v': v,
            'com': com
        }

# Display Migrate Button with Header
st.markdown("<h5 style='text-align: left; color: black;'>Click here to Migrate the data into SQL tables</h5>", unsafe_allow_html=True)

# Migrate Data for Selected Channels
if st.button("Migrate"):
    if 'channel_data' in st.session_state and st.session_state['channel_data']:
        for channel_name, data in st.session_state['channel_data'].items():
            # Migrate the data for each channel using the stored session state
            display = tables(data['c'], data['p'], data['v'], data['com'])
            st.write(f"### Migrating Data for Channel: {channel_name}")
            st.code(display)
    else:
        st.error("Please collect and store data first.")

    
    
st.markdown("<h5 style='text-align: left; color: black;'>Select the table you want to view</h5>", unsafe_allow_html=True)

frames = st.radio(
    '',
    ('None','Channel', 'Playlist', 'Video', 'Comment')
)


st.write('You selected:', frames)

if frames=='None':
    st.write("select a table")
elif frames=='Channel':
    display_channels()
elif frames=='Playlist':
    display_playlists()
elif frames=='Video':
    display_videos()
elif frames=='Comment':
    display_comments()

query = st.selectbox(
    'Select an analysis to perform',
    ('None', 'What are the names of all the videos and their corresponding channels?',
     'Which channels have the most number of videos, and how many videos do they have?',
     'What are the top 10 most viewed videos and their respective channels?',
     'How many comments were made on each video, and what are their corresponding video names?',
     'Which videos have the highest number of likes, and what are their corresponding channel names?',
     'What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
     'What is the total number of views for each channel, and what are their corresponding channel names?',
     'What are the names of all the channels that have published videos in the year 2022?',
     'What is the average duration of all videos in each channel, and what are their corresponding channel names?',
     'Which videos have the highest number of comments, and what are their corresponding channel names?'),
    index=0  # Set the default index to 0 (None)
)

# Display a default table if no specific query is selected
if query == 'None':
    st.write("Please select an analysis from the dropdown menu.")
else:
    if query == 'What are the names of all the videos and their corresponding channels?':
        one()
    elif query == 'Which channels have the most number of videos, and how many videos do they have?':
        two()
    elif query == 'What are the top 10 most viewed videos and their respective channels?':
        three()
    elif query == 'How many comments were made on each video, and what are their corresponding video names?':
        four()
    elif query == 'Which videos have the highest number of likes, and what are their corresponding channel names?':
        five()
    elif query == 'What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        six()
    elif query == 'What is the total number of views for each channel, and what are their corresponding channel names?':
        seven()
    elif query == 'What are the names of all the channels that have published videos in the year 2022?':
        eight()
    elif query == 'What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        nine()
    elif query == 'Which videos have the highest number of comments, and what are their corresponding channel names?':
        ten()   