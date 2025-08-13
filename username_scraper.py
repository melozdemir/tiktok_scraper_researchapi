import json
import requests
import datetime
import pandas as pd
import numpy as np


def format_time(timestamp):
    # Formats a date into "YYYYMMDD" format required by the API
    return timestamp.strftime("%Y%m%d")


def post_request(client, url, payload):
    # Sends a POST request to the given URL with the provided payload
    try:
        response = client.post(url, json=payload)
        response.raise_for_status()  # Checks if the request was successful
        # print(f"Response from {url}: {response.json()}")  # Uncomment for debugging if needed
        print(f"Response from {url}:")
        # Prints the formatted JSON response
        print(json.dumps(response.json(), indent=4))
        return response.json()
    except requests.RequestException as e:
        # Error handling if the request fails
        print(f"Request failed: {e}, {response.text}")
        return None


# Initialize a session for all subsequent requests
client = requests.Session()
client.headers.update({
    'Content-Type': 'application/json',
    # Replace with actual access token
    'Authorization': 'Bearer clt.2.4PlFsFB0NMunpmG8jonnO5dGf2Y3uPUwarRC3C4CdU3_kpTUUezLIPQyk6PNY2TW7vuwX3J1TmeKK8YhWf1LBA*2'
})

# Base URL for the API
base_url = "https://open.tiktokapis.com/v2/research"


def fetch_user_info(client, username):
    # Retrieves user information for a specific username
    user_url = f"{base_url}/user/info/?fields=display_name,bio_description,avatar_url,is_verified,follower_count,following_count,likes_count,video_count"
    payload_user = {
        "username": username
    }
    response_user = client.post(user_url, json=payload_user)
    if response_user.status_code == 200:
        user_info = response_user.json()
        # Checks if the response contains the expected data
        if 'data' in user_info and user_info['data']:
            user_data = user_info['data']
            # Adds the username to the dictionary
            user_data['username'] = username
            # Creates a DataFrame from the user data
            user_df = pd.DataFrame([user_data])
            # Defines the column order with 'username' as the first column
            columns_order = ['username', 'display_name', 'bio_description', 'avatar_url', 'is_verified', 'following_count',
                             'follower_count', 'video_count', 'likes_count']
            # Reorders the DataFrame columns accordingly
            user_df = user_df[columns_order]
            # Saves user data to a CSV file
            user_df.to_csv('user_info.csv', index=False)
            print("User information fetched and saved to CSV.")
        else:
            print("User data not found in response.")
    else:
        print(
            f"Failed to fetch user info: {response_user.status_code}, {response_user.text}")


def fetch_videos_and_comments(client, username, start_date, end_date):
    # Formats the date values for the API request
    formatted_start_date = format_time(start_date)
    formatted_end_date = format_time(end_date)

    # Initializes the video query URL and variables for pagination
    video_url = f"{base_url}/video/query/?fields=id,create_time,username,region_code,video_description,music_id,like_count,comment_count,share_count,view_count,effect_ids,hashtag_names,playlist_id,voice_to_text,is_stem_verified,video_duration"
    cursor = 0
    has_more = True
    all_videos = []

    # Loop to fetch all videos as long as has_more is True
    while has_more:
        video_payload = {
            "query": {
                "and": [{"field_name": "username", "operation": "EQ", "field_values": [username]}]
            },
            "start_date": formatted_start_date,
            "end_date": formatted_end_date,
            "max_count": 100,
            "cursor": cursor,
        }
        videos_info = post_request(client, video_url, video_payload)
        if videos_info and 'data' in videos_info and 'videos' in videos_info['data']:
            # Adds the retrieved videos to the list
            videos = videos_info['data']['videos']
            all_videos.extend(videos)
            # Checks if more pages exist and updates the cursor
            has_more = videos_info['data'].get('has_more', False)
            cursor += 100  # Increase cursor to fetch next set of data
        else:
            print("No videos found or error in fetching videos.")
            if videos_info:
                print("Response payload:", videos_info)
            break

    # Create a DataFrame with all videos
    if all_videos:
        videos_df = pd.DataFrame(all_videos)
        # Define the desired column order for the DataFrame
        video_columns_order = ['username', 'id', 'create_time', 'region_code', 'video_description', 'music_id', 'like_count', 'comment_count',
                               'share_count', 'view_count', 'effect_ids', 'hashtag_names', 'playlist_id', 'voice_to_text', 'is_stem_verified', 'video_duration']

        # Add missing columns with NaN if necessary
        for col in video_columns_order:
            if col not in videos_df.columns:
                videos_df[col] = np.nan

        # Reorder DataFrame columns and save to CSV
        videos_df = videos_df[video_columns_order]
        videos_df.to_csv('videos_info.csv', index=False)
        print("Video information fetched and saved to CSV.")
    else:
        print("No videos found.")

    # Fetch comments using a cursor loop for each video
    all_comments_df = pd.DataFrame()

    for video in all_videos:
        video_id = video['id']
        cursor = 0
        has_more = True

        # Loop to fetch comments for each video
        while has_more:
            comments_url = f"{base_url}/video/comment/list/?fields=id,text,video_id,parent_comment_id,like_count,reply_count,create_time"
            comments_payload = {
                "video_id": video_id,
                "max_count": 100,
                "cursor": cursor
            }
            comments_info = post_request(
                client, comments_url, comments_payload)
            if comments_info and 'data' in comments_info and 'comments' in comments_info['data']:
                comments = comments_info['data']['comments']
                if comments:
                    comments_df = pd.DataFrame(comments)
                    # Filters out deleted comments without text
                    comments_df = comments_df[comments_df['text'].notna() & (
                        comments_df['text'] != "")]

                    # Define column order for comments
                    comments_columns_order = [
                        'id', 'video_id', 'parent_comment_id', 'text', 'like_count', 'reply_count', 'create_time']
                    comments_df = comments_df[comments_columns_order]

                    # Append comments to the total DataFrame
                    all_comments_df = pd.concat(
                        [all_comments_df, comments_df], ignore_index=True)
                # Update cursor and check for more pages
                has_more = comments_info['data'].get('has_more', False)
                cursor += 100
            else:
                has_more = False  # End loop if no more comments

    # Save all comments to a CSV file if available
    if not all_comments_df.empty:
        all_comments_df.to_csv(
            'all_comments.csv', index=False, encoding='utf-8-sig')
        print("All comments fetched and saved to CSV.")
    else:
        print("No comments found for any video.")


# Example call of the functions
username = "kamalaharris" #change the username 
# fetch_user_info(client, username)
# Specify start date in the format (year, month, day), e.g. the date 01.02.2024 is (2024, 2, 1)
start_date = datetime.date(2024, 11, 1)
# Specify end date in the format (year, month, day), e.g. the date 31.02.2024 is (2024, 2, 31)
end_date = datetime.date(2024, 11, 10)
fetch_videos_and_comments(client, username, start_date, end_date)
