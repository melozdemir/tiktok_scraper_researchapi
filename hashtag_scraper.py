import json
import requests
import datetime
import pandas as pd
import numpy as np


def format_time(timestamp):
    return timestamp.strftime("%Y%m%d")


def post_request(client, url, payload):
    try:
        response = client.post(url, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        try:
            error_response = response.json()
        except:
            error_response = response.text
        print(f"Request failed: {e}\nResponse: {error_response}")
        return None


def daterange_chunks(start_date, end_date, max_days=30):
    current_start = start_date
    while current_start <= end_date:
        current_end = min(
            current_start + datetime.timedelta(days=max_days - 1), end_date)
        yield current_start, current_end
        current_start = current_end + datetime.timedelta(days=1)


# === TikTok API Setup ===
client = requests.Session()
client.headers.update({
    'Content-Type': 'application/json',
    'Authorization': 'Bearer '  # << REPLACE THIS
})

base_url = "https://open.tiktokapis.com/v2/research"


def fetch_and_save_by_hashtag(client, hashtag, start_date, end_date):
    video_url = f"{base_url}/video/query/?fields=id,create_time,username,video_description,voice_to_text,region_code,hashtag_names,like_count,comment_count,share_count"
    all_videos = []

    print(f"\n Fetching videos for hashtag: #{hashtag}")

    for chunk_start, chunk_end in daterange_chunks(start_date, end_date, max_days=30):
        formatted_start_date = format_time(chunk_start)
        formatted_end_date = format_time(chunk_end)
        print(
            f" Querying from {formatted_start_date} to {formatted_end_date}")

        cursor = 0
        has_more = True
        search_id = None

        while has_more:
            video_payload = {
                "query": {
                    "and": [
                        {"operation": "EQ", "field_name": "hashtag_name",
                            "field_values": [hashtag]},
                        {"operation": "GTE", "field_name": "create_date",
                            "field_values": [formatted_start_date]},
                        {"operation": "LTE", "field_name": "create_date",
                            "field_values": [formatted_end_date]}
                    ]
                },
                "start_date": formatted_start_date,
                "end_date": formatted_end_date,
                "max_count": 100,
                "cursor": cursor
            }

            if search_id:
                video_payload["search_id"] = search_id

            videos_info = post_request(client, video_url, video_payload)

            if videos_info and 'data' in videos_info:
                videos = videos_info['data'].get('videos', [])
                if videos:
                    all_videos.extend(videos)
                    has_more = videos_info['data'].get('has_more', False)
                    cursor += 100
                    search_id = videos_info['data'].get('search_id', None)
                    print(f" Fetched {len(all_videos)} total so far:")
                else:
                    print(" No more videos returned.")
                    has_more = False
            else:
                print(" Failed to fetch more videos.")
                has_more = False

    if all_videos:
        df = pd.DataFrame(all_videos)

        video_columns = [
            'id', 'create_time', 'username', 'video_description',
            'voice_to_text', 'region_code', 'hashtag_names',
            'like_count', 'comment_count', 'share_count'
        ]

        for col in video_columns:
            if col not in df.columns:
                df[col] = np.nan

        df = df[video_columns]
        csv_filename = f"{hashtag.lower()}_videos.csv"
        df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
        print(f" Saved {len(df)} videos to '{csv_filename}'")

        # Summary stats
        summary = {
            "hashtag": hashtag,
            "total_videos": len(df),
            "total_likes": int(df["like_count"].fillna(0).sum()),
            "total_comments": int(df["comment_count"].fillna(0).sum()),
            "total_shares": int(df["share_count"].fillna(0).sum()),
        }

        return summary

    else:
        print(" No videos found.")
        return {
            "hashtag": hashtag,
            "total_videos": 0,
            "total_likes": 0,
            "total_comments": 0,
            "total_shares": 0,

        }


# === Main Execution ===
if __name__ == "__main__":
    start_date = datetime.date(2023, 10, 7)
    end_date = datetime.date(2023, 11, 11)
    hashtags_to_search = ["freepalestine", "standwithisrael"]

    summaries = []

    for tag in hashtags_to_search:
        summary = fetch_and_save_by_hashtag(client, tag, start_date, end_date)
        summaries.append(summary)

    summary_df = pd.DataFrame(summaries)
    summary_df.to_csv("hashtag_summary.csv", index=False, encoding="utf-8-sig")
    print(f"\n Summary saved to 'hashtag_summary.csv'")
