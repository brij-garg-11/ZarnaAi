import os
import requests
import json
import csv
from youtube_transcript_api import YouTubeTranscriptApi as YTApi


API_KEY = "AIzaSyASvhhRlQWODFz35C3r1sSScQZTAME1uz8"
CHANNEL_ID = "UC5Gb9pWYSfcpEdTb6vf9Dbg"

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"


def youtube_get(endpoint: str, params: dict):
    if not API_KEY:
        raise ValueError("Missing YOUTUBE_API_KEY environment variable.")

    params["key"] = API_KEY
    response = requests.get(f"{YOUTUBE_API_BASE}/{endpoint}", params=params, timeout=30)
    response.raise_for_status()
    return response.json()

def get_uploads_playlist_id(channel_id: str):
    data = youtube_get(
        "channels",
        {
            "part": "contentDetails,snippet",
            "id": channel_id,
        },
    )

    items = data.get("items", [])
    if not items:
        print("No channel found for that ID.")
        return None

    channel = items[0]
    title = channel["snippet"]["title"]
    uploads_playlist_id = channel["contentDetails"]["relatedPlaylists"]["uploads"]

    print("Channel title:", title)
    print("Uploads playlist ID:", uploads_playlist_id)
    return uploads_playlist_id

def get_all_video_ids(playlist_id: str):
    video_ids = []
    next_page_token = None

    while True:
        params = {
            "part": "contentDetails",
            "playlistId": playlist_id,
            "maxResults": 50,
        }

        if next_page_token:
            params["pageToken"] = next_page_token

        data = youtube_get("playlistItems", params)

        for item in data.get("items", []):
            video_id = item["contentDetails"]["videoId"]
            video_ids.append(video_id)

        next_page_token = data.get("nextPageToken")

        if not next_page_token:
            break

    print(f"Total videos found: {len(video_ids)}")
    return video_ids

def get_transcript_for_video(video_id: str):
    try:
        transcript = YTApi().fetch(video_id)

        print(f"Transcript found for {video_id}")
        print("First 3 lines:")
        for chunk in transcript[:3]:
            print(chunk)

        return transcript

    except Exception as e:
        print(f"No transcript found for {video_id}")
        print("Error:", e)
        return None

def clean_transcript_text(transcript):
    lines = []

    for chunk in transcript:
        text = chunk.text.strip()

        if text:
            lines.append(text)

    full_text = " ".join(lines)

    return full_text

def save_transcript_json(transcript, video_id: str):
    output_path = f"Transcripts/youtube/{video_id}_transcript.json"

    cleaned_text = clean_transcript_text(transcript)

    data = {
        "video_id": video_id,
        "full_text": cleaned_text,
        "length": len(cleaned_text)
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Saved transcript to {output_path}")

def fetch_and_save_all_transcripts(videos):
    results = []

    for i, video in enumerate(videos, start=1):
        video_id = video["video_id"]
        title = video["title"]
        published_at = video["published_at"]

        transcript = get_transcript_for_video(video_id)

        transcript_exists = transcript is not None
        transcript_path = ""

        if transcript_exists:
            save_transcript_json(transcript, video_id)
            transcript_path = f"Transcripts/youtube/{video_id}_transcript.json"

        results.append(
            {
                "video_id": video_id,
                "title": title,
                "published_at": published_at,
                "transcript_exists": transcript_exists,
                "transcript_path": transcript_path,
            }
        )

        print(f"Transcript progress: {i}/{len(videos)}")

    return results

def save_transcript_index_csv(rows, output_path):
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "video_id",
                "title",
                "published_at",
                "transcript_exists",
                "transcript_path",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)
        

def get_video_details(video_ids):
    all_videos = []

    # YouTube API allows max 50 per request
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i+50]

        data = youtube_get(
            "videos",
            {
                "part": "snippet,contentDetails,statistics",
                "id": ",".join(chunk),
            },
        )

        for item in data.get("items", []):
            snippet = item["snippet"]

            video_data = {
                "video_id": item["id"],
                "title": snippet.get("title"),
                "description": snippet.get("description"),
                "published_at": snippet.get("publishedAt"),
            }

            all_videos.append(video_data)

        print(f"Processed {i + len(chunk)} videos")

    return all_videos

def save_video_metadata_csv(videos, output_path):
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["video_id", "title", "description", "published_at"]
        )
        writer.writeheader()
        writer.writerows(videos)

if __name__ == "__main__":
    print("Using channel ID:", CHANNEL_ID)

    playlist_id = get_uploads_playlist_id(CHANNEL_ID)
    video_ids = get_all_video_ids(playlist_id)

    videos = get_video_details(video_ids)

    with open("processed/youtube/video_metadata.json", "w", encoding="utf-8") as f:
        json.dump(videos, f, ensure_ascii=False, indent=2)

    save_video_metadata_csv(videos, "processed/youtube/video_metadata.csv")

    transcript_rows = fetch_and_save_all_transcripts(videos)
    save_transcript_index_csv(
        transcript_rows,
        "processed/youtube/transcript_index.csv"
    )

    print("Finished saving transcript index.")