"""Lấy dữ liệu từ YouTube Data API v3."""

import os
import requests
from dotenv import load_dotenv
import logging

load_dotenv()

API_KEY = os.getenv("API_KEY")
BASE_URL = "https://www.googleapis.com/youtube/v3"

PAGES_PER_RUN = 10    
MAX_TOTAL_PAGES = 50  
ARTISTS = [
    {"handle": "@Sontungmtp", "dataset": "SONTUNG"},
    {"handle": "@mytam", "dataset": "MYTAM"},
    {"handle": "@DenVau1305", "dataset": "DENVAU"},
    {"handle": "@j97imjack", "dataset": "JACK"},
    {"handle": "@HoangThuyLinh1108", "dataset": "HOANGTHUYLINH"}
]

class QuotaExceeded(Exception):
    pass

class CommentsDisabled(Exception):
    pass

# 1. Gọi YouTube API
def call_api(endpoint: str, params: dict) -> dict:
    response = requests.get(
        f"{BASE_URL}/{endpoint}",
        params=params,
        headers={"x-goog-api-key": API_KEY},
        timeout=30
    )
    if response.status_code == 403:
        error = response.json().get("error", {})
        reasons = [e.get("reason") for e in error.get("errors", [])]
        if "quotaExceeded" in reasons or "dailyLimitExceeded" in reasons:
            raise QuotaExceeded("YouTube API đã hết quota.")
        if "commentsDisabled" in reasons:
            raise CommentsDisabled("Video đã tắt bình luận.")
    response.raise_for_status()
    return response.json()



# 2. Lấy Upload Playlist ID của kênh
def get_playlistId(channel_handle: str) -> str:
    data = call_api("channels", {
        "part": "contentDetails",
        "forHandle": channel_handle,
        "fields": "items(contentDetails(relatedPlaylists(uploads)))"
    })
    if not data.get("items"):
        raise ValueError(f"Không tìm thấy kênh: {channel_handle}")
    return data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]


# 3. Lấy toàn bộ Video IDs trong Upload Playlist
def get_video_ids(playlist_id: str) -> list[str]:
    video_ids = []
    page_token = None

    while True:
        params = {
            "part": "contentDetails",
            "playlistId": playlist_id,
            "maxResults": 50,
            "fields": "nextPageToken,items(contentDetails(videoId))"
        }

        if page_token:
            params["pageToken"] = page_token

        data = call_api("playlistItems", params)

        video_ids.extend(
            item["contentDetails"]["videoId"]
            for item in data.get("items", [])
            if item.get("contentDetails", {}).get("videoId")
        )

        page_token = data.get("nextPageToken")

        if not page_token:
            break

    return list(dict.fromkeys(video_ids))


# 4. Lấy statistics của các video, tối đa 50 video/request
def get_video_stats(video_ids: list[str]) -> list[dict]:
    videos = []

    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]

        data = call_api("videos", {
            "part": "snippet,statistics",
            "id": ",".join(batch),
            "fields": "items(id,snippet(title,publishedAt),statistics(viewCount,likeCount,commentCount))"
        })

        videos.extend(data.get("items", []))

    return videos


# 5. Lấy tối đa N trang bình luận, hỗ trợ tiếp tục từ page_token
def get_comment_threads(video_id: str, max_pages: int = 10, page_token=None):
    threads = []
    pages_fetched = 0

    for _ in range(max_pages):
        params = {
            "part": "snippet",
            "videoId": video_id,
            "maxResults": 100,
            "order": "time",
            "textFormat": "plainText",
            "fields": (
                "nextPageToken,"
                "items(id,snippet(videoId,totalReplyCount,"
                "topLevelComment(id,snippet("
                "publishedAt,authorDisplayName,textDisplay,likeCount))))"
            )
        }

        if page_token:
            params["pageToken"] = page_token

        data = call_api("commentThreads", params)

        threads.extend(data.get("items", []))
        pages_fetched += 1
        page_token = data.get("nextPageToken")

        if not page_token:
            break

    return threads, page_token, pages_fetched




# 6. Lấy một đợt comment cho từng video, nhớ tiến độ bằng page_token
def crawl_comments(videos: list[dict], state: dict) -> tuple[dict[str, list], dict]:
    """Trả về ({video_id: threads}, state mới)."""
    new_state, raw = dict(state), {}

    for video in videos:
        vid = video["id"]
        cur = state.get(vid, {"page_token": None, "pages": 0})
            # Nếu đã đủ 50 trang thì bắt đầu lại từ trang đầu
        if cur["pages"] >= MAX_TOTAL_PAGES:
            cur = {"page_token": None, "pages": 0}
        try:
            threads, token, fetched = get_comment_threads(
                vid,
                max_pages=min(PAGES_PER_RUN, MAX_TOTAL_PAGES - cur["pages"]),
                page_token=cur["page_token"],
            )
        except QuotaExceeded:
            logging.warning("Hết quota YouTube API.")
            break
        except CommentsDisabled:
            logging.warning("Video %s đã tắt bình luận.", vid)
            continue
        except requests.RequestException as e:
            status = e.response.status_code if e.response is not None else "Unknown"
            detail = e.response.text[:500] if e.response is not None else str(e)
            logging.error("Lỗi video %s: HTTP %s - %s", vid, status, detail)
            if status == 400:
                new_state[vid] = {"page_token": None, "pages": 0}
            continue

        raw[vid] = threads
        pages = cur["pages"] + fetched
        if token is None or pages >= MAX_TOTAL_PAGES:
            new_state[vid] = {"page_token": None, "pages": 0}  # hết comment hoặc đủ 50 trang
        else:
            new_state[vid] = {"page_token": token, "pages": pages}
            logging.info("Video %s: %s trang, tiến độ %s/%s", vid, fetched, pages, MAX_TOTAL_PAGES)
    return raw, new_state

