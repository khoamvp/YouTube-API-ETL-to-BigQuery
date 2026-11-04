"""Transform dữ liệu YouTube thành các bảng BigQuery."""

def to_int(value):
    return int(value) if value is not None else None

# 1. Transform video statistics
def transform_videos(raw_videos: list[dict]) -> list[dict]:
    videos = []

    for item in raw_videos:
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})

        videos.append({
            "id": item["id"],
            "publishedAt": snippet.get("publishedAt"),
            "title": snippet.get("title"),
            "viewCount": to_int(stats.get("viewCount", 0)),
            "likeCount": to_int(stats.get("likeCount", 0)),
            "commentCount": to_int(stats.get("commentCount", 0))
        })

    return videos


# 2. Transform comment threads
def transform_comments(raw_threads: list[dict]) -> list[dict]:
    comments = []

    for item in raw_threads:
        snippet = item.get("snippet", {})
        top = snippet.get("topLevelComment", {})
        comment = top.get("snippet", {})

        comment_id = top.get("id")

        if not comment_id:
            continue

        comments.append({
            "comment_id": comment_id,
            "video_id": snippet.get("videoId"),
            "publishedAt": comment.get("publishedAt"),
            "author": comment.get("authorDisplayName"),
            "text": comment.get("textDisplay"),
            "likeCount": int(comment.get("likeCount", 0)),
            "totalReplyCount": int(snippet.get("totalReplyCount", 0))
        })

    return comments