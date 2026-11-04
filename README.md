# YouTube Data API → BigQuery ETL

Pipeline Python thu thập thống kê video và bình luận công khai từ kênh YouTube, lưu dữ liệu gốc dạng JSON, chuyển đổi và nạp vào **Google BigQuery**. Thiết kế để chạy định kỳ trên Google Cloud VM bằng **cron**.

## 1. Tổng quan

```text
YouTube Data API v3
  ├─ channels.list       → Upload playlist ID
  ├─ playlistItems.list  → Danh sách video ID
  ├─ videos.list         → Statistics của tất cả video
  └─ commentThreads.list → Bình luận của Top 10 video (theo views)
            ↓
      RAW JSON (local)
            ↓
       Transform Python
            ↓
       Google BigQuery
       ├─ videos   (WRITE_TRUNCATE)
       └─ comments (MERGE theo comment_id)
            ↑
   crawl_state.json (checkpoint)
```

- **Kênh hiện tại:** `@Sontungmtp` (đặt trong `extract.py`).
- **Top K:** `10` video có lượt xem cao nhất (`main.py`).
- **Video:** lấy tất cả video trong upload playlist và cập nhật lại bảng ở mỗi lần chạy.
- **Comments:** lấy tối đa `10` trang/video/lần chạy, `100` bình luận/trang; sau `50` trang tích lũy thì bắt đầu lại từ trang đầu. Dữ liệu được MERGE để tránh trùng `comment_id`.
- **BigQuery:** dataset `YoutubeAPI`, location `asia-southeast1` (Singapore).

## 2. Cấu trúc source code

| File | Vai trò |
|---|---|
| `etl/extract.py` | Gọi YouTube API, phân trang, lấy statistics/comments, quản lý checkpoint trong bộ nhớ. |
| `etl/storage.py` | Đọc/ghi JSON và ghi file an toàn qua `.tmp` + `os.replace()`. |
| `etl/transform.py` | Chuẩn hóa dữ liệu thành các cột phục vụ BigQuery. |
| `etl/load.py` | Load bảng `videos` và MERGE bảng `comments`. |
| `etl/main.py` | Điều phối toàn bộ quy trình ETL. |

## Project Structure

```text
youtube-etl/
├── data/
│   ├── crawl_state.json             # Lưu tiến độ crawl comments
│   └── raw/
│       ├── video_stats.json         # RAW statistics của tất cả video
│       └── comments/
│           ├── 30KI5SuECuc/
│           │   ├── 20261104T142823518918Z.json
│           │   └── 20261104T143649064749Z.json
│           ├── 32sYGCOYJUM/
│           │   └── ...
│           └── ...                  # Mỗi video có một thư mục riêng
│
├── etl/
│   ├── __init__.py
│   ├── extract.py                    # Gọi YouTube Data API v3
│   ├── storage.py                    # Đọc/ghi RAW JSON
│   ├── transform.py                  # Làm sạch và chuyển đổi dữ liệu
│   └── load.py                       # Load và MERGE vào BigQuery
│
├── logs/
│   └── cron.log                      # Log chạy tự động
│
├── main.py                           # Điều phối ETL pipeline
├── run_cron.sh                       # Script chạy cron
├── requirements.txt                  # Python dependencies
└── README.md                         # Tài liệu project
```

## 3. Lấy YouTube API key

1. Mở [Google Cloud Console](https://console.cloud.google.com/), tạo hoặc chọn **Project**.
2. Vào **APIs & Services → Library**, tìm **YouTube Data API v3** và chọn **Enable**.
3. Vào **APIs & Services → Credentials → Create credentials → API key**.
4. Giới hạn key cho **YouTube Data API v3** nếu phù hợp; lưu key trong file `.env` ở thư mục chạy project:

```dotenv
API_KEY=YOUR_YOUTUBE_API_KEY
```

`extract.py` đọc key qua `python-dotenv` và gửi tới API qua HTTP header `x-goog-api-key`. Vì project chỉ đọc dữ liệu **công khai**, không cần OAuth đăng nhập tài khoản kênh.


## 4. API sử dụng và JSON nhận được

Base URL: `https://www.googleapis.com/youtube/v3`

| Endpoint | Tham số chính | Dữ liệu lấy |
|---|---|---|
| `channels` | `part=contentDetails`, `forHandle=@Sontungmtp` | `relatedPlaylists.uploads` |
| `playlistItems` | `part=contentDetails`, `playlistId`, `maxResults=50` | `videoId`, `nextPageToken` |
| `videos` | `part=snippet,statistics`, tối đa 50 ID/request | `id`, `title`, `publishedAt`, `viewCount`, `likeCount`, `commentCount` |
| `commentThreads` | `part=snippet`, `videoId`, `maxResults=100`, `order=time` | Top-level comments, `totalReplyCount`, `nextPageToken` |

Các ví dụ JSON dưới đây **được rút gọn và dùng dữ liệu minh họa**, đúng cấu trúc field mà code yêu cầu.

**Ví dụ `channels` → upload playlist ID:**

```json
{"items": [{"contentDetails": {"relatedPlaylists": {"uploads": "UUexample123"}}}]}
```

**Ví dụ `playlistItems` → video IDs:**

```json
{"nextPageToken": "NEXT_TOKEN", "items": [{"contentDetails": {"videoId": "video123"}}]}
```

**Ví dụ `videos` → thống kê:**

```json
[
  {
    "id": "video123",
    "snippet": {"title": "Example song", "publishedAt": "2025-01-01T08:00:00Z"},
    "statistics": {"viewCount": "1200000", "likeCount": "25000", "commentCount": "1500"}
  }
]
```

**Ví dụ `commentThreads` → bình luận gốc:**

```json
{
  "nextPageToken": "NEXT_TOKEN",
  "items": [
    {
      "id": "thread123",
      "snippet": {
        "videoId": "video123",
        "totalReplyCount": 2,
        "topLevelComment": {
          "id": "comment123",
          "snippet": {
            "publishedAt": "2025-01-02T09:00:00Z",
            "authorDisplayName": "Viewer A",
            "textDisplay": "Great song!",
            "likeCount": 10
          }
        }
      }
    }
  ]
}
```

**Lưu ý:** JSON trên là response minh họa của API. Khi ghi RAW, code lưu `items` (danh sách threads), **không lưu** `nextPageToken` cùng file bình luận; token được quản lý riêng trong state.

## 5. Cấu trúc file dữ liệu

```text
data/
├── crawl_state.json
└── raw/
    ├── video_stats.json
    └── comments/
        └── <video_id>/
            └── <run_id>.json
```

- `video_stats.json`: **JSON array** các video, ghi đè mỗi lần chạy.
- `<video_id>/<run_id>.json`: **JSON object** `{ "video_id": ..., "threads": [...] }`; mỗi lượt chạy tạo file riêng (`run_id` theo UTC), không ghi đè lịch sử bình luận.
- `crawl_state.json`: **JSON object**, key là video ID, lưu `page_token` và số trang `pages`.

**Ví dụ file RAW comments:**

```json
{
  "video_id": "video123",
  "threads": [
    {
      "id": "thread123",
      "snippet": {
        "videoId": "video123",
        "totalReplyCount": 2,
        "topLevelComment": {
          "id": "comment123",
          "snippet": {
            "publishedAt": "2025-01-02T09:00:00Z",
            "authorDisplayName": "Viewer A",
            "textDisplay": "Great song!",
            "likeCount": 10
          }
        }
      }
    }
  ]
}
```

**Ví dụ `crawl_state.json`:**

```json
{
  "video123": {"page_token": "NEXT_TOKEN", "pages": 10},
  "video456": {"page_token": null, "pages": 0}
}
```

`page_token: null, pages: 0` nghĩa là lượt tiếp theo sẽ bắt đầu từ trang đầu; trạng thái này xuất hiện khi đã hết trang hoặc đã đạt ngưỡng 50 trang.

## 6. Transform và bảng BigQuery

Các trường số lấy từ YouTube API thường có dạng chuỗi (`"1200000"`). `transform.py` chuyển sang số nguyên trước khi load.

**Bảng `YoutubeAPI.videos`** — Full Refresh (`WRITE_TRUNCATE`):

| Column | BigQuery type | Nội dung |
|---|---|---|
| `id` | STRING | Video ID |
| `publishedAt` | TIMESTAMP | Thời điểm đăng video |
| `title` | STRING | Tên video |
| `viewCount` | INT64 | Lượt xem |
| `likeCount` | INT64 | Lượt thích |
| `commentCount` | INT64 | Tổng số bình luận do YouTube thống kê |

**Bảng `YoutubeAPI.comments`** — MERGE theo `comment_id`:

| Column | BigQuery type | Nội dung |
|---|---|---|
| `comment_id` | STRING | ID bình luận gốc |
| `video_id` | STRING | Video chứa bình luận |
| `publishedAt` | TIMESTAMP | Thời điểm đăng bình luận |
| `author` | STRING | Tên người bình luận |
| `text` | STRING | Nội dung bình luận |
| `likeCount` | INT64 | Lượt thích bình luận |
| `totalReplyCount` | INT64 | Số phản hồi được báo cáo |

**Ví dụ record sau transform:**

```json
{"id": "video123", "publishedAt": "2025-01-01T08:00:00Z", "title": "Example song", "viewCount": 1200000, "likeCount": 25000, "commentCount": 1500}
```

```json
{"comment_id": "comment123", "video_id": "video123", "publishedAt": "2025-01-02T09:00:00Z", "author": "Viewer A", "text": "Great song!", "likeCount": 10, "totalReplyCount": 2}
```

Quy trình load comments: **loại trùng trong batch → staging_comments → MERGE UPDATE/INSERT → xóa staging**. Dataset BigQuery cần được tạo trước.



## 7. Chạy tự động bằng cron (Google Cloud VM)

```bash
#!/bin/bash

cd /home/khoa/Downloads/project1 || exit 1

export GOOGLE_APPLICATION_CREDENTIALS="/home/khoa/Downloads/youtubeapi.json"

/home/khoa/Downloads/project1/.venv/bin/python main.py >> logs/cron.log 2>&1
```


Ví dụ chạy **05:00 mỗi ngày theo timezone của VM** và ghi log:

```cron
0 5 * * * /home/ubuntu/youtube-etl/run_cron.sh >> /home/ubuntu/youtube-etl/cron.log 2>&1
```
