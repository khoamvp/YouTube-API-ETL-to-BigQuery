# YouTube Data API → BigQuery ETL

Pipeline Python thu thập **thống kê video** và **bình luận công khai** của 5 nghệ sĩ Việt Nam qua YouTube Data API v3; lưu RAW JSON trên Linux và BigQuery, sau đó transform để phân tích. Thiết kế chạy định kỳ trên Google Cloud VM bằng **cron**.

## 1. Tổng quan

```text
Cron → main.py → for artist in ARTISTS (5 nghệ sĩ)
                    |
                    v
          YouTube Data API v3
          ├─ channels.list       → Upload playlist ID
          ├─ playlistItems.list  → Video IDs
          ├─ videos.list         → Thống kê toàn bộ video
          └─ commentThreads.list → Comments của Top 10 video theo views
                    |
                    v
         RAW JSON (local, theo nghệ sĩ)
                    |
                    +──────────────→ BigQuery raw_videos / raw_comments
                    |
                    v
             Transform Python
                    |
                    v
          BigQuery videos / comments
                    |
                    v
         Cập nhật crawl_state.json
```

**Danh sách nghệ sĩ** (cấu hình trong `etl/extract.py`):

| Nghệ sĩ | YouTube handle | BigQuery dataset |
|---|---|---|
| Sơn Tùng M-TP | `@Sontungmtp` | `SONTUNG` |
| Mỹ Tâm | `@mytam` | `MYTAM` |
| Đen Vâu | `@DenVau1305` | `DENVAU` |
| Jack | `@j97imjack` | `JACK` |
| Hoàng Thùy Linh | `@HoangThuyLinh1108` | `HOANGTHUYLINH` |

> Các handle là cấu hình hiện tại của project; cần kiểm tra lại nếu YouTube đổi handle hoặc kênh không truy cập được.

- `TOP_K = 10`: chọn 10 video nhiều lượt xem nhất **mỗi nghệ sĩ** để crawl comments.
- `PAGES_PER_RUN = 10`, `maxResults = 100`: tối đa **1.000 bình luận gốc/video/lần chạy**.
- `MAX_TOTAL_PAGES = 50`: khi đạt 50 trang hoặc hết trang, lần crawl sau bắt đầu lại từ đầu.
- **Chỉ lấy top-level comments**; có `totalReplyCount` nhưng không lấy nội dung replies.
- BigQuery location: `asia-southeast1` (Singapore). **5 datasets × 4 bảng = 20 bảng chính**.

## 2. Các thành phần

| File | Nhiệm vụ |
|---|---|
| `etl/extract.py` | Danh sách nghệ sĩ, gọi API, phân trang và tính checkpoint mới trong bộ nhớ. |
| `etl/storage.py` | Đọc/ghi RAW JSON và checkpoint trên local (`.tmp` + `os.replace()`). |
| `etl/transform.py` | Chuyển dữ liệu JSON thành các cột cho BigQuery. |
| `etl/load.py` | Tạo dataset, load RAW, full refresh videos và MERGE comments. |
| `main.py` | Điều phối các bước ETL trong `main()` cho lần lượt 5 nghệ sĩ. |
| `run_cron.sh` | Script chạy tự động trên VM. |

## 3. Cấu trúc thư mục

```text
youtube-etl/
├── data/
│   ├── SONTUNG/
│   │   ├── crawl_state.json
│   │   └── raw/
│   │       ├── video_stats.json
│   │       └── comments/
│   │           └── <video_id>/
│   │               ├── 2026-11-05_05-00-00.json
│   │               └── 2026-11-06_05-00-00.json
│   ├── MYTAM/
│   ├── DENVAU/
│   ├── JACK/
│   └── HOANGTHUYLINH/
├── etl/
│   ├── __init__.py
│   ├── extract.py
│   ├── storage.py
│   ├── transform.py
│   └── load.py
├── logs/
│   └── cron.log
├── main.py
├── run_cron.sh
├── requirements.txt
├── .env                  # Không commit
├── .gitignore
└── README.md
```

Mỗi nghệ sĩ có **checkpoint và RAW riêng**. Thư mục và file dữ liệu sẽ được tạo khi chạy; `data/`, `logs/`, `.env` và file Service Account **không nên đưa lên GitHub**.

## 4. Tạo YouTube API key và cấu hình

1. Truy cập [Google Cloud Console](https://console.cloud.google.com/), tạo/chọn project.
2. Vào **APIs & Services → Library**, bật **YouTube Data API v3**.
3. Vào **APIs & Services → Credentials → Create credentials → API key**; nên giới hạn key chỉ dùng cho YouTube Data API v3.
4. Tạo `.env` tại thư mục gốc project:

```dotenv
API_KEY=YOUR_YOUTUBE_API_KEY
```

`extract.py` đọc key bằng `python-dotenv` và gửi qua header `x-goog-api-key`. Chỉ đọc dữ liệu công khai nên không cần OAuth của chủ kênh. Để load BigQuery, VM cần quyền qua Service Account (gắn vào VM hoặc trỏ đến file JSON key bằng `GOOGLE_APPLICATION_CREDENTIALS`).

## 5. API và định dạng JSON

Base URL: `https://www.googleapis.com/youtube/v3`

| Endpoint | Mục đích | Trường chính |
|---|---|---|
| `channels.list` | Tìm upload playlist theo `forHandle` | `relatedPlaylists.uploads` |
| `playlistItems.list` | Lấy toàn bộ video ID, 50 ID/trang | `videoId`, `nextPageToken` |
| `videos.list` | Lấy thông tin và thống kê, tối đa 50 ID/request | `id`, `snippet`, `statistics` |
| `commentThreads.list` | Lấy bình luận gốc, tối đa 100/trang, `order=time` | `items`, `nextPageToken` |

**Ví dụ response rút gọn** (dữ liệu minh họa):

```json
{"items": [{"contentDetails": {"relatedPlaylists": {"uploads": "UUexample123"}}}]}
```

```json
{"nextPageToken": "NEXT_TOKEN", "items": [{"contentDetails": {"videoId": "video123"}}]}
```

**RAW video** — `data/<dataset>/raw/video_stats.json` là một **JSON array**, ghi đè mỗi lần chạy:

```json
[
  {
    "id": "video123",
    "snippet": {"title": "Example song", "publishedAt": "2025-01-01T08:00:00Z"},
    "statistics": {"viewCount": "1200000", "likeCount": "25000", "commentCount": "1500"}
  }
]
```

**RAW comments** — `data/<dataset>/raw/comments/<video_id>/<run_id>.json` là một **JSON object**; mỗi video có một file cho mỗi lần chạy:

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

**Checkpoint** — `data/<dataset>/crawl_state.json` lưu phân trang riêng từng video:

```json
{
  "video123": {"page_token": "NEXT_TOKEN", "pages": 10},
  "video456": {"page_token": null, "pages": 0}
}
```

`page_token: null` và `pages: 0` nghĩa là lần tiếp theo bắt đầu từ trang đầu. Token không được lưu chung trong RAW comments. `run_id` là thời điểm chạy theo UTC, ví dụ `2026-11-05_05-00-00`.

## 6. BigQuery: 5 datasets, mỗi dataset 4 bảng

```text
Google Cloud Project
├── SONTUNG/
│   ├── raw_videos
│   ├── raw_comments
│   ├── videos
│   └── comments
├── MYTAM/             (cùng 4 bảng)
├── DENVAU/            (cùng 4 bảng)
├── JACK/              (cùng 4 bảng)
└── HOANGTHUYLINH/     (cùng 4 bảng)
```

| Bảng | Schema | Cách cập nhật |
|---|---|---|
| `raw_videos` | `run_id STRING`, `raw_data JSON` | **WRITE_TRUNCATE**: chỉ giữ snapshot RAW mới nhất. |
| `raw_comments` | `run_id STRING`, `video_id STRING`, `raw_data JSON` | **WRITE_APPEND**: mỗi file RAW comments tương ứng một dòng, lưu lịch sử các lần chạy. |
| `videos` | Các cột statistics đã transform | **WRITE_TRUNCATE**: dữ liệu hiện tại. |
| `comments` | Các cột comment đã transform | **MERGE** theo `comment_id`: cập nhật cũ, thêm mới. |

**Schema `videos`:**

| Cột | Kiểu |
|---|---|
| `id` | STRING |
| `publishedAt` | TIMESTAMP |
| `title` | STRING |
| `viewCount` | INT64 |
| `likeCount` | INT64 |
| `commentCount` | INT64 |

**Schema `comments`:**

| Cột | Kiểu |
|---|---|
| `comment_id` | STRING |
| `video_id` | STRING |
| `publishedAt` | TIMESTAMP |
| `author` | STRING |
| `text` | STRING |
| `likeCount` | INT64 |
| `totalReplyCount` | INT64 |

`load.py` tạo dataset nếu chưa tồn tại, sau đó load các bảng tương ứng. Comments đã transform được khử trùng trong batch → load vào `staging_comments` → `MERGE` → xóa staging. **RAW comments không khử trùng**; một bình luận có thể xuất hiện ở nhiều lần crawl.

## 7. Cài đặt và chạy

```bash
cd /home/khoa/Downloads/project1
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

Cần cấu hình `.env` và xác thực BigQuery trước khi chạy. Kiểm tra kết quả trong `data/<dataset>/` và 4 bảng của mỗi dataset trên BigQuery.

## 8. Chạy tự động bằng cron trên VM

Ví dụ `run_cron.sh` (thay đường dẫn cho đúng máy của bạn):

```bash
#!/bin/bash
cd /home/khoa/Downloads/project1 || exit 1
mkdir -p logs
export GOOGLE_APPLICATION_CREDENTIALS="/home/khoa/Downloads/youtubeapi.json"
/home/khoa/Downloads/project1/.venv/bin/python main.py >> logs/cron.log 2>&1
```

Cấp quyền và chạy thử:

```bash
chmod +x run_cron.sh
./run_cron.sh
```

Mở `crontab -e` và thêm lịch chạy lúc **05:00 mỗi ngày theo timezone của VM**:

```cron
0 5 * * * /home/khoa/Downloads/project1/run_cron.sh
```

## 9. Lưu ý

- Một cron job xử lý **lần lượt 5 nghệ sĩ**; mỗi nghệ sĩ có dataset, RAW và checkpoint riêng.
- Checkpoint được lưu **sau khi các bước BigQuery load thành công** cho nghệ sĩ đó.
- Bình luận có thể bị thiếu hoặc trùng giữa các lần crawl nếu nội dung YouTube thay đổi trong lúc phân trang; bảng `comments` dùng MERGE để giảm trùng theo `comment_id`.
- `raw_comments` dùng APPEND nên chạy lại có thể tạo thêm batch giống nhau; `raw_videos` ghi đè.
- Không commit `.env`, Service Account JSON, `data/`, logs và `.venv/` lên GitHub.

**Tài liệu:** [YouTube Data API](https://developers.google.com/youtube/v3/docs) · [BigQuery JSON](https://cloud.google.com/bigquery/docs/json-data) · [BigQuery Python Client](https://cloud.google.com/python/docs/reference/bigquery/latest)
