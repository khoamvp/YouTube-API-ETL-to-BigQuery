"""Chạy quy trình ETL YouTube Data API → BigQuery."""
import logging
from datetime import datetime, timezone
from etl.extract import get_playlistId, get_video_ids, get_video_stats, crawl_comments
from etl.storage import load_json, save_json, save_raw
from etl.transform import transform_videos, transform_comments
from etl.load import load_rows

TOP_K = 10
STATE_FILE = "data/crawl_state.json"
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    # 1. Lấy danh sách video
    playlist_id = get_playlistId()
    video_ids = get_video_ids(playlist_id)
    logging.info("Tổng số video: %s", len(video_ids))
    # 2. Lấy statistics và lưu RAW
    video_stats = get_video_stats(video_ids)
    save_raw("data/raw/video_stats.json", video_stats)
    # 3. Transform và Load bảng videos
    videos = transform_videos(video_stats)
    load_rows("videos", videos)
    # 4. Chọn Top K video có lượt xem cao nhất
    top_videos = sorted(video_stats, key=lambda x: int(x.get("statistics", {}).get("viewCount", 0)), reverse=True)[:TOP_K]
    logging.info("Đã chọn Top %s video.", len(top_videos))
    # 5. Thu thập comments theo từng video
    state = load_json(STATE_FILE)
    raw, new_state = crawl_comments(top_videos, state)
    # 6. Lưu RAW và Transform comments
    all_comments = []
    for vid, threads in raw.items():
        path = f"data/raw/comments/{vid}/{run_id}.json"
        save_raw(path, {"video_id": vid, "threads": threads})
        all_comments.extend(transform_comments(threads))
    # 7. MERGE comments lên BigQuery một lần
    if all_comments:
        load_rows("comments", all_comments)
    # 8. Lưu tiến độ sau khi Load thành công
    save_json(STATE_FILE, new_state)
    logging.info("Đã xử lý %s comments. Hoàn thành ETL.", len(all_comments))

if __name__ == "__main__":
    main()