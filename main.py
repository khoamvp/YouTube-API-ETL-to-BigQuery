import logging
from datetime import datetime, timezone
from etl.extract import ARTISTS, get_playlistId, get_video_ids, get_video_stats, crawl_comments
from etl.storage import load_json, save_json, save_raw
from etl.transform import transform_videos, transform_comments
from etl.load import ensure_dataset, load_raw, load_rows

TOP_K = 5
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    # 1. Duyệt lần lượt 10 nghệ sĩ
    for artist in ARTISTS:
        handle = artist["handle"]
        dataset = artist["dataset"]
        run_id = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
        logging.info("Đang xử lý: %s", dataset)
        # 2. EXTRACT - Lấy videos
        playlist_id = get_playlistId(handle)
        video_ids = get_video_ids(playlist_id)
        video_stats = get_video_stats(video_ids)
        # 3. EXTRACT - Lấy comments Top 10 videos
        top_videos = sorted(video_stats, key=lambda x: int(x.get("statistics", {}).get("viewCount", 0)), reverse=True)[:TOP_K]
        state_file = f"data/{dataset}/crawl_state.json"
        state = load_json(state_file)
        raw_comments, new_state = crawl_comments(top_videos, state)
        # 4. STORAGE - Lưu RAW JSON local
        save_raw(f"data/{dataset}/raw/video_stats.json", video_stats)
        raw_rows = []
        for vid, threads in raw_comments.items():
            raw_data = {"video_id": vid, "threads": threads}
            save_raw(f"data/{dataset}/raw/comments/{vid}/{run_id}.json", raw_data)
            raw_rows.append({"run_id": run_id, "video_id": vid, "raw_data": raw_data})        # 5. TRANSFORM - Chuyển dữ liệu thành bảng
        videos = transform_videos(video_stats)
        comments = []
        for threads in raw_comments.values():
            comments.extend(transform_comments(threads))
        # 6. LOAD - Đưa dữ liệu lên BigQuery
        ensure_dataset(dataset)
        load_raw("raw_videos", [{"run_id": run_id, "raw_data": video_stats}], dataset)        
        load_raw("raw_comments", raw_rows, dataset)
        load_rows("videos", videos, dataset)
        if comments:
            load_rows("comments", comments, dataset)
        # 7. STORAGE - Lưu checkpoint sau khi Load thành công
        save_json(state_file, new_state)
        logging.info("Hoàn thành %s: %s comments", dataset, len(comments))
    logging.info("Hoàn thành ETL.")

if __name__ == "__main__":
    main()