"""Load dữ liệu lên Google BigQuery."""
from google.cloud import bigquery
import logging

client = bigquery.Client(location="asia-southeast1")
DATASET = "youtubeAPI"
SCHEMAS = {
    "videos": [
        ("id", "STRING"),
        ("publishedAt", "TIMESTAMP"),
        ("title", "STRING"),
        ("viewCount", "INT64"),
        ("likeCount", "INT64"),
        ("commentCount", "INT64")
    ],
    "comments": [
        ("comment_id", "STRING"),
        ("video_id", "STRING"),
        ("publishedAt", "TIMESTAMP"),
        ("author", "STRING"),
        ("text", "STRING"),
        ("likeCount", "INT64"),
        ("totalReplyCount", "INT64")
    ]
}

def load_rows(table: str, rows: list[dict]) -> None:
    # 1. Kiểm tra dữ liệu đầu vào
    if table not in SCHEMAS:
        raise ValueError("Table không thuộc SCHEMAS")
    if not rows:
        if table == "videos":
            raise ValueError("Không có dữ liệu videos")
        return
    # 2. Tạo table_id và schema
    table_id = f"{client.project}.{DATASET}.{table}"
    schema = [bigquery.SchemaField(name, dtype) for name, dtype in SCHEMAS[table]]
    # 3. Videos: Full Refresh
    if table == "videos":
        config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_json(rows, table_id, job_config=config)
        job.result()
        logging.info("Đã Load %s videos.", job.output_rows)
        return
    # 4. Comments: Loại trùng theo comment_id
    rows = list({row["comment_id"]: row for row in rows}.values())
    # 5. Tạo bảng comments nếu chưa tồn tại
    client.create_table(bigquery.Table(table_id, schema=schema), exists_ok=True)
    # 6. Tạo staging
    staging_id = f"{client.project}.{DATASET}.staging_comments"
    config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
    try:
        # 7. Load rows lên staging
        client.load_table_from_json(rows, staging_id, job_config=config).result()
        # 8. MERGE: UPDATE hoặc INSERT
        sql = f"""
            MERGE `{table_id}` T
            USING `{staging_id}` S
            ON T.comment_id = S.comment_id
            WHEN MATCHED THEN UPDATE SET
                T.video_id = S.video_id,
                T.publishedAt = S.publishedAt,
                T.author = S.author,
                T.text = S.text,
                T.likeCount = S.likeCount,
                T.totalReplyCount = S.totalReplyCount
            WHEN NOT MATCHED THEN INSERT ROW
        """
        # 9. Thực thi MERGE
        job = client.query(sql)
        job.result()
        logging.info("MERGE comments: %s dòng gửi lên, %s dòng bị tác động.", len(rows), job.num_dml_affected_rows)
    finally:
        # 10. Xóa bảng staging
        client.delete_table(staging_id, not_found_ok=True)