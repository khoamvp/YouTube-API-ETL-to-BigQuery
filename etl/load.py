"""Load dữ liệu lên Google BigQuery."""
from google.cloud import bigquery
import logging
import json
RAW_SCHEMAS = {
    "raw_videos": [("run_id", "STRING"), ("raw_data", "JSON")],
    "raw_comments": [("run_id", "STRING"), ("video_id", "STRING"), ("raw_data", "JSON")]
}

client = bigquery.Client(location="asia-southeast1")


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


# 1. Tạo dataset nếu chưa tồn tại
def ensure_dataset(dataset: str):
    dataset_id = f"{client.project}.{dataset}"
    obj = bigquery.Dataset(dataset_id)
    obj.location = "asia-southeast1"
    client.create_dataset(obj, exists_ok=True)

# 2. Load RAW JSON lên BigQuery
def load_raw(table: str, rows: list[dict], dataset: str):
    if not rows:
        return
    table_id = f"{client.project}.{dataset}.{table}"
    schema = [bigquery.SchemaField(name, dtype) for name, dtype in RAW_SCHEMAS[table]]
    mode = "WRITE_TRUNCATE" if table == "raw_videos" else "WRITE_APPEND"
    config = bigquery.LoadJobConfig(schema=schema, write_disposition=mode)
    client.load_table_from_json(rows, table_id, job_config=config).result()
    logging.info("Đã Load %s.%s", dataset, table)

def load_rows(table: str, rows: list[dict], dataset: str) -> None:
    if table not in SCHEMAS:
        raise ValueError("Table không thuộc SCHEMAS")
    if not rows:
        if table == "videos":
            raise ValueError("Không có dữ liệu videos")
        return
    # Tạo table_id và schema
    table_id = f"{client.project}.{dataset}.{table}"
    schema = [bigquery.SchemaField(name, dtype) for name, dtype in SCHEMAS[table]]
    # Videos: Full Refresh
    if table == "videos":
        config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_json(rows, table_id, job_config=config)
        job.result()
        logging.info("Đã Load %s videos.", job.output_rows)
        return
    # Comments: Loại trùng theo comment_id
    rows = list({row["comment_id"]: row for row in rows}.values())
    # Tạo bảng comments nếu chưa tồn tại
    client.create_table(bigquery.Table(table_id, schema=schema), exists_ok=True)
    # Tạo staging
    staging_id = f"{client.project}.{dataset}.staging_comments"
    config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
    try:
        # Load rows lên staging
        client.load_table_from_json(rows, staging_id, job_config=config).result()
        # MERGE: UPDATE hoặc INSERT
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
        # MERGE
        job = client.query(sql)
        job.result()
        logging.info("MERGE comments: %s dòng gửi lên, %s dòng bị tác động.", len(rows), job.num_dml_affected_rows)
    finally:
        # Xóa staging
        client.delete_table(staging_id, not_found_ok=True)


