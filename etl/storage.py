"""Đọc và lưu dữ liệu RAW JSON."""

import json
import os


# 1. Đọc file JSON
def load_json(path: str) -> dict:
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}


# 2. Ghi JSON an toàn bằng file tạm
def save_json(path: str, data) -> None:
    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)
    temp_path = path + ".tmp"
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        
    os.replace(temp_path, path)


# 3. Lưu RAW JSON
def save_raw(path: str, data) -> str:
    save_json(path, data)
    return path