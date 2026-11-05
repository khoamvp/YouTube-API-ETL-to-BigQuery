#!/bin/bash
cd /home/pmk20060311/YouTube-API-ETL-to-BigQuery || exit 1
export GOOGLE_APPLICATION_CREDENTIALS="/home/pmk20060311/youtubeapi.json"
mkdir -p logs
.venv/bin/python main.py >> logs/cron.log 2>&1