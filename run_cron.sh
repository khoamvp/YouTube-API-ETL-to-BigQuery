#!/bin/bash

cd /home/khoa/Downloads/project1 || exit 1

export GOOGLE_APPLICATION_CREDENTIALS="/home/khoa/Downloads/youtubeapi.json"

/home/khoa/Downloads/project1/.venv/bin/python main.py >> logs/cron.log 2>&1