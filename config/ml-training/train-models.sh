#!/bin/sh
set -eu

export PYTHONPATH=/app

cd /app
python3 /app/ml/train.py --feature-group customer_realtime
python3 /app/ml/train.py --feature-group campaign
python3 /app/ml/train.py --feature-group advertiser
