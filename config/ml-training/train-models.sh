#!/bin/sh
set -eu

export PYTHONPATH=/app

cd /app
python3 /app/ml/train.py --feature-group customer_realtime --skip-empty
python3 /app/ml/train.py --feature-group campaign --skip-empty
python3 /app/ml/train.py --feature-group advertiser --skip-empty
