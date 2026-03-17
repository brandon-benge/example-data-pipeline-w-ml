#!/bin/sh
set -eu

python3 /app/ml/train.py --feature-group customer_realtime
python3 /app/ml/train.py --feature-group campaign
python3 /app/ml/train.py --feature-group advertiser
