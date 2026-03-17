FROM python:3.11-slim

WORKDIR /app

COPY requirements-ml.txt /app/requirements-ml.txt
RUN python -m pip install --no-cache-dir -r /app/requirements-ml.txt

COPY ml /app/ml
COPY config/features /app/config/features
COPY config/ml-training/train-models.sh /app/bootstrap/train-models.sh

ENV PYTHONUNBUFFERED=1

CMD ["/bin/sh", "/app/bootstrap/train-models.sh"]
