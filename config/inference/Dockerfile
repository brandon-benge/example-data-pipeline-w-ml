FROM python:3.11-slim

WORKDIR /app

COPY requirements-ml.txt /app/requirements-ml.txt
RUN python -m pip install --no-cache-dir -r /app/requirements-ml.txt

COPY ml /app/ml
COPY config/features /app/config/features

ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "ml.inference_api:app", "--host", "0.0.0.0", "--port", "8010"]
