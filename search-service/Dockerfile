FROM python:3.11-slim

WORKDIR /app

# Установка системных зависимостей для faiss-cpu
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY search-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY search-service/app/ .
COPY movies_embeddings.npy .

CMD ["python", "search_service.py"] 