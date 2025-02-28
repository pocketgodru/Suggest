FROM python:3.10-slim

WORKDIR /app

# Оптимизация для контейнера
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TRANSFORMERS_OFFLINE=1 \
    CUDA_VISIBLE_DEVICES="" \
    TORCH_DEVICE="cpu" \
    OMP_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 \
    SKLEARN_ALLOW_DEPRECATED_SKLEARN_PACKAGE_INSTALL=True

# Устанавливаем минимальные зависимости для сборки
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Копируем requirements.txt
COPY search-service/requirements.txt .

# Устанавливаем пакеты, используя предкомпилированные бинарники где возможно
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --only-binary=:all: numpy==1.26.4 && \
    pip install --no-cache-dir --only-binary=:all: scikit-learn==1.3.2 && \
    pip install --no-cache-dir torch==2.2.0 --index-url https://download.pytorch.org/whl/cpu && \
    pip install --no-cache-dir sentence-transformers==2.5.1 && \
    pip install --no-cache-dir faiss-cpu==1.7.4 && \
    pip install --no-cache-dir Flask==3.0.2 Flask-Cors==4.0.0 && \
    pip install --no-cache-dir python-dotenv==1.0.1 && \
    pip install --no-cache-dir pymongo==4.6.1 redis==5.0.1 && \
    pip install --no-cache-dir transformers==4.37.2

# Копируем файлы приложения
COPY search-service/app/ .
COPY movies_embeddings.npy .

# Минимизируем размер контейнера
RUN apt-get purge -y --auto-remove build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Используем непривилегированного пользователя
RUN useradd -m appuser
USER appuser

# Открываем порт для API
EXPOSE 5002

CMD ["python", "search_service.py"] 