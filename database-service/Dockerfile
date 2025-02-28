FROM python:3.10-slim

WORKDIR /app

# Оптимизация для контейнера
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Установка зависимостей для работы с Redis и MongoDB
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    wget \
    && rm -rf /var/lib/apt/lists/*

COPY database-service/requirements.txt .

# Устанавливаем зависимости, используя предкомпилированные бинарники где возможно
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir pymongo==4.6.1 redis==5.0.1 Flask==3.0.2 Flask-Cors==4.0.0 python-dotenv==1.0.1 ijson

# Минимизируем размер контейнера
RUN apt-get purge -y --auto-remove build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY database-service/app/ .
COPY movie.json .

# Создаем каталог для хранения данных
RUN mkdir -p /data/db

# Используем непривилегированного пользователя
RUN useradd -m appuser && \
    chown -R appuser:appuser /app /data

USER appuser

# Делаем init_db.py исполняемым
RUN chmod +x init_db.py

# Порт для API
EXPOSE 5001

# Запуск скрипта инициализации и основного сервиса
CMD ["sh", "-c", "python init_db.py && python database_service.py"] 