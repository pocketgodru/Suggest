FROM python:3.10-slim

WORKDIR /app

# Оптимизация для контейнера
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Устанавливаем необходимые системные пакеты
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Копируем requirements.txt
COPY web-service/requirements.txt ./requirements.txt

# Устанавливаем зависимости
# Для тяжелых пакетов используем предкомпилированные бинарники где возможно
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --only-binary=:all: numpy==1.26.4 scikit-learn==1.3.2 2>/dev/null || true && \
    pip install --no-cache-dir -r requirements.txt

# Минимизируем размер контейнера
RUN apt-get purge -y --auto-remove build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Создаем директории для шаблонов и статических файлов
RUN mkdir -p /app/templates /app/static

# Копируем файлы приложения
COPY web-service/app/ ./

# Используем непривилегированного пользователя
RUN useradd -m appuser
USER appuser

# Устанавливаем порт
EXPOSE 5000

CMD ["python", "web_service.py"] 