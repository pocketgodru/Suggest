FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ .

# Вместо автоматического запуска, просто поддерживаем контейнер в работе
CMD ["tail", "-f", "/dev/null"] 