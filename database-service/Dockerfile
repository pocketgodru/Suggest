FROM python:3.11-slim

WORKDIR /app

COPY database-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY database-service/app/ .
COPY movie.json .

CMD ["python", "database_service.py"] 