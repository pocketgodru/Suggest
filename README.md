# Микросервисная архитектура поиска фильмов

Проект разделен на три основных микросервиса:

## 1. Веб-интерфейс (web-service)

Сервис отвечает за:
- Принятие пользовательских запросов и отправку их в поисковый сервис
- Отображение результатов поиска
- Рендеринг HTML-страниц
- Обработку API-запросов от клиента

Основные эндпоинты:
- `/` - Главная страница
- `/dml` - Веб-интерфейс поиска
- `/search_movies` - API-поиск фильмов
- `/get_genres` - Получение списка жанров
- `/get_countries` - Получение списка стран
- `/get_categories` - Получение списка категорий
- `/get_movie/<movie_id>` - Получение данных фильма

## 2. Поисковой сервис (search-service)

Сервис отвечает за:
- Обработку пользовательского запроса
- Генерацию эмбеддингов и поиск по векторным представлениям
- Работу с FAISS для быстрого поиска
- Кэширование результатов поиска

Основные эндпоинты:
- `/search` - Поиск фильмов
- `/update_index` - Обновление поискового индекса

## 3. Сервис базы данных (database-service)

Сервис отвечает за:
- Хранение и кэширование фильмов в MongoDB и Redis
- Предоставление REST API для взаимодействия с базами данных
- Обновление данных

Основные эндпоинты:
- `/movies/<movie_id>` - Получение фильма по ID
- `/genres` - Получение списка жанров
- `/countries` - Получение списка стран
- `/categories` - Получение списка категорий
- `/sync/mongodb-to-redis` - Синхронизация данных между MongoDB и Redis

## Запуск проекта

1. Установите Docker и Docker Compose

2. Клонируйте репозиторий:
```bash
git clone <repository_url>
cd <repository_name>
```

3. Создайте файл .env в корневой директории:
```
MONGO_URI=mongodb://mongodb:27017
MONGO_DB=movies_db
MONGO_COLLECTION=movies
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_DB=0
SEARCH_SERVICE_URL=http://search:5002
DATABASE_SERVICE_URL=http://database:5001
```

4. Запустите сервисы:
```bash
docker-compose up -d
```

5. Загрузите данные в MongoDB:
```bash
# Подключитесь к контейнеру database-service
docker-compose exec database python -c "from mongo_client import MongoMovieClient; client = MongoMovieClient(); client.clear_and_load_movies('movie.json')"
```

6. Синхронизируйте данные с Redis:
```bash
curl -X POST http://localhost:5001/sync/mongodb-to-redis
```

7. Откройте веб-интерфейс:
```
http://localhost:5000
```

## Архитектура

```
+----------------+     +-----------------+     +------------------+
|  Web Service   |     | Search Service  |     | Database Service |
|  (Flask)       |     | (Flask + FAISS) |     | (Flask)         |
+----------------+     +-----------------+     +------------------+
        |                     |                        |
        |                     |                        |
        |                     v                        v
        |              +------------+           +------------+
        |              |  MongoDB   |           |   Redis    |
        |              +------------+           +------------+
        |                     ^                        ^
        |                     |                        |
        +---------------------+------------------------+
```

## Технологии

- **Web Service**: Flask, HTML, CSS, JavaScript
- **Search Service**: Flask, FAISS, SentenceTransformer
- **Database Service**: Flask, MongoDB, Redis
- **Базы данных**: MongoDB, Redis
- **Контейнеризация**: Docker, Docker Compose