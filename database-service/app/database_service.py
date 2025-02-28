from flask import Flask, jsonify, request
from redis_client import RedisMovieClient
from mongo_client import MongoMovieClient
import os
from dotenv import load_dotenv
from flask_cors import CORS
import threading
import time
import datetime

load_dotenv()

app = Flask(__name__)
CORS(app)  # Разрешаем CORS для всех маршрутов

# Инициализация клиентов баз данных
redis_client = RedisMovieClient(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=int(os.getenv("REDIS_DB", 0))
)

mongo_client = MongoMovieClient(
    host=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    db_name=os.getenv("MONGO_DB", "movies_db"),
    collection_name=os.getenv("MONGO_COLLECTION", "movies")
)

def auto_sync_mongodb_to_redis():
    """
    Функция для автоматической синхронизации Redis с MongoDB.
    Запускается в отдельном потоке для проверки и синхронизации данных.
    """
    # Задержка перед первой синхронизацией, чтобы дать время Redis полностью запуститься
    time.sleep(10)  
    
    try:
        # Проверяем, нужна ли синхронизация (если в Redis нет данных)
        redis_movies_count = len(redis_client.get_all_movies() or [])
        
        if redis_movies_count == 0:
            app.logger.info("🔄 Автоматическая синхронизация: Redis пуст, запускаем синхронизацию из MongoDB")
            redis_client.load_from_mongodb(mongo_client)
            app.logger.info("✅ Автоматическая синхронизация завершена")
        else:
            app.logger.info(f"✅ Синхронизация не требуется. В Redis уже есть {redis_movies_count} фильмов")
    except Exception as e:
        app.logger.error(f"❌ Ошибка при автоматической синхронизации: {str(e)}")

@app.route("/health")
def health_check():
    return jsonify({"status": "healthy"})

@app.route("/movies/<int:movie_id>")
def get_movie(movie_id):
    movie = redis_client.get_movie_by_id(movie_id)
    if movie:
        return jsonify(movie)
    return jsonify({"error": "Movie not found"}), 404

@app.route("/movies/search")
def search_movies():
    query = request.args.get("query", "")
    
    # Исправление кодировки для русских букв, если запрос содержит некорректные символы
    if not all(ord(c) < 128 for c in query) and any(c == 'Ð' for c in query):
        try:
            # Пробуем декодировать из latin1 в utf-8 (исправить кодировку)
            query = query.encode('latin1').decode('utf-8')
            app.logger.info(f"Запрос после декодирования: {query}")
        except Exception as e:
            app.logger.error(f"Ошибка декодирования запроса: {str(e)}")
    
    genre = request.args.get("genre")
    year = request.args.get("year")
    movie_type = request.args.get("type")
    country = request.args.get("country")
    category = request.args.get("category")
    
    results = redis_client.search_movies(
        query=query,
        genre=genre,
        year=year,
        movie_type=movie_type,
        country=country,
        category=category
    )
    
    return jsonify(results)

# Добавляем альтернативный маршрут для совместимости с web-service
@app.route("/search_movies")
def search_movies_alternative():
    """
    Альтернативный маршрут для поиска фильмов.
    Использует ту же логику, что и /movies/search
    """
    return search_movies()

@app.route("/genres")
def get_genres():
    genres = redis_client.get_genres()
    return jsonify(genres)

@app.route("/countries")
def get_countries():
    countries = redis_client.get_countries()
    return jsonify(countries)

@app.route("/categories")
def get_categories():
    categories = redis_client.get_categories()
    return jsonify(categories)

@app.route("/create_index", methods=["POST"])
def create_index():
    """
    Создает индекс RediSearch для полнотекстового поиска.
    Этот эндпоинт вызывается web-service для инициализации индекса при запуске.
    """
    try:
        # Вызываем метод _ensure_search_index из redis_client для создания индекса
        redis_client._ensure_search_index()
        return jsonify({"status": "success", "message": "Индекс RediSearch успешно создан"})
    except Exception as e:
        app.logger.error(f"❌ Ошибка при создании индекса RediSearch: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/sync/mongodb-to-redis", methods=["POST"])
def sync_mongodb_to_redis():
    try:
        result = redis_client.load_from_mongodb(mongo_client)
        return jsonify({"status": "success", "message": f"Синхронизировано {result} фильмов"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# Новые маршруты для системы лайков и рекомендаций

@app.route("/like_movie", methods=["POST"])
def like_movie():
    """Поставить лайк фильму"""
    data = request.get_json()
    if not data or "user_id" not in data or "movie_id" not in data:
        return jsonify({"status": "error", "message": "Неверные параметры запроса"}), 400
    
    user_id = data["user_id"]
    movie_id = data["movie_id"]
    
    try:
        # Сохраняем лайк в Redis
        result = redis_client.like_movie(user_id, movie_id)
        
        # Сохраняем лайк в MongoDB
        try:
            # Преобразуем movie_id к целому числу, если возможно
            movie_id_int = int(movie_id) if str(movie_id).isdigit() else movie_id
            
            # Проверяем, существует ли уже такой лайк
            existing_like = mongo_client.db["likes"].find_one({
                "user_id": user_id, 
                "movie_id": movie_id_int
            })
            
            if not existing_like:
                # Сохраняем лайк в MongoDB
                mongo_client.db["likes"].insert_one({
                    "user_id": user_id,
                    "movie_id": movie_id_int,
                    "timestamp": datetime.datetime.utcnow()
                })
                app.logger.info(f"Лайк сохранен в MongoDB: user_id={user_id}, movie_id={movie_id_int}")
        except Exception as mongo_err:
            app.logger.error(f"Ошибка при сохранении лайка в MongoDB: {str(mongo_err)}")
        
        return jsonify({"status": "success", "liked": True})
    except Exception as e:
        app.logger.error(f"Ошибка при добавлении лайка: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/unlike_movie", methods=["POST"])
def unlike_movie():
    """Убрать лайк с фильма"""
    data = request.get_json()
    if not data or "user_id" not in data or "movie_id" not in data:
        return jsonify({"status": "error", "message": "Неверные параметры запроса"}), 400
    
    user_id = data["user_id"]
    movie_id = data["movie_id"]
    
    try:
        # Удаляем лайк из Redis
        result = redis_client.unlike_movie(user_id, movie_id)
        
        # Удаляем лайк из MongoDB
        try:
            # Преобразуем movie_id к целому числу, если возможно
            movie_id_int = int(movie_id) if str(movie_id).isdigit() else movie_id
            
            # Удаляем лайк из MongoDB
            mongo_client.db["likes"].delete_one({
                "user_id": user_id, 
                "movie_id": movie_id_int
            })
            app.logger.info(f"Лайк удален из MongoDB: user_id={user_id}, movie_id={movie_id_int}")
        except Exception as mongo_err:
            app.logger.error(f"Ошибка при удалении лайка из MongoDB: {str(mongo_err)}")
        
        return jsonify({"status": "success", "liked": False})
    except Exception as e:
        app.logger.error(f"Ошибка при удалении лайка: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/remove_all_likes", methods=["POST"])
def remove_all_likes():
    """Удалить все лайки пользователя"""
    data = request.get_json()
    if not data or "user_id" not in data:
        return jsonify({"status": "error", "message": "Неверные параметры запроса"}), 400
    
    user_id = data["user_id"]
    
    try:
        # Удаляем лайки из Redis
        result = redis_client.remove_all_likes(user_id)
        
        # Удаляем лайки из MongoDB
        try:
            delete_result = mongo_client.db["likes"].delete_many({"user_id": user_id})
            app.logger.info(f"Удалено {delete_result.deleted_count} лайков из MongoDB для пользователя {user_id}")
        except Exception as mongo_err:
            app.logger.error(f"Ошибка при удалении лайков из MongoDB: {str(mongo_err)}")
        
        return jsonify({"status": "success", "message": "Все лайки удалены"})
    except Exception as e:
        app.logger.error(f"Ошибка при удалении всех лайков: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/get_liked_movies/<user_id>")
def get_liked_movies(user_id):
    """Получить список лайкнутых фильмов пользователя"""
    try:
        limit = request.args.get("limit", 100, type=int)
        movies = redis_client.get_user_liked_movies(user_id)
        if movies:
            # Ограничиваем количество возвращаемых фильмов
            return jsonify({
                "status": "success", 
                "movies": movies[:limit] if len(movies) > limit else movies,
                "total": len(movies)
            })
        else:
            return jsonify({"status": "success", "movies": [], "total": 0})
    except Exception as e:
        app.logger.error(f"Ошибка при получении лайкнутых фильмов: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/is_movie_liked", methods=["POST"])
def is_movie_liked():
    """Проверить, лайкнул ли пользователь фильм (POST метод)"""
    data = request.get_json()
    if not data or "user_id" not in data or "movie_id" not in data:
        return jsonify({"status": "error", "message": "Неверные параметры запроса"}), 400
    
    user_id = data["user_id"]
    movie_id = data["movie_id"]
    
    try:
        result = redis_client.is_movie_liked(user_id, movie_id)
        return jsonify({"liked": result})
    except Exception as e:
        app.logger.error(f"Ошибка при проверке лайка: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/is_movie_liked/<user_id>/<movie_id>", methods=["GET"])
def is_movie_liked_get(user_id, movie_id):
    """Проверить, лайкнул ли пользователь фильм (GET метод)"""
    try:
        result = redis_client.is_movie_liked(user_id, movie_id)
        return jsonify({"liked": result})
    except Exception as e:
        app.logger.error(f"Ошибка при проверке лайка: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/get_recommendations/<user_id>")
def get_recommendations(user_id):
    """
    Получение рекомендаций фильмов для пользователя на основе его лайков.
    """
    try:
        # Получаем лайкнутые фильмы пользователя
        liked_movies = mongo_client.db["likes"].find({"user_id": user_id})
        liked_movie_ids = []
        
        # Преобразуем ID фильмов к нужному формату
        for like in liked_movies:
            movie_id = like["movie_id"]
            # Добавляем ID в список
            liked_movie_ids.append(movie_id)
        
        app.logger.info(f"Получены лайки пользователя {user_id}: {liked_movie_ids}")
        
        if not liked_movie_ids:
            app.logger.warning(f"Пользователь {user_id} не имеет лайков")
            return jsonify({"movies": [], "total": 0})
        
        # Получаем рекомендации на основе лайкнутых фильмов
        recommended_movies = redis_client.get_recommendations(liked_movie_ids, limit=10)
        app.logger.info(f"Получены рекомендации для пользователя {user_id}: {len(recommended_movies)} фильмов")
        
        return jsonify({"movies": recommended_movies, "total": len(recommended_movies)})
    except Exception as e:
        app.logger.error(f"Ошибка при получении рекомендаций: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/get_user_rating/<user_id>/<movie_id>")
def get_user_rating(user_id, movie_id):
    """
    Получение рейтинга пользователя для конкретного фильма.
    """
    try:
        # Ищем рейтинг в MongoDB
        rating = mongo_client.db["ratings"].find_one(
            {"user_id": user_id, "movie_id": int(movie_id)}
        )
        
        if rating:
            return jsonify({"rating": rating["rating"], "status": "success"})
        else:
            return jsonify({"rating": 0, "status": "not_rated"})
    except Exception as e:
        app.logger.error(f"Ошибка при получении рейтинга: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/save_rating", methods=["POST"])
def save_rating():
    """
    Сохранение рейтинга пользователя для фильма.
    """
    try:
        data = request.get_json()
        user_id = data.get("user_id")
        movie_id = data.get("movie_id")
        rating_value = data.get("rating")
        
        if not user_id or not movie_id or not rating_value:
            return jsonify({"status": "error", "message": "Отсутствуют обязательные параметры"}), 400
        
        # Преобразуем movie_id к целому числу
        try:
            movie_id = int(movie_id)
        except ValueError:
            return jsonify({"status": "error", "message": "Неверный формат movie_id"}), 400
        
        # Сохраняем или обновляем рейтинг в MongoDB
        result = mongo_client.db["ratings"].update_one(
            {"user_id": user_id, "movie_id": movie_id},
            {"$set": {"rating": int(rating_value)}},
            upsert=True
        )
        
        return jsonify({"status": "success", "message": "Рейтинг сохранен"})
    except Exception as e:
        app.logger.error(f"Ошибка при сохранении рейтинга: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/get_comments/<movie_id>")
def get_comments(movie_id):
    """
    Получение комментариев для конкретного фильма.
    """
    try:
        # Преобразуем movie_id к целому числу
        try:
            movie_id = int(movie_id)
        except ValueError:
            return jsonify({"status": "error", "message": "Неверный формат movie_id"}), 400
        
        # Ищем комментарии в MongoDB
        comments = list(mongo_client.db["comments"].find(
            {"movie_id": movie_id},
            {"_id": 0}  # Исключаем поле _id из результата
        ).sort("created_at", -1))  # Сортируем по дате создания в обратном порядке
        
        return jsonify({"comments": comments, "total": len(comments)})
    except Exception as e:
        app.logger.error(f"Ошибка при получении комментариев: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/save_comment", methods=["POST"])
def save_comment():
    """
    Сохранение комментария пользователя для фильма.
    """
    try:
        data = request.get_json()
        user_id = data.get("user_id")
        movie_id = data.get("movie_id")
        comment_text = data.get("comment")
        
        if not user_id or not movie_id or not comment_text:
            return jsonify({"status": "error", "message": "Отсутствуют обязательные параметры"}), 400
        
        # Преобразуем movie_id к целому числу
        try:
            movie_id = int(movie_id)
        except ValueError:
            return jsonify({"status": "error", "message": "Неверный формат movie_id"}), 400
        
        # Получаем текущее время в UTC
        current_time = datetime.datetime.utcnow()
        
        # Сохраняем комментарий в MongoDB
        comment = {
            "user_id": user_id,
            "movie_id": movie_id,
            "text": comment_text,
            "created_at": current_time
        }
        
        result = mongo_client.db["comments"].insert_one(comment)
        
        # Удаляем _id из ответа и преобразуем datetime в строку
        comment["_id"] = str(result.inserted_id)
        comment["created_at"] = comment["created_at"].isoformat()
        
        return jsonify({"status": "success", "comment": comment})
    except Exception as e:
        app.logger.error(f"Ошибка при сохранении комментария: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

# Получение популярных фильмов
@app.route("/get_popular_movies")
def get_popular_movies():
    """
    Возвращает список популярных фильмов.
    """
    try:
        # Получаем limit из параметров запроса, по умолчанию 10
        limit = request.args.get("limit", 10, type=int)
        
        # Получаем популярные фильмы через Redis
        popular_movies = redis_client.get_popular_movies(limit)
        
        app.logger.info(f"Возвращаем {len(popular_movies)} популярных фильмов")
        return jsonify(popular_movies)
    except Exception as e:
        app.logger.error(f"Ошибка при получении популярных фильмов: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    # Запускаем автоматическую синхронизацию в отдельном потоке
    sync_thread = threading.Thread(target=auto_sync_mongodb_to_redis)
    sync_thread.daemon = True  # Поток будет автоматически завершен при выходе из основного потока
    sync_thread.start()
    
    app.run(host="0.0.0.0", port=5001, debug=True) 