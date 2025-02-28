from flask import Flask, jsonify, request
from redis_client import RedisMovieClient
from mongo_client import MongoMovieClient
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

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

@app.route("/genres")
def get_genres():
    genres = redis_client.get_all_genres()
    return jsonify(genres)

@app.route("/countries")
def get_countries():
    countries = redis_client.get_all_countries()
    return jsonify(countries)

@app.route("/categories")
def get_categories():
    categories = redis_client.get_all_categories()
    return jsonify(categories)

@app.route("/sync/mongodb-to-redis", methods=["POST"])
def sync_mongodb_to_redis():
    try:
        print("🔄 Начинаем синхронизацию MongoDB → Redis...")
        # Проверяем подключение к MongoDB
        mongo_count = mongo_client.collection.count_documents({})
        print(f"📊 Количество документов в MongoDB: {mongo_count}")
        
        # Проверяем подключение к Redis
        redis_count = len(redis_client.redis_client.keys("movie:*"))
        print(f"📊 Количество фильмов в Redis до синхронизации: {redis_count}")
        
        # Выполняем синхронизацию
        success = redis_client.load_from_mongodb(mongo_client)
        
        if success:
            new_redis_count = len(redis_client.redis_client.keys("movie:*"))
            print(f"✅ Синхронизация завершена успешно!")
            print(f"📊 Количество фильмов в Redis после синхронизации: {new_redis_count}")
            return jsonify({"status": "success", "movies_count": new_redis_count})
        else:
            print("❌ Ошибка при синхронизации")
            return jsonify({"status": "error", "message": "Failed to sync data"}), 500
    except Exception as e:
        error_msg = f"❌ Ошибка синхронизации: {str(e)}"
        print(error_msg)
        return jsonify({"status": "error", "message": error_msg}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001) 