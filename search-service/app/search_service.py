from flask import Flask, jsonify, request
from turbo_search import TurboMovieSearch
import os
from dotenv import load_dotenv
import requests

load_dotenv()

app = Flask(__name__)

# Инициализация поисковой системы
search_engine = TurboMovieSearch(
    mongo_host=os.getenv("MONGO_URI", "mongodb://mongodb:27017"),
    mongo_db=os.getenv("MONGO_DB", "movies_db"),
    mongo_collection=os.getenv("MONGO_COLLECTION", "movies")
)

@app.route("/health")
def health_check():
    return jsonify({"status": "healthy"})

@app.route("/search")
def search():
    query = request.args.get("query", "")
    year = request.args.get("year")
    genre = request.args.get("genre")
    top_k = int(request.args.get("top_k", 10))
    search_mode = request.args.get("search_mode", "semantic")
    
    try:
        # Получаем URL сервиса базы данных
        db_service_url = os.getenv("DATABASE_SERVICE_URL", "http://database:5001")
        print(f"🔍 Поисковый запрос: {query} (режим: {search_mode})")
        
        if search_mode == "redis":
            # Поиск по названию через Redis
            params = {
                "query": query,
                "year": year if year else "",
                "genre": genre if genre else "",
                "type": request.args.get("type", ""),
                "country": request.args.get("country", ""),
                "category": request.args.get("category", "")
            }
            
            print(f"📨 Отправка запроса к Redis: {params}")
            try:
                response = requests.get(f"{db_service_url}/movies/search", params=params, timeout=10)
                print(f"📥 Ответ от Redis: {response.status_code}")
                
                if response.status_code == 200:
                    results = response.json()
                    print(f"✅ Найдено результатов: {len(results)}")
                    return jsonify(results)
                else:
                    print(f"❌ Ошибка при поиске через Redis: {response.status_code}")
                    print(f"Ответ: {response.text}")
                    return jsonify([])
            except requests.exceptions.RequestException as e:
                print(f"❌ Ошибка при обращении к Redis: {str(e)}")
                return jsonify([])
        else:
            # Семантический поиск через FAISS
            try:
                results = search_engine.search(
                    query=query,
                    top_k=top_k,
                    year_filter=year,
                    genre_filter=genre
                )
                
                # Получаем полные данные о фильмах
                movies = []
                for result in results:
                    movie_id = result.get("id")
                    if movie_id:
                        try:
                            response = requests.get(f"{db_service_url}/movies/{movie_id}")
                            if response.status_code == 200:
                                movie_data = response.json()
                                movie_data["relevance_score"] = result.get("relevance_score", 0)
                                movies.append(movie_data)
                        except requests.exceptions.RequestException as e:
                            print(f"❌ Ошибка при получении данных фильма {movie_id}: {str(e)}")
                            continue
                    else:
                        # Если ID нет, используем данные из результата поиска
                        movies.append(result)
                
                return jsonify(movies)
            except Exception as e:
                print(f"❌ Ошибка при семантическом поиске: {str(e)}")
                return jsonify([])
            
    except Exception as e:
        print(f"❌ Ошибка при поиске: {str(e)}")
        return jsonify([])

@app.route("/movie/<movie_id>")
def get_movie(movie_id):
    """Получение информации о фильме"""
    try:
        db_service_url = os.getenv("DATABASE_SERVICE_URL", "http://database:5001")
        print(f"🎬 Запрос информации о фильме: {movie_id}")
        
        response = requests.get(f"{db_service_url}/movies/{movie_id}", timeout=10)
        print(f"📥 Ответ от сервиса БД: {response.status_code}")
        
        if response.status_code == 200:
            movie_data = response.json()
            print(f"✅ Данные фильма получены")
            return jsonify(movie_data)
        else:
            print(f"❌ Ошибка при получении фильма: {response.status_code}")
            print(f"Ответ: {response.text}")
            return jsonify({"error": "Фильм не найден"}), 404
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка при запросе к сервису БД: {str(e)}")
        return jsonify({"error": "Ошибка при получении данных фильма"}), 500
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {str(e)}")
        return jsonify({"error": "Внутренняя ошибка сервера"}), 500

@app.route("/like_movie", methods=["POST"])
def like_movie():
    """Добавление лайка к фильму"""
    try:
        data = request.json
        if not data or "movie_id" not in data:
            return jsonify({"error": "Не указан ID фильма"}), 400
            
        db_service_url = os.getenv("DATABASE_SERVICE_URL", "http://database:5001")
        print(f"👍 Запрос на лайк фильма: {data}")
        
        response = requests.post(f"{db_service_url}/movies/like", json=data, timeout=10)
        print(f"📥 Ответ от сервиса БД: {response.status_code}")
        
        if response.status_code == 200:
            return jsonify(response.json())
        else:
            print(f"❌ Ошибка при добавлении лайка: {response.status_code}")
            print(f"Ответ: {response.text}")
            return jsonify({"error": "Не удалось добавить лайк"}), response.status_code
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка при запросе к сервису БД: {str(e)}")
        return jsonify({"error": "Ошибка при добавлении лайка"}), 500
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {str(e)}")
        return jsonify({"error": "Внутренняя ошибка сервера"}), 500

@app.route("/update_index", methods=["POST"])
def update_index():
    try:
        # Перезагружаем данные из MongoDB
        search_engine.metadata = search_engine._load_metadata()
        search_engine.movie_count = len(search_engine.metadata)
        
        # Обновляем индекс
        search_engine._precompute_features()
        
        # Очищаем кэш
        search_engine.search_cache.clear()
        search_engine.cache_hits = 0
        search_engine.total_searches = 0
        
        return jsonify({"status": "success", "message": "Индекс успешно обновлен"})
    except Exception as e:
        print(f"❌ Ошибка при обновлении индекса: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002) 