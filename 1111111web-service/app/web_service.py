from flask import Flask, request, jsonify, render_template
import os
from dotenv import load_dotenv
import requests

load_dotenv()

app = Flask(__name__)

# Конфигурация сервисов
SEARCH_SERVICE_URL = os.getenv("SEARCH_SERVICE_URL", "http://localhost:5002")
DATABASE_SERVICE_URL = os.getenv("DATABASE_SERVICE_URL", "http://localhost:5001")

@app.route("/")
def index():
    return render_template("home.html")

@app.route("/dml")
def search_page():
    query = request.args.get("query", "")
    year_filter = request.args.get("year", "")
    genre_filter = request.args.get("genre", "")
    movie_type = request.args.get("type", "")
    country_filter = request.args.get("country", "")
    category_filter = request.args.get("category", "")
    search_mode = request.args.get("search_mode", "redis")  # По умолчанию используем Redis
    
    if not query:
        return render_template("dml.html", 
                            movies=[], 
                            query="",
                            current_year=year_filter,
                            current_genre=genre_filter,
                            current_type=movie_type,
                            current_country=country_filter,
                            current_category=category_filter,
                            current_mode=search_mode)
    
    try:
        # Поиск фильмов через поисковой сервис
        search_params = {
            "query": query,
            "year": year_filter,
            "genre": genre_filter,
            "type": movie_type,
            "country": country_filter,
            "category": category_filter,
            "search_mode": search_mode
        }
        
        response = requests.get(f"{SEARCH_SERVICE_URL}/search", params=search_params)
        if response.status_code == 200:
            movies = response.json()
        else:
            movies = []
            
        return render_template("dml.html", 
                             movies=movies, 
                             query=query,
                             current_year=year_filter,
                             current_genre=genre_filter,
                             current_type=movie_type,
                             current_country=country_filter,
                             current_category=category_filter,
                             current_mode=search_mode)
    except Exception as e:
        print(f"Ошибка поиска: {str(e)}")
        return render_template("dml.html", 
                             movies=[], 
                             query=query,
                             error="Произошла ошибка при поиске фильмов",
                             current_year=year_filter,
                             current_genre=genre_filter,
                             current_type=movie_type,
                             current_country=country_filter,
                             current_category=category_filter,
                             current_mode=search_mode)

@app.route("/search_movies")
def api_search():
    query = request.args.get("query", "")
    year_filter = request.args.get("year", "")
    genre_filter = request.args.get("genre", "")
    movie_type = request.args.get("type", "")
    country_filter = request.args.get("country", "")
    category_filter = request.args.get("category", "")
    search_mode = request.args.get("search_mode", "redis")  # По умолчанию используем Redis
    
    if not query:
        return jsonify([])
    
    try:
        # Поиск фильмов через поисковой сервис
        search_params = {
            "query": query,
            "year": year_filter,
            "genre": genre_filter,
            "type": movie_type,
            "country": country_filter,
            "category": category_filter,
            "search_mode": search_mode
        }
        
        response = requests.get(f"{SEARCH_SERVICE_URL}/search", params=search_params)
        if response.status_code == 200:
            return jsonify(response.json())
        else:
            return jsonify([])
    except Exception as e:
        print(f"Ошибка поиска: {str(e)}")
        return jsonify([])

@app.route("/get_genres")
def get_genres():
    try:
        response = requests.get(f"{DATABASE_SERVICE_URL}/genres")
        if response.status_code == 200:
            return jsonify(response.json())
        return jsonify([])
    except Exception as e:
        print(f"Ошибка получения жанров: {str(e)}")
        return jsonify([])

@app.route("/get_countries")
def get_countries():
    try:
        response = requests.get(f"{DATABASE_SERVICE_URL}/countries")
        if response.status_code == 200:
            return jsonify(response.json())
        return jsonify([])
    except Exception as e:
        print(f"Ошибка получения стран: {str(e)}")
        return jsonify([])

@app.route("/get_categories")
def get_categories():
    try:
        response = requests.get(f"{DATABASE_SERVICE_URL}/categories")
        if response.status_code == 200:
            return jsonify(response.json())
        return jsonify([])
    except Exception as e:
        print(f"Ошибка получения категорий: {str(e)}")
        return jsonify([])

@app.route("/get_movie/<movie_id>")
def get_movie(movie_id):
    try:
        response = requests.get(f"{DATABASE_SERVICE_URL}/movies/{movie_id}")
        if response.status_code == 200:
            return jsonify(response.json())
        return jsonify({"error": "Movie not found"}), 404
    except Exception as e:
        print(f"Ошибка получения фильма: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000) 