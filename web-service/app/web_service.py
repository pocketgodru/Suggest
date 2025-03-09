from flask import Flask, request, jsonify, render_template, redirect, url_for, session
import requests
import os
from dotenv import load_dotenv
import logging
from flask_cors import CORS
import uuid
import threading
import time
import json

# Загружаем переменные окружения
load_dotenv()

app = Flask(__name__)
CORS(app)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "secret_key_for_session")

# Настройка логирования
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# URL сервисов из переменных окружения
SEARCH_SERVICE_URL = os.getenv("SEARCH_SERVICE_URL", "http://search:5002")
DATABASE_SERVICE_URL = os.getenv("DATABASE_SERVICE_URL", "http://database:5001")

# Инициализация индекса Redis при запуске приложения
def init_redis_index():
    """
    Инициализирует индекс Redis для поиска фильмов.
    Вызывается при запуске приложения.
    """
    try:
        logger.info("Попытка инициализации индекса Redis...")
        init_url = f"{DATABASE_SERVICE_URL}/create_index"
        response = requests.post(init_url, timeout=30)
        
        if response.status_code == 200:
            logger.info("Индекс Redis успешно инициализирован")
            return True
        else:
            logger.warning(f"Не удалось инициализировать индекс Redis: {response.status_code}, {response.text}")
            return False
    except Exception as e:
        logger.error(f"Ошибка при инициализации индекса Redis: {str(e)}")
        return False

# Функция для отложенной инициализации Redis индекса
def init_redis_with_delay(delay_seconds=20):
    """Инициализирует Redis индекс после указанной задержки"""
    time.sleep(delay_seconds)
    try:
        # Проверяем здоровье сервиса базы данных
        health_url = f"{DATABASE_SERVICE_URL}/health"
        health_response = requests.get(health_url, timeout=5)
        if health_response.status_code == 200:
            logger.info("database-service доступен, инициализируем индекс Redis...")
            init_redis_index()
        else:
            logger.warning(f"database-service не готов, ждем еще 10 секунд...")
            time.sleep(10)
            init_redis_index()
    except Exception as e:
        logger.error(f"Ошибка при проверке доступности database-service: {str(e)}")
        logger.info("Пробуем инициализировать индекс после дополнительной задержки...")
        time.sleep(10)
        try:
            init_redis_index()
        except Exception as ex:
            logger.error(f"Финальная ошибка при инициализации индекса: {str(ex)}")

# Запускаем отложенную инициализацию при старте приложения в отдельном потоке
thread = threading.Thread(target=init_redis_with_delay)
thread.daemon = True
thread.start()
logger.info("Запущен поток для отложенной инициализации индекса Redis")

# Эндпоинт для поиска фильмов
@app.route("/search_movies")
def search_movies_api():
    """
    API для поиска фильмов. Перенаправляет запрос на search-service или database-service
    в зависимости от параметра search_mode.
    """
    query = request.args.get("query", "")
    search_mode = request.args.get("search_mode", "redis")  # По умолчанию используем Redis
    year = request.args.get("year")
    genre = request.args.get("genre")
    type_param = request.args.get("type")
    country = request.args.get("country")
    category = request.args.get("category")
    limit = request.args.get("limit", 50)
    
    # Логируем запрос
    logger.info(f"Запрос поиска: query={query}, mode={search_mode}, фильтры: год={year}, жанр={genre}, тип={type_param}, страна={country}, категория={category}, limit={limit}")
    
    # Определяем, какой сервис использовать для поиска
    try:
        if search_mode == "faiss":
            # Используем search-service (векторный поиск)
            search_url = f"{SEARCH_SERVICE_URL}/search"
            
            # Для FAISS нужен непустой запрос
            if not query:
                # Если запрос пустой, получаем популярные фильмы
                search_url = f"{DATABASE_SERVICE_URL}/get_popular_movies"
                params = {"limit": limit}
                logger.info("Пустой запрос в режиме FAISS: запрашиваем популярные фильмы")
            else:
                params = {"query": query, "limit": limit}
                
            response = requests.get(
                search_url,
                params=params,
                timeout=30
            )
        else:
            # Используем database-service (поиск по тексту/фильтрам)
            # Если запрос пустой, запрашиваем все фильмы
            if not query:
                search_url = f"{DATABASE_SERVICE_URL}/movies/all"
                params = {"limit": limit}  # Ограничиваем количество результатов
                logger.info("Пустой запрос: запрашиваем все фильмы из базы данных")
            else:
                search_url = f"{DATABASE_SERVICE_URL}/search_movies"
                params = {"query": query, "limit": limit}
            
            # Добавляем фильтры, если они указаны
            if year:
                params["year"] = year
            if genre:
                params["genre"] = genre
            if type_param:
                params["type"] = type_param
            if country:
                params["country"] = country
            if category:
                params["category"] = category
            
            try:    
                logger.info(f"Отправляем запрос к URL: {search_url} с параметрами: {params}")
                response = requests.get(
                    search_url,
                    params=params,
                    timeout=60
                )
                
                # Логируем ответ для отладки
                if response.status_code != 200:
                    logger.warning(f"Получен неуспешный ответ: {response.status_code}, текст: {response.text[:200]}")
                
                # Если получаем ошибку "no such index", запрашиваем все фильмы
                if response.status_code != 200 and ("no such index" in response.text or "movie_idx: no such index" in response.text):
                    logger.warning("Ошибка RediSearch: индекс не найден. Запрашиваем все фильмы")
                    search_url = f"{DATABASE_SERVICE_URL}/movies/all"
                    params = {"limit": limit}  # Ограничиваем количество результатов
                    response = requests.get(
                        search_url,
                        params=params,
                        timeout=60
                    )
            except Exception as e:
                logger.error(f"Ошибка при запросе к базе данных: {str(e)}")
                # Запрашиваем все фильмы при ошибке
                search_url = f"{DATABASE_SERVICE_URL}/movies/all"
                params = {"limit": limit}
                response = requests.get(
                    search_url,
                    params=params,
                    timeout=60
                )
        
        if response.status_code == 200:
            # Получаем результаты поиска
            result_data = response.json()
            
            # При запросе популярных фильмов возможен особый формат ответа
            if "/get_popular_movies" in search_url and isinstance(result_data, dict) and "movies" in result_data:
                result_data = result_data.get("movies", [])
            
            # Преобразуем данные в единый формат для всех режимов поиска
            transformed_data = []
            
            # Определяем, в каком формате пришли данные
            source_format = "faiss" if search_mode == "faiss" else "redis"
            
            # Обрабатываем каждый фильм
            for movie in result_data:
                if not isinstance(movie, dict):
                    logger.warning(f"Пропускаем некорректные данные фильма: {movie}")
                    continue
                    
                # Преобразование формата каждого фильма
                transformed_movie = {
                    "id": movie.get("id", 0),
                    "name": movie.get("name", ""),
                    "alternativeName": movie.get("alternativeName", ""),
                    "year": movie.get("year", 0),
                    "description": movie.get("description", ""),
                    "shortDescription": movie.get("shortDescription", ""),
                    "type": movie.get("type", "movie"),
                    "isSeries": movie.get("isSeries", False),
                    "ageRating": movie.get("ageRating", None),
                    "category": movie.get("category", ""),
                    "status": movie.get("status", None)
                }
                
                # Обработка постера
                poster = movie.get("poster")
                if not poster:
                    transformed_movie["poster"] = {
                        "url": "/static/default-poster.jpg",
                        "previewUrl": "/static/default-poster.jpg"
                    }
                elif isinstance(poster, str):
                    if poster.startswith("{") and poster.endswith("}"):
                        try:
                            poster_dict = json.loads(poster.replace("'", '"'))
                            if isinstance(poster_dict, dict):
                                transformed_movie["poster"] = poster_dict
                            else:
                                transformed_movie["poster"] = {
                                    "url": str(poster),
                                    "previewUrl": str(poster)
                                }
                        except:
                            transformed_movie["poster"] = {
                                "url": str(poster),
                                "previewUrl": str(poster)
                            }
                    else:
                        transformed_movie["poster"] = {
                            "url": str(poster),
                            "previewUrl": str(poster)
                        }
                elif isinstance(poster, dict):
                    if "url" not in poster:
                        transformed_movie["poster"] = {
                            "url": "/static/default-poster.jpg",
                            "previewUrl": "/static/default-poster.jpg"
                        }
                    else:
                        transformed_movie["poster"] = poster
                else:
                    transformed_movie["poster"] = {
                        "url": "/static/default-poster.jpg",
                        "previewUrl": "/static/default-poster.jpg"
                    }
                
                # Логируем для отладки структуру movie
                logger.debug(f"Обрабатываем фильм: id={movie.get('id')}, name={movie.get('name')}")
                if "rating" in movie:
                    logger.debug(f"Исходный рейтинг: {movie['rating']} (тип: {type(movie['rating'])})")
                
                # Преобразуем рейтинг в числовой формат
                if "rating" in movie:
                    # Сохраняем оригинальный объект рейтинга для отображения источника
                    transformed_movie["original_rating"] = movie["rating"]
                    
                    # Режим Redis может возвращать рейтинг в различных форматах
                    # Специальная обработка для Redis-режима
                    if source_format == "redis":
                        logger.debug(f"Применяем специальную обработку для Redis-режима")
                        
                        # Если рейтинг - строка "0" или 0, проверим другие возможные поля
                        if movie["rating"] == "0" or movie["rating"] == 0:
                            logger.debug(f"Рейтинг равен 0, ищем в других полях")
                            
                            # Проверяем наличие поля filmRating
                            if "filmRating" in movie and movie["filmRating"]:
                                try:
                                    film_rating = float(movie["filmRating"])
                                    if film_rating > 0:
                                        transformed_movie["rating"] = film_rating
                                        transformed_movie["original_rating"] = {"kp": film_rating}
                                        logger.debug(f"Использован рейтинг из filmRating: {film_rating}")
                                except (ValueError, TypeError):
                                    logger.debug(f"Ошибка при преобразовании filmRating: {movie['filmRating']}")
                            
                            # Проверяем наличие ratingKp напрямую
                            elif "ratingKp" in movie and movie["ratingKp"]:
                                try:
                                    kp_rating = float(movie["ratingKp"])
                                    if kp_rating > 0:
                                        transformed_movie["rating"] = kp_rating
                                        transformed_movie["original_rating"] = {"kp": kp_rating}
                                        logger.debug(f"Использован рейтинг из ratingKp: {kp_rating}")
                                except (ValueError, TypeError):
                                    logger.debug(f"Ошибка при преобразовании ratingKp: {movie['ratingKp']}")
                                    
                            # Проверяем наличие votes.kp
                            elif "votes" in movie and isinstance(movie["votes"], dict):
                                logger.debug(f"Проверяем votes: {movie['votes']}")
                                if "kp" in movie["votes"] and movie["votes"]["kp"] > 0:
                                    # Если есть голоса, но нет рейтинга, пробуем найти в другом месте
                                    if "ratingKp" in movie and movie["ratingKp"]:
                                        try:
                                            kp_rating = float(movie["ratingKp"])
                                            if kp_rating > 0:
                                                transformed_movie["rating"] = kp_rating
                                                transformed_movie["original_rating"] = {"kp": kp_rating}
                                                logger.debug(f"Использован рейтинг из ratingKp: {kp_rating}")
                                        except (ValueError, TypeError):
                                            logger.debug(f"Ошибка при преобразовании ratingKp: {movie['ratingKp']}")
                            
                            # Ищем рейтинг в поле rating_kp
                            elif "rating_kp" in movie and movie["rating_kp"]:
                                try:
                                    kp_rating = float(movie["rating_kp"])
                                    if kp_rating > 0:
                                        transformed_movie["rating"] = kp_rating
                                        transformed_movie["original_rating"] = {"kp": kp_rating}
                                        logger.debug(f"Использован рейтинг из rating_kp: {kp_rating}")
                                except (ValueError, TypeError):
                                    logger.debug(f"Ошибка при преобразовании rating_kp: {movie['rating_kp']}")
                            
                            # Ищем рейтинг в поле kp_rating
                            elif "kp_rating" in movie and movie["kp_rating"]:
                                try:
                                    kp_rating = float(movie["kp_rating"])
                                    if kp_rating > 0:
                                        transformed_movie["rating"] = kp_rating
                                        transformed_movie["original_rating"] = {"kp": kp_rating}
                                        logger.debug(f"Использован рейтинг из kp_rating: {kp_rating}")
                                except (ValueError, TypeError):
                                    logger.debug(f"Ошибка при преобразовании kp_rating: {movie['kp_rating']}")
                                    
                            # Ищем рейтинг в поле imdb_rating
                            elif "imdb_rating" in movie and movie["imdb_rating"]:
                                try:
                                    imdb_rating = float(movie["imdb_rating"])
                                    if imdb_rating > 0:
                                        transformed_movie["rating"] = imdb_rating
                                        transformed_movie["original_rating"] = {"imdb": imdb_rating}
                                        logger.debug(f"Использован рейтинг из imdb_rating: {imdb_rating}")
                                except (ValueError, TypeError):
                                    logger.debug(f"Ошибка при преобразовании imdb_rating: {movie['imdb_rating']}")
                                    
                            # Преобразуем существующий рейтинг в float, если возможно
                            if isinstance(movie["rating"], str) and movie["rating"].replace('.', '', 1).isdigit():
                                try:
                                    rating_value = float(movie["rating"])
                                    if rating_value > 0:
                                        transformed_movie["rating"] = rating_value
                                        transformed_movie["original_rating"] = {"rating": rating_value}
                                        logger.debug(f"Преобразован строковый рейтинг: {rating_value}")
                                except (ValueError, TypeError):
                                    logger.debug(f"Ошибка при преобразовании строкового рейтинга: {movie['rating']}")
                                    
                        # Дальнейшая обработка, если рейтинг строка, но не "0"
                        elif isinstance(movie["rating"], str) and movie["rating"] != "0":
                            try:
                                rating_value = float(movie["rating"])
                                if rating_value > 0:
                                    transformed_movie["rating"] = rating_value
                                    transformed_movie["original_rating"] = {"rating": rating_value}
                                    logger.debug(f"Использован строковый рейтинг: {rating_value}")
                                else:
                                    transformed_movie["rating"] = 0
                            except (ValueError, TypeError):
                                logger.debug(f"Ошибка при преобразовании строкового рейтинга: {movie['rating']}")
                                transformed_movie["rating"] = 0
                        
                        # Если рейтинг все еще не установлен, ищем в дополнительных полях
                        if "rating" not in transformed_movie or transformed_movie["rating"] == 0:
                            # Проверка дополнительных полей рейтингов
                            logger.debug("Проверка дополнительных полей рейтингов")
                            
                            # Проверка movieRating
                            if "movieRating" in movie and movie["movieRating"]:
                                try:
                                    movie_rating = float(movie["movieRating"])
                                    if movie_rating > 0:
                                        transformed_movie["rating"] = movie_rating
                                        transformed_movie["rating_source"] = "КП"
                                        logger.debug(f"Использован рейтинг из movieRating: {movie_rating}")
                                except (ValueError, TypeError):
                                    pass
                    
                    # Стандартная обработка для всех форматов
                    if isinstance(movie["rating"], dict):
                        # Вычисляем средний рейтинг из всех доступных источников
                        rating_values = []
                        
                        # Проверяем рейтинг Кинопоиска
                        if "kp" in movie["rating"] and movie["rating"]["kp"]:
                            try:
                                kp_rating = float(movie["rating"]["kp"])
                                if kp_rating > 0:
                                    rating_values.append(kp_rating)
                            except (ValueError, TypeError):
                                pass
                                
                        # Проверяем рейтинг IMDb
                        if "imdb" in movie["rating"] and movie["rating"]["imdb"]:
                            try:
                                imdb_rating = float(movie["rating"]["imdb"])
                                if imdb_rating > 0:
                                    rating_values.append(imdb_rating)
                            except (ValueError, TypeError):
                                pass
                                
                        # Проверяем рейтинг TMDB
                        if "tmdb" in movie["rating"] and movie["rating"]["tmdb"]:
                            try:
                                tmdb_rating = float(movie["rating"]["tmdb"])
                                if tmdb_rating > 0:
                                    rating_values.append(tmdb_rating)
                            except (ValueError, TypeError):
                                pass
                        
                        # Вычисляем средний рейтинг, если есть значения
                        if rating_values:
                            transformed_movie["rating"] = sum(rating_values) / len(rating_values)
                            # Логируем вычисление среднего рейтинга
                            logger.debug(f"Вычислен средний рейтинг {transformed_movie['rating']} из значений {rating_values}")
                        else:
                            # Если нет конкретных полей, ищем первое числовое значение
                            for key, value in movie["rating"].items():
                                if value and isinstance(value, (int, float)) or (isinstance(value, str) and value.replace('.', '', 1).isdigit()):
                                    try:
                                        rating_value = float(value)
                                        if rating_value > 0:
                                            transformed_movie["rating"] = rating_value
                                            break
                                    except (ValueError, TypeError):
                                        pass
                            
                            # Если рейтинг не найден, устанавливаем 0
                            if "rating" not in transformed_movie or not transformed_movie["rating"]:
                                transformed_movie["rating"] = 0
                    elif isinstance(movie["rating"], (int, float)):
                        transformed_movie["rating"] = float(movie["rating"]) if movie["rating"] > 0 else 0
                    elif isinstance(movie["rating"], str):
                        try:
                            rating_value = float(movie["rating"])
                            transformed_movie["rating"] = rating_value if rating_value > 0 else 0
                        except (ValueError, TypeError):
                            transformed_movie["rating"] = 0
                    else:
                        transformed_movie["rating"] = 0
                else:
                    transformed_movie["rating"] = 0
                    transformed_movie["original_rating"] = None
                
                # Проверяем другие возможные поля с рейтингами
                if transformed_movie["rating"] == 0:
                    if "vote_average" in movie and movie["vote_average"]:
                        try:
                            vote_average = float(movie["vote_average"])
                            if vote_average > 0:
                                transformed_movie["rating"] = vote_average
                                transformed_movie["rating_source"] = "TMDB"
                        except (ValueError, TypeError):
                            pass
                    elif "imdb_rating" in movie and movie["imdb_rating"]:
                        try:
                            imdb_rating = float(movie["imdb_rating"])
                            if imdb_rating > 0:
                                transformed_movie["rating"] = imdb_rating
                                transformed_movie["rating_source"] = "IMDb"
                        except (ValueError, TypeError):
                            pass
                
                # Преобразуем жанры в формат массива строк
                genres = []
                if "genres" in movie:
                    if isinstance(movie["genres"], list):
                        # Если жанры - это список объектов с полем name
                        if movie["genres"] and isinstance(movie["genres"][0], dict) and "name" in movie["genres"][0]:
                            genres = [g["name"] for g in movie["genres"]]
                        else:
                            # Если жанры - уже список строк
                            genres = movie["genres"]
                    elif isinstance(movie["genres"], str):
                        # Если жанры в виде строки с разделителями
                        genres = movie["genres"].split("|" if "|" in movie["genres"] else ",")
                        genres = [g.strip() for g in genres if g.strip()]
                transformed_movie["genres"] = genres
                
                # Преобразуем страны в формат массива строк
                countries = []
                if "countries" in movie:
                    if isinstance(movie["countries"], list):
                        # Если страны - это список объектов с полем name
                        if movie["countries"] and isinstance(movie["countries"][0], dict) and "name" in movie["countries"][0]:
                            countries = [c["name"] for c in movie["countries"]]
                        else:
                            # Если страны - уже список строк
                            countries = movie["countries"]
                    elif isinstance(movie["countries"], str):
                        # Если страны в виде строки с разделителями
                        countries = movie["countries"].split("|" if "|" in movie["countries"] else ",")
                        countries = [c.strip() for c in countries if c.strip()]
                transformed_movie["countries"] = countries
                
                # Преобразуем постер в строку URL
                if "poster" in movie:
                    if isinstance(movie["poster"], dict):
                        # Проверяем наличие разных форматов URL
                        if "url" in movie["poster"]:
                            transformed_movie["poster"] = movie["poster"]["url"]
                        elif "previewUrl" in movie["poster"]:
                            transformed_movie["poster"] = movie["poster"]["previewUrl"]
                        else:
                            # Если нет явных URL, логируем и пробуем другие поля
                            logger.debug(f"Объект постера без URL: {movie['poster']}")
                            if "backdrop" in movie and movie["backdrop"]:
                                if isinstance(movie["backdrop"], dict) and "url" in movie["backdrop"]:
                                    transformed_movie["poster"] = movie["backdrop"]["url"]
                                else:
                                    transformed_movie["poster"] = movie["backdrop"]
                            elif "logo" in movie and movie["logo"]:
                                if isinstance(movie["logo"], dict) and "url" in movie["logo"]:
                                    transformed_movie["poster"] = movie["logo"]["url"]
                                else:
                                    transformed_movie["poster"] = movie["logo"]
                            else:
                                transformed_movie["poster"] = ""
                    elif isinstance(movie["poster"], str):
                        # Если постер - это URL
                        transformed_movie["poster"] = movie["poster"]
                    else:
                        transformed_movie["poster"] = ""
                else:
                    # Пробуем другие поля с изображениями
                    if "backdrop" in movie and movie["backdrop"]:
                        if isinstance(movie["backdrop"], dict) and "url" in movie["backdrop"]:
                            transformed_movie["poster"] = movie["backdrop"]["url"]
                        else:
                            transformed_movie["poster"] = movie["backdrop"]
                    elif "logo" in movie and movie["logo"]:
                        if isinstance(movie["logo"], dict) and "url" in movie["logo"]:
                            transformed_movie["poster"] = movie["logo"]["url"]
                        else:
                            transformed_movie["poster"] = movie["logo"]
                    else:
                        transformed_movie["poster"] = ""
                
                # Логируем для отладки постер и жанры
                logger.debug(f"Обработка фильма {transformed_movie.get('name')}: постер={transformed_movie.get('poster', '')[:50]}..., жанры={transformed_movie.get('genres', [])}")
                
                # Добавляем обработанный фильм в список
                transformed_data.append(transformed_movie)
                
                # Логируем итоговый рейтинг
                logger.debug(f"Итоговый рейтинг для фильма {transformed_movie.get('name')}: {transformed_movie.get('rating')}")
            
            # Логируем количество найденных фильмов
            logger.info(f"Найдено {len(transformed_data)} фильмов по запросу '{query}' в режиме {search_mode}")
            
            return jsonify(transformed_data)
            
        else:
            logger.error(f"Ошибка при поиске фильмов: {response.text}")
            # В случае ошибки при поиске, возвращаем пустой список вместо ошибки
            return jsonify([])
            
    except Exception as e:
        logger.error(f"Ошибка при поиске фильмов через API: {str(e)}")
        # В случае исключения возвращаем пустой список вместо ошибки
        return jsonify([])

# Эндпоинт для получения рекомендаций
@app.route("/get_recommendations/<user_id>")
def get_recommendations_api(user_id):
    """
    API для получения рекомендаций фильмов на основе лайкнутых фильмов пользователя,
    используя векторный поиск (FAISS) для нахождения похожих по описанию.
    """
    try:
        logger.info(f"Запрос рекомендаций для пользователя {user_id}")
        
        # 1. Получаем лайкнутые фильмы через database-service
        liked_movies_url = f"{DATABASE_SERVICE_URL}/get_liked_movies/{user_id}"
        liked_response = requests.get(liked_movies_url, timeout=10)
        
        if liked_response.status_code != 200:
            logger.error(f"Не удалось получить лайкнутые фильмы: {liked_response.text}")
            return jsonify({"movies": [], "total": 0})
        
        liked_data = liked_response.json()
        liked_movies = liked_data.get("movies", [])
        liked_movie_ids = [str(movie.get("id")) for movie in liked_movies if movie.get("id")]
        
        logger.info(f"Найдено {len(liked_movies)} лайкнутых фильмов")
        
        # Если нет лайкнутых фильмов, возвращаем пустой список
        if not liked_movies:
            logger.info(f"У пользователя {user_id} нет лайкнутых фильмов")
            return jsonify({"movies": [], "total": 0})
        
        # 2. Группируем описания лайкнутых фильмов для более эффективного поиска
        all_recommendations = []
        
        # Ограничиваем количество фильмов для рекомендаций
        sample_size = min(5, len(liked_movies))
        import random
        # Берем случайные фильмы из лайкнутых, если их больше 5
        sampled_movies = random.sample(liked_movies, sample_size) if len(liked_movies) > sample_size else liked_movies
        
        # Группируем фильмы по 3 для комбинированного поиска
        for i in range(0, len(sampled_movies), 3):
            movie_batch = sampled_movies[i:i+3]
            
            # Собираем описания из группы фильмов
            descriptions = []
            for movie in movie_batch:
                desc = movie.get("description", "")
                if not desc and movie.get("name"):
                    desc = movie.get("name", "")
                if desc:
                    descriptions.append(desc)
            
            if not descriptions:
                continue
                
            # Объединяем описания для поиска
            combined_description = " ".join(descriptions)
            
            # Используем векторный поиск для нахождения похожих фильмов
            search_url = f"{SEARCH_SERVICE_URL}/search"
            search_params = {"query": combined_description, "limit": 10}
            
            try:
                search_response = requests.get(search_url, params=search_params, timeout=20)
                
                if search_response.status_code == 200:
                    # Добавляем найденные фильмы в список рекомендаций
                    similar_movies = search_response.json()
                    
                    # Добавляем информацию о релевантности для каждого фильма
                    for similar_movie in similar_movies:
                        # Проверяем, что имеем корректную структуру данных
                        if isinstance(similar_movie, dict):
                            # Указываем источник рекомендации (группа фильмов)
                            source_names = [m.get("name", "") for m in movie_batch if m.get("name")]
                            similar_movie["source_movie_names"] = source_names
                            all_recommendations.append(similar_movie)
                        else:
                            logger.error(f"Ошибка при поиске похожих фильмов: {search_response.text}")
                            continue
            except Exception as e:
                logger.error(f"Исключение при поиске похожих фильмов: {str(e)}")
                continue
        
        # 3. Отфильтровываем дубликаты и уже лайкнутые фильмы
        unique_recommendations = {}
        
        for movie in all_recommendations:
            movie_id = str(movie.get("id"))
            
            # Пропускаем уже лайкнутые фильмы
            if movie_id in liked_movie_ids:
                continue
                
            # Если фильм уже есть в рекомендациях, обновляем его релевантность
            if movie_id in unique_recommendations:
                # Увеличиваем релевантность для фильмов, которые рекомендуются несколько раз
                unique_recommendations[movie_id]["relevance_score"] = max(
                    unique_recommendations[movie_id].get("relevance_score", 0),
                    movie.get("relevance_score", 0)
                ) + 0.1  # Добавляем небольшой бонус за повторное появление
            else:
                unique_recommendations[movie_id] = movie
        
        # 4. Преобразуем словарь в список и сортируем по релевантности
        recommendations = list(unique_recommendations.values())
        recommendations.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)
        
        # 5. Ограничиваем количество рекомендаций
        limit = 10
        final_recommendations = recommendations[:limit]
        
        # Если рекомендаций недостаточно, дополняем их популярными фильмами
        if len(final_recommendations) < limit:
            logger.info(f"Недостаточно рекомендаций, добавляем популярные фильмы")
            
            # Получаем популярные фильмы
            popular_url = f"{DATABASE_SERVICE_URL}/get_popular_movies"
            popular_params = {"limit": limit - len(final_recommendations)}
            
            try:
                popular_response = requests.get(popular_url, params=popular_params, timeout=10)
                
                if popular_response.status_code == 200:
                    popular_movies = popular_response.json()
                    
                    # Добавляем популярные фильмы, которых еще нет в рекомендациях и не лайкнуты
                    for movie in popular_movies:
                        movie_id = str(movie.get("id"))
                        
                        if movie_id not in liked_movie_ids and movie_id not in unique_recommendations:
                            movie["relevance_score"] = 0.1  # Низкая релевантность для популярных фильмов
                            final_recommendations.append(movie)
                            
                            # Если достигли лимита, прерываем
                            if len(final_recommendations) >= limit:
                                break
                else:
                    logger.error(f"Ошибка при получении популярных фильмов: {popular_response.text}")
            except Exception as e:
                logger.error(f"Исключение при получении популярных фильмов: {str(e)}")
        
        # 6. Преобразуем данные в единый формат
        transformed_movies = []
        for movie in final_recommendations:
            # Преобразование формата каждого фильма
            transformed_movie = {
                "id": movie.get("id", 0),
                "name": movie.get("name", ""),
                "alternativeName": movie.get("alternativeName", ""),
                "year": movie.get("year", 0),
                "description": movie.get("description", ""),
                "shortDescription": movie.get("shortDescription", ""),
                "type": movie.get("type", "movie"),
                "isSeries": movie.get("isSeries", False),
                "ageRating": movie.get("ageRating", None),
                "category": movie.get("category", ""),
                "status": movie.get("status", None)
            }
            
            # Обработка постера
            poster = movie.get("poster")
            if not poster:
                transformed_movie["poster"] = {
                    "url": "/static/default-poster.jpg",
                    "previewUrl": "/static/default-poster.jpg"
                }
            elif isinstance(poster, str):
                if poster.startswith("{") and poster.endswith("}"):
                    try:
                        poster_dict = json.loads(poster.replace("'", '"'))
                        if isinstance(poster_dict, dict):
                            transformed_movie["poster"] = poster_dict
                        else:
                            transformed_movie["poster"] = {
                                "url": str(poster),
                                "previewUrl": str(poster)
                            }
                    except:
                        transformed_movie["poster"] = {
                            "url": str(poster),
                            "previewUrl": str(poster)
                        }
                else:
                    transformed_movie["poster"] = {
                        "url": str(poster),
                        "previewUrl": str(poster)
                    }
            elif isinstance(poster, dict):
                if "url" not in poster:
                    transformed_movie["poster"] = {
                        "url": "/static/default-poster.jpg",
                        "previewUrl": "/static/default-poster.jpg"
                    }
                else:
                    transformed_movie["poster"] = poster
            else:
                transformed_movie["poster"] = {
                    "url": "/static/default-poster.jpg",
                    "previewUrl": "/static/default-poster.jpg"
                }
            
            # Логируем для отладки
            logger.debug(f"Обрабатываем фильм для рекомендаций: id={movie.get('id')}, name={movie.get('name')}")
            if "rating" in movie:
                logger.debug(f"Исходный рейтинг: {movie['rating']} (тип: {type(movie['rating'])})")
            
            # Преобразуем рейтинг в числовой формат
            if "rating" in movie:
                # Сохраняем оригинальный объект рейтинга для отображения источника
                transformed_movie["original_rating"] = movie["rating"]
                
                # Дополнительная обработка для разных форматов рейтингов
                # Определяем, является ли это данными из Redis или FAISS
                # Для данных из Redis рейтинг может быть строкой "0" или числом 0
                if movie["rating"] == "0" or movie["rating"] == 0:
                    logger.debug(f"Рейтинг равен 0, ищем в других полях")
                    
                    # Проверяем наличие поля filmRating
                    if "filmRating" in movie and movie["filmRating"]:
                        try:
                            film_rating = float(movie["filmRating"])
                            if film_rating > 0:
                                transformed_movie["rating"] = film_rating
                                transformed_movie["original_rating"] = {"kp": film_rating}
                                logger.debug(f"Использован рейтинг из filmRating: {film_rating}")
                        except (ValueError, TypeError):
                            logger.debug(f"Ошибка при преобразовании filmRating: {movie['filmRating']}")
                    
                    # Проверяем наличие ratingKp напрямую
                    elif "ratingKp" in movie and movie["ratingKp"]:
                        try:
                            kp_rating = float(movie["ratingKp"])
                            if kp_rating > 0:
                                transformed_movie["rating"] = kp_rating
                                transformed_movie["original_rating"] = {"kp": kp_rating}
                                logger.debug(f"Использован рейтинг из ratingKp: {kp_rating}")
                        except (ValueError, TypeError):
                            logger.debug(f"Ошибка при преобразовании ratingKp: {movie['ratingKp']}")
                            
                    # Проверяем наличие votes.kp
                    elif "votes" in movie and isinstance(movie["votes"], dict):
                        logger.debug(f"Проверяем votes: {movie['votes']}")
                        if "kp" in movie["votes"] and movie["votes"]["kp"] > 0:
                            # Если есть голоса, но нет рейтинга, пробуем найти в другом месте
                            if "ratingKp" in movie and movie["ratingKp"]:
                                try:
                                    kp_rating = float(movie["ratingKp"])
                                    if kp_rating > 0:
                                        transformed_movie["rating"] = kp_rating
                                        transformed_movie["original_rating"] = {"kp": kp_rating}
                                        logger.debug(f"Использован рейтинг из ratingKp: {kp_rating}")
                                except (ValueError, TypeError):
                                    logger.debug(f"Ошибка при преобразовании ratingKp: {movie['ratingKp']}")
                    
                    # Ищем рейтинг в поле rating_kp
                    elif "rating_kp" in movie and movie["rating_kp"]:
                        try:
                            kp_rating = float(movie["rating_kp"])
                            if kp_rating > 0:
                                transformed_movie["rating"] = kp_rating
                                transformed_movie["original_rating"] = {"kp": kp_rating}
                                logger.debug(f"Использован рейтинг из rating_kp: {kp_rating}")
                        except (ValueError, TypeError):
                            logger.debug(f"Ошибка при преобразовании rating_kp: {movie['rating_kp']}")
                    
                    # Ищем рейтинг в поле kp_rating
                    elif "kp_rating" in movie and movie["kp_rating"]:
                        try:
                            kp_rating = float(movie["kp_rating"])
                            if kp_rating > 0:
                                transformed_movie["rating"] = kp_rating
                                transformed_movie["original_rating"] = {"kp": kp_rating}
                                logger.debug(f"Использован рейтинг из kp_rating: {kp_rating}")
                        except (ValueError, TypeError):
                            logger.debug(f"Ошибка при преобразовании kp_rating: {movie['kp_rating']}")
                            
                    # Ищем рейтинг в поле imdb_rating
                    elif "imdb_rating" in movie and movie["imdb_rating"]:
                        try:
                            imdb_rating = float(movie["imdb_rating"])
                            if imdb_rating > 0:
                                transformed_movie["rating"] = imdb_rating
                                transformed_movie["original_rating"] = {"imdb": imdb_rating}
                                logger.debug(f"Использован рейтинг из imdb_rating: {imdb_rating}")
                        except (ValueError, TypeError):
                            logger.debug(f"Ошибка при преобразовании imdb_rating: {movie['imdb_rating']}")
                
                # Дальнейшая обработка, если рейтинг строка, но не "0"
                elif isinstance(movie["rating"], str) and movie["rating"] != "0":
                    try:
                        rating_value = float(movie["rating"])
                        if rating_value > 0:
                            transformed_movie["rating"] = rating_value
                            transformed_movie["original_rating"] = {"rating": rating_value}
                            logger.debug(f"Использован строковый рейтинг: {rating_value}")
                        else:
                            transformed_movie["rating"] = 0
                    except (ValueError, TypeError):
                        logger.debug(f"Ошибка при преобразовании строкового рейтинга: {movie['rating']}")
                        transformed_movie["rating"] = 0
                
                # Стандартная обработка для всех форматов
                elif isinstance(movie["rating"], dict):
                    # Вычисляем средний рейтинг из всех доступных источников
                    rating_values = []
                    
                    # Проверяем рейтинг Кинопоиска
                    if "kp" in movie["rating"] and movie["rating"]["kp"]:
                        try:
                            kp_rating = float(movie["rating"]["kp"])
                            if kp_rating > 0:
                                rating_values.append(kp_rating)
                        except (ValueError, TypeError):
                            pass
                            
                    # Проверяем рейтинг IMDb
                    if "imdb" in movie["rating"] and movie["rating"]["imdb"]:
                        try:
                            imdb_rating = float(movie["rating"]["imdb"])
                            if imdb_rating > 0:
                                rating_values.append(imdb_rating)
                        except (ValueError, TypeError):
                            pass
                            
                    # Проверяем рейтинг TMDB
                    if "tmdb" in movie["rating"] and movie["rating"]["tmdb"]:
                        try:
                            tmdb_rating = float(movie["rating"]["tmdb"])
                            if tmdb_rating > 0:
                                rating_values.append(tmdb_rating)
                        except (ValueError, TypeError):
                            pass
                    
                    # Вычисляем средний рейтинг, если есть значения
                    if rating_values:
                        transformed_movie["rating"] = sum(rating_values) / len(rating_values)
                        # Логируем вычисление среднего рейтинга
                        logger.debug(f"Вычислен средний рейтинг {transformed_movie['rating']} из значений {rating_values}")
                    else:
                        # Если нет конкретных полей, ищем первое числовое значение
                        for key, value in movie["rating"].items():
                            if value and isinstance(value, (int, float)) or (isinstance(value, str) and value.replace('.', '', 1).isdigit()):
                                try:
                                    rating_value = float(value)
                                    if rating_value > 0:
                                        transformed_movie["rating"] = rating_value
                                        break
                                except (ValueError, TypeError):
                                    pass
                        
                        # Если рейтинг не найден, устанавливаем 0
                        if "rating" not in transformed_movie or not transformed_movie["rating"]:
                            transformed_movie["rating"] = 0
                        # Здесь был неправильный elif, который был не на том же уровне вложенности
                
                # Стандартная обработка в зависимости от типа rating
                if isinstance(movie["rating"], (int, float)):
                    transformed_movie["rating"] = float(movie["rating"]) if float(movie["rating"]) > 0 else 0
                elif isinstance(movie["rating"], dict):
                    # Обработка уже выполнена выше
                    pass
                elif isinstance(movie["rating"], str):
                    try:
                        str_rating = float(movie["rating"])
                        transformed_movie["rating"] = str_rating if str_rating > 0 else 0
                    except (ValueError, TypeError):
                        transformed_movie["rating"] = 0
                else:
                    transformed_movie["rating"] = 0
                    transformed_movie["original_rating"] = None
                
                # Проверяем другие возможные поля с рейтингами
                if "rating" not in transformed_movie or transformed_movie["rating"] == 0:
                    if "vote_average" in movie and movie["vote_average"]:
                        try:
                            vote_average = float(movie["vote_average"])
                            if vote_average > 0:
                                transformed_movie["rating"] = vote_average
                                transformed_movie["rating_source"] = "TMDB"
                        except (ValueError, TypeError):
                            pass
                    elif "imdb_rating" in movie and movie["imdb_rating"]:
                        try:
                            imdb_rating = float(movie["imdb_rating"])
                            if imdb_rating > 0:
                                transformed_movie["rating"] = imdb_rating
                                transformed_movie["rating_source"] = "IMDb"
                        except (ValueError, TypeError):
                            pass
                    elif "movieRating" in movie and movie["movieRating"]:
                        try:
                            movie_rating = float(movie["movieRating"])
                            if movie_rating > 0:
                                transformed_movie["rating"] = movie_rating
                                transformed_movie["rating_source"] = "КП"
                        except (ValueError, TypeError):
                            pass
                
                transformed_movies.append(transformed_movie)
        
        logger.info(f"Сформировано {len(transformed_movies)} рекомендаций для пользователя {user_id}")
        return jsonify({"movies": transformed_movies, "total": len(transformed_movies)})
    except Exception as e:
        logger.error(f"Ошибка при получении рекомендаций через API: {str(e)}")
        return jsonify({"status": "error", "message": str(e), "movies": [], "total": 0}), 500

# Функция для генерации ID пользователя или получения существующего
def get_user_id():
    if 'user_id' not in session:
        session['user_id'] = str(uuid.uuid4())
    return session['user_id']

# Главная страница
@app.route("/")
def index():
    return render_template("home.html")

# Страница поиска
@app.route("/dml")
def search_page():
    query = request.args.get("query", "")
    year_filter = request.args.get("year", "")
    genre_filter = request.args.get("genre", "")
    movie_type = request.args.get("type", "")
    country_filter = request.args.get("country", "")
    category_filter = request.args.get("category", "")
    search_mode = request.args.get("search_mode", "redis")  # По умолчанию Redis
    
    if not query:
        return render_template("dml.html", 
                            movies=[], 
                            query="",
                            current_year=year_filter,
                            current_genre=genre_filter,
                            current_type=movie_type,
                            current_country=country_filter,
                            current_category=category_filter,
                            search_mode=search_mode)
    
    try:
        # Определяем, какой сервис использовать для поиска
        if search_mode == "vector":
            # Используем search-service для векторного поиска
            search_url = f"{SEARCH_SERVICE_URL}/search"
            params = {
                "query": query,
                "year": year_filter,
                "genre": genre_filter
            }
            response = requests.get(search_url, params=params, timeout=10)
            
            if response.status_code == 200:
                movies = response.json()
            else:
                logger.error(f"Ошибка при обращении к search-service: {response.text}")
                movies = []
        else:
            # Используем database-service для поиска через Redis
            search_url = f"{DATABASE_SERVICE_URL}/movies/search"
            params = {
                "query": query,
                "year": year_filter,
                "genre": genre_filter,
                "type": movie_type,
                "country": country_filter,
                "category": category_filter
            }
            response = requests.get(search_url, params=params, timeout=10)
            
            if response.status_code == 200:
                movies = response.json()
            else:
                logger.error(f"Ошибка при обращении к database-service: {response.text}")
                movies = []
        
        return render_template("dml.html", 
                              movies=movies, 
                              query=query,
                              current_year=year_filter,
                              current_genre=genre_filter,
                              current_type=movie_type,
                              current_country=country_filter,
                              current_category=category_filter,
                              search_mode=search_mode)
    
    except Exception as e:
        logger.error(f"Ошибка при выполнении поиска: {str(e)}")
        return render_template("dml.html", 
                              movies=[], 
                              error=str(e),
                              query=query,
                              current_year=year_filter,
                              current_genre=genre_filter,
                              current_type=movie_type,
                              current_country=country_filter,
                              current_category=category_filter,
                              search_mode=search_mode)

# Страница фильма
@app.route("/movie/<int:movie_id>")
def movie_page(movie_id):
    try:
        # Получаем информацию о фильме через database-service
        movie_url = f"{DATABASE_SERVICE_URL}/movies/{movie_id}"
        response = requests.get(movie_url, timeout=5)
        
        if response.status_code == 200:
            movie = response.json()
            
            # Проверяем, лайкнул ли пользователь этот фильм
            user_id = get_user_id()
            like_check_url = f"{DATABASE_SERVICE_URL}/is_movie_liked/{user_id}/{movie_id}"
            like_response = requests.get(like_check_url, timeout=5)
            
            if like_response.status_code == 200:
                is_liked = like_response.json().get("liked", False)
            else:
                is_liked = False
                
            return render_template("movie.html", movie=movie, is_liked=is_liked)
        else:
            logger.error(f"Ошибка при получении фильма ID={movie_id}: {response.text}")
            return render_template("error.html", error="Фильм не найден"), 404
    
    except Exception as e:
        logger.error(f"Ошибка при загрузке страницы фильма: {str(e)}")
        return render_template("error.html", error=str(e)), 500

# Страница поиска по жанру
@app.route("/genre/<genre>")
def genre_page(genre):
    try:
        # Получаем фильмы по жанру через database-service
        search_url = f"{DATABASE_SERVICE_URL}/movies/search"
        params = {"genre": genre}
        response = requests.get(search_url, params=params, timeout=10)
        
        if response.status_code == 200:
            movies = response.json()
            return render_template("genre.html", genre=genre, movies=movies)
        else:
            logger.error(f"Ошибка при получении фильмов по жанру {genre}: {response.text}")
            return render_template("genre.html", genre=genre, movies=[])
            
    except Exception as e:
        logger.error(f"Ошибка при загрузке страницы жанра: {str(e)}")
        return render_template("error.html", error=str(e)), 500

# Лайкнуть фильм
@app.route("/like_movie", methods=["POST"])
def like_movie():
    try:
        data = request.get_json()
        movie_id = data.get("movie_id")
        user_id = get_user_id()
        
        if not movie_id:
            return jsonify({"status": "error", "message": "Не указан ID фильма"}), 400
        
        # Отправляем запрос на лайк в database-service
        like_url = f"{DATABASE_SERVICE_URL}/like_movie"
        payload = {"user_id": user_id, "movie_id": movie_id}
        response = requests.post(like_url, json=payload, timeout=5)
        
        if response.status_code == 200:
            return jsonify({"status": "success", "liked": True})
        else:
            logger.error(f"Ошибка при лайке фильма: {response.text}")
            return jsonify({"status": "error", "message": "Не удалось поставить лайк"}), 500
            
    except Exception as e:
        logger.error(f"Ошибка при лайке фильма: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

# Убрать лайк с фильма
@app.route("/unlike_movie", methods=["POST"])
def unlike_movie():
    try:
        data = request.get_json()
        movie_id = data.get("movie_id")
        user_id = get_user_id()
        
        if not movie_id:
            return jsonify({"status": "error", "message": "Не указан ID фильма"}), 400
        
        # Отправляем запрос на удаление лайка в database-service
        unlike_url = f"{DATABASE_SERVICE_URL}/unlike_movie"
        payload = {"user_id": user_id, "movie_id": movie_id}
        response = requests.post(unlike_url, json=payload, timeout=5)
        
        if response.status_code == 200:
            return jsonify({"status": "success", "liked": False})
        else:
            logger.error(f"Ошибка при удалении лайка: {response.text}")
            return jsonify({"status": "error", "message": "Не удалось удалить лайк"}), 500
            
    except Exception as e:
        logger.error(f"Ошибка при удалении лайка: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

# Страница лайкнутых фильмов
@app.route("/liked")
def liked_movies_page():
    try:
        user_id = get_user_id()
        
        # Получаем лайкнутые фильмы через database-service
        liked_url = f"{DATABASE_SERVICE_URL}/get_liked_movies/{user_id}"
        response = requests.get(liked_url, timeout=10)
        
        if response.status_code == 200:
            result = response.json()
            movies = result.get("movies", [])
            total = result.get("total", 0)
            return render_template("liked.html", movies=movies, total=total)
        else:
            logger.error(f"Ошибка при получении лайкнутых фильмов: {response.text}")
            return render_template("liked.html", movies=[], total=0)
            
    except Exception as e:
        logger.error(f"Ошибка при загрузке страницы лайкнутых фильмов: {str(e)}")
        return render_template("error.html", error=str(e)), 500

# Страница рекомендаций
@app.route("/recommendations")
def recommendations_page():
    try:
        user_id = get_user_id()
        
        # Получаем рекомендации через database-service
        recs_url = f"{DATABASE_SERVICE_URL}/get_recommendations/{user_id}"
        response = requests.get(recs_url, timeout=10)
        
        if response.status_code == 200:
            result = response.json()
            movies = result.get("movies", [])
            total = result.get("total", 0)
            return render_template("recommendations.html", movies=movies, total=total)
        else:
            logger.error(f"Ошибка при получении рекомендаций: {response.text}")
            return render_template("recommendations.html", movies=[], total=0)
            
    except Exception as e:
        logger.error(f"Ошибка при загрузке страницы рекомендаций: {str(e)}")
        return render_template("error.html", error=str(e)), 500

# Очистка всех лайков
@app.route("/remove_all_likes", methods=["POST"])
def remove_all_likes():
    try:
        user_id = get_user_id()
        
        # Отправляем запрос на удаление всех лайков в database-service
        clear_url = f"{DATABASE_SERVICE_URL}/remove_all_likes"
        payload = {"user_id": user_id}
        response = requests.post(clear_url, json=payload, timeout=5)
        
        if response.status_code == 200:
            return jsonify({"status": "success", "message": "Все лайки удалены"})
        else:
            logger.error(f"Ошибка при удалении всех лайков: {response.text}")
            return jsonify({"status": "error", "message": "Не удалось удалить лайки"}), 500
            
    except Exception as e:
        logger.error(f"Ошибка при удалении всех лайков: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

# Получение жанров для формы поиска
@app.route("/api/genres")
def get_genres():
    try:
        # Получаем жанры через database-service
        genres_url = f"{DATABASE_SERVICE_URL}/genres"
        response = requests.get(genres_url, timeout=5)
        
        if response.status_code == 200:
            return jsonify(response.json())
        else:
            logger.error(f"Ошибка при получении жанров: {response.text}")
            return jsonify([])
            
    except Exception as e:
        logger.error(f"Ошибка при получении жанров: {str(e)}")
        return jsonify([])

# Получение стран для формы поиска
@app.route("/api/countries")
def get_countries():
    try:
        # Получаем страны через database-service
        countries_url = f"{DATABASE_SERVICE_URL}/countries"
        response = requests.get(countries_url, timeout=5)
        
        if response.status_code == 200:
            return jsonify(response.json())
        else:
            logger.error(f"Ошибка при получении стран: {response.text}")
            return jsonify([])
            
    except Exception as e:
        logger.error(f"Ошибка при получении стран: {str(e)}")
        return jsonify([])

# Эндпоинт для проверки работоспособности сервиса
@app.route("/health")
def health_check():
    """
    Эндпоинт для проверки работоспособности web-service и его зависимостей.
    """
    status = {
        "service": "web-service",
        "status": "ok",
        "dependencies": {
            "search-service": "unknown",
            "database-service": "unknown"
        }
    }
    
    # Проверяем доступность search-service
    try:
        search_health_url = f"{SEARCH_SERVICE_URL}/health"
        search_response = requests.get(search_health_url, timeout=3)
        status["dependencies"]["search-service"] = "ok" if search_response.status_code == 200 else "error"
    except Exception as e:
        logger.error(f"Ошибка при проверке работоспособности search-service: {str(e)}")
        status["dependencies"]["search-service"] = "error"
    
    # Проверяем доступность database-service
    try:
        database_health_url = f"{DATABASE_SERVICE_URL}/health"
        database_response = requests.get(database_health_url, timeout=3)
        status["dependencies"]["database-service"] = "ok" if database_response.status_code == 200 else "error"
    except Exception as e:
        logger.error(f"Ошибка при проверке работоспособности database-service: {str(e)}")
        status["dependencies"]["database-service"] = "error"
    
    # Если одна из зависимостей недоступна, общий статус тоже будет "error"
    if status["dependencies"]["search-service"] == "error" or status["dependencies"]["database-service"] == "error":
        status["status"] = "error"
        return jsonify(status), 503  # Service Unavailable
    
    return jsonify(status)

@app.route("/get_user_rating/<user_id>/<movie_id>")
def get_user_rating(user_id, movie_id):
    """
    API для получения рейтинга пользователя для фильма.
    """
    try:
        # Отправляем запрос в database-service
        rating_url = f"{DATABASE_SERVICE_URL}/get_user_rating/{user_id}/{movie_id}"
        response = requests.get(rating_url, timeout=5)
        
        if response.status_code == 200:
            return jsonify(response.json())
        else:
            logger.error(f"Ошибка при получении рейтинга: {response.text}")
            return jsonify({"status": "error", "message": "Не удалось получить рейтинг"}), response.status_code
            
    except Exception as e:
        logger.error(f"Ошибка при получении рейтинга: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/save_rating", methods=["POST"])
def save_rating():
    """
    API для сохранения рейтинга пользователя для фильма.
    """
    try:
        data = request.get_json()
        user_id = data.get("user_id")
        movie_id = data.get("movie_id")
        rating = data.get("rating")
        
        if not user_id or not movie_id or not rating:
            return jsonify({"status": "error", "message": "Отсутствуют обязательные параметры"}), 400
        
        # Отправляем запрос в database-service
        rating_url = f"{DATABASE_SERVICE_URL}/save_rating"
        payload = {"user_id": user_id, "movie_id": movie_id, "rating": rating}
        response = requests.post(rating_url, json=payload, timeout=5)
        
        if response.status_code == 200:
            return jsonify(response.json())
        else:
            logger.error(f"Ошибка при сохранении рейтинга: {response.text}")
            return jsonify({"status": "error", "message": "Не удалось сохранить рейтинг"}), response.status_code
            
    except Exception as e:
        logger.error(f"Ошибка при сохранении рейтинга: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/get_comments/<movie_id>")
def get_comments(movie_id):
    """
    API для получения комментариев к фильму.
    """
    try:
        # Отправляем запрос в database-service
        comments_url = f"{DATABASE_SERVICE_URL}/get_comments/{movie_id}"
        response = requests.get(comments_url, timeout=30)
        
        if response.status_code == 200:
            return jsonify(response.json())
        else:
            logger.error(f"Ошибка при получении комментариев: {response.text}")
            return jsonify({"status": "error", "message": "Не удалось получить комментарии"}), response.status_code
            
    except Exception as e:
        logger.error(f"Ошибка при получении комментариев: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/save_comment", methods=["POST"])
def save_comment():
    """
    API для сохранения комментария пользователя к фильму.
    """
    try:
        data = request.get_json()
        user_id = data.get("user_id")
        movie_id = data.get("movie_id")
        comment_text = data.get("comment")
        
        if not user_id or not movie_id or not comment_text:
            return jsonify({"status": "error", "message": "Отсутствуют обязательные параметры"}), 400
        
        # Отправляем запрос в database-service
        comment_url = f"{DATABASE_SERVICE_URL}/save_comment"
        payload = {"user_id": user_id, "movie_id": movie_id, "comment": comment_text}
        response = requests.post(comment_url, json=payload, timeout=5)
        
        if response.status_code == 200:
            return jsonify(response.json())
        else:
            logger.error(f"Ошибка при сохранении комментария: {response.text}")
            return jsonify({"status": "error", "message": "Не удалось сохранить комментарий"}), response.status_code
            
    except Exception as e:
        logger.error(f"Ошибка при сохранении комментария: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/is_movie_liked/<user_id>/<movie_id>", methods=["GET"])
def is_movie_liked_api(user_id, movie_id):
    """
    API для проверки, лайкнул ли пользователь фильм.
    """
    try:
        # Отправляем запрос в database-service
        like_url = f"{DATABASE_SERVICE_URL}/is_movie_liked/{user_id}/{movie_id}"
        response = requests.get(like_url, timeout=5)
        
        if response.status_code == 200:
            return jsonify(response.json())
        else:
            logger.error(f"Ошибка при проверке лайка: {response.text}")
            return jsonify({"status": "error", "message": "Не удалось проверить статус лайка"}), response.status_code
            
    except Exception as e:
        logger.error(f"Ошибка при проверке лайка: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/get_movie/<movie_id>")
def get_movie_api(movie_id):
    """
    API для получения данных о фильме. Перенаправляет запрос на database-service.
    """
    try:
        # Получаем информацию о фильме через database-service
        movie_url = f"{DATABASE_SERVICE_URL}/movies/{movie_id}"
        response = requests.get(movie_url, timeout=30)
        
        if response.status_code == 200:
            movie_data = response.json()
            
            # Обрабатываем рейтинг фильма
            if "rating" in movie_data:
                # Сохраняем оригинальный объект рейтинга
                movie_data["original_rating"] = movie_data["rating"]
                
                if isinstance(movie_data["rating"], dict):
                    # Вычисляем средний рейтинг из доступных источников
                    rating_values = []
                    
                    # Проверяем рейтинг Кинопоиска
                    if "kp" in movie_data["rating"] and movie_data["rating"]["kp"]:
                        try:
                            kp_rating = float(movie_data["rating"]["kp"])
                            if kp_rating > 0:
                                rating_values.append(kp_rating)
                                logger.debug(f"Найден рейтинг КиноПоиска: {kp_rating}")
                        except (ValueError, TypeError):
                            pass
                    
                    # Проверяем рейтинг IMDb
                    if "imdb" in movie_data["rating"] and movie_data["rating"]["imdb"]:
                        try:
                            imdb_rating = float(movie_data["rating"]["imdb"])
                            if imdb_rating > 0:
                                rating_values.append(imdb_rating)
                                logger.debug(f"Найден рейтинг IMDb: {imdb_rating}")
                        except (ValueError, TypeError):
                            pass
                    
                    # Проверяем рейтинг TMDB
                    if "tmdb" in movie_data["rating"] and movie_data["rating"]["tmdb"]:
                        try:
                            tmdb_rating = float(movie_data["rating"]["tmdb"])
                            if tmdb_rating > 0:
                                rating_values.append(tmdb_rating)
                                logger.debug(f"Найден рейтинг TMDB: {tmdb_rating}")
                        except (ValueError, TypeError):
                            pass
                    
                    # Вычисляем средний рейтинг, если есть значения
                    if rating_values:
                        movie_data["rating"] = sum(rating_values) / len(rating_values)
                        logger.debug(f"Вычислен средний рейтинг {movie_data['rating']} из значений {rating_values}")
                    else:
                        # Если нет конкретных полей, ищем первое числовое значение
                        for key, value in movie_data["rating"].items():
                            if value and (isinstance(value, (int, float)) or (isinstance(value, str) and value.replace('.', '', 1).isdigit())):
                                try:
                                    rating_value = float(value)
                                    if rating_value > 0:
                                        movie_data["rating"] = rating_value
                                        logger.debug(f"Найден рейтинг из поля {key}: {rating_value}")
                                        break
                                except (ValueError, TypeError):
                                    pass
                        
                        # Если рейтинг не найден, устанавливаем 0
                        if "rating" not in movie_data or not movie_data["rating"]:
                            movie_data["rating"] = 0
                
                elif isinstance(movie_data["rating"], (int, float)):
                    movie_data["rating"] = float(movie_data["rating"]) if movie_data["rating"] > 0 else 0
                elif isinstance(movie_data["rating"], str):
                    try:
                        rating_value = float(movie_data["rating"])
                        movie_data["rating"] = rating_value if rating_value > 0 else 0
                    except (ValueError, TypeError):
                        movie_data["rating"] = 0
                else:
                    movie_data["rating"] = 0
            else:
                movie_data["rating"] = 0
                movie_data["original_rating"] = None
            
            # Проверяем другие возможные поля с рейтингами
            if movie_data["rating"] == 0:
                if "vote_average" in movie_data and movie_data["vote_average"]:
                    try:
                        vote_average = float(movie_data["vote_average"])
                        if vote_average > 0:
                            movie_data["rating"] = vote_average
                            movie_data["rating_source"] = "TMDB"
                    except (ValueError, TypeError):
                        pass
                elif "imdb_rating" in movie_data and movie_data["imdb_rating"]:
                    try:
                        imdb_rating = float(movie_data["imdb_rating"])
                        if imdb_rating > 0:
                            movie_data["rating"] = imdb_rating
                            movie_data["rating_source"] = "IMDb"
                    except (ValueError, TypeError):
                        pass
            
            logger.info(f"Получен фильм ID={movie_id} с рейтингом {movie_data['rating']}")
            return jsonify(movie_data)
        else:
            logger.error(f"Ошибка при получении фильма ID={movie_id}: {response.text}")
            return jsonify({"status": "error", "message": "Произошла ошибка при получении данных о фильме"}), response.status_code
            
    except Exception as e:
        logger.error(f"Ошибка при получении данных о фильме через API: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/filter", methods=["POST"])
def filter_movies():
    """
    Эндпоинт для фильтрации списка фильмов по заданным критериям.
    Принимает JSON с полями:
    - movies: список фильмов для фильтрации
    - filters: объект с фильтрами (genres, years, rating)
    """
    try:
        data = request.json
        
        if not data or "movies" not in data or "filters" not in data:
            return jsonify({"error": "Неверный формат данных"}), 400
        
        movies = data["movies"]
        filters = data["filters"]
        
        if not isinstance(movies, list):
            return jsonify({"error": "Поле 'movies' должно быть списком"}), 400
        
        # Если нет фильмов для фильтрации, возвращаем пустой список
        if not movies:
            return jsonify([])
        
        # Получаем фильтры
        genres = filters.get("genres", [])
        years = filters.get("years", [])
        ratings = filters.get("rating", [])
        
        # Фильтруем фильмы
        filtered_movies = movies
        
        # Фильтрация по жанрам
        if genres:
            genre_filtered = []
            for movie in filtered_movies:
                movie_genres = []
                
                # Обрабатываем разные форматы данных жанров
                if "genres" in movie:
                    if isinstance(movie["genres"], list):
                        # Могут быть строки или объекты с полем name
                        if movie["genres"] and isinstance(movie["genres"][0], dict) and "name" in movie["genres"][0]:
                            movie_genres = [g["name"].lower() for g in movie["genres"]]
                        else:
                            movie_genres = [g.lower() for g in movie["genres"]]
                    elif isinstance(movie["genres"], str):
                        movie_genres = [g.strip().lower() for g in movie["genres"].split("|")]
                
                # Проверяем, содержит ли фильм хотя бы один из выбранных жанров
                if any(genre.lower() in movie_genres for genre in genres):
                    genre_filtered.append(movie)
            
            filtered_movies = genre_filtered
        
        # Фильтрация по годам
        if years:
            year_filtered = []
            for movie in filtered_movies:
                movie_year = None
                
                # Обрабатываем разные форматы данных года
                if "year" in movie:
                    try:
                        movie_year = int(movie["year"])
                    except (ValueError, TypeError):
                        # Если год не удалось преобразовать в число, пропускаем
                        continue
                elif "release_date" in movie:
                    try:
                        # Предполагаем, что release_date в формате "YYYY-MM-DD"
                        movie_year = int(movie["release_date"].split("-")[0])
                    except (ValueError, TypeError, IndexError, AttributeError):
                        continue
                
                if movie_year is None:
                    continue
                
                
                # Проверяем, входит ли год фильма в один из выбранных диапазонов
                for year_range in years:
                    if "-" in year_range:
                        # Диапазон годов (например, "2000-2010")
                        try:
                            start_year, end_year = map(int, year_range.split("-"))
                            if start_year <= movie_year <= end_year:
                                year_filtered.append(movie)
                                break
                        except (ValueError, TypeError):
                            continue
                    else:
                        # Конкретный год
                        try:
                            if movie_year == int(year_range):
                                year_filtered.append(movie)
                                break
                        except (ValueError, TypeError):
                            continue
            
            filtered_movies = year_filtered
        
        # Фильтрация по рейтингу
        if ratings:
            rating_filtered = []
            for movie in filtered_movies:
                movie_rating = None
                
                # Обрабатываем разные форматы данных рейтинга
                if "rating" in movie:
                    # Рейтинг может быть числом или объектом с полями kp, imdb, tmdb
                    if isinstance(movie["rating"], dict):
                        # Вычисляем средний рейтинг из всех доступных источников
                        rating_values = []
                        
                        # Проверяем рейтинг Кинопоиска
                        if "kp" in movie["rating"] and movie["rating"]["kp"]:
                            try:
                                kp_rating = float(movie["rating"]["kp"])
                                if kp_rating > 0:
                                    rating_values.append(kp_rating)
                            except (ValueError, TypeError):
                                pass
                                
                        # Проверяем рейтинг IMDb
                        if "imdb" in movie["rating"] and movie["rating"]["imdb"]:
                            try:
                                imdb_rating = float(movie["rating"]["imdb"])
                                if imdb_rating > 0:
                                    rating_values.append(imdb_rating)
                            except (ValueError, TypeError):
                                pass
                                
                        # Проверяем рейтинг TMDB
                        if "tmdb" in movie["rating"] and movie["rating"]["tmdb"]:
                            try:
                                tmdb_rating = float(movie["rating"]["tmdb"])
                                if tmdb_rating > 0:
                                    rating_values.append(tmdb_rating)
                            except (ValueError, TypeError):
                                pass
                        
                        # Вычисляем средний рейтинг, если есть значения
                        if rating_values:
                            movie_rating = sum(rating_values) / len(rating_values)
                        else:
                            # Если нет конкретных полей, ищем первое числовое значение
                            for key, value in movie["rating"].items():
                                if value and isinstance(value, (int, float)) or (isinstance(value, str) and value.replace('.', '', 1).isdigit()):
                                    try:
                                        rating_value = float(value)
                                        if rating_value > 0:
                                            movie_rating = rating_value
                                            break
                                    except (ValueError, TypeError):
                                        pass
                    else:
                        try:
                            rating_value = float(movie["rating"])
                            if rating_value > 0:
                                movie_rating = rating_value
                        except (ValueError, TypeError):
                            pass
                elif "vote_average" in movie:
                    try:
                        movie_rating = float(movie["vote_average"])
                    except (ValueError, TypeError):
                        pass
                elif "imdb_rating" in movie:
                    try:
                        movie_rating = float(movie["imdb_rating"])
                    except (ValueError, TypeError):
                        pass
                
                if movie_rating is None or movie_rating <= 0:
                    continue
                
                # Проверяем, входит ли рейтинг фильма в один из выбранных диапазонов
                for rating_range in ratings:
                    if "-" in rating_range:
                        # Диапазон рейтингов (например, "6.0-7.9")
                        try:
                            start_rating, end_rating = map(float, rating_range.split("-"))
                            if start_rating <= movie_rating <= end_rating:
                                rating_filtered.append(movie)
                                break
                        except (ValueError, TypeError):
                            continue
                    else:
                        # Конкретный рейтинг или специальный случай
                        if rating_range == "0-5.9" and movie_rating < 6.0:
                            rating_filtered.append(movie)
                            break
                        elif rating_range == "6.0-7.9" and 6.0 <= movie_rating < 8.0:
                            rating_filtered.append(movie)
                            break
                        elif rating_range == "8.0-10.0" and movie_rating >= 8.0:
                            rating_filtered.append(movie)
                            break
            
            filtered_movies = rating_filtered
        
        # Проверяем, что все элементы в результатах - словари
        valid_results = []
        for item in filtered_movies:
            if isinstance(item, dict):
                # Проверяем, что все значения в словаре имеют корректные типы
                sanitized_item = {}
                for key, value in item.items():
                    if value is None:
                        sanitized_item[key] = ""
                    elif isinstance(value, (str, int, float, bool)):
                        sanitized_item[key] = value
                    elif isinstance(value, list):
                        # Проверяем, что все элементы списка имеют корректные типы
                        sanitized_list = []
                        for list_item in value:
                            if list_item is None:
                                sanitized_list.append("")
                            elif isinstance(list_item, dict):
                                # Если элемент списка является словарем, преобразуем его в строку
                                sanitized_list.append(str(list_item))
                            else:
                                sanitized_list.append(str(list_item))
                        sanitized_item[key] = sanitized_list
                    elif isinstance(value, dict):
                        # Рекурсивно санитизируем вложенные словари
                        sanitized_dict = {}
                        for dict_key, dict_value in value.items():
                            if dict_value is None:
                                sanitized_dict[dict_key] = ""
                            elif isinstance(dict_value, (str, int, float, bool)):
                                sanitized_dict[dict_key] = dict_value
                            else:
                                sanitized_dict[dict_key] = str(dict_value)
                        sanitized_item[key] = sanitized_dict
                    else:
                        sanitized_item[key] = str(value)
                valid_results.append(sanitized_item)
        
        return jsonify(valid_results)
    except Exception as e:
        app.logger.error(f"Ошибка при фильтрации фильмов: {str(e)}")
        return jsonify({"error": "Произошла ошибка при фильтрации фильмов"}), 500

# Получение текущего ID пользователя
@app.route("/api/user_id")
def get_user_id_api():
    """Возвращает текущий ID пользователя из сессии."""
    user_id = get_user_id()  # Получаем или генерируем ID пользователя
    return jsonify({"user_id": user_id})

# Ручное создание индекса Redis
@app.route("/create_redis_index", methods=["POST"])
def create_redis_index():
    """
    Эндпоинт для ручного создания индекса Redis.
    Может быть вызван администратором.
    """
    try:
        init_redis_index()
        return jsonify({"status": "success", "message": "Попытка создания индекса Redis выполнена успешно"})
    except Exception as e:
        logger.error(f"Ошибка при ручном создании индекса Redis: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    # Пробуем инициализировать индекс Redis перед запуском
    try:
        logger.info("Запускаем инициализацию индекса Redis...")
        init_redis_index()
    except Exception as e:
        logger.error(f"Не удалось инициализировать индекс Redis: {str(e)}")
    
    # Запускаем веб-сервис
    app.run(host="0.0.0.0", port=5000, debug=True)
