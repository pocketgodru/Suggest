import os
import logging
import numpy as np
from flask import Flask, request, jsonify
from flask_cors import CORS
import faiss
from time import time, sleep
import requests
from dotenv import load_dotenv
from pymongo import MongoClient
from bson import json_util, ObjectId
import json
from datetime import datetime
import re
import hashlib
from sklearn.preprocessing import normalize

# Загружаем переменные окружения
load_dotenv()

# Настройка API для Hugging Face
API_URL = "https://api-inference.huggingface.co/pipeline/feature-extraction/intfloat/multilingual-e5-large-instruct"
HEADERS = {"Authorization": "Bearer "} #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

app = Flask(__name__)
CORS(app)

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Глобальная переменная для хранения единственного экземпляра TurboMovieSearch
_turbo_movie_search_instance = None

class TurboMovieSearch:
    def __init__(self):
        logger.info("🚀 Инициализация поисковой системы...")
        
        # Получаем данные MongoDB из переменных окружения
        mongo_uri = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
        mongo_db = os.getenv("MONGO_DB", "movies_db")
        mongo_collection = os.getenv("MONGO_COLLECTION", "movies")
        
        logger.info(f"Подключение к MongoDB: {mongo_uri}, БД: {mongo_db}, Коллекция: {mongo_collection}")
        
        self.client = MongoClient(mongo_uri)
        self.db = self.client[mongo_db]
        self.collection = self.db[mongo_collection]

        # Загружаем данные из MongoDB
        self.metadata = self._load_metadata()
        self.embeddings = self._load_or_generate_embeddings()

        # FAISS Index
        self.index = faiss.IndexFlatL2(self.embeddings.shape[1])
        self.index.add(self.embeddings)

        # Предварительный расчёт для поиска по жанрам и годам
        self._precompute_features()
        
        # Инициализация кэша результатов поиска
        self.search_cache = {}
        self.cache_hits = 0
        self.total_searches = 0
        
        # Сохраняем количество фильмов для отслеживания изменений
        self.movie_count = len(self.metadata)
        
        logger.info("✅ Поисковая система готова к работе!")

    def _load_metadata(self):
        """Загружает метаданные из MongoDB"""
        logger.info("📊 Загрузка метаданных фильмов из MongoDB...")
        
        max_retries = 5
        retry_interval = 5  # начальное время ожидания в секундах
        
        for retry in range(max_retries):
            start_time = time()
            try:
                movies = list(self.collection.find({}, {
                    "_id": 1, 
                    "id": 1,  # Добавляем поле id если оно есть
                    "name": 1, 
                    "alternativeName": 1,
                    "description": 1, 
                    "shortDescription": 1,
                    "year": 1, 
                    "genres": 1,
                    "rating": 1,
                    "poster": 1,
                    "type": 1,
                    "countries": 1
                }))
                
                if not movies:
                    if retry < max_retries - 1:
                        logger.warning(f"⚠️ В MongoDB не найдены фильмы (попытка {retry+1}/{max_retries}). Повторная попытка через {retry_interval} сек...")
                        sleep(retry_interval)
                        retry_interval *= 1.5  # Увеличиваем интервал с каждой попыткой
                        continue
                    else:
                        logger.warning("⚠️ В MongoDB не найдены фильмы после всех попыток!")
                        return []
                
                # Фильтруем некорректные данные
                filtered_movies = []
                for movie in movies:
                    # Проверяем, что фильм имеет ID и имя
                    if "_id" not in movie or "name" not in movie:
                        continue
                        
                    # Фильтруем фильмы без названия
                    if not movie.get("name"):
                        continue
                        
                    # Проверка на фиктивные записи
                    if isinstance(movie.get("name"), str) and "тестовый_фильм" in movie.get("name", "").lower():
                        continue
                        
                    # Проверяем корректность года
                    if not isinstance(movie.get("year"), int) or movie.get("year") < 1900:
                        if "year" in movie:
                            try:
                                movie["year"] = int(movie["year"])
                            except:
                                movie["year"] = 2000
                        else:
                            movie["year"] = 2000
                            
                    # Сохраняем оригинальный MongoDB ID
                    movie["mongodb_id"] = str(movie["_id"])
                            
                    filtered_movies.append(movie)
                    
                load_time = time() - start_time
                logger.info(f"✅ Загружено {len(filtered_movies)} фильмов из {len(movies)} ({len(movies) - len(filtered_movies)} отфильтровано) за {load_time:.2f} сек")
                
                return filtered_movies
            except Exception as e:
                logger.error(f"❌ Ошибка при загрузке фильмов из MongoDB: {str(e)}")
                if retry < max_retries - 1:
                    logger.warning(f"⏳ Повторная попытка через {retry_interval} сек...")
                    sleep(retry_interval)
                    retry_interval *= 1.5
                else:
                    logger.error("❌ Не удалось загрузить данные из MongoDB после всех попыток")
                    return []

    def _load_or_generate_embeddings(self):
        """Загружает эмбеддинги из файла или генерирует новые"""
        embeddings_file = os.getenv("EMBEDDINGS_FILE", "movies_embeddings.npy")
        
        # Проверяем наличие файла с эмбеддингами
        try:
            logger.info(f"Попытка загрузки эмбеддингов из файла: {embeddings_file}")
            embeddings = np.load(embeddings_file)
            
            # Проверяем наличие фильмов в MongoDB
            if not self.metadata:
                # Пытаемся подождать и перезагрузить данные из MongoDB
                max_retries = 5
                wait_time = 10  # начальное время ожидания в секундах
                
                for retry in range(max_retries):
                    logger.warning(f"⚠️ В MongoDB нет фильмов, но найдены эмбеддинги для {embeddings.shape[0]} фильмов")
                    logger.info(f"Ожидание загрузки данных в MongoDB... ({retry+1}/{max_retries})")
                    
                    # Ждем некоторое время перед повторной попыткой
                    sleep(wait_time)
                    
                    # Перезагружаем метаданные из MongoDB
                    self.metadata = self._load_metadata()
                    
                    if self.metadata:
                        logger.info(f"✅ Данные появились в MongoDB: {len(self.metadata)} фильмов")
                        break
                    
                    # Увеличиваем время ожидания для следующей попытки
                    wait_time *= 1.5
                
                # Если после всех попыток данных все еще нет, не создаем фиктивные записи
                if not self.metadata:
                    logger.error("❌ В MongoDB так и не появились данные после нескольких попыток ожидания")
                    logger.error("❌ Сервис не может быть запущен без реальных данных из MongoDB")
                    raise Exception("В MongoDB нет данных о фильмах. Дождитесь загрузки данных в базу и перезапустите сервис.")
            
            # Проверяем соответствие размерностей
            if len(self.metadata) != embeddings.shape[0]:
                logger.warning(f"⚠️ Несоответствие размеров: {len(self.metadata)} фильмов в базе, но {embeddings.shape[0]} эмбеддингов в файле")
                logger.info("🔄 Генерация новых эмбеддингов с актуальными данными...")
                return self._generate_embeddings()
                
            logger.info(f"✅ Эмбеддинги загружены из файла: {embeddings.shape}")
            return embeddings
        except Exception as e:
            logger.warning(f"⚠️ Не удалось загрузить эмбеддинги из файла: {str(e)}")
            logger.info("🔄 Генерация новых эмбеддингов...")
            return self._generate_embeddings()
    
    def _generate_embeddings(self):
        """Генерирует эмбеддинги для фильмов"""
        logger.info("🧠 Генерация эмбеддингов для фильмов...")
        
        start_time = time()
        
        # Формируем текстовые представления фильмов
        texts = []
        for movie in self.metadata:
            # Базовые поля
            name = movie.get("name", "")
            alt_name = movie.get("alternativeName", "")
            description = movie.get("description", "") or movie.get("shortDescription", "") or ""
            
            # Жанры
            genres_text = ""
            genres = movie.get("genres", [])
            if genres and isinstance(genres, list):
                genre_names = []
                for genre in genres:
                    if isinstance(genre, dict) and "name" in genre:
                        genre_names.append(genre["name"])
                    elif isinstance(genre, str):
                        genre_names.append(genre)
                genres_text = " ".join(genre_names)
            
            # Формируем итоговый текст, разделяя поля пробелами
            text_fields = []
            if name:
                text_fields.append(name)
            if alt_name and alt_name != name:
                text_fields.append(alt_name)
            if genres_text:
                text_fields.append(genres_text)
            if description:
                # Сокращаем описание, чтобы не перегружать эмбеддинг
                description_words = description.split()[:100]
                text_fields.append(" ".join(description_words))
                
            # Объединяем все поля в одну строку
            text = " ".join(text_fields)
            
            # Убираем лишние пробелы и символы
            text = re.sub(r'\s+', ' ', text).strip()
            texts.append(text)
            
        logger.info(f"🔤 Подготовлено {len(texts)} текстовых представлений фильмов")
        
        # Генерируем эмбеддинги через Hugging Face API
        embeddings = []
        batch_size = 10  # Размер пакета для API запросов
        
        try:
            for i in range(0, len(texts), batch_size):
                batch = texts[i:i+batch_size]
                logger.info(f"Обработка пакета {i//batch_size + 1}/{len(texts)//batch_size + 1}")
                
                # Получаем эмбеддинги для каждого текста в пакете
                batch_embeddings = []
                for text in batch:
                    embedding = self.get_embedding(text)
                    if embedding is not None:
                        batch_embeddings.append(embedding)
                    else:
                        # В случае ошибки используем нулевой вектор
                        logger.warning(f"Не удалось получить эмбеддинг для текста, используем нулевой вектор")
                        batch_embeddings.append(np.zeros(1024))  # Размерность вектора multilingual-e5-large
                
                embeddings.extend(batch_embeddings)
                
                # Небольшая пауза между пакетами, чтобы не перегружать API
                sleep(1)
            
            embeddings = np.array(embeddings)
            
            # Сохраняем эмбеддинги
            np.save("movies_embeddings.npy", embeddings)
            logger.info(f"💾 Эмбеддинги сохранены в файл (форма: {embeddings.shape})")
            
            embedding_time = time() - start_time
            logger.info(f"✅ Эмбеддинги успешно сгенерированы за {embedding_time:.2f} сек!")
            
            return embeddings
        except Exception as e:
            logger.error(f"❌ Ошибка при генерации эмбеддингов: {str(e)}")
            raise

    def _precompute_features(self):
        """Предварительно вычисляем нормализованные признаки"""
        years = np.array([item.get('year', 2000) for item in self.metadata], dtype=np.float32)
        self.norm_years = (years - years.min()) / (years.max() - years.min()) if years.max() > years.min() else years

        self.genre_index = {}
        for idx, item in enumerate(self.metadata):
            for genre in item.get('genres', []):
                # Обрабатываем случай, когда жанр представлен как словарь с полем 'name'
                if isinstance(genre, dict) and 'name' in genre:
                    genre_name = genre['name']
                else:
                    genre_name = genre
                
                if genre_name not in self.genre_index:
                    self.genre_index[genre_name] = []
                self.genre_index[genre_name].append(idx)

        self.embeddings = normalize(self.embeddings)
    
    def _get_cache_key(self, query, year_filter, genre_filter):
        """Создает уникальный ключ для кэширования результатов поиска"""
        key = f"{query}|{year_filter}|{genre_filter}"
        return hashlib.md5(key.encode()).hexdigest()
    
    def check_for_updates(self):
        """Проверяет, появились ли новые фильмы в базе данных"""
        # Запрашиваем только количество документов, не загружая их
        current_count = self.collection.count_documents({})
        
        if current_count > self.movie_count:
            logger.info(f"🔄 Обнаружены новые фильмы! Было: {self.movie_count}, стало: {current_count}")
            
            # Перезагружаем метаданные
            self.metadata = self._load_metadata()
            self.movie_count = len(self.metadata)
            
            # Обновляем эмбеддинги
            self.embeddings = self._generate_embeddings()
            
            # Обновляем FAISS индекс
            self.index = faiss.IndexFlatL2(self.embeddings.shape[1])
            self.index.add(self.embeddings)
            
            # Пересчитываем признаки
            self._precompute_features()
            
            # Очищаем кэш
            self.search_cache.clear()
            self.cache_hits = 0
            self.total_searches = 0
            
            logger.info("✅ Поисковая система обновлена!")
            return True
        
        return False

    def get_embedding(self, text):
        """Получение эмбеддинга через Hugging Face API"""
        try:
            payload = {"inputs": f"query: {text}"}
            response = requests.post(API_URL, headers=HEADERS, json=payload)
            if response.status_code != 200:
                logger.error(f"Ошибка API: {response.text}")
                return None
            return np.array(response.json())
        except Exception as e:
            logger.error(f"Ошибка при получении эмбеддинга: {str(e)}")
            return None

    def search(self, query, top_k=10, year_filter=None, genre_filter=None):
        """Выполняет векторный поиск фильмов"""
        start_time = time()
        self.total_searches += 1
        
        # Проверяем кэш
        cache_key = self._get_cache_key(query, year_filter, genre_filter)
        if cache_key in self.search_cache:
            self.cache_hits += 1
            hit_rate = (self.cache_hits / self.total_searches) * 100
            logger.info(f"🔍 Кэш-хит! ({self.cache_hits}/{self.total_searches}, {hit_rate:.1f}%)")
            return self._prepare_results_for_json(self.search_cache[cache_key])

        clean_query, year_boost, genres = self._parse_query(query)

        if year_filter:
            year_boost = (int(year_filter) - 1900) / 125
        if genre_filter:
            genres.append(genre_filter.lower())

        # Получаем эмбеддинг через API
        query_embedding = self.get_embedding(clean_query)
        if query_embedding is None:
            logger.error("Не удалось получить эмбеддинг для запроса")
            return []

        # Нормализуем эмбеддинг
        query_embedding = query_embedding / np.linalg.norm(query_embedding)

        # Получаем топ-N фильмов по косинусному сходству
        faiss_top_k = min(100, len(self.metadata))
        
        text_scores = np.dot(self.embeddings, query_embedding.T).flatten()
        year_scores = np.zeros_like(text_scores)
        genre_scores = np.zeros_like(text_scores)

        if year_boost is not None:
            year_scores = 1.0 - np.abs(self.norm_years - year_boost)

        if genres:
            for genre in genres:
                genre_key = genre
                if genre_key not in self.genre_index:
                    for key in self.genre_index.keys():
                        if key.lower() == genre.lower():
                            genre_key = key
                            break
                if genre_key in self.genre_index:
                    genre_scores[self.genre_index[genre_key]] += 0.1

        total_scores = 0.85 * text_scores + 0.05 * year_scores + 0.1 * genre_scores

        indices = np.argpartition(total_scores, -faiss_top_k)[-faiss_top_k:]
        best_indices = indices[np.argsort(-total_scores[indices])]

        # Собираем результаты
        results = []
        for idx in best_indices:
            if total_scores[idx] > 0.1:  # Пороговое значение релевантности
                movie = self.metadata[idx].copy()
                movie["relevance_score"] = float(total_scores[idx])
                results.append(movie)
                if len(results) >= top_k:
                    break

        # Сохраняем результаты в кэш
        self.search_cache[cache_key] = results
        
        # Ограничиваем размер кэша
        if len(self.search_cache) > 1000:
            random_key = next(iter(self.search_cache))
            del self.search_cache[random_key]

        logger.info(f"⏱ Поиск за {time() - start_time:.2f}s | Найдено {len(results)} фильмов")
        return self._prepare_results_for_json(results)

    def _parse_query(self, query):
        """Извлечение фильтров из запроса"""
        year_match = re.search(r'\b(19\d{2}|20[0-2]\d)\b', query)
        year_boost = None
        if year_match:
            year = int(year_match.group())
            year_boost = (year - 1900) / 125

        genres = []
        for genre in self.genre_index.keys():
            # Нам не нужно обрабатывать словари genres, т.к. self.genre_index уже содержит строки
            if isinstance(genre, str) and re.search(r'\b' + re.escape(genre) + r'\b', query, re.IGNORECASE):
                genres.append(genre)

        clean_query = query
        return clean_query, year_boost, genres

    def _prepare_results_for_json(self, results):
        """Подготавливает результаты для JSON, используя оригинальные ID из MongoDB"""
        prepared_results = []
        for result in results:
            # Создаем копию словаря
            prepared_result = {}
            
            # Используем сохраненный MongoDB ID
            if "mongodb_id" in result:
                prepared_result["_id"] = result["mongodb_id"]
                prepared_result["movie_id"] = result["mongodb_id"]
            else:
                # Если по какой-то причине нет MongoDB ID, создаем временный
                temp_name = result["name"].replace(" ", "_").lower()
                prepared_result["_id"] = f"temp_{temp_name}"
                prepared_result["movie_id"] = prepared_result["_id"]
                logger.warning(f"MongoDB ID не найден для фильма '{result['name']}', создан временный ID")
            
            # Копируем остальные поля из результата
            for key, value in result.items():
                if key not in ["_id", "movie_id", "mongodb_id"]:
                    prepared_result[key] = value
            
            prepared_results.append(prepared_result)
        
        return prepared_results 

# Функция для получения единственного экземпляра TurboMovieSearch (Singleton)
def get_turbo_movie_search_instance():
    global _turbo_movie_search_instance
    if _turbo_movie_search_instance is None:
        _turbo_movie_search_instance = TurboMovieSearch()
    return _turbo_movie_search_instance

# API маршруты

@app.route("/health")
def health_check():
    """
    Эндпоинт для проверки работоспособности сервиса.
    Не требует инициализации TurboMovieSearch.
    """
    return jsonify({"status": "ok", "service": "search-service"}), 200

@app.route("/search")
def search_api():
    """API для векторного поиска фильмов"""
    try:
        query = request.args.get("query", "")
        year_filter = request.args.get("year")
        genre_filter = request.args.get("genre")
        top_k = request.args.get("limit", 10, type=int)
        
        if not query:
            return jsonify([])
        
        searcher = get_turbo_movie_search_instance()
        results = searcher.search(query, top_k=top_k, year_filter=year_filter, genre_filter=genre_filter)
        #logger.info(f"{results}")
        return jsonify(results)
    
    except Exception as e:
        logger.error(f"Ошибка при выполнении поиска: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/status")
def status():
    """Информация о состоянии поисковой системы"""
    try:
        searcher = get_turbo_movie_search_instance()
        
        status_info = {
            "movies_count": searcher.movie_count,
            "cache_size": len(searcher.search_cache),
            "cache_hit_rate": f"{(searcher.cache_hits / searcher.total_searches * 100):.1f}%" if searcher.total_searches > 0 else "0.0%",
            "total_searches": searcher.total_searches,
            "embeddings_shape": list(searcher.embeddings.shape),
            "genres_count": len(searcher.genre_index)
        }
        
        return jsonify(status_info)
    
    except Exception as e:
        logger.error(f"Ошибка при получении статуса: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    # Инициализируем поисковую систему при запуске
    get_turbo_movie_search_instance()
    
    # Запускаем Flask-сервер
    port = int(os.getenv("PORT", 5002))
    debug_mode = os.getenv("FLASK_DEBUG", "0").lower() in ["1", "true", "yes"]
    app.run(host="0.0.0.0", port=port, debug=debug_mode)
