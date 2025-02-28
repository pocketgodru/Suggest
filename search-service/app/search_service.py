from flask import Flask, request, jsonify
import os
from dotenv import load_dotenv
import numpy as np
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer
from sklearn.preprocessing import normalize
import faiss
from time import time
import re
import torch
import hashlib
from functools import lru_cache
import logging
from flask_cors import CORS

# Загружаем переменные окружения
load_dotenv()

# Принудительно используем только CPU
os.environ["CUDA_VISIBLE_DEVICES"] = ""
torch.set_num_threads(1)  # Ограничиваем количество потоков для CPU

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
        mongo_uri = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
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

        # Всегда используем CPU, независимо от доступности других устройств
        device = "cpu"
        logger.info(f"🖥 Используем устройство: {device}")
        
        # Загрузка модели для эмбеддингов
        model_name = "intfloat/multilingual-e5-large-instruct"
        cache_folder = "model_cache"
        
        logger.info(f"Загрузка модели: {model_name} из {cache_folder}")
        
        self.model = SentenceTransformer(
            model_name,
            device=device,
            cache_folder=cache_folder
        )

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
        """Загружает фильмы из MongoDB"""
        movies = list(self.collection.find({}, {"_id": 0}))
        logger.info(f"📥 Загружено {len(movies)} фильмов из MongoDB")
        return movies

    def _load_or_generate_embeddings(self):
        """Генерирует или загружает эмбеддинги фильмов"""
        try:
            embeddings_file = os.getenv("EMBEDDINGS_FILE", "movies_embeddings.npy")
            logger.info(f"Попытка загрузки эмбеддингов из файла: {embeddings_file}")
            
            embeddings = np.load(embeddings_file, mmap_mode='r')
            # Проверяем, соответствует ли количество эмбеддингов количеству фильмов
            if len(self.metadata) > 0 and len(embeddings) == len(self.metadata):
                logger.info(f"Успешно загружены эмбеддинги для {len(embeddings)} фильмов")
                return embeddings
            elif len(self.metadata) == 0:
                # Если в MongoDB нет фильмов, но есть эмбеддинги, используем их
                logger.warning(f"⚠️ В MongoDB нет фильмов, но найдены эмбеддинги для {len(embeddings)} фильмов")
                # Создаем фиктивные метаданные на основе размера эмбеддингов
                self.metadata = [{"name": f"Пример фильма {i}", "year": 2022} for i in range(len(embeddings))]
                self.movie_count = len(self.metadata)
                logger.info(f"📥 Создано {len(self.metadata)} фиктивных записей для инициализации")
                return embeddings
            else:
                logger.warning(f"⚠️ Количество эмбеддингов ({len(embeddings)}) не соответствует количеству фильмов ({len(self.metadata)}). Генерируем заново.")
                return self._generate_embeddings()
        except (FileNotFoundError, IndexError) as e:
            logger.warning(f"🛠 Ошибка при загрузке эмбеддингов: {str(e)}. Генерируем эмбеддинги...")
            return self._generate_embeddings()
    
    def _generate_embeddings(self):
        """Генерирует эмбеддинги для всех фильмов"""
        # Если нет фильмов, создаем минимальный набор данных
        if len(self.metadata) == 0:
            logger.warning("❌ Нет фильмов для генерации эмбеддингов. Создаем минимальный набор данных.")
            # Создаем фиктивные данные - один фильм с нулевым эмбеддингом размерности 384 (как у модели MiniLM-L12)
            dummy_embeddings = np.zeros((1, 384), dtype=np.float32)
            self.metadata = [{"name": "Пример фильма", "year": 2022}]
            self.movie_count = 1
            logger.info("📥 Создан минимальный набор данных для инициализации")
            return dummy_embeddings
            
        # Всегда используем CPU
        device = "cpu"
        
        # Комбинируем название и описание для более точного поиска
        texts = []
        for movie in self.metadata:
            name = movie["name"]
            description = movie.get("description", "")
            short_description = movie.get("shortDescription", "")
            
            # Если есть описание, используем его вместе с названием
            if description:
                text = f"{name}. {description}"
            # Если есть только короткое описание, используем его
            elif short_description:
                text = f"{name}. {short_description}"
            # Если нет описаний, используем только название
            else:
                text = name
                
            texts.append(text)
        
        logger.info(f"🔄 Генерация эмбеддингов для {len(texts)} фильмов...")
        model = SentenceTransformer("intfloat/multilingual-e5-large-instruct",
                                    device=device,
                                    cache_folder='model_cache')
        embeddings = model.encode(
            texts, 
            convert_to_numpy=True, 
            normalize_embeddings=True, 
            batch_size=64, 
            show_progress_bar=True
        )
        
        embeddings_file = os.getenv("EMBEDDINGS_FILE", "movies_embeddings.npy")
        logger.info(f"💾 Сохраняем эмбеддинги в файл: {embeddings_file}")
        np.save(embeddings_file, embeddings)
        return embeddings

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
        current_movies = list(self.collection.find({}, {"_id": 0}))
        current_count = len(current_movies)
        
        if current_count > self.movie_count:
            logger.info(f"🔄 Обнаружены новые фильмы! Было: {self.movie_count}, стало: {current_count}")
            self.metadata = current_movies
            self.movie_count = current_count
            
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

    def search(self, query, top_k=10, year_filter=None, genre_filter=None):
        """
        Выполняет векторный поиск фильмов
        
        Args:
            query: Текстовый запрос для поиска
            top_k: Максимальное количество результатов
            year_filter: Год для фильтрации
            genre_filter: Жанр для фильтрации
            
        Returns:
            Список фильмов, соответствующих запросу
        """
        start_time = time()
        self.total_searches += 1
        
        # Проверяем, появились ли новые фильмы
        self.check_for_updates()
        
        # Проверяем кэш
        cache_key = self._get_cache_key(query, year_filter, genre_filter)
        if cache_key in self.search_cache:
            self.cache_hits += 1
            hit_rate = (self.cache_hits / self.total_searches) * 100
            logger.info(f"🔍 Кэш-хит! ({self.cache_hits}/{self.total_searches}, {hit_rate:.1f}%)")
            return self.search_cache[cache_key]

        clean_query, year_boost, genres = self._parse_query(query)

        if year_filter:
            year_boost = (int(year_filter) - 1900) / 125
        if genre_filter:
            genres.append(genre_filter.lower())

        query_embedding = self.model.encode(
            clean_query,
            convert_to_numpy=True,
            normalize_embeddings=True
        )

        # Получаем топ-N фильмов по косинусному сходству
        faiss_top_k = min(100, len(self.metadata))  # Ограничиваем количество результатов
        
        text_scores = np.dot(self.embeddings, query_embedding.T).flatten()
        year_scores = np.zeros_like(text_scores)
        genre_scores = np.zeros_like(text_scores)

        if year_boost is not None:
            year_scores = 1.0 - np.abs(self.norm_years - year_boost)

        if genres:
            for genre in genres:
                genre_key = genre
                
                # Если не нашли жанр напрямую, ищем его в ключах с учетом регистра
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
                results.append(self.metadata[idx])
                
                # Добавляем оценку релевантности
                results[-1]["relevance_score"] = float(total_scores[idx])
                
                # Ограничиваем количество результатов
                if len(results) >= top_k:
                    break
        
        # Сохраняем результаты в кэш
        self.search_cache[cache_key] = results
        
        # Ограничиваем размер кэша (максимум 1000 запросов)
        if len(self.search_cache) > 1000:
            # Удаляем случайный ключ
            random_key = next(iter(self.search_cache))
            del self.search_cache[random_key]

        logger.info(f"⏱ Поиск за {time() - start_time:.2f}s | Найдено {len(results)} фильмов")
        return results

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
