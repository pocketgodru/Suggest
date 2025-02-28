import numpy as np
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer
from sklearn.preprocessing import normalize
import faiss
import torch
import hashlib
import os
import re
from time import time
from typing import List, Dict, Any

class TurboMovieSearch:
    def __init__(self, mongo_host="mongodb://mongodb:27017", mongo_db="movies_db", mongo_collection="movies"):
        print("🚀 Инициализация поисковой системы...")
        self.client = MongoClient(mongo_host)
        self.db = self.client[mongo_db]
        self.collection = self.db[mongo_collection]

        # Загружаем данные из MongoDB
        self.metadata = self._load_metadata()
        self.embeddings = self._load_or_generate_embeddings()

        # FAISS Index
        self.index = faiss.IndexFlatL2(self.embeddings.shape[1])
        self.index.add(self.embeddings)

        # Определяем оптимальное устройство для модели
        device = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"
        print(f"🖥 Используем устройство: {device}")
        
        self.model = SentenceTransformer(
            'sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2',
            device=device,
            cache_folder='model_cache'
        )

        # Предварительный расчёт для поиска по жанрам и годам
        self._precompute_features()
        
        # Инициализация кэша результатов поиска
        self.search_cache = {}
        self.cache_hits = 0
        self.total_searches = 0
        
        # Сохраняем количество фильмов для отслеживания изменений
        self.movie_count = len(self.metadata)
        
        print("✅ Поисковая система готова к работе!")

    def _load_metadata(self):
        """Загружает фильмы из MongoDB"""
        movies = list(self.collection.find({}, {"_id": 0}))
        print(f"📥 Загружено {len(movies)} фильмов из MongoDB")
        return movies

    def _load_or_generate_embeddings(self):
        """Загружает существующие эмбеддинги"""
        try:
            # Проверяем несколько возможных путей к файлу
            possible_paths = [
                "/app/movies_embeddings.npy",  # Монтированный файл
                "movies_embeddings.npy",  # В текущей директории
                "../movies_embeddings.npy",  # На уровень выше
            ]
            
            for path in possible_paths:
                try:
                    print(f"🔍 Пробуем загрузить эмбеддинги из {path}...")
                    embeddings = np.load(path)
                    print(f"✅ Эмбеддинги успешно загружены из {path}")
                    print(f"📊 Размер эмбеддингов: {embeddings.shape}")
                    
                    # Проверяем, соответствует ли количество эмбеддингов количеству фильмов
                    if len(embeddings) == len(self.metadata):
                        return embeddings
                    else:
                        print(f"⚠️ Количество эмбеддингов ({len(embeddings)}) не соответствует количеству фильмов ({len(self.metadata)})")
                        continue
                        
                except (FileNotFoundError, PermissionError) as e:
                    print(f"⚠️ Не удалось загрузить из {path}: {str(e)}")
                    continue
            
            print("❌ Файл эмбеддингов не найден или не соответствует количеству фильмов")
            raise FileNotFoundError("Файл эмбеддингов не найден или некорректен")
                
        except Exception as e:
            print(f"❌ Ошибка при загрузке эмбеддингов: {str(e)}")
            raise Exception("Невозможно загрузить эмбеддинги")

    def _precompute_features(self):
        """Предварительно вычисляем нормализованные признаки"""
        years = np.array([item.get('year', 2000) for item in self.metadata], dtype=np.float32)
        self.norm_years = (years - years.min()) / (years.max() - years.min())

        self.genre_index = {}
        for idx, item in enumerate(self.metadata):
            for genre in item.get('genres', []):
                if genre not in self.genre_index:
                    self.genre_index[genre] = []
                self.genre_index[genre].append(idx)

        self.embeddings = normalize(self.embeddings)
    
    def _get_cache_key(self, query, year_filter, genre_filter):
        """Создает уникальный ключ для кэширования результатов поиска"""
        key = f"{query}|{year_filter}|{genre_filter}"
        return hashlib.md5(key.encode()).hexdigest()

    def _parse_query(self, query: str):
        """Извлечение фильтров из запроса"""
        year_match = re.search(r'\b(19\d{2}|20[0-2]\d)\b', query)
        year_boost = None
        if year_match:
            year = int(year_match.group())
            year_boost = (year - 1900) / 125

        genres = []
        for genre in self.genre_index.keys():
            if re.search(r'\b' + re.escape(genre) + r'\b', query, re.IGNORECASE):
                genres.append(genre)

        clean_query = re.sub(r'\b\d{4}\b', '', query).strip()
        return clean_query, year_boost, genres

    def search(self, query: str, top_k=10, year_filter=None, genre_filter=None):
        """
        Поиск фильмов по запросу с учетом фильтров
        """
        start_time = time()
        self.total_searches += 1
        
        # Проверяем кэш
        cache_key = self._get_cache_key(query, year_filter, genre_filter)
        if cache_key in self.search_cache:
            self.cache_hits += 1
            hit_rate = (self.cache_hits / self.total_searches) * 100
            print(f"🔍 Кэш-хит! ({self.cache_hits}/{self.total_searches}, {hit_rate:.1f}%)")
            return self.search_cache[cache_key]

        clean_query, year_boost, genres = self._parse_query(query)

        if year_filter:
            try:
                year_boost = (int(year_filter) - 1900) / 125
            except (ValueError, TypeError):
                year_boost = None
                
        if genre_filter:
            genres.append(genre_filter.lower())

        # Получаем эмбеддинг запроса
        query_embedding = self.model.encode(
            clean_query,
            convert_to_numpy=True,
            normalize_embeddings=True
        )

        # Вычисляем текстовое сходство
        text_scores = np.dot(self.embeddings, query_embedding.T).flatten()
        
        # Инициализируем дополнительные скоры
        year_scores = np.zeros_like(text_scores)
        genre_scores = np.zeros_like(text_scores)

        # Учитываем год, если указан
        if year_boost is not None:
            year_scores = 1.0 - np.abs(self.norm_years - year_boost)

        # Учитываем жанры
        if genres:
            for genre in genres:
                if genre in self.genre_index:
                    genre_scores[self.genre_index[genre]] += 0.1

        # Комбинируем все скоры с весами
        total_scores = 0.85 * text_scores + 0.05 * year_scores + 0.1 * genre_scores

        # Получаем топ-K результатов
        indices = np.argpartition(total_scores, -top_k)[-top_k:]
        best_indices = indices[np.argsort(-total_scores[indices])]

        # Формируем результаты
        results = []
        for idx in best_indices:
            if total_scores[idx] > 0.1:  # Фильтруем низкорелевантные результаты
                movie = self.metadata[idx].copy()
                movie['relevance_score'] = float(total_scores[idx])
                results.append(movie)

        # Сохраняем в кэш
        self.search_cache[cache_key] = results
        
        # Ограничиваем размер кэша
        if len(self.search_cache) > 1000:
            random_key = next(iter(self.search_cache))
            del self.search_cache[random_key]

        print(f"⏱ Поиск за {time() - start_time:.2f}s | Найдено {len(results)} фильмов")
        return results 