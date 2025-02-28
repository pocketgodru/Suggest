from pymongo import MongoClient
import json

from pymongo import MongoClient
import json

class MongoMovieClient:
    def __init__(self, host="mongodb://127.0.0.1:27017", db_name="movies_db", collection_name="movies"):
        """Подключаемся к MongoDB"""
        self.client = MongoClient(host)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def clear_and_load_movies(self, json_path):
        """Очищает базу и загружает фильмы из JSON-файла с нормализацией данных"""
        with open(json_path, "r", encoding="utf-8") as f:
            movies_data = json.load(f)

        self.collection.delete_many({})  # Полный сброс MongoDB
        movies_list = []
        seen_ids = set()

        for category, movies in movies_data.items():
            for movie in movies:
                movie_id = movie.get("id")

                if not isinstance(movie_id, int):
                    print(f"⚠️ Пропущен фильм без корректного ID: {movie}")
                    continue
                if movie_id in seen_ids:
                    print(f"⚠️ Дубликат ID: {movie_id}, пропускаем...")
                    continue

                seen_ids.add(movie_id)

                release_years = movie.get("releaseYears", [])
                release_year = release_years[0]["start"] if release_years and isinstance(release_years[0], dict) else movie.get("year", 2000)
                poster = movie.get("poster") or {}  # Если poster = None, заменяем на пустой словарь


                normalized_movie = {
                    "_id": movie_id,
                    "name": movie.get("name", ""),
                    "type": movie.get("type", ""),
                    "year": movie.get("year", 2000),
                    "description": movie.get("description", "") or "",
                    "shortDescription": movie.get("shortDescription", "") or "",
                    "status": movie.get("status", ""),
                    "rating": movie.get("rating", {}).get("kp", 0.0),
                    "ageRating": movie.get("ageRating"),
                    "poster": poster.get("url", ""),
                    "genres": [g["name"].lower() for g in movie.get("genres", []) if isinstance(g, dict)],
                    "countries": [c["name"] for c in movie.get("countries", []) if isinstance(c, dict)],
                    "releaseYear": release_year,
                    "isSeries": movie.get("isSeries", False),
                    "category": category,
}


                movies_list.append(normalized_movie)

        if movies_list:
            self.collection.insert_many(movies_list)
            print(f"✅ Загружено {len(movies_list)} фильмов в MongoDB")
        else:
            print("⚠️ В MongoDB не загружено ни одного фильма! Проверь JSON-файл.")

    def get_movie_by_id(self, movie_id):
        """Возвращает фильм по ID"""
        return self.collection.find_one({"_id": movie_id}, {"_id": 0})

    def get_movies(self):
        """Возвращает все фильмы (с ID)"""
        return list(self.collection.find({}))

    def get_all_genres(self):
        """Возвращает список всех уникальных жанров из базы данных"""
        try:
            # Используем агрегацию MongoDB для получения уникальных жанров
            pipeline = [
                {"$unwind": "$genres"},  # Разворачиваем массив жанров
                {"$group": {"_id": "$genres"}},  # Группируем по жанрам
                {"$sort": {"_id": 1}}  # Сортируем по алфавиту
            ]
            
            genres = [doc["_id"].capitalize() for doc in self.collection.aggregate(pipeline)]
            return genres
        except Exception as e:
            print(f"Ошибка при получении жанров из MongoDB: {e}")
            return []

    def clear_db(self):
        """Полностью очищает базу данных"""
        self.collection.delete_many({})
        print("🗑️ База данных очищена")
