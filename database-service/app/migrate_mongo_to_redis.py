from mongo_client import MongoMovieClient
from redis_client import RedisMovieClient
import time


class MongoToRedisMigrator:
    def __init__(self):
        self.mongo_client = MongoMovieClient()
        self.redis_client = RedisMovieClient()

    def clear_redis(self):
        """Очищает все данные в Redis."""
        try:
            # Получаем все ключи и удаляем их
            self.redis_client.redis_client.flushall()
            print(f"🧹 Redis очищен. Удалены все ключи.")
            return True
        except Exception as e:
            print(f"❌ Ошибка при очистке Redis: {str(e)}")
            return False

    def migrate_movies(self):
        """Переносит данные из MongoDB в Redis."""
        start_time = time.time()

        # Сначала очищаем Redis
        print("🧹 Начинаем очистку Redis...")
        if not self.clear_redis():
            print("⚠️ Не удалось очистить Redis. Миграция прервана.")
            return

        movies_list = self.mongo_client.get_movies()
        if not movies_list:
            print("⚠️ В MongoDB нет данных для переноса!")
            return

        print(f"📦 Загружено {len(movies_list)} фильмов из MongoDB. Начинаем перенос в Redis...")

        # Создаем список нормализованных фильмов
        normalized_movies = []

        for movie in movies_list:
            movie_id = movie.get("_id")

            if not movie_id:
                print(f"⚠️ Пропущен фильм без ID: {movie}")
                continue

            # Обработка полей фильма
            release_years = movie.get("releaseYears", [])
            release_year = None
            if release_years and isinstance(release_years, list) and len(release_years) > 0:
                if isinstance(release_years[0], dict) and "start" in release_years[0]:
                    release_year = release_years[0]["start"]
            
            if release_year is None:
                release_year = movie.get("year", 2000)

            # Обработка постера
            poster = movie.get("poster", "")
            if poster is None:
                poster = ""
            elif isinstance(poster, dict):
                # Если постер - словарь, извлекаем URL
                poster = poster.get("url", "")

            # Обработка жанров
            genres = []
            movie_genres = movie.get("genres", [])
            if movie_genres and isinstance(movie_genres, list):
                for genre in movie_genres:
                    if isinstance(genre, dict) and "name" in genre:
                        genres.append(genre["name"].lower())
                    elif isinstance(genre, str):
                        genres.append(genre.lower())

            # Обработка стран
            countries = []
            movie_countries = movie.get("countries", [])
            if movie_countries and isinstance(movie_countries, list):
                for country in movie_countries:
                    if isinstance(country, dict) and "name" in country:
                        countries.append(country["name"])
                    elif isinstance(country, str):
                        countries.append(country)

            # Создаем нормализованный фильм
            normalized_movie = {
                "_id": movie_id,
                "name": movie.get("name", ""),
                "type": movie.get("type", ""),
                "year": movie.get("year", 2000),
                "description": movie.get("description", "") or "",
                "shortDescription": movie.get("shortDescription", "") or "",
                "status": movie.get("status", ""),
                "rating": movie.get("rating", 0),
                "ageRating": movie.get("ageRating", ""),
                "poster": poster,
                "genres": genres,
                "countries": countries,
                "releaseYear": release_year,
                "isSeries": movie.get("isSeries", False),
                "category": movie.get("category", ""),
                "alternativeName": movie.get("alternativeName", "")
            }

            normalized_movies.append(normalized_movie)

        # Сохраняем все фильмы в Redis
        saved_count = self.redis_client.save_movies_bulk(normalized_movies)

        elapsed_time = time.time() - start_time
        print(f"✅ Успешно перенесено {saved_count} фильмов в Redis за {elapsed_time:.2f} сек!")

if __name__ == "__main__":
    migrator = MongoToRedisMigrator()
    migrator.migrate_movies()
