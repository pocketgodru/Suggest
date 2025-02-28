from redis import Redis
import time
import json
from functools import wraps

def redis_error_handler(func):
    """Декоратор для обработки ошибок Redis"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"❌ Ошибка Redis в {func.__name__}: {str(e)}")
            import traceback
            print(f"🔍 Детали ошибки:\n{traceback.format_exc()}")
            return None
    return wrapper

class RedisMovieClient:
    def __init__(self, host="localhost", port=6379, db=0, auto_load_from_mongo=False):
        """Инициализация клиента Redis."""
        try:
            print(f"🔄 Подключение к Redis на {host}:{port}...")
            self.redis_client = Redis(host=host, port=port, db=db, decode_responses=True)
            
            # Ждем, пока Redis будет готов
            max_retries = 30
            retry_interval = 2
            for attempt in range(max_retries):
                try:
                    if self.redis_client.ping():
                        print("✅ Подключение к Redis установлено")
                        break
                except Exception as e:
                    if "loading" in str(e).lower():
                        print(f"⏳ Redis загружается... Попытка {attempt + 1}/{max_retries}")
                        time.sleep(retry_interval)
                        continue
                    raise
            else:
                raise Exception("Redis не ответил после всех попыток подключения")
        
            # Проверяем количество фильмов в базе
            movie_count = len(self.redis_client.keys("movie:*") or [])
            print(f"📊 В базе данных {movie_count} фильмов")

            # Проверяем наличие индекса RediSearch
            self._ensure_search_index()

            # Автоматическая загрузка данных из MongoDB при инициализации
            if auto_load_from_mongo and movie_count == 0:
                from mongo_client import MongoMovieClient
                mongo_client = MongoMovieClient()
                self.load_from_mongodb(mongo_client)
        except Exception as e:
            print(f"❌ Ошибка подключения к Redis: {str(e)}")
            import traceback
            print(f"🔍 Детали ошибки:\n{traceback.format_exc()}")
            self.redis_client = None

    @redis_error_handler
    def _ensure_search_index(self):
        """Проверяет наличие индекса RediSearch и создает его при необходимости."""
        try:
            # Проверяем, существует ли индекс
            index_exists = False
            try:
                # Пытаемся получить информацию об индексе
                self.redis_client.execute_command("FT.INFO", "movie_idx")
                index_exists = True
                print("✅ Индекс RediSearch уже существует")
            except Exception:
                # Индекс не существует
                index_exists = False
                print("⚠️ Индекс RediSearch не найден, будет создан новый")

            # Если индекс не существует, создаем его
            if not index_exists:
                # Удаляем старый индекс, если он существует (на всякий случай)
                try:
                    self.redis_client.execute_command("FT.DROPINDEX", "movie_idx")
                except Exception:
                    pass

                # Создаем новый индекс
                # Индексируем поля: name, description, shortDescription
                # Префикс movie: указывает, что индексировать нужно только ключи, начинающиеся с movie:
                create_index_cmd = [
                    "FT.CREATE", "movie_idx", "ON", "HASH", "PREFIX", "1", "movie:",
                    "SCHEMA",
                    "name", "TEXT", "WEIGHT", "5.0",
                    "description", "TEXT", "WEIGHT", "1.0",
                    "shortDescription", "TEXT", "WEIGHT", "2.0"
                ]
                self.redis_client.execute_command(*create_index_cmd)
                print("✅ Создан новый индекс RediSearch для фильмов")
        except Exception as e:
            print(f"❌ Ошибка при создании индекса RediSearch: {str(e)}")
            raise

    @redis_error_handler
    def save_movie(self, movie):
        """Сохраняет один фильм в Redis."""
        if not self.redis_client:
            print("⚠️ Соединение с Redis не установлено")
            return False
            
        # Получаем ID фильма
        movie_id = None
        if "id" in movie:
            movie_id = movie["id"]
        elif "_id" in movie:
            movie_id = movie["_id"]
            
        if movie_id is None:
            print(f"⚠️ Пропущен фильм без ID: {movie}")
            return False
            
        # Преобразуем ID в строку и добавляем префикс "movie:"
        redis_id = f"movie:{movie_id}"
        
        # Создаем копию фильма для Redis
        redis_movie = self._prepare_movie_for_redis(movie)
        
        # Сохраняем фильм в Redis
        self.redis_client.hset(redis_id, mapping=redis_movie)
        
        print(f"📝 Сохранен фильм в Redis: {redis_id} -> {redis_movie.get('name', 'Без названия')}")
        return True

    @redis_error_handler
    def save_movies_bulk(self, movies_list):
        """Сохраняет список фильмов в Redis."""
        if not self.redis_client:
            print("❌ Соединение с Redis не установлено")
            return 0
            
        if not movies_list:
            print("⚠️ Пустой список фильмов")
            return 0
            
        try:
            # Проверяем состояние Redis перед сохранением
            info = self.redis_client.info()
            if info.get('loading', 0) == 1:
                print("⚠️ Redis загружает данные в память, ожидаем...")
                # Ждем пока Redis будет готов
                for _ in range(30):  # Максимум 30 секунд
                    time.sleep(1)
                    info = self.redis_client.info()
                    if info.get('loading', 0) == 0:
                        print("✅ Redis готов к работе")
                        break
                else:
                    raise Exception("Redis слишком долго загружает данные")
            
            print(f"📝 Начинаем сохранение {len(movies_list)} фильмов в Redis...")
            pipeline = self.redis_client.pipeline(transaction=False)  # Отключаем транзакции для скорости
            saved_count = 0

            for i, movie in enumerate(movies_list, 1):
                # Получаем ID фильма
                movie_id = None
                if "id" in movie:
                    movie_id = movie["id"]
                elif "_id" in movie:
                    movie_id = movie["_id"]
                    
                if movie_id is None:
                    print(f"⚠️ Пропущен фильм без ID: {movie}")
                    continue

                # Преобразуем ID в строку и добавляем префикс "movie:"
                redis_id = f"movie:{movie_id}"
                
                # Создаем копию фильма для Redis
                redis_movie = self._prepare_movie_for_redis(movie)
                
                # Сохраняем фильм в Redis
                pipeline.hset(redis_id, mapping=redis_movie)
                
                saved_count += 1
                if i % 1000 == 0:  # Логируем каждую 1000 фильмов
                    print(f"⏳ Обработано {i}/{len(movies_list)} фильмов...")
                    # Выполняем промежуточное сохранение
                    pipeline.execute()
                    pipeline = self.redis_client.pipeline(transaction=False)
                
            # Выполняем оставшиеся команды в pipeline
            if saved_count % 1000 != 0:
                print("💾 Применяем финальные изменения в Redis...")
                pipeline.execute()
            
            # Проверяем фактическое количество фильмов в Redis
            actual_count = len(self.redis_client.keys("movie:*"))
            print(f"📊 Фактическое количество фильмов в Redis: {actual_count}")
            print(f"✅ Загружено {saved_count} фильмов в Redis!")
            return saved_count
            
        except Exception as e:
            print(f"❌ Ошибка при сохранении фильмов в Redis: {str(e)}")
            import traceback
            print(f"🔍 Детали ошибки:\n{traceback.format_exc()}")
            return 0

    def _prepare_movie_for_redis(self, movie):
        """Подготавливает фильм для сохранения в Redis."""
        # Создаем копию фильма для Redis
        redis_movie = {}
        
        # Обязательные поля
        redis_movie["name"] = str(movie.get("name", "Без названия") or "Без названия")
        
        # Обработка жанров
        genres = movie.get("genres", [])
        if genres is None:
            genres = []
        if isinstance(genres, list):
            redis_movie["genres"] = "|".join([str(g) for g in genres if g])
        else:
            redis_movie["genres"] = str(genres)
            
        # Обработка года
        year = movie.get("year", 2000)
        if year is None:
            year = 2000
        redis_movie["year"] = str(year)
        
        # Обработка типа
        movie_type = movie.get("type", "movie")
        if movie_type is None:
            movie_type = "movie"
        redis_movie["type"] = str(movie_type)
        
        # Обработка описаний
        description = movie.get("description", "")
        if description is None:
            description = ""
        redis_movie["description"] = str(description)
        
        short_description = movie.get("shortDescription", "")
        if short_description is None:
            short_description = ""
        redis_movie["shortDescription"] = str(short_description)
        
        # Обработка рейтинга
        rating = movie.get("rating", 0)
        try:
            rating = float(rating)
        except (ValueError, TypeError):
            rating = 0
        redis_movie["rating"] = str(rating)
        
        # Обработка постера
        poster = movie.get("poster", "")
        if poster is None:
            poster = ""
        redis_movie["poster"] = str(poster)
        
        # Дополнительные поля
        status = movie.get("status", "")
        if status is None:
            status = ""
        redis_movie["status"] = str(status)
        
        age_rating = movie.get("ageRating", "")
        if age_rating is None:
            age_rating = ""
        redis_movie["ageRating"] = str(age_rating)
        
        # Обработка стран
        countries = movie.get("countries", [])
        if countries is None:
            countries = []
        if isinstance(countries, list):
            redis_movie["countries"] = "|".join([str(c) for c in countries if c])
        else:
            redis_movie["countries"] = str(countries)
            
        # Обработка releaseYear
        release_year = movie.get("releaseYear", year)
        if release_year is None:
            release_year = year
        redis_movie["releaseYear"] = str(release_year)
        
        # Обработка isSeries
        is_series = movie.get("isSeries", False)
        if is_series is None:
            is_series = False
        redis_movie["isSeries"] = "1" if is_series else "0"
        
        # Обработка категории
        category = movie.get("category", "")
        if category is None:
            category = ""
        redis_movie["category"] = str(category)
        
        return redis_movie

    @redis_error_handler
    def search_movies(self, query="", genre=None, year=None, movie_type=None, country=None, category=None):
        """Поиск фильмов в Redis"""
        try:
            print(f"🔍 Поиск фильмов в Redis:")
            print(f"  Запрос: {query}")
            print(f"  Жанр: {genre}")
            print(f"  Год: {year}")
            print(f"  Тип: {movie_type}")
            print(f"  Страна: {country}")
            print(f"  Категория: {category}")

            # Формируем базовый поисковый запрос
            search_query = []
            
            # Добавляем поиск по названию, если есть запрос
            if query and len(query.strip()) > 0:
                # Экранируем специальные символы в запросе
                escaped_query = query.replace('"', '\\"').strip()
                search_query.append(f'(@name:"{escaped_query}"*)')
            
            # Добавляем фильтры, если они указаны
            if genre and len(genre.strip()) > 0:
                search_query.append(f'(@genres:{{{genre.lower()}}})')
            
            if year and str(year).strip():
                search_query.append(f'(@year:[{year} {year}])')
                
            if movie_type and len(movie_type.strip()) > 0:
                search_query.append(f'(@type:"{movie_type}")')
                
            if country and len(country.strip()) > 0:
                search_query.append(f'(@countries:{{{country}}})')
                
            if category and len(category.strip()) > 0:
                search_query.append(f'(@category:"{category}")')

            # Если нет ни одного условия поиска, возвращаем пустой результат
            if not search_query:
                print("❌ Пустой поисковый запрос")
                return []

            # Объединяем все условия через AND
            final_query = " & ".join(search_query)
            print(f"📝 Итоговый запрос к Redis: {final_query}")

            # Выполняем поиск
            try:
                results = self.redis_client.ft("movies_idx").search(final_query)
                print(f"✅ Найдено результатов: {results.total}")
                
                # Преобразуем результаты в список словарей
                movies = []
                for doc in results.docs:
                    movie_data = {k: v for k, v in doc.__dict__.items() if not k.startswith('__')}
                    # Удаляем технические поля
                    movie_data.pop('id', None)
                    movie_data.pop('payload', None)
                    movies.append(movie_data)
                
                return movies
            except Exception as e:
                print(f"❌ Ошибка при выполнении поиска: {str(e)}")
                return []

        except Exception as e:
            print(f"❌ Ошибка в search_movies: {str(e)}")
            return []

    @redis_error_handler
    def get_movie_by_id(self, movie_id):
        """Возвращает фильм по ID."""
        if not self.redis_client:
            return None
            
        redis_id = f"movie:{movie_id}"
        movie_data = self.redis_client.hgetall(redis_id)
        
        if not movie_data:
            return None
            
        return movie_data

    @redis_error_handler
    def get_all_genres(self):
        """Возвращает список всех уникальных жанров."""
        if not self.redis_client:
            return []
            
        genres_set = set()
        
        # Получаем все фильмы
        for key in self.redis_client.scan_iter("movie:*"):
            movie_genres = self.redis_client.hget(key, "genres")
            if movie_genres:
                genres_set.update(movie_genres.split("|"))
                
        return sorted(list(genres_set))

    @redis_error_handler
    def get_all_countries(self):
        """Возвращает список всех уникальных стран."""
        if not self.redis_client:
            return []
            
        countries_set = set()
        
        # Получаем все фильмы
        for key in self.redis_client.scan_iter("movie:*"):
            movie_countries = self.redis_client.hget(key, "countries")
            if movie_countries:
                countries_set.update(movie_countries.split("|"))
                
        return sorted(list(countries_set))

    @redis_error_handler
    def get_all_categories(self):
        """Возвращает список всех уникальных категорий."""
        if not self.redis_client:
            return []
            
        categories_set = set()
        
        # Получаем все фильмы
        for key in self.redis_client.scan_iter("movie:*"):
            category = self.redis_client.hget(key, "category")
            if category:
                categories_set.add(category)
                
        return sorted(list(categories_set))

    @redis_error_handler
    def flush_db(self):
        """Полностью очищает базу данных Redis."""
        if not self.redis_client:
            return False
            
        self.redis_client.flushdb()
        print("🗑️ База данных Redis очищена")
        return True

    @redis_error_handler
    def load_from_mongodb(self, mongo_client):
        """Загружает фильмы из MongoDB в Redis."""
        if not self.redis_client:
            print("❌ Соединение с Redis не установлено")
            return False
            
        try:
            print("📥 Получаем фильмы из MongoDB...")
            # Получаем все фильмы из MongoDB
            movies = mongo_client.get_movies()
            
            if not movies:
                print("⚠️ Нет фильмов для загрузки из MongoDB")
                return False
                
            print(f"📊 Найдено {len(movies)} фильмов в MongoDB")
            
            # Проверяем подключение к Redis перед очисткой
            if not self.redis_client.ping():
                print("❌ Redis не отвечает на ping")
                return False
            
            # Очищаем существующие данные в Redis
            print("🗑️ Очищаем существующие данные в Redis...")
            self.flush_db()
            
            # Сохраняем фильмы в Redis
            saved_count = self.save_movies_bulk(movies)
            
            if saved_count > 0:
                print(f"✅ Загружено {saved_count} фильмов из MongoDB в Redis")
                return True
            else:
                print("❌ Не удалось сохранить фильмы в Redis")
                return False
                
        except Exception as e:
            print(f"❌ Ошибка при загрузке данных из MongoDB в Redis: {str(e)}")
            import traceback
            print(f"🔍 Детали ошибки:\n{traceback.format_exc()}")
            return False 