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
            return None
    return wrapper

class RedisMovieClient:
    def __init__(self, host="localhost", port=6379, db=0, auto_load_from_mongo=False):
        """Инициализация клиента Redis."""
        try:
            self.redis_client = Redis(host=host, port=port, db=db, decode_responses=True)
            self.redis_client.ping()  # Проверяем соединение
            print("✅ Подключение к Redis установлено")
        
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
        
        # Индексируем фильм
        self._index_movie(redis_id, redis_movie)
        
        print(f"📝 Сохранен фильм в Redis: {redis_id} -> {redis_movie.get('name', 'Без названия')}")
        return True

    @redis_error_handler
    def save_movies_bulk(self, movies_list):
        """Сохраняет список фильмов в Redis."""
        if not self.redis_client:
            print("⚠️ Соединение с Redis не установлено")
            return 0
            
        if not movies_list:
            print("⚠️ Пустой список фильмов")
            return 0
            
        pipeline = self.redis_client.pipeline()
        saved_count = 0

        for movie in movies_list:
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
            
            # Индексируем фильм
            self._index_movie_pipeline(pipeline, redis_id, redis_movie)
            
            saved_count += 1
            
        # Выполняем все команды в pipeline
        pipeline.execute()
        print(f"✅ Загружено {saved_count} фильмов в Redis!")
        return saved_count

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
        
        # Преобразуем жанры в список строк
        genre_list = []
        if isinstance(genres, list):
            for genre in genres:
                if isinstance(genre, str):
                    # Проверяем, не является ли жанр строковым представлением словаря
                    if genre.startswith("{") and genre.endswith("}") and "'name':" in genre:
                        try:
                            # Пытаемся извлечь имя жанра из строкового представления
                            import ast
                            genre_dict = ast.literal_eval(genre)
                            if isinstance(genre_dict, dict) and 'name' in genre_dict:
                                genre_list.append(genre_dict['name'])
                            else:
                                genre_list.append(genre)
                        except:
                            genre_list.append(genre)
                    else:
                        genre_list.append(genre)
                elif isinstance(genre, dict) and 'name' in genre:
                    genre_list.append(genre['name'])
                else:
                    genre_list.append(str(genre))
        
        redis_movie["genres"] = "|".join([str(g) for g in genre_list if g])
            
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

        if rating is None:
            redis_movie["rating"] = "0"
        elif isinstance(rating, dict):
            # В MongoDB рейтинг хранится как словарь с разными источниками (kp, imdb, etc.)
            rating_value = 0
            rating_count = 0
            
            # Проверяем рейтинг Кинопоиска
            if "kp" in rating and rating["kp"] is not None:
                try:
                    kp_rating = float(rating["kp"])
                    if kp_rating > 0:
                        rating_value += kp_rating
                        rating_count += 1
                except (ValueError, TypeError):
                    pass
                    
            # Проверяем рейтинг IMDb
            if "imdb" in rating and rating["imdb"] is not None:
                try:
                    imdb_rating = float(rating["imdb"])
                    if imdb_rating > 0:
                        rating_value += imdb_rating
                        rating_count += 1
                except (ValueError, TypeError):
                    pass
            
            # Вычисляем средний рейтинг, если есть значения
            if rating_count > 0:
                redis_movie["rating"] = str(rating_value / rating_count)
            else:
                redis_movie["rating"] = "0"
        else:
            try:
                rating = float(rating)
                redis_movie["rating"] = str(rating)
            except (ValueError, TypeError):
                redis_movie["rating"] = "0"
        
        # Обработка постера
        poster = movie.get("poster", "")
        if poster is None:
            poster = ""
        
        # Если постер - строковое представление словаря, извлекаем из него URL
        if isinstance(poster, str) and poster.startswith("{") and poster.endswith("}") and "'url':" in poster:
            try:
                import ast
                poster_dict = ast.literal_eval(poster)
                if isinstance(poster_dict, dict) and 'url' in poster_dict:
                    redis_movie["poster"] = json.dumps(poster_dict)
                else:
                    redis_movie["poster"] = str(poster)
            except:
                redis_movie["poster"] = str(poster)
        elif isinstance(poster, dict) and 'url' in poster:
            redis_movie["poster"] = json.dumps(poster)
        else:
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
        
        # Преобразуем страны в список строк
        country_list = []
        if isinstance(countries, list):
            for country in countries:
                if isinstance(country, str):
                    # Проверяем, не является ли страна строковым представлением словаря
                    if country.startswith("{") and country.endswith("}") and "'name':" in country:
                        try:
                            # Пытаемся извлечь имя страны из строкового представления
                            import ast
                            country_dict = ast.literal_eval(country)
                            if isinstance(country_dict, dict) and 'name' in country_dict:
                                country_list.append(country_dict['name'])
                            else:
                                country_list.append(country)
                        except:
                            country_list.append(country)
                    else:
                        country_list.append(country)
                elif isinstance(country, dict) and 'name' in country:
                    country_list.append(country['name'])
                else:
                    country_list.append(str(country))
        
        redis_movie["countries"] = "|".join([str(c) for c in country_list if c])
            
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
        
        # Обработка альтернативного названия
        alternative_name = movie.get("alternativeName", "")
        if alternative_name is None:
            alternative_name = ""
        redis_movie["alternativeName"] = str(alternative_name)
        
        return redis_movie

    def _index_movie(self, redis_id, redis_movie):
        """Индексирует фильм для быстрого поиска."""
        if not self.redis_client:
            return
            
        try:
            # Индексируем по жанрам
            genres = redis_movie.get("genres", "")
            if genres:
                for genre in genres.split("|"):
                    if genre:
                        self.redis_client.sadd(f"genre:{genre.lower()}", redis_id)
            
            # Индексируем по году
            year = redis_movie.get("year")
            if year:
                self.redis_client.sadd(f"year:{year}", redis_id)
            
            # Индексируем по типу
            movie_type = redis_movie.get("type")
            if movie_type:
                self.redis_client.sadd(f"type:{movie_type.lower()}", redis_id)
                
            # Индексируем по странам
            countries = redis_movie.get("countries", "")
            if countries:
                for country in countries.split("|"):
                    if country:
                        self.redis_client.sadd(f"country:{country.lower()}", redis_id)
                        
            # Индексируем по категории
            category = redis_movie.get("category")
            if category:
                self.redis_client.sadd(f"category:{category.lower()}", redis_id)
        except Exception as e:
            print(f"❌ Ошибка при индексации фильма {redis_id}: {str(e)}")

    def _index_movie_pipeline(self, pipeline, redis_id, redis_movie):
        """Индексирует фильм для быстрого поиска с использованием pipeline."""
        try:
            # Индексируем по жанрам
            genres = redis_movie.get("genres", "")
            if genres:
                if isinstance(genres, str):
                    # Если жанры - строка, разделяем по символу |
                    genre_list = genres.split("|")
                elif isinstance(genres, list):
                    # Если жанры - список, используем его напрямую
                    genre_list = genres
                else:
                    genre_list = []
                
                for genre in genre_list:
                    if genre:
                        pipeline.sadd(f"genre:{str(genre).lower()}", redis_id)
            
            # Индексируем по году
            year = redis_movie.get("year")
            if year:
                pipeline.sadd(f"year:{year}", redis_id)
            
            # Индексируем по типу
            movie_type = redis_movie.get("type")
            if movie_type:
                pipeline.sadd(f"type:{str(movie_type).lower()}", redis_id)
                
            # Индексируем по странам
            countries = redis_movie.get("countries", "")
            if countries:
                if isinstance(countries, str):
                    # Если страны - строка, разделяем по символу |
                    country_list = countries.split("|")
                elif isinstance(countries, list):
                    # Если страны - список, используем его напрямую
                    country_list = countries
                else:
                    country_list = []
                
                for country in country_list:
                    if country:
                        pipeline.sadd(f"country:{str(country).lower()}", redis_id)
                        
            # Индексируем по категории
            category = redis_movie.get("category")
            if category:
                pipeline.sadd(f"category:{str(category).lower()}", redis_id)
        except Exception as e:
            print(f"❌ Ошибка при индексации фильма {redis_id} в pipeline: {str(e)}")

    @redis_error_handler
    def search_movies(self, query="", genre=None, year=None, movie_type=None, country=None, category=None):
        """Поиск фильмов в Redis с использованием RediSearch."""
        start_time = time.time()
        
        if not self.redis_client:
            print("⚠️ Соединение с Redis не установлено")
            return []
            
        # Преобразуем все параметры в строки
        query = str(query) if query is not None else ""
        genre = str(genre) if genre is not None else ""
        year = str(year) if year is not None else ""
        movie_type = str(movie_type) if movie_type is not None else ""
        country = str(country) if country is not None else ""
        category = str(category) if category is not None else ""
        
        print(f"🔍 Поиск: query='{query}', genre='{genre}', year='{year}', type='{movie_type}', country='{country}', category='{category}'")
        
        # Получаем ключи фильмов
        movie_keys = set()
        
        # Если есть фильтры, используем индексы
        if genre or year or movie_type or country or category:
            # Получаем фильмы по жанру
            if genre:
                genre_keys = self.redis_client.smembers(f"genre:{genre.lower()}") or set()
                if not movie_keys:
                    movie_keys = genre_keys
                else:
                    movie_keys &= genre_keys
            
            # Получаем фильмы по году
            if year:
                year_keys = self.redis_client.smembers(f"year:{year}") or set()
                if not movie_keys:
                    movie_keys = year_keys
                else:
                    movie_keys &= year_keys
            
            # Получаем фильмы по типу
            if movie_type:
                type_keys = self.redis_client.smembers(f"type:{movie_type.lower()}") or set()
                if not movie_keys:
                    movie_keys = type_keys
                else:
                    movie_keys &= type_keys
                    
            # Получаем фильмы по стране
            if country:
                country_keys = self.redis_client.smembers(f"country:{country.lower()}") or set()
                if not movie_keys:
                    movie_keys = country_keys
                else:
                    movie_keys &= country_keys
                    
            # Получаем фильмы по категории
            if category:
                category_keys = self.redis_client.smembers(f"category:{category.lower()}") or set()
                if not movie_keys:
                    movie_keys = category_keys
                else:
                    movie_keys &= category_keys
        
        # Результаты поиска
        results = []
        
        # Если есть поисковый запрос, используем RediSearch
        if query:
            try:
                # Формируем запрос для RediSearch
                search_query = query.replace('-', ' ').replace('_', ' ')
                
                # Если есть фильтры и уже получены ключи фильмов, добавляем их в запрос
                if movie_keys:
                    # Преобразуем ключи в список ID для фильтрации
                    movie_ids = [key.replace("movie:", "") for key in movie_keys]
                    
                    # Выполняем поиск с фильтрацией по ID
                    # Используем @id:{id1 | id2 | ...} для фильтрации по ID
                    id_filter = " | ".join(movie_ids)
                    ft_query = f"{search_query}"
                    
                    # Выполняем поиск с ограничением по количеству результатов
                    search_results = self.redis_client.execute_command(
                        "FT.SEARCH", "movie_idx", ft_query, 
                        "LIMIT", "0", "100",
                        "RETURN", "3", "id", "name", "rating"
                    )
                else:
                    # Выполняем поиск без фильтрации
                    search_results = self.redis_client.execute_command(
                        "FT.SEARCH", "movie_idx", search_query, 
                        "LIMIT", "0", "100",
                        "RETURN", "3", "id", "name", "rating"
                    )
                
                # Обрабатываем результаты поиска
                if search_results and len(search_results) > 0:
                    # Первый элемент - количество результатов
                    total_results = search_results[0]
                    
                    # Остальные элементы - ключи и данные
                    for i in range(1, len(search_results), 2):
                        if i + 1 < len(search_results):
                            key = search_results[i]  # Ключ фильма
                            movie_data = search_results[i + 1]  # Данные фильма
                            
                            # Преобразуем список в словарь
                            movie_dict = {}
                            for j in range(0, len(movie_data), 2):
                                if j + 1 < len(movie_data):
                                    movie_dict[movie_data[j]] = movie_data[j + 1]
                            
                            # Получаем ID фильма
                            movie_id = key.replace("movie:", "")
                            
                            # Загружаем полные данные фильма
                            full_movie = self.get_movie_by_id(movie_id)
                            if full_movie:
                                results.append(full_movie)
            except Exception as e:
                print(f"❌ Ошибка при поиске через RediSearch: {str(e)}")
                
                # Если поиск через RediSearch не удался, используем обычный поиск
                # Если нет фильтров или результат пуст, получаем все фильмы
                if not movie_keys:
                    movie_keys = self.redis_client.keys("movie:*") or []
                
                print(f"📊 Найдено {len(movie_keys)} фильмов по фильтрам")
                
                # Фильтруем по поисковому запросу (старый метод)
                for key in movie_keys:
                    try:
                        # Получаем только необходимые поля фильма для поиска
                        movie_data = self.redis_client.hmget(key, "name", "description", "shortDescription")
                        if not movie_data or not any(movie_data):
                            continue
                        
                        # Распаковываем данные
                        name, description, short_description = movie_data
                        
                        # Проверяем соответствие поисковому запросу
                        name = str(name or "").lower()
                        description = str(description or "").lower()
                        short_description = str(short_description or "").lower()
                        query_lower = query.lower()
                        
                        if query_lower in name or query_lower in description or query_lower in short_description:
                            # Загружаем полные данные фильма
                            movie_id = key.replace("movie:", "")
                            full_movie = self.get_movie_by_id(movie_id)
                            if full_movie:
                                results.append(full_movie)
                    except Exception as e:
                        print(f"❌ Ошибка при обработке фильма {key}: {str(e)}")
                        continue
        else:
            # Если нет поискового запроса, просто загружаем фильмы по фильтрам
            for key in movie_keys:
                try:
                    # Загружаем полные данные фильма
                    movie_id = key.replace("movie:", "")
                    full_movie = self.get_movie_by_id(movie_id)
                    if full_movie:
                        results.append(full_movie)
                except Exception as e:
                    print(f"❌ Ошибка при обработке фильма {key}: {str(e)}")
                    continue
        
        # Сортируем результаты по рейтингу (от большего к меньшему)
        results.sort(key=lambda x: x.get("rating", 0), reverse=True)
        
        elapsed_time = time.time() - start_time
        print(f"✅ Найдено {len(results)} фильмов за {elapsed_time:.2f} сек")
        
        # Ограничиваем количество результатов
        return results[:30]

    def _convert_redis_to_movie(self, key, movie_data):
        """Преобразует данные из Redis в словарь Python."""
        return self._prepare_movie_for_client(key, movie_data)
        
    def _prepare_movie_for_client(self, key, movie_data):
        """Подготавливает данные фильма для клиента."""
        result = {}
        
        # ID фильма (без префикса "movie:")
        result["id"] = key.replace("movie:", "")
        
        # Название
        result["name"] = str(movie_data.get("name", "Без названия") or "Без названия")
        
        # Обработка года
        try:
            year_value = movie_data.get("year")
            if year_value is None or year_value == "":
                result["year"] = 2000
            else:
                result["year"] = int(year_value)
        except (ValueError, TypeError):
            result["year"] = 2000
            
        # Обработка жанров
        genres = movie_data.get("genres", "")
        if genres is None:
            result["genres"] = []
        elif isinstance(genres, str):
            genre_list = [g for g in genres.split("|") if g]
            result["genres"] = []
            for genre in genre_list:
                # Проверяем, не является ли жанр строковым представлением словаря
                if genre.startswith("{") and genre.endswith("}") and "'name':" in genre:
                    try:
                        # Пытаемся извлечь имя жанра из строкового представления
                        import ast
                        genre_dict = ast.literal_eval(genre)
                        if isinstance(genre_dict, dict) and 'name' in genre_dict:
                            result["genres"].append(genre_dict['name'])
                        else:
                            result["genres"].append(genre)
                    except:
                        result["genres"].append(genre)
                else:
                    result["genres"].append(genre)
        else:
            result["genres"] = []
            
        # Обработка типа
        result["type"] = str(movie_data.get("type", "movie") or "movie")
        
        # Обработка описаний
        result["description"] = str(movie_data.get("description", "") or "")
        result["shortDescription"] = str(movie_data.get("shortDescription", "") or "")
        
        # Если описание отсутствует, используем короткое описание и наоборот
        if not result["description"] and result["shortDescription"]:
            result["description"] = result["shortDescription"]
        elif not result["shortDescription"] and result["description"]:
            result["shortDescription"] = result["description"]
            
        # Обработка альтернативного названия
        result["alternativeName"] = str(movie_data.get("alternativeName", "") or "")
        
        # Обработка рейтинга
        rating_value = movie_data.get("rating", 0)
        if rating_value is None:
            result["rating"] = 0.0
        else:
            try:
                result["rating"] = float(rating_value)
            except (ValueError, TypeError):
                result["rating"] = 0.0
            
        # Обработка постера
        poster = str(movie_data.get("poster", "") or "")
        if not poster:
            result["poster"] = "/static/default-poster.jpg"
        else:
            # Проверяем, не является ли постер строковым представлением словаря
            if poster.startswith("{") and poster.endswith("}") and ("'url':" in poster or '"url":' in poster):
                try:
                    # Пытаемся извлечь URL превью постера из строкового представления
                    import ast
                    import json
                    
                    # Пробуем распарсить как JSON, если не получается, используем ast
                    try:
                        poster_dict = json.loads(poster.replace("'", '"'))
                    except:
                        poster_dict = ast.literal_eval(poster)
                        
                    if isinstance(poster_dict, dict):
                        if 'previewUrl' in poster_dict:
                            result["poster"] = poster_dict['previewUrl']
                        elif 'url' in poster_dict:
                            result["poster"] = poster_dict['url']
                        else:
                            result["poster"] = poster
                    else:
                        result["poster"] = poster
                except:
                    result["poster"] = poster
            else:
                result["poster"] = poster
            
        # Дополнительные поля
        result["status"] = str(movie_data.get("status", "") or "")
        result["ageRating"] = str(movie_data.get("ageRating", "") or "")
        
        # Обработка стран
        countries = movie_data.get("countries", "")
        if countries is None:
            result["countries"] = []
        elif isinstance(countries, str):
            country_list = [c for c in countries.split("|") if c]
            result["countries"] = []
            for country in country_list:
                # Проверяем, не является ли страна строковым представлением словаря
                if country.startswith("{") and country.endswith("}") and "'name':" in country:
                    try:
                        # Пытаемся извлечь имя страны из строкового представления
                        import ast
                        country_dict = ast.literal_eval(country)
                        if isinstance(country_dict, dict) and 'name' in country_dict:
                            result["countries"].append(country_dict['name'])
                        else:
                            result["countries"].append(country)
                    except:
                        result["countries"].append(country)
                else:
                    result["countries"].append(country)
        else:
            result["countries"] = []
            
        # Обработка releaseYear
        try:
            release_year_value = movie_data.get("releaseYear")
            if release_year_value is None or release_year_value == "":
                result["releaseYear"] = result["year"]
            else:
                result["releaseYear"] = int(release_year_value)
        except (ValueError, TypeError):
            result["releaseYear"] = result["year"]
            
        # Обработка isSeries
        is_series_value = movie_data.get("isSeries")
        if is_series_value is None:
            result["isSeries"] = False
        else:
            result["isSeries"] = str(is_series_value) == "1"
            
        # Обработка категории
        result["category"] = str(movie_data.get("category", "") or "")
        
        # Обработка альтернативного названия
        result["alternativeName"] = str(movie_data.get("alternativeName", "") or "")
        
        return result

    @redis_error_handler
    def get_movie_by_id(self, movie_id):
        """Получает фильм из Redis по ID."""
        if not self.redis_client:
            return None
            
        # Преобразуем ID в строку и добавляем префикс "movie:"
        redis_id = f"movie:{movie_id}"
        
        # Получаем фильм из Redis
        movie_data = self.redis_client.hgetall(redis_id)
        
        if not movie_data:
            return None
            
        # Преобразуем данные из Redis в словарь Python
        movie = self._convert_redis_to_movie(redis_id, movie_data)
        
        return movie
        
    @redis_error_handler
    def get_all_movies(self):
        """Получает список всех фильмов из Redis."""
        if not self.redis_client:
            return []
            
        # Получаем все ключи фильмов с префиксом "movie:"
        movie_keys = self.redis_client.keys("movie:*")
        
        if not movie_keys:
            return []
            
        # Получаем данные для каждого фильма
        movies = []
        for key in movie_keys:
            movie_data = self.redis_client.hgetall(key)
            if movie_data:
                movie = self._convert_redis_to_movie(key, movie_data)
                movies.append(movie)
                
        return movies

    @redis_error_handler
    def get_all_genres(self):
        """Получает список всех жанров из Redis."""
        if not self.redis_client:
            print("⚠️ Соединение с Redis не установлено")
            return []
            
        genre_keys = self.redis_client.keys("genre:*") or []
        genres = []
        
        for key in genre_keys:
            if key and isinstance(key, str):
                parts = key.split(":")
                if len(parts) > 1 and parts[1]:
                    genres.append(parts[1].capitalize())
        
        return sorted(genres)

    @redis_error_handler
    def get_all_countries(self):
        """Получает список всех стран из Redis."""
        if not self.redis_client:
            print("⚠️ Соединение с Redis не установлено")
            return []
            
        country_keys = self.redis_client.keys("country:*") or []
        countries = []
        
        for key in country_keys:
            if key and isinstance(key, str):
                parts = key.split(":")
                if len(parts) > 1 and parts[1]:
                    countries.append(parts[1].capitalize())
        
        return sorted(countries)

    @redis_error_handler
    def get_all_categories(self):
        """Получает список всех категорий из Redis."""
        if not self.redis_client:
            print("⚠️ Соединение с Redis не установлено")
            return []
            
        category_keys = self.redis_client.keys("category:*") or []
        categories = []
        
        for key in category_keys:
            if key and isinstance(key, str):
                parts = key.split(":")
                if len(parts) > 1 and parts[1]:
                    categories.append(parts[1].capitalize())
        
        return sorted(categories)

    @redis_error_handler
    def flush_db(self):
        """Полностью очищает базу Redis."""
        if not self.redis_client:
            print("⚠️ Соединение с Redis не установлено")
            return False
            
        self.redis_client.flushdb()
        print("🗑️ База данных Redis очищена!")
        return True
        
    @redis_error_handler
    def load_from_mongodb(self, mongo_client):
        """Загружает данные из MongoDB в Redis."""
        if not self.redis_client:
            print("⚠️ Соединение с Redis не установлено")
            return 0
            
        print("🔄 Начинаю загрузку данных из MongoDB в Redis...")
        
        # Получаем все фильмы из MongoDB
        movies = mongo_client.get_movies()
        
        if not movies:
            print("⚠️ В MongoDB нет фильмов для загрузки!")
            return 0
            
        # Очищаем Redis перед загрузкой
        self.flush_db()
        
        # Загружаем данные в Redis
        saved_count = self.save_movies_bulk(movies)
        
        print(f"✅ Загрузка завершена! Загружено {saved_count} фильмов из MongoDB в Redis.")
        return saved_count

    @redis_error_handler
    def like_movie(self, user_id, movie_id):
        """Добавляет фильм в список лайкнутых пользователем."""
        if not self.redis_client:
            print("⚠️ Соединение с Redis не установлено")
            return False
            
        if movie_id is None or movie_id == '':
            print("⚠️ Получен пустой ID фильма")
            return False
            
        # Обработка ID для совместимости между различными режимами поиска
        original_movie_id = str(movie_id)
        
        # Если ID не начинается с "movie:", добавляем префикс для хранения в Redis
        if not str(movie_id).startswith("movie:"):
            redis_id = f"movie:{movie_id}"
        else:
            redis_id = str(movie_id)
            movie_id = movie_id.replace("movie:", "")
            
        # Проверяем, существует ли фильм
        if not self.redis_client.exists(redis_id):
            print(f"⚠️ Фильм с ID {redis_id} не найден в Redis напрямую")
            
            # Если ID начинается с "temp_", это временный ID из поиска по описанию (FAISS)
            if str(original_movie_id).startswith("temp_") or str(movie_id).startswith("temp_"):
                # Создаем заглушку для фильма из FAISS
                print(f"⚠️ Создаем заглушку для фильма с ID {redis_id}")
                movie_name = original_movie_id.replace("temp_", "").replace("_", " ")
                if movie_name.startswith("movie:"):
                    movie_name = movie_name.replace("movie:", "")
                self.redis_client.hset(redis_id, "name", movie_name)
                self.redis_client.hset(redis_id, "year", "2023")
            else:
                # Попробуем найти фильм по другим форматам ID
                # Например, если ID содержит только цифры, попробуем найти фильм с таким ID
                if original_movie_id.isdigit():
                    # Проверяем, есть ли фильмы с ID, содержащим эти цифры
                    matching_keys = self.redis_client.keys(f"movie:*{original_movie_id}*")
                    if matching_keys:
                        # Используем первый найденный ключ
                        redis_id = matching_keys[0].decode('utf-8') if isinstance(matching_keys[0], bytes) else matching_keys[0]
                        movie_id = redis_id.replace("movie:", "")
                        print(f"✅ Найден фильм с похожим ID: {redis_id}")
                    else:
                        # Если фильм не найден, создаем заглушку
                        print(f"⚠️ Создаем заглушку для фильма с ID {redis_id}")
                        self.redis_client.hset(redis_id, "name", f"Фильм {original_movie_id}")
                        self.redis_client.hset(redis_id, "year", "2023")
                else:
                    # Создаем заглушку для фильма
                    print(f"⚠️ Создаем заглушку для фильма с ID {redis_id}")
                    self.redis_client.hset(redis_id, "name", f"Фильм {original_movie_id}")
                    self.redis_client.hset(redis_id, "year", "2023")
            
        # Добавляем фильм в список лайкнутых пользователем
        user_likes_key = f"user:{user_id}:likes"
        self.redis_client.sadd(user_likes_key, movie_id)
        
        # Добавляем пользователя в список тех, кто лайкнул фильм
        movie_likes_key = f"{redis_id}:liked_by"
        self.redis_client.sadd(movie_likes_key, user_id)
        
        print(f"✅ Пользователь {user_id} лайкнул фильм {movie_id}")
        return True
        
    @redis_error_handler
    def unlike_movie(self, user_id, movie_id):
        """Удаляет фильм из списка лайкнутых пользователем."""
        if not self.redis_client:
            print("⚠️ Соединение с Redis не установлено")
            return False
            
        if movie_id is None or movie_id == '':
            print("⚠️ Получен пустой ID фильма")
            return False
            
        # Обработка ID для совместимости между различными режимами поиска
        original_movie_id = str(movie_id)
            
        # Если ID начинается с "movie:", удаляем префикс
        if str(movie_id).startswith("movie:"):
            movie_id = str(movie_id).replace("movie:", "")
            redis_id = f"movie:{movie_id}"
        else:
            redis_id = f"movie:{movie_id}"
            
        # Удаляем фильм из списка лайкнутых пользователем
        user_likes_key = f"user:{user_id}:likes"
        self.redis_client.srem(user_likes_key, movie_id)
        
        # Удаляем пользователя из списка тех, кто лайкнул фильм
        movie_likes_key = f"{redis_id}:liked_by"
        self.redis_client.srem(movie_likes_key, user_id)
        
        print(f"✅ Пользователь {user_id} удалил лайк фильма {movie_id}")
        return True
        
    @redis_error_handler
    def get_user_liked_movies(self, user_id):
        """Возвращает список фильмов, лайкнутых пользователем."""
        if not self.redis_client:
            print("⚠️ Соединение с Redis не установлено")
            return []
            
        # Получаем ID лайкнутых фильмов
        user_likes_key = f"user:{user_id}:likes"
        movie_ids = self.redis_client.smembers(user_likes_key)
        
        if not movie_ids:
            return []
            
        # Получаем данные фильмов
        movies = []
        for movie_id in movie_ids:
            movie = self.get_movie_by_id(movie_id)
            if movie:
                movies.append(movie)
                
        return movies
        
    @redis_error_handler
    def is_movie_liked(self, user_id, movie_id):
        """Проверяет, лайкнул ли пользователь фильм."""
        if not self.redis_client:
            print("⚠️ Соединение с Redis не установлено")
            return False
            
        if movie_id is None or movie_id == '':
            print("⚠️ Получен пустой ID фильма")
            return False
            
        # Если ID начинается с "movie:", удаляем префикс
        if str(movie_id).startswith("movie:"):
            movie_id = str(movie_id).replace("movie:", "")
            
        # Проверяем, есть ли фильм в списке лайкнутых пользователем
        user_likes_key = f"user:{user_id}:likes"
        return self.redis_client.sismember(user_likes_key, movie_id)
        
    @redis_error_handler
    def remove_all_likes(self, user_id):
        """Удаляет все лайки пользователя."""
        if not self.redis_client:
            print("⚠️ Соединение с Redis не установлено")
            return False
            
        try:
            # Получаем ID всех лайкнутых фильмов
            user_likes_key = f"user:{user_id}:likes"
            liked_movie_ids = self.redis_client.smembers(user_likes_key)
            
            if not liked_movie_ids:
                # Если пользователь ничего не лайкал, просто возвращаем True
                return True
                
            # Удаляем связи между пользователем и фильмами (лайки)
            pipeline = self.redis_client.pipeline()
            
            # Удаляем записи о лайках из множества лайков пользователя
            pipeline.delete(user_likes_key)
            
            # Удаляем записи о лайках из множеств лайков фильмов
            for movie_id in liked_movie_ids:
                movie_likes_key = f"movie:{movie_id}:likes"
                pipeline.srem(movie_likes_key, user_id)
                
            # Выполняем все операции в транзакции
            pipeline.execute()
            
            print(f"🗑️ Удалены все лайки пользователя {user_id}")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка при удалении всех лайков пользователя {user_id}: {str(e)}")
            return False
        
    @redis_error_handler
    def get_recommendations(self, liked_movie_ids, limit=10):
        """
        Возвращает рекомендации фильмов на основе списка идентификаторов лайкнутых фильмов.
        
        :param liked_movie_ids: Список идентификаторов лайкнутых фильмов
        :param limit: Максимальное количество рекомендаций
        :return: Список рекомендованных фильмов
        """
        if not self.redis_client:
            print("⚠️ Соединение с Redis не установлено")
            return []
            
        if not liked_movie_ids:
            # Если список лайкнутых фильмов пуст, возвращаем пустой список
            print("⚠️ Список лайкнутых фильмов пуст, рекомендации не формируются")
            return []
            
        # Собираем жанры и годы лайкнутых фильмов
        genres = set()
        years = set()
        
        for movie_id in liked_movie_ids:
            movie = self.get_movie_by_id(movie_id)
            if not movie:
                continue
                
            # Собираем жанры
            if "genres" in movie:
                if isinstance(movie["genres"], list):
                    for genre in movie["genres"]:
                        genres.add(genre.lower())
                elif isinstance(movie["genres"], str):
                    for genre in movie["genres"].split("|"):
                        if genre:
                            genres.add(genre.lower())
                            
            # Собираем годы
            if "year" in movie:
                years.add(str(movie["year"]))
                
        # Получаем фильмы по жанрам и годам
        movie_keys = set()
        
        # Получаем фильмы по жанрам
        for genre in genres:
            genre_keys = self.redis_client.smembers(f"genre:{genre}") or set()
            movie_keys.update(genre_keys)
            
        # Получаем фильмы по годам
        for year in years:
            year_keys = self.redis_client.smembers(f"year:{year}") or set()
            movie_keys.update(year_keys)
            
        # Исключаем уже лайкнутые фильмы
        liked_movie_keys = {f"movie:{movie_id}" for movie_id in liked_movie_ids}
        movie_keys = movie_keys - liked_movie_keys
        
        # Если нет рекомендаций, возвращаем популярные фильмы
        if not movie_keys:
            return self.get_popular_movies(limit)
            
        # Получаем данные фильмов
        recommendations = []
        for key in movie_keys:
            movie_id = key.replace("movie:", "")
            movie = self.get_movie_by_id(movie_id)
            if movie:
                recommendations.append(movie)
                
        # Сортируем по рейтингу
        def get_rating(movie):
            rating = movie.get("rating", 0)
            if isinstance(rating, dict) and "kp" in rating:
                return rating["kp"]
            elif isinstance(rating, (int, float)):
                return rating
            else:
                try:
                    return float(rating)
                except (ValueError, TypeError):
                    return 0
        
        recommendations.sort(key=get_rating, reverse=True)
        
        # Ограничиваем количество рекомендаций
        return recommendations[:limit]
        
    @redis_error_handler
    def get_popular_movies(self, limit=10):
        """Возвращает популярные фильмы."""
        if not self.redis_client:
            print("⚠️ Соединение с Redis не установлено")
            return []
            
        # Получаем все фильмы
        movie_keys = self.redis_client.keys("movie:*")
        
        if not movie_keys:
            return []
            
        # Получаем данные фильмов
        movies = []
        for key in movie_keys:
            movie_id = key.replace("movie:", "")
            movie = self.get_movie_by_id(movie_id)
            if movie:
                movies.append(movie)
                
        # Сортируем по рейтингу
        movies.sort(key=lambda x: x.get("rating", 0), reverse=True)
        
        # Ограничиваем количество фильмов
        return movies[:limit]

    @redis_error_handler
    def add_movie_comment(self, user_id, movie_id, comment_text):
        """Добавляет комментарий к фильму."""
        if not self.redis_client:
            print("⚠️ Соединение с Redis не установлено")
            return False
            
        if not user_id or not movie_id or not comment_text:
            print("⚠️ Неверные данные для добавления комментария")
            return False
            
        try:
            # Обработка ID для совместимости между различными режимами поиска
            if not str(movie_id).startswith("movie:"):
                redis_id = f"movie:{movie_id}"
            else:
                redis_id = str(movie_id)
                movie_id = movie_id.replace("movie:", "")
                
            # Создаем уникальный ID для комментария
            comment_id = self.redis_client.incr(f"next_comment_id")
            
            # Формируем данные комментария
            comment_data = {
                "id": comment_id,
                "user_id": user_id,
                "movie_id": movie_id,
                "text": comment_text,
                "created_at": int(time.time())
            }
            
            # Сохраняем комментарий
            comment_key = f"comment:{comment_id}"
            self.redis_client.hset(comment_key, mapping=comment_data)
            
            # Добавляем ID комментария в список комментариев к фильму
            self.redis_client.zadd(f"{redis_id}:comments", {comment_id: int(time.time())})
            
            # Добавляем ID комментария в список комментариев пользователя
            self.redis_client.zadd(f"user:{user_id}:comments", {comment_id: int(time.time())})
            
            print(f"✅ Пользователь {user_id} добавил комментарий к фильму {movie_id}")
            return True
        except Exception as e:
            print(f"❌ Ошибка при добавлении комментария: {str(e)}")
            return False
            
    @redis_error_handler
    def get_movie_comments(self, movie_id, count=20):
        """Получает комментарии к фильму."""
        if not self.redis_client:
            print("⚠️ Соединение с Redis не установлено")
            return []
            
        if not movie_id:
            print("⚠️ Неверный ID фильма")
            return []
            
        try:
            # Обработка ID для совместимости между различными режимами поиска
            if not str(movie_id).startswith("movie:"):
                redis_id = f"movie:{movie_id}"
            else:
                redis_id = str(movie_id)
                
            # Получаем ID комментариев к фильму (сортированных по времени, от новых к старым)
            comment_ids = self.redis_client.zrevrange(f"{redis_id}:comments", 0, count - 1)
            
            comments = []
            for comment_id in comment_ids:
                comment_data = self.redis_client.hgetall(f"comment:{comment_id}")
                if comment_data:
                    # Получаем данные пользователя для отображения имени
                    user_id = comment_data.get("user_id")
                    user_name = "Пользователь"  # Имя по умолчанию
                    
                    # Преобразуем временную метку в читаемый формат
                    created_at = int(comment_data.get("created_at", 0))
                    formatted_date = time.strftime("%d.%m.%Y %H:%M", time.localtime(created_at))
                    
                    comments.append({
                        "id": comment_data.get("id"),
                        "user_id": user_id,
                        "user": user_name,
                        "text": comment_data.get("text", ""),
                        "date": formatted_date,
                        "created_at": created_at
                    })
            
            return comments
        except Exception as e:
            print(f"❌ Ошибка при получении комментариев: {str(e)}")
            return []
            
    @redis_error_handler
    def rate_movie(self, user_id, movie_id, rating):
        """Оценивает фильм пользователем."""
        if not self.redis_client:
            print("⚠️ Соединение с Redis не установлено")
            return False
            
        if not user_id or not movie_id:
            print("⚠️ Неверные данные для оценки фильма")
            return False
            
        try:
            # Проверяем валидность рейтинга (от 1 до 5)
            rating = int(rating)
            if rating < 1 or rating > 5:
                print(f"⚠️ Некорректный рейтинг: {rating}. Должен быть от 1 до 5.")
                return False
                
            # Обработка ID для совместимости между различными режимами поиска
            if not str(movie_id).startswith("movie:"):
                redis_id = f"movie:{movie_id}"
            else:
                redis_id = str(movie_id)
                movie_id = movie_id.replace("movie:", "")
                
            # Сохраняем рейтинг пользователя для фильма
            user_rating_key = f"user:{user_id}:ratings"
            self.redis_client.hset(user_rating_key, movie_id, rating)
            
            # Обновляем усредненный рейтинг фильма
            movie_ratings_key = f"{redis_id}:ratings"
            
            # Добавляем рейтинг в список рейтингов
            self.redis_client.hset(movie_ratings_key, user_id, rating)
            
            # Вычисляем средний рейтинг
            all_ratings = self.redis_client.hvals(movie_ratings_key)
            if all_ratings:
                avg_rating = sum(float(r) for r in all_ratings) / len(all_ratings)
                # Обновляем средний рейтинг в данных фильма
                self.redis_client.hset(redis_id, "user_rating", f"{avg_rating:.1f}")
                
            print(f"✅ Пользователь {user_id} оценил фильм {movie_id} на {rating}")
            return True
        except Exception as e:
            print(f"❌ Ошибка при оценке фильма: {str(e)}")
            return False
            
    @redis_error_handler
    def get_user_movie_rating(self, user_id, movie_id):
        """Получает оценку пользователя для фильма."""
        if not self.redis_client:
            print("⚠️ Соединение с Redis не установлено")
            return None
            
        if not user_id or not movie_id:
            print("⚠️ Неверные данные для получения оценки")
            return None
            
        try:
            # Обработка ID
            if str(movie_id).startswith("movie:"):
                movie_id = movie_id.replace("movie:", "")
                
            # Получаем рейтинг пользователя для фильма
            user_rating_key = f"user:{user_id}:ratings"
            rating = self.redis_client.hget(user_rating_key, movie_id)
            
            if rating:
                return int(rating)
            return None
        except Exception as e:
            print(f"❌ Ошибка при получении оценки: {str(e)}")
            return None
            
    @redis_error_handler
    def get_movie_avg_rating(self, movie_id):
        """Получает средний рейтинг фильма."""
        if not self.redis_client:
            print("⚠️ Соединение с Redis не установлено")
            return 0
            
        if not movie_id:
            print("⚠️ Неверный ID фильма")
            return 0
            
        try:
            # Обработка ID
            if not str(movie_id).startswith("movie:"):
                redis_id = f"movie:{movie_id}"
            else:
                redis_id = str(movie_id)
                
            # Сначала пытаемся получить сохраненный средний рейтинг
            saved_rating = self.redis_client.hget(redis_id, "user_rating")
            if saved_rating:
                return float(saved_rating)
                
            # Если сохраненного рейтинга нет, вычисляем
            movie_ratings_key = f"{redis_id}:ratings"
            all_ratings = self.redis_client.hvals(movie_ratings_key)
            
            if all_ratings:
                avg_rating = sum(float(r) for r in all_ratings) / len(all_ratings)
                # Сохраняем средний рейтинг для будущих запросов
                self.redis_client.hset(redis_id, "user_rating", f"{avg_rating:.1f}")
                return avg_rating
            
            return 0
        except Exception as e:
            print(f"❌ Ошибка при получении среднего рейтинга: {str(e)}")
            return 0
            
    @redis_error_handler
    def get_similar_movies(self, movie_id, count=6):
        """Получает похожие фильмы для указанного фильма."""
        if not self.redis_client:
            print("⚠️ Соединение с Redis не установлено")
            return []
            
        if not movie_id:
            print("⚠️ Неверный ID фильма")
            return []
            
        try:
            # Обработка ID
            if not str(movie_id).startswith("movie:"):
                redis_id = f"movie:{movie_id}"
            else:
                redis_id = str(movie_id)
                movie_id = movie_id.replace("movie:", "")
                
            # Получаем данные текущего фильма
            movie_data = self.redis_client.hgetall(redis_id)
            if not movie_data:
                print(f"⚠️ Фильм с ID {redis_id} не найден")
                return []
                
            # Извлекаем жанры фильма
            genres = movie_data.get("genres", "").split("|") if movie_data.get("genres") else []
            year = movie_data.get("year")
            
            similar_movie_keys = set()
            
            # Ищем фильмы с похожими жанрами
            for genre in genres:
                if not genre:
                    continue
                genre_keys = self.redis_client.smembers(f"genre:{genre.lower()}")
                similar_movie_keys.update(genre_keys)
                
            # Удаляем текущий фильм из списка похожих
            similar_movie_keys.discard(redis_id)
            
            # Если список пуст, берем фильмы из того же года
            if not similar_movie_keys and year:
                year_keys = self.redis_client.smembers(f"year:{year}")
                similar_movie_keys.update(year_keys)
                similar_movie_keys.discard(redis_id)
                
            # Если все еще пусто, берем случайные фильмы
            if not similar_movie_keys:
                all_keys = list(self.redis_client.keys("movie:*"))
                all_keys.remove(redis_id) if redis_id in all_keys else None
                import random
                random.shuffle(all_keys)
                similar_movie_keys = set(all_keys[:min(count, len(all_keys))])
                
            # Преобразуем ключи в данные фильмов
            similar_movies = []
            for key in list(similar_movie_keys)[:count]:
                movie = self._prepare_movie_for_client(key, self.redis_client.hgetall(key))
                if movie:
                    similar_movies.append(movie)
                    
            return similar_movies
        except Exception as e:
            print(f"❌ Ошибка при получении похожих фильмов: {str(e)}")
            return []
