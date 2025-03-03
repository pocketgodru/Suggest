import requests
import json
import time
import argparse
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from collections import defaultdict
from typing import Dict, List, Optional, Set
from datetime import datetime
import sys

# Константы
API_BASE_URL = "https://api.kinopoisk.dev/v1.4"
API_GENRES_URL = f"{API_BASE_URL}/movie/possible-values-by-field"
API_MOVIES_URL = f"{API_BASE_URL}/movie"
MAX_REQUESTS_PER_KEY = 5
REQUEST_DELAY = 0.5
DEFAULT_LIMIT = 250

# Список API-ключей
API_KEYS = [
    "YZXC9V2-8FMMZFR-P1W7JS7-B8W6JM2",
    "DKVQN04-29V44MD-JRSKAYM-7G1B8XP",
    "SRQBFFF-ZWD4J9Y-HE99ABA-45YDAHM",
    "GR9DGVD-FDZ45MK-G9P976P-K5TEJB0",
    "7QJJV3T-A2T4KR0-KTDFZJM-J13T7NX",
    "AZ94M48-EM344YZ-P2JB02A-40VX0EY",
    "PJVA2E4-P1QMS61-JFG04QA-FG6Y0WA",
    "B3JZFMX-1A8MR6J-HWV0XEF-3S3FPVQ",
    "NFA6FC2-38B4QSX-PS2GVCQ-HCQBQ8R",
]

# Функция для получения фильмов по жанру
def get_movies_by_genre(
    genre: str,
    max_pages: int,
    limit: int = DEFAULT_LIMIT,
    api_key_index: int = 0,
    progress_bar: Optional[tqdm] = None
) -> List[Dict]:
    """
    Получает фильмы по указанному жанру.
    
    Args:
        genre: Название жанра
        max_pages: Максимальное количество страниц для получения
        limit: Количество фильмов на странице
        api_key_index: Индекс API ключа для использования
        progress_bar: Объект прогресс-бара для отображения прогресса
    
    Returns:
        List[Dict]: Список фильмов
    """
    url = API_MOVIES_URL
    headers = {
        "accept": "application/json",
        "X-API-KEY": API_KEYS[api_key_index]
    }
    movies = []
    
    for page in range(1, max_pages + 1):
        try:
            params = {
                "page": page,
                "limit": limit,
                "genres.name": genre,
                "year": "!2025",
                "notNullFields": "description"
            }
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()["docs"]
            movies.extend(data)
            
            if progress_bar:
                progress_bar.update(1)
                
        except requests.exceptions.RequestException as e:
            print(f"[API-{api_key_index}] Ошибка при получении фильмов для жанра {genre} на странице {page}: {e}")
            break
        except json.JSONDecodeError as e:
            print(f"[API-{api_key_index}] Ошибка при разборе JSON для жанра {genre} на странице {page}: {e}")
            break
            
        time.sleep(REQUEST_DELAY)
    
    return movies

# Функция для получения списка жанров
def get_all_genres() -> List[Dict]:
    """
    Получает список всех доступных жанров.
    
    Returns:
        List[Dict]: Список жанров
    """
    headers = {
        "accept": "application/json",
        "X-API-KEY": API_KEYS[0]
    }
    
    try:
        response = requests.get(API_GENRES_URL, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при получении списка жанров: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"Ошибка при разборе JSON списка жанров: {e}")
        return []

# Функция для многопоточной выгрузки фильмов
def fetch_movies_parallel(genres, max_pages=150, limit=250):
    all_movies = {}
    total_pages = len(genres) * max_pages  # Общее количество страниц для всех жанров
    with tqdm(total=total_pages, desc="Загрузка страниц", unit="стр.") as progress_bar:
        with ThreadPoolExecutor(max_workers=len(API_KEYS)) as executor:
            # Назначаем API-ключи по очереди
            future_to_genre = {
                executor.submit(get_movies_by_genre, genre["name"], max_pages, limit, idx % len(API_KEYS), progress_bar): genre["name"]
                for idx, genre in enumerate(genres)
            }
            for future in future_to_genre:
                genre_name = future_to_genre[future]
                try:
                    movies = future.result()
                    if genre_name in all_movies:
                        # Если жанр уже есть, объединяем фильмы, избегая дубликатов
                        existing_ids = {movie["id"] for movie in all_movies[genre_name]}
                        new_movies = [movie for movie in movies if movie["id"] not in existing_ids]
                        all_movies[genre_name].extend(new_movies)
                    else:
                        all_movies[genre_name] = movies
                except Exception as e:
                    print(f"Ошибка при обработке жанра {genre_name}: {e}")
    return all_movies

TOTAL_KEYS = len(API_KEYS)  # Количество ключей
TOTAL_REQUESTS = MAX_REQUESTS_PER_KEY * TOTAL_KEYS  # Общий лимит запросов

def check_required_fields(movie: Dict) -> List[str]:
    """
    Проверяет наличие всех обязательных полей в фильме.
    
    Args:
        movie: Словарь с данными фильма
    
    Returns:
        List[str]: Список отсутствующих полей
    """
    required_fields = {
        'description': 'описания',
        'name': 'названия',
        'year': 'года'
    }
    
    missing_fields = []
    
    # Проверяем основные поля
    for field, field_name in required_fields.items():
        if field not in movie or not movie[field]:
            missing_fields.append(field_name)
    
    # Проверяем poster и previewUrl
    poster = movie.get('poster', {})
    if not isinstance(poster, dict) or not poster.get('previewUrl'):
        missing_fields.append('постера')
    
    return missing_fields

def remove_duplicates_and_invalid(data: Dict[str, List[Dict]], output_file: str) -> None:
    """
    Удаляет дубликаты и невалидные записи из данных фильмов.
    
    Args:
        data: Словарь с фильмами, сгруппированными по жанрам
        output_file: Путь к файлу для сохранения результатов
    """
    seen_movies: Set[int] = set()
    movies_by_genre: Dict[str, List[Dict]] = defaultdict(list)
    stats = {
        'duplicate_count': 0,
        'missing_fields': defaultdict(int),
        'total_removed': 0,
        'total_valid': 0
    }
    
    # Обрабатываем каждый жанр
    for genre, movies in data.items():
        for movie in movies:
            movie_id = movie['id']
            
            # Проверяем дубликаты
            if movie_id in seen_movies:
                stats['duplicate_count'] += 1
                continue
            
            # Проверяем обязательные поля
            missing_fields = check_required_fields(movie)
            if missing_fields:
                for field in missing_fields:
                    stats['missing_fields'][field] += 1
                stats['total_removed'] += 1
                continue
            
            # Фильм прошел все проверки
            seen_movies.add(movie_id)
            movies_by_genre[genre].append(movie)
            stats['total_valid'] += 1

    # Сохраняем результат
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(dict(movies_by_genre), f, ensure_ascii=False, indent=2)
        print(f"✅ Данные успешно сохранены в файл {output_file}")
    except IOError as e:
        print(f"❌ Ошибка при сохранении файла: {e}")
        return
    
    # Выводим подробную статистику
    print("\n📊 Общая статистика:")
    print(f"   • Всего фильмов обработано: {stats['total_valid'] + stats['total_removed']}")
    print(f"   • Сохранено фильмов: {stats['total_valid']}")
    print(f"   • Удалено фильмов: {stats['total_removed']}")
    print(f"   • Найдено дубликатов: {stats['duplicate_count']}")
    print(f"   • Количество жанров: {len(movies_by_genre)}")
    
    print("\n❌ Причины удаления:")
    for field, count in stats['missing_fields'].items():
        print(f"   • Отсутствует {field}: {count} фильмов")
    
    print("\n📋 Распределение по жанрам:")
    for genre, movies in movies_by_genre.items():
        print(f"   • {genre}: {len(movies)} фильмов")

def main():
    """
    Основная функция для запуска парсера с различными параметрами.
    """
    parser = argparse.ArgumentParser(
        description='Парсер фильмов с Кинопоиска',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--mode',
        choices=['fetch', 'clean', 'all'],
        default='all',
        help='Режим работы: fetch - только получение данных, clean - только очистка, all - полный цикл'
    )
    
    parser.add_argument(
        '--output',
        default=f'movies_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json',
        help='Путь к выходному файлу'
    )
    
    parser.add_argument(
        '--input',
        help='Путь к входному файлу для режима clean'
    )
    
    parser.add_argument(
        '--max-pages',
        type=int,
        default=150,
        help='Максимальное количество страниц для каждого жанра'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        default=250,
        help='Количество фильмов на странице'
    )
    
    args = parser.parse_args()
    
    try:
        if args.mode in ['fetch', 'all']:
            print("\n🎬 Начинаем получение данных с Кинопоиска...")
            print(f"📊 Параметры:")
            print(f"   • Максимум страниц на жанр: {args.max_pages}")
            print(f"   • Фильмов на странице: {args.limit}")
            print(f"   • Выходной файл: {args.output}")
            
            genres = get_all_genres()
            if not genres:
                print("❌ Не удалось получить список жанров")
                sys.exit(1)
                
            print(f"\n📋 Найдено жанров: {len(genres)}")
            data = fetch_movies_parallel(genres, max_pages=args.max_pages, limit=args.limit)
            
            if args.mode == 'fetch':
                # Сохраняем сырые данные
                with open(args.output, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                print(f"\n✅ Сырые данные сохранены в {args.output}")
                return
        
        if args.mode in ['clean', 'all']:
            print("\n🧹 Начинаем очистку данных...")
            
            # Определяем входной файл
            input_file = args.input if args.mode == 'clean' else args.output
            
            try:
                with open(input_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except FileNotFoundError:
                print(f"❌ Файл {input_file} не найден")
                sys.exit(1)
            except json.JSONDecodeError:
                print(f"❌ Ошибка при чтении JSON из файла {input_file}")
                sys.exit(1)
            
            remove_duplicates_and_invalid(data, args.output)
            
    except KeyboardInterrupt:
        print("\n⚠️ Программа прервана пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Произошла ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()


