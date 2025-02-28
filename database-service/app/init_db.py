#!/usr/bin/env python3
"""
Скрипт для инициализации базы данных MongoDB из файла movie.json.
Оптимизирован для работы с большими файлами.
"""
import json
import os
import sys
import time
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

# Получаем данные MongoDB из переменных окружения
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB = os.getenv("MONGO_DB", "movies_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "movies")
JSON_PATH = os.getenv("MOVIE_JSON_PATH", "/app/movie.json")
BATCH_SIZE = 1000  # Размер пакета для вставки данных

def main():
    print(f"🚀 Запуск инициализации базы данных из {JSON_PATH}")
    
    # Ждем, пока MongoDB станет доступной
    client = None
    max_attempts = 10
    attempts = 0
    
    while attempts < max_attempts:
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            print("✅ Успешное подключение к MongoDB")
            break
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            attempts += 1
            print(f"⏳ Попытка {attempts}/{max_attempts} подключения к MongoDB... ({str(e)})")
            time.sleep(3)
    
    if attempts == max_attempts:
        print("❌ Не удалось подключиться к MongoDB")
        sys.exit(1)
    
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    
    # Проверяем, есть ли уже данные в коллекции
    count = collection.count_documents({})
    if count > 0:
        print(f"ℹ️ База данных уже содержит {count} фильмов. Пропускаем инициализацию.")
        return
    
    # Проверяем наличие файла movie.json
    if not os.path.exists(JSON_PATH):
        print(f"❌ Файл {JSON_PATH} не найден")
        sys.exit(1)
    
    # Проверяем размер файла
    file_size = os.path.getsize(JSON_PATH)
    print(f"📊 Размер файла {JSON_PATH}: {file_size / (1024 * 1024):.2f} МБ")
    
    # Загружаем и обрабатываем данные построчно для экономии памяти
    print(f"📝 Чтение файла {JSON_PATH}...")
    
    try:
        # Первый проход: определяем, является ли файл массивом объектов или объектом категорий
        with open(JSON_PATH, 'r', encoding='utf-8') as f:
            first_char = f.read(1).strip()
        
        # Проверяем формат файла и выбираем стратегию обработки
        if first_char == '[':
            process_array_format(JSON_PATH, collection, file_size)
        elif first_char == '{':
            process_category_format(JSON_PATH, collection, file_size)
        else:
            print("❌ Неподдерживаемый формат файла")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Ошибка при обработке файла: {str(e)}")
        
        # Если возникла ошибка, создаем минимальный набор данных
        if collection.count_documents({}) == 0:
            test_movies = [
                {"name": "Пример фильма 1", "year": 2022, "genres": ["драма", "комедия"], "category": "фильм"},
                {"name": "Пример фильма 2", "year": 2021, "genres": ["боевик", "триллер"], "category": "фильм"},
                {"name": "Пример фильма 3", "year": 2020, "genres": ["фантастика", "приключения"], "category": "фильм"}
            ]
            collection.insert_many(test_movies)
            print(f"⚠️ Добавлены тестовые данные вместо полного файла")
        
        sys.exit(1)

def process_array_format(file_path, collection, file_size):
    """Обработка файла в формате массива объектов JSON"""
    print("🔄 Обработка файла в формате массива объектов...")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        # Пропускаем первый символ '['
        f.read(1)
        
        buffer = ""
        open_braces = 0
        in_string = False
        escape_next = False
        
        batch = []
        total_inserted = 0
        char_count = 0
        progress_interval = file_size // 10  # Для отображения прогресса
        
        # Читаем файл посимвольно
        print("🔄 Начинаем обработку данных...")
        while True:
            char = f.read(1)
            if not char:  # Конец файла
                break
            
            char_count += 1
            if char_count % progress_interval == 0:
                progress_percent = (char_count / file_size) * 100
                print(f"⏳ Обработано {progress_percent:.2f}% файла...")
            
            buffer += char
            
            # Логика для отслеживания строк и вложенных объектов
            if escape_next:
                escape_next = False
                continue
            
            if char == '\\':
                escape_next = True
                continue
            
            if char == '"' and not escape_next:
                in_string = not in_string
                continue
            
            if in_string:
                continue
            
            if char == '{':
                open_braces += 1
            elif char == '}':
                open_braces -= 1
                if open_braces == 0:  # Найден полный объект
                    try:
                        movie_obj = json.loads(buffer)
                        batch.append(movie_obj)
                        
                        # Вставляем пакет, если достигнут BATCH_SIZE
                        if len(batch) >= BATCH_SIZE:
                            collection.insert_many(batch)
                            total_inserted += len(batch)
                            print(f"✅ Добавлено {total_inserted} фильмов в базу данных")
                            batch = []
                        
                        # Очищаем буфер для следующего объекта
                        buffer = ""
                        
                        # Пропускаем запятую и пробелы
                        next_char = f.read(1)
                        while next_char in [',', ' ', '\n', '\r', '\t']:
                            next_char = f.read(1)
                        
                        # Если следующий символ не запятая, возвращаем указатель назад
                        if next_char and next_char not in [',', ' ', '\n', '\r', '\t', ']']:
                            f.seek(f.tell() - 1)
                        
                    except json.JSONDecodeError as e:
                        print(f"⚠️ Ошибка декодирования объекта: {str(e)}")
                        buffer = ""
        
        # Вставляем оставшиеся объекты
        if batch:
            collection.insert_many(batch)
            total_inserted += len(batch)
        
        print(f"✅ Всего добавлено {total_inserted} фильмов в базу данных")

def process_category_format(file_path, collection, file_size):
    """Обработка файла в формате объекта с категориями"""
    print("🔄 Обработка файла в формате объекта с категориями...")
    
    try:
        # Из-за структуры файла, мы не можем обработать его посимвольно
        # Будем делать это по частям, используя словарь Python
        with open(file_path, 'r', encoding='utf-8') as f:
            # Загружаем полный файл в память (это может быть ресурсоемко)
            print("⏳ Загрузка файла в память...")
            data = json.load(f)
        
        total_inserted = 0
        batch = []
        categories = list(data.keys())
        total_categories = len(categories)
        
        print(f"📋 Найдено {total_categories} категорий: {', '.join(categories)}")
        
        # Обрабатываем каждую категорию
        for i, category in enumerate(categories):
            print(f"⏳ Обработка категории {category} ({i+1}/{total_categories})...")
            movies = data[category]
            
            for movie in movies:
                # Добавляем категорию к фильму
                movie['category'] = category
                
                batch.append(movie)
                
                # Вставляем пакет, если достигнут BATCH_SIZE
                if len(batch) >= BATCH_SIZE:
                    collection.insert_many(batch)
                    total_inserted += len(batch)
                    print(f"✅ Добавлено {total_inserted} фильмов в базу данных")
                    batch = []
        
        # Вставляем оставшиеся объекты
        if batch:
            collection.insert_many(batch)
            total_inserted += len(batch)
        
        print(f"✅ Всего добавлено {total_inserted} фильмов в базу данных")
    
    except json.JSONDecodeError as e:
        print(f"❌ Ошибка при декодировании JSON: {str(e)}")
        print(f"⚠️ Попробуем альтернативный подход с потоковой обработкой...")
        process_category_format_streaming(file_path, collection, file_size)
    
    except Exception as e:
        print(f"❌ Ошибка при обработке файла категорий: {str(e)}")
        raise

def process_category_format_streaming(file_path, collection, file_size):
    """Альтернативный метод обработки файла категорий с меньшим потреблением памяти"""
    print("🔄 Использование потоковой обработки для файла категорий...")
    
    import ijson  # Убедитесь, что этот пакет установлен
    
    total_inserted = 0
    batch = []
    
    try:
        with open(file_path, 'rb') as f:
            # Получаем итератор для всех пар ключ-значение в корневом объекте
            parser = ijson.kvitems(f, '')
            
            for category, movies in parser:
                print(f"⏳ Обработка категории {category}...")
                
                for movie in movies:
                    # Добавляем категорию к фильму
                    movie['category'] = category
                    
                    batch.append(movie)
                    
                    # Вставляем пакет, если достигнут BATCH_SIZE
                    if len(batch) >= BATCH_SIZE:
                        collection.insert_many(batch)
                        total_inserted += len(batch)
                        print(f"✅ Добавлено {total_inserted} фильмов в базу данных")
                        batch = []
            
            # Вставляем оставшиеся объекты
            if batch:
                collection.insert_many(batch)
                total_inserted += len(batch)
            
            print(f"✅ Всего добавлено {total_inserted} фильмов в базу данных")
    
    except Exception as e:
        print(f"❌ Ошибка при потоковой обработке файла категорий: {str(e)}")
        
        if 'ijson' in str(e):
            print("⚠️ Модуль ijson не установлен. Устанавливаем тестовые данные...")
            # Создаем тестовые данные
            test_movies = [
                {"name": "Пример фильма 1", "year": 2022, "genres": ["драма", "комедия"], "category": "фильм"},
                {"name": "Пример фильма 2", "year": 2021, "genres": ["боевик", "триллер"], "category": "фильм"},
                {"name": "Пример фильма 3", "year": 2020, "genres": ["фантастика", "приключения"], "category": "фильм"}
            ]
            collection.insert_many(test_movies)
            print(f"✅ Добавлены тестовые данные вместо полного файла")
        
        raise

if __name__ == "__main__":
    main() 