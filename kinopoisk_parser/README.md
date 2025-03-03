# Парсер Кинопоиска

Парсер для получения и обработки данных о фильмах с Кинопоиска с использованием API.

## Возможности парсера

- **Получение фильмов** — загрузка информации о фильмах с помощью API Кинопоиска
- **Обработка данных** — очистка, фильтрация и удаление дубликатов
- **Многопоточная работа** — параллельное получение данных для ускорения
- **Обход ограничений API** — использование нескольких API-ключей
- **Подробная статистика** — вывод информации о количестве фильмов по жанрам

## Использование

### Запуск через Makefile (рекомендуется)

Для удобного использования парсера были добавлены команды в основной Makefile проекта:

```bash
# Получение фильмов с Кинопоиска
make parser-fetch

# Обработка полученных данных
make parser-clean

# Полный цикл: получение и обработка данных
make parser-all
```

**Примечания:**
- Все команды показывают прогресс выполнения и измеряют время работы
- Полученные фильмы сохраняются в каталоге `kinopoisk_parser/data/`
- Обработанные данные сохраняются в `movie.json` в корне проекта

### Локальный запуск

1. Установите зависимости:
```bash
pip install -r requirements.txt
```

2. Запустите парсер:
```bash
python src/parser.py [параметры]
```

### Запуск через Docker

#### Вариант 1: Через Docker Compose и exec

1. Убедитесь, что сервисы запущены:
```bash
docker compose up -d
```

2. Выполните команду в контейнере:
```bash
docker compose exec parser python parser.py [параметры]
```

#### Вариант 2: Docker напрямую

1. Соберите образ:
```bash
docker build -t kinopoisk-parser ./kinopoisk_parser/
```

2. Запустите контейнер:
```bash
docker run -v $(pwd)/kinopoisk_parser/data:/app/data -v $(pwd)/movie.json:/app/movie.json kinopoisk-parser python parser.py [параметры]
```

## Параметры запуска

- `--mode`: Режим работы
  - `fetch` - только получение данных
  - `clean` - только очистка данных
  - `all` - полный цикл (по умолчанию)
- `--output`: Путь к выходному файлу (по умолчанию: /app/movie.json)
- `--input`: Путь к входному файлу для режима clean (по умолчанию: /app/data/input.json)
- `--max-pages`: Максимальное количество страниц для каждого жанра (по умолчанию: 150)
- `--limit`: Количество фильмов на странице (по умолчанию: 250)

## Алгоритм работы

### Режим fetch (получение данных)

1. Получение списка доступных жанров через API Кинопоиска
2. Параллельный запуск запросов для каждого жанра, используя пул API-ключей
3. Постраничное получение фильмов (до max-pages страниц по limit фильмов на странице)
4. Объединение результатов и сохранение в JSON-файл

### Режим clean (очистка данных)

1. Загрузка сырых данных из файла JSON
2. Удаление фильмов-дубликатов по ID
3. Удаление записей без обязательных полей (описание, название и т.д.)
4. Сохранение очищенных данных в итоговый файл

### Режим all (полный цикл)

Последовательное выполнение режимов fetch и clean в одном запуске.

## Примеры использования

### Через Makefile

```bash
# Получение данных
make parser-fetch

# Обработка данных
make parser-clean

# Полный цикл
make parser-all
```

### Через Docker Compose

```bash
# Получение данных
docker compose exec parser python parser.py --mode fetch

# Обработка данных
docker compose exec parser python parser.py --mode clean --input /app/data/input.json --output /app/movie.json

# Полный цикл
docker compose exec parser python parser.py --mode all --output /app/movie.json
```

### Docker напрямую

```bash
# Получение данных
docker run -v $(pwd)/kinopoisk_parser/data:/app/data -v $(pwd)/movie.json:/app/movie.json kinopoisk-parser python parser.py --mode fetch

# Обработка данных
docker run -v $(pwd)/kinopoisk_parser/data:/app/data -v $(pwd)/movie.json:/app/movie.json kinopoisk-parser python parser.py --mode clean --input /app/data/input.json --output /app/movie.json

# Полный цикл
docker run -v $(pwd)/kinopoisk_parser/data:/app/data -v $(pwd)/movie.json:/app/movie.json kinopoisk-parser python parser.py --mode all --output /app/movie.json
```

## Структура проекта

```
kinopoisk_parser/
├── data/               # Директория для хранения данных
├── src/               # Исходный код
│   └── parser.py      # Основной файл парсера
├── docker-compose.yml # Конфигурация Docker Compose
├── Dockerfile         # Конфигурация Docker
├── README.md          # Документация
├── requirements.txt   # Зависимости проекта
└── .gitignore        # Игнорируемые файлы
```

## После обновления базы фильмов

После успешного запуска парсера вам необходимо:

1. Загрузить данные в MongoDB:
```bash
make init-db
```

2. Синхронизировать данные с Redis:
```bash
make sync-db
```

## Примечания

- Все данные сохраняются в директории `data/`
- При использовании Docker или Docker Compose директория `data/` монтируется в контейнер
- Для остановки парсера используйте Ctrl+C или `docker-compose down`
- При использовании общего docker-compose парсер будет ждать готовности MongoDB
- Результаты парсинга будут доступны в общем файле `movie.json` 