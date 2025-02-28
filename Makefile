# Makefile для проекта кинопоиска
# Совместим с Linux, macOS и Windows (через WSL)

# Цвета для красивого вывода
GREEN = \033[0;32m
YELLOW = \033[0;33m
RED = \033[0;31m
BLUE = \033[0;34m
PURPLE = \033[0;35m
CYAN = \033[0;36m
NC = \033[0m # No Color
BOLD = \033[1m

# Определение OS
ifeq ($(OS),Windows_NT)
	OS_NAME = windows
else
	UNAME_S := $(shell uname -s)
	ifeq ($(UNAME_S),Linux)
		OS_NAME = linux
	endif
	ifeq ($(UNAME_S),Darwin)
		OS_NAME = macos
	endif
endif

.PHONY: all build run stop clean help init

# Запуск всего проекта
all: build run init

# Помощь
help:
	@echo -e "$(BLUE)=== Система поиска фильмов ===$(NC)"
	@echo -e "$(GREEN)Доступные команды:$(NC)"
	@echo -e "  $(YELLOW)make$(NC)       - Собрать и запустить весь проект"
	@echo -e "  $(YELLOW)make build$(NC) - Собрать все контейнеры"
	@echo -e "  $(YELLOW)make init$(NC)  - Инициализировать базы данных"
	@echo -e "  $(YELLOW)make run$(NC)   - Запустить все сервисы"
	@echo -e "  $(YELLOW)make stop$(NC)  - Остановить все сервисы"
	@echo -e "  $(YELLOW)make clean$(NC) - Очистить все контейнеры и тома"

# Сборка контейнеров
build:
	@echo -e "$(BLUE)➤ Сборка контейнеров...$(NC)"
	docker compose build
	@echo -e "$(GREEN)✓ Сборка завершена$(NC)"

# Запуск сервисов
run:
	@echo -e "$(BLUE)➤ Запуск сервисов...$(NC)"
	docker compose up -d
	@echo -e "$(BLUE)➤ Ожидание запуска MongoDB...$(NC)"
	@for i in $$(seq 1 60); do \
		if docker compose exec -T mongodb mongosh --eval "db.serverStatus()" > /dev/null 2>&1; then \
			echo -e "$(GREEN)✓ MongoDB готова к работе$(NC)"; \
			break; \
		fi; \
		if [ $$i -eq 60 ]; then \
			echo -e "$(RED)✗ Превышено время ожидания MongoDB$(NC)"; \
			exit 1; \
		fi; \
		sleep 2; \
	done
	@echo -e "$(BLUE)➤ Ожидание запуска Redis...$(NC)"
	@for i in $$(seq 1 30); do \
		if docker compose exec -T redis redis-cli ping > /dev/null 2>&1; then \
			echo -e "$(GREEN)✓ Redis готов к работе$(NC)"; \
			break; \
		fi; \
		if [ $$i -eq 30 ]; then \
			echo -e "$(RED)✗ Превышено время ожидания Redis$(NC)"; \
			exit 1; \
		fi; \
		sleep 2; \
	done
	@echo -e "$(GREEN)✓ Сервисы запущены!$(NC)"
	@echo -e "$(YELLOW)Веб-интерфейс доступен по адресу: http://localhost:5000$(NC)"
	@echo -e "$(YELLOW)Redis Commander доступен по адресу: http://localhost:8001$(NC)"

# Инициализация баз данных
init:
	@echo -e "$(BLUE)➤ Инициализация баз данных...$(NC)"
	@if [ -f movie.json ]; then \
		echo -e "$(GREEN)Загрузка данных в MongoDB...$(NC)"; \
		for i in $$(seq 1 30); do \
			if docker compose exec -T database python -c "from mongo_client import MongoMovieClient; client = MongoMovieClient(); client.clear_and_load_movies('movie.json')" 2>/dev/null; then \
				echo -e "$(GREEN)✓ Данные успешно загружены в MongoDB$(NC)"; \
				echo -e "$(BLUE)➤ Синхронизация Redis...$(NC)"; \
				if curl -s -X POST http://localhost:5001/sync/mongodb-to-redis | grep -q "success"; then \
					echo -e "$(GREEN)✓ Redis успешно синхронизирован$(NC)"; \
				else \
					echo -e "$(RED)✗ Ошибка синхронизации Redis$(NC)"; \
				fi; \
				break; \
			fi; \
			if [ $$i -eq 30 ]; then \
				echo -e "$(RED)✗ Превышено время ожидания загрузки данных$(NC)"; \
				exit 1; \
			fi; \
			echo -e "$(YELLOW)Попытка $$i из 30. Ожидание готовности MongoDB...$(NC)"; \
			sleep 5; \
		done; \
	else \
		echo -e "$(RED)⚠ Файл movie.json не найден. Пропускаем инициализацию баз данных.$(NC)"; \
	fi

# Остановка сервисов
stop:
	@echo -e "$(BLUE)➤ Остановка сервисов...$(NC)"
	docker compose down
	@echo -e "$(GREEN)✓ Сервисы остановлены$(NC)"

# Очистка
clean:
	@echo -e "$(BLUE)➤ Очистка системы...$(NC)"
	docker compose down -v
	docker system prune -f
	@echo -e "$(GREEN)✓ Система очищена$(NC)"

# Основная цель
all: help

# Справка по доступным командам
help:
	@echo -e "$(BOLD)$(BLUE)=== Подскажем: Система поиска и рекомендации фильмов ===$(NC)"
	@echo -e "$(BOLD)$(GREEN)Доступные команды:$(NC)"
	@echo -e "  $(YELLOW)make install$(NC)            - Установить все зависимости (Python, Redis, MongoDB)"
	@echo -e "  $(YELLOW)make install-python-deps$(NC) - Установить только Python зависимости"
	@echo -e "  $(YELLOW)make install-redis$(NC)       - Установить только Redis"
	@echo -e "  $(YELLOW)make install-redisearch$(NC)  - Установить модуль RediSearch"
	@echo -e "  $(YELLOW)make install-mongodb$(NC)     - Установить только MongoDB"
	@echo -e "  $(YELLOW)make setup$(NC)               - Настроить проект после установки зависимостей"
	@echo -e "  $(YELLOW)make init-db$(NC)             - Инициализировать базу данных Redis из MongoDB"
	@echo -e "  $(YELLOW)make run$(NC)                 - Запустить приложение"
	@echo -e "  $(YELLOW)make status$(NC)              - Проверить статус служб Redis и MongoDB"
	@echo -e "  $(YELLOW)make test$(NC)                - Запустить тесты"
	@echo -e "  $(YELLOW)make clean$(NC)               - Очистить кэши и временные файлы"
	@echo -e "  $(YELLOW)make build$(NC)                - Собрать все контейнеры"
	@echo -e "  $(YELLOW)make stop$(NC)                - Остановить все сервисы"
	@echo -e ""
	@echo -e "$(BOLD)$(CYAN)Информация о системе:$(NC)"
	@echo -e "  • Обнаруженная ОС: $(PURPLE)$(OS_NAME)$(NC)"
	@echo -e "  • Python версия: $(PURPLE)$(shell python3 --version 2>/dev/null || python --version 2>/dev/null || echo 'не установлен')$(NC)"
	@echo -e ""
	@echo -e "$(BOLD)$(RED)Примечание:$(NC) Для Windows рекомендуется использовать WSL (Windows Subsystem for Linux)"

# Установка всех зависимостей
install: install-python-deps install-redis install-redisearch install-mongodb
	@echo -e "$(GREEN)✓ Все зависимости успешно установлены!$(NC)"

# Установка Python зависимостей
install-python-deps:
	@echo -e "$(BLUE)➤ Установка Python зависимостей...$(NC)"
	@pip3 install -q flask pymongo redis numpy pandas scikit-learn || pip install -q flask pymongo redis numpy pandas scikit-learn
	@echo -e "$(GREEN)✓ Python зависимости установлены$(NC)"

# Установка Redis
install-redis:
	@echo -e "$(BLUE)➤ Установка Redis...$(NC)"
ifeq ($(OS_NAME),linux)
	@if ! command -v redis-server > /dev/null; then \
		echo -e "$(YELLOW)Установка Redis на Linux...$(NC)"; \
		sudo apt-get update && sudo apt-get install -y redis-server || \
		(sudo yum install -y epel-release && sudo yum install -y redis) || \
		(sudo dnf install -y redis); \
		sudo systemctl enable redis; \
		sudo systemctl start redis; \
	else \
		echo -e "$(GREEN)Redis уже установлен на Linux.$(NC)"; \
	fi
else ifeq ($(OS_NAME),macos)
	@if ! command -v redis-server > /dev/null; then \
		echo -e "$(YELLOW)Установка Redis на macOS...$(NC)"; \
		brew install redis || (echo -e "$(RED)Не удалось установить Redis через Homebrew. Установите Homebrew или Redis вручную.$(NC)" && exit 1); \
		brew services start redis; \
	else \
		echo -e "$(GREEN)Redis уже установлен на macOS.$(NC)"; \
	fi
else ifeq ($(OS_NAME),windows)
	@echo -e "$(YELLOW)Для Windows установите Redis через WSL или скачайте Windows-версию Redis с https://github.com/microsoftarchive/redis/releases$(NC)"
else
	@echo -e "$(RED)Неподдерживаемая операционная система. Установите Redis вручную.$(NC)"
endif
	@echo -e "$(GREEN)✓ Redis настроен$(NC)"

# Установка RediSearch
install-redisearch:
	@echo -e "$(BLUE)➤ Установка модуля RediSearch...$(NC)"
ifeq ($(OS_NAME),linux)
	@echo -e "$(YELLOW)Установка RediSearch на Linux...$(NC)"
	@if command -v docker > /dev/null; then \
		echo -e "$(YELLOW)Используем Docker для установки Redis с RediSearch...$(NC)"; \
		if ! docker ps | grep -q redis-stack; then \
			docker run -d --name redis-stack -p 6379:6379 -p 8001:8001 redis/redis-stack:latest; \
		else \
			echo -e "$(GREEN)Контейнер redis-stack уже запущен.$(NC)"; \
		fi; \
	else \
		echo -e "$(YELLOW)Docker не установлен, пробуем альтернативные способы...$(NC)"; \
		echo -e "$(RED)Для полнофункционального RediSearch рекомендуется использовать Docker или скачать redis-stack.$(NC)"; \
		echo -e "$(YELLOW)Скачиваем и устанавливаем redis с RediSearch вручную...$(NC)"; \
		mkdir -p redisearch_tmp && cd redisearch_tmp && \
		curl -fsSL https://packages.redis.io/redis-stack/redis-stack-server-6.2.6-v7.linux.x86_64.tar.gz -o redis-stack.tar.gz && \
		tar xvzf redis-stack.tar.gz && \
		cd $(ls -d */|head -n 1) && \
		sudo ./bin/redis-server --loadmodule ./lib/redisearch.so; \
	fi
else ifeq ($(OS_NAME),macos)
	@echo -e "$(YELLOW)Установка RediSearch на macOS...$(NC)"
	@if command -v docker > /dev/null; then \
		echo -e "$(YELLOW)Используем Docker для установки Redis с RediSearch...$(NC)"; \
		if ! docker ps | grep -q redis-stack; then \
			docker run -d --name redis-stack -p 6379:6379 -p 8001:8001 redis/redis-stack:latest; \
		else \
			echo -e "$(GREEN)Контейнер redis-stack уже запущен.$(NC)"; \
		fi; \
	else \
		echo -e "$(YELLOW)Docker не установлен, пробуем установить через Homebrew...$(NC)"; \
		brew tap redis-stack/redis-stack && \
		brew install redis-stack && \
		brew services start redis-stack; \
	fi
else ifeq ($(OS_NAME),windows)
	@echo -e "$(YELLOW)Для Windows рекомендуется использовать Docker:$(NC)"
	@echo -e "$(YELLOW)docker run -d --name redis-stack -p 6379:6379 -p 8001:8001 redis/redis-stack:latest$(NC)"
else
	@echo -e "$(RED)Неподдерживаемая операционная система. Установите RediSearch вручную.$(NC)"
endif
	@echo -e "$(GREEN)✓ RediSearch настроен$(NC)"

# Установка MongoDB
install-mongodb:
	@echo -e "$(BLUE)➤ Установка MongoDB...$(NC)"
ifeq ($(OS_NAME),linux)
	@if ! command -v mongod > /dev/null; then \
		echo -e "$(YELLOW)Установка MongoDB на Linux...$(NC)"; \
		if [ -f /etc/debian_version ]; then \
			wget -qO - https://www.mongodb.org/static/pgp/server-7.0.asc | sudo apt-key add - && \
			echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/7.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list && \
			sudo apt-get update && \
			sudo apt-get install -y mongodb-org && \
			sudo systemctl start mongod && \
			sudo systemctl enable mongod; \
		elif [ -f /etc/redhat-release ]; then \
			echo "[mongodb-org-7.0]" | sudo tee /etc/yum.repos.d/mongodb-org-7.0.repo && \
			echo "name=MongoDB Repository" | sudo tee -a /etc/yum.repos.d/mongodb-org-7.0.repo && \
			echo "baseurl=https://repo.mongodb.org/yum/redhat/8/mongodb-org/7.0/x86_64/" | sudo tee -a /etc/yum.repos.d/mongodb-org-7.0.repo && \
			echo "gpgcheck=1" | sudo tee -a /etc/yum.repos.d/mongodb-org-7.0.repo && \
			echo "enabled=1" | sudo tee -a /etc/yum.repos.d/mongodb-org-7.0.repo && \
			echo "gpgkey=https://www.mongodb.org/static/pgp/server-7.0.asc" | sudo tee -a /etc/yum.repos.d/mongodb-org-7.0.repo && \
			sudo yum install -y mongodb-org && \
			sudo systemctl start mongod && \
			sudo systemctl enable mongod; \
		else \
			echo -e "$(RED)Неподдерживаемый дистрибутив Linux. Установите MongoDB вручную.$(NC)"; \
		fi; \
	else \
		echo -e "$(GREEN)MongoDB уже установлен на Linux.$(NC)"; \
	fi
else ifeq ($(OS_NAME),macos)
	@if ! command -v mongod > /dev/null; then \
		echo -e "$(YELLOW)Установка MongoDB на macOS...$(NC)"; \
		brew tap mongodb/brew && \
		brew install mongodb-community && \
		brew services start mongodb-community; \
	else \
		echo -e "$(GREEN)MongoDB уже установлен на macOS.$(NC)"; \
	fi
else ifeq ($(OS_NAME),windows)
	@echo -e "$(YELLOW)Для Windows установите MongoDB через WSL или скачайте Windows-версию MongoDB с https://www.mongodb.com/try/download/community$(NC)"
else
	@echo -e "$(RED)Неподдерживаемая операционная система. Установите MongoDB вручную.$(NC)"
endif
	@echo -e "$(GREEN)✓ MongoDB настроен$(NC)"

# Настройка проекта после установки зависимостей
setup:
	@echo -e "$(BLUE)➤ Настройка проекта...$(NC)"
	@echo -e "$(YELLOW)Проверка подключения к Redis...$(NC)"
	@redis-cli ping || echo -e "$(RED)Redis не запущен или недоступен!$(NC)"
	@echo -e "$(YELLOW)Проверка подключения к MongoDB...$(NC)"
	@if command -v mongosh > /dev/null; then \
		mongosh --quiet --eval "db.version()" || echo -e "$(RED)MongoDB не запущен или недоступен!$(NC)"; \
	else \
		mongo --quiet --eval "db.version()" || echo -e "$(RED)MongoDB не запущен или недоступен!$(NC)"; \
	fi
	@echo -e "$(GREEN)✓ Настройка проекта завершена$(NC)"

# Инициализация базы данных Redis из MongoDB
init-db:
	@echo -e "$(BLUE)➤ Инициализация базы данных...$(NC)"
	@echo -e "$(YELLOW)Проверка наличия файла movie.json...$(NC)"
	@if [ -f movie.json ]; then \
		echo -e "$(GREEN)Найден файл movie.json. Загрузка данных в MongoDB...$(NC)"; \
		python3 -c "from mongo_client import MongoMovieClient; MongoMovieClient().clear_and_load_movies('movie.json')" || \
		python -c "from mongo_client import MongoMovieClient; MongoMovieClient().clear_and_load_movies('movie.json')"; \
	else \
		echo -e "$(RED)Файл movie.json не найден. Пропускаем загрузку в MongoDB.$(NC)"; \
	fi
	@echo -e "$(YELLOW)Перенос данных из MongoDB в Redis...$(NC)"
	@python3 migrate_mongo_to_redis.py || python migrate_mongo_to_redis.py
	@echo -e "$(GREEN)✓ База данных инициализирована$(NC)"

# Проверка статуса сервисов
status:
	@echo -e "$(BLUE)➤ Проверка статуса служб...$(NC)"
	@echo -e "$(BOLD)$(CYAN)Статус Redis:$(NC)"
ifeq ($(OS_NAME),linux)
	@if systemctl is-active --quiet redis.service || systemctl is-active --quiet redis-server.service; then \
		echo -e "$(GREEN)✓ Redis запущен$(NC)"; \
	else \
		if docker ps | grep -q redis-stack; then \
			echo -e "$(GREEN)✓ Redis запущен в Docker контейнере redis-stack$(NC)"; \
		else \
			echo -e "$(RED)✗ Redis не запущен$(NC)"; \
		fi; \
	fi
else ifeq ($(OS_NAME),macos)
	@if brew services list | grep -q redis.*started; then \
		echo -e "$(GREEN)✓ Redis запущен через Homebrew$(NC)"; \
	else \
		if brew services list | grep -q redis-stack.*started; then \
			echo -e "$(GREEN)✓ Redis-stack запущен через Homebrew$(NC)"; \
		else \
			if docker ps | grep -q redis-stack; then \
				echo -e "$(GREEN)✓ Redis запущен в Docker контейнере redis-stack$(NC)"; \
			else \
				echo -e "$(RED)✗ Redis не запущен$(NC)"; \
			fi; \
		fi; \
	fi
else
	@echo -e "$(YELLOW)Невозможно проверить статус Redis на данной ОС$(NC)"
endif
	@redis-cli ping 2>/dev/null && echo -e "$(GREEN)✓ Redis отвечает на ping$(NC)" || echo -e "$(RED)✗ Redis не отвечает на ping$(NC)"

	@echo -e "$(BOLD)$(CYAN)Статус MongoDB:$(NC)"
ifeq ($(OS_NAME),linux)
	@if systemctl is-active --quiet mongod.service; then \
		echo -e "$(GREEN)✓ MongoDB запущен$(NC)"; \
	else \
		echo -e "$(RED)✗ MongoDB не запущен$(NC)"; \
	fi
else ifeq ($(OS_NAME),macos)
	@if brew services list | grep -q mongodb-community.*started; then \
		echo -e "$(GREEN)✓ MongoDB запущен через Homebrew$(NC)"; \
	else \
		echo -e "$(RED)✗ MongoDB не запущен$(NC)"; \
	fi
else
	@echo -e "$(YELLOW)Невозможно проверить статус MongoDB на данной ОС$(NC)"
endif
	@if command -v mongosh > /dev/null; then \
		mongosh --quiet --eval "db.version()" >/dev/null 2>&1 && echo -e "$(GREEN)✓ MongoDB отвечает$(NC)" || echo -e "$(RED)✗ MongoDB не отвечает$(NC)"; \
	else \
		mongo --quiet --eval "db.version()" >/dev/null 2>&1 && echo -e "$(GREEN)✓ MongoDB отвечает$(NC)" || echo -e "$(RED)✗ MongoDB не отвечает$(NC)"; \
	fi

# Запуск тестов
test:
	@echo -e "$(BLUE)➤ Запуск тестов...$(NC)"
	@echo -e "$(YELLOW)Тест подключения к Redis...$(NC)"
	@python3 -c "from redis import Redis; r = Redis(); print(f'PING = {r.ping()}'); print('Redis работает корректно!')" || \
	python -c "from redis import Redis; r = Redis(); print(f'PING = {r.ping()}'); print('Redis работает корректно!')" || \
	echo -e "$(RED)✗ Не удалось подключиться к Redis$(NC)"
	
	@echo -e "$(YELLOW)Тест подключения к MongoDB...$(NC)"
	@python3 -c "from pymongo import MongoClient; client = MongoClient(); print(f'Версия MongoDB: {client.server_info()[\"version\"]}'); print('MongoDB работает корректно!')" || \
	python -c "from pymongo import MongoClient; client = MongoClient(); print(f'Версия MongoDB: {client.server_info()[\"version\"]}'); print('MongoDB работает корректно!')" || \
	echo -e "$(RED)✗ Не удалось подключиться к MongoDB$(NC)"
	
	@echo -e "$(YELLOW)Запуск теста Redis Search...$(NC)"
	@if [ -f test_redis_search.py ]; then \
		python3 test_redis_search.py || python test_redis_search.py || echo -e "$(RED)✗ Ошибка при запуске теста RediSearch$(NC)"; \
	else \
		echo -e "$(RED)✗ Файл test_redis_search.py не найден$(NC)"; \
	fi
	
	@echo -e "$(GREEN)✓ Тесты завершены$(NC)" 