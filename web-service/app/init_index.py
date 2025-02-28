#!/usr/bin/env python3
# Скрипт для инициализации индекса Redis

import requests
import time
import os
import logging
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Загружаем переменные окружения
load_dotenv()

# URL сервисов из переменных окружения
DATABASE_SERVICE_URL = os.getenv("DATABASE_SERVICE_URL", "http://database:5001")
WEB_SERVICE_URL = os.getenv("WEB_SERVICE_URL", "http://localhost:5000")

def wait_for_service(url, max_attempts=10, delay=5):
    """Ожидает доступности сервиса"""
    for attempt in range(max_attempts):
        try:
            response = requests.get(f"{url}/health", timeout=5)
            if response.status_code == 200:
                logger.info(f"Сервис {url} доступен!")
                return True
            else:
                logger.warning(f"Сервис {url} вернул код {response.status_code}, ожидание...")
        except Exception as e:
            logger.warning(f"Ошибка при проверке сервиса {url}: {str(e)}")
        
        logger.info(f"Попытка {attempt+1}/{max_attempts}, ожидание {delay} сек...")
        time.sleep(delay)
    
    logger.error(f"Сервис {url} недоступен после {max_attempts} попыток")
    return False

def init_redis_index():
    """Инициализирует индекс Redis"""
    try:
        # Сначала проверяем доступность database-service
        if not wait_for_service(DATABASE_SERVICE_URL):
            logger.error("Database-service недоступен, не удалось инициализировать индекс")
            return False
        
        # Затем проверяем доступность web-service
        if not wait_for_service(WEB_SERVICE_URL):
            logger.warning("Web-service недоступен, попробуем инициализировать напрямую")
            # Прямая инициализация через database-service
            init_url = f"{DATABASE_SERVICE_URL}/create_index"
            response = requests.post(init_url, timeout=30)
            if response.status_code == 200:
                logger.info("Индекс Redis успешно инициализирован напрямую")
                return True
            else:
                logger.error(f"Не удалось инициализировать индекс напрямую: {response.status_code}, {response.text}")
                return False
        
        # Инициализация через web-service
        logger.info("Инициализация индекса через web-service...")
        init_url = f"{WEB_SERVICE_URL}/create_redis_index"
        response = requests.post(init_url, timeout=30)
        
        if response.status_code == 200:
            logger.info("Индекс Redis успешно инициализирован")
            return True
        else:
            logger.error(f"Не удалось инициализировать индекс: {response.status_code}, {response.text}")
            
            # Пробуем инициализировать напрямую
            logger.info("Пробуем инициализировать напрямую через database-service...")
            init_url = f"{DATABASE_SERVICE_URL}/create_index"
            response = requests.post(init_url, timeout=30)
            
            if response.status_code == 200:
                logger.info("Индекс Redis успешно инициализирован напрямую")
                return True
            else:
                logger.error(f"Не удалось инициализировать индекс напрямую: {response.status_code}, {response.text}")
                return False
    except Exception as e:
        logger.error(f"Ошибка при инициализации индекса Redis: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info("Запуск скрипта инициализации индекса Redis...")
    # Добавляем начальную задержку для уверенности, что все сервисы запущены
    time.sleep(10)
    success = init_redis_index()
    logger.info(f"Результат инициализации индекса: {'Успешно' if success else 'Неудачно'}") 