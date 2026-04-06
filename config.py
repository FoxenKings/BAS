"""
Configuration module for Assets Management System
"""
import os
from datetime import timedelta
from dotenv import load_dotenv

# Загружаем .env из корня проекта
_BASE_DIR = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(_BASE_DIR, '.env'))

class Config:
    """Основные настройки приложения"""

    # База данных
    BASE_DIR = _BASE_DIR
    DATABASE = os.path.join(BASE_DIR, 'data', 'assets.db')

    # Секретный ключ — ОБЯЗАТЕЛЕН, читается только из .env / переменных окружения
    SECRET_KEY = os.environ.get('SECRET_KEY')
    if not SECRET_KEY:
        raise RuntimeError(
            "SECRET_KEY не задан!\n"
            "Создайте файл .env на основе .env.example и задайте SECRET_KEY.\n"
            "Генерация ключа: python -c \"import secrets; print(secrets.token_hex(32))\""
        )

    # Режим отладки — НИКОГДА не включайте в продакшн
    DEBUG = os.environ.get('DEBUG', 'false').lower() == 'true'

    # Настройки сессии
    PERMANENT_SESSION_LIFETIME = timedelta(hours=8)  # Рабочий день
    SESSION_COOKIE_SECURE = os.environ.get('HTTPS_ENABLED', 'false').lower() == 'true'
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Strict'
    
    # Настройки загрузки файлов
    MAX_CONTENT_LENGTH = 10 * 1024 * 1024  # 10 MB
    UPLOAD_FOLDER = os.path.join(BASE_DIR, 'uploads')
    
    # Настройки кэширования
    SEND_FILE_MAX_AGE_DEFAULT = timedelta(hours=1)
    
    # Часовой пояс планировщика задач
    SCHEDULER_TIMEZONE = os.environ.get('SCHEDULER_TIMEZONE', 'Europe/Moscow')

    # Включение/отключение функций
    ENABLE_BATCH_TRACKING = True
    ENABLE_SERIAL_NUMBERS = True
    ENABLE_CALIBRATION = True
    ENABLE_MAINTENANCE = True
    
    # Настройки для API
    API_RATE_LIMIT = 100  # запросов в минуту
    
    # Настройки отчетов
    REPORT_PAGE_SIZE = 50
    MAX_EXPORT_ROWS = 10000
    
    @staticmethod
    def init_app(app):
        """Инициализация приложения"""
        # Создаем необходимые директории
        os.makedirs(Config.UPLOAD_FOLDER, exist_ok=True)
        os.makedirs(os.path.dirname(Config.DATABASE), exist_ok=True)