"""
Модуль базы данных для системы управления активами - НОВАЯ АРХИТЕКТУРА
Полный переход на номенклатурный учет с категориями, партиями и экземплярами
"""
import sqlite3
import os
import hashlib
import bcrypt
import json
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple
import traceback
from contextlib import contextmanager
import logging
from utils.search import build_where as _build_search_where
logger = logging.getLogger(__name__)

class Database:
    """
    Класс базы данных для новой архитектуры номенклатурного учета.
    
    Все данные хранятся в единой структуре:
    - categories (иерархические категории)
    - nomenclatures (карточки товаров/материалов)
    - instances (экземпляры для индивидуального учета)
    - batches (партии для партионного учета)
    - stocks (запасы для количественного учета)
    - documents (документооборот)
    """
    
    def __init__(self, db_name: str = "data/assets.db"):
        """
        Инициализация подключения к базе данных.

        Args:
            db_name: путь к файлу базы данных SQLite
        """
        self.db_name = db_name
        self.connection = None
        self.cursor = None
        self._in_transaction = False  # флаг для transaction() менеджера
        self._lock = threading.RLock()  # защита от одновременного доступа из нескольких потоков
        self.connect()  # Устанавливаем соединение
        
        # Инициализируем таблицы новой архитектуры
        self.initialize_new_architecture()
    
    def connect(self):
        """Установка соединения с базой данных."""
        try:
            # Создаем директорию для данных, если её нет
            os.makedirs(os.path.dirname(self.db_name), exist_ok=True)
            
            # Подключаемся к SQLite (check_same_thread=False для работы в многопоточном режиме Flask)
            self.connection = sqlite3.connect(self.db_name, check_same_thread=False)
            # Переопределяем LOWER() для корректной работы с кириллицей
            # (встроенный SQLite LOWER обрабатывает только ASCII)
            self.connection.create_function('lower', 1, lambda s: s.lower() if isinstance(s, str) else s)
            # Настраиваем возврат строк как словарей
            self.connection.row_factory = sqlite3.Row
            self.cursor = self.connection.cursor()
            
            # Включаем поддержку внешних ключей
            self.cursor.execute("PRAGMA foreign_keys = ON")
            # Оптимизация производительности
            self.cursor.execute("PRAGMA journal_mode = WAL")  # Write-Ahead Logging
            self.cursor.execute("PRAGMA synchronous = NORMAL")
            self.cursor.execute("PRAGMA cache_size = -2000")  # 2MB кэш
            self.cursor.execute("PRAGMA temp_store = MEMORY")  # Временные таблицы в памяти
            self.cursor.execute("PRAGMA mmap_size = 30000000000")  # Memory-mapped I/O
            self.connection.commit()
            
            logger.info(f"✅ Подключение к базе данных: {self.db_name}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к базе данных: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """
        Контекстный менеджер для работы с соединением.
        Обеспечивает автоматический откат транзакции при ошибке.
        """
        try:
            yield self.connection
        except Exception as e:
            self.connection.rollback()
            raise e
        finally:
            # НЕ закрываем соединение здесь! Оно будет жить всё время работы приложения
            pass

    @contextmanager
    def transaction(self):
        """
        Контекстный менеджер для атомарных транзакций.
        Внутри блока execute_query не делает автоматический commit.
        При выходе без исключения — COMMIT, при исключении — ROLLBACK.

        Использование:
            with db.transaction():
                db.execute_query("INSERT ...")
                db.execute_query("UPDATE ...")
        """
        with self._lock:
            self._in_transaction = True
            try:
                yield
                self.connection.commit()
            except Exception:
                self.connection.rollback()
                raise
            finally:
                self._in_transaction = False

    def execute_query(self, query, params=None, fetch_all=False):
        """Выполнение SQL-запроса с возвратом результатов."""
        with self._lock:
            cursor = None
            try:
                if params is None:
                    params = []

                # Всегда создаем новый курсор
                cursor = self.connection.cursor()

                # Выполняем запрос
                cursor.execute(query, params)

                _q = query.strip().upper()
                if _q.startswith('SELECT') or _q.startswith('WITH') or _q.startswith('EXPLAIN'):
                    if fetch_all:
                        rows = cursor.fetchall()
                        result = []
                        for row in rows:
                            # Преобразуем Row в dict, если это sqlite3.Row
                            if hasattr(row, 'keys'):
                                result.append(dict(row))
                            else:
                                # Если это кортеж, создаем словарь вручную
                                col_names = [description[0] for description in cursor.description]
                                result.append(dict(zip(col_names, row)))
                        cursor.close()
                        return result
                    else:
                        row = cursor.fetchone()
                        result = None
                        if row:
                            if hasattr(row, 'keys'):
                                result = dict(row)
                            else:
                                col_names = [description[0] for description in cursor.description]
                                result = dict(zip(col_names, row))
                        cursor.close()
                        return result
                else:
                    if not self._in_transaction:
                        self.connection.commit()
                    rowcount = cursor.rowcount
                    cursor.close()
                    return rowcount

            except Exception as e:
                logger.debug(f"Ошибка выполнения запроса: {e}")
                logger.debug(f"Запрос: {query}")
                logger.debug(f"Параметры: {params}")
                if cursor:
                    cursor.close()
                self.connection.rollback()
                raise
    
    def column_exists(self, table, column):
        """Проверяет существование колонки в таблице"""
        try:
            # Валидация имени таблицы: только буквы, цифры и _
            import re as _re
            if not _re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', table):
                logger.warning(f"column_exists: недопустимое имя таблицы: {table!r}")
                return False
            cursor = self.connection.cursor()
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [col[1] for col in cursor.fetchall()]
            cursor.close()
            return column in columns
        except Exception as e:
            logger.debug(f"Ошибка проверки колонки {table}.{column}: {e}")
            return False
    
    def initialize_new_architecture(self):
        """Проверка и создание отсутствующих таблиц и полей."""
        try:
            logger.debug("🔄 Проверка структуры базы данных...")
            
            # Проверяем, какие таблицы уже существуют
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            existing_tables = [row[0] for row in self.cursor.fetchall()]
            logger.debug(f"📊 Существующие таблицы: {', '.join(existing_tables)}")
            
            # ============ 1. ПОСЛЕДОВАТЕЛЬНОСТИ ============
            if 'sequences' not in existing_tables:
                logger.debug("📦 Создание таблицы sequences...")
                self.cursor.execute("""
                    CREATE TABLE sequences (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        sequence_type VARCHAR(30) NOT NULL,
                        prefix VARCHAR(10) NOT NULL,
                        year INTEGER NOT NULL,
                        last_number INTEGER DEFAULT 0,
                        format VARCHAR(30) DEFAULT '{PREFIX}-{YEAR}-{NUMBER:06d}',
                        UNIQUE(sequence_type, prefix, year)
                    )
                """)
            
            # ============ 2. ПРАВИЛА КАТЕГОРИЙ ============
            if 'category_rules' not in existing_tables:
                logger.debug("📦 Создание таблицы category_rules...")
                self.cursor.execute("""
                    CREATE TABLE category_rules (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        rule_type VARCHAR(30) NOT NULL,
                        rule_value VARCHAR(255) NOT NULL,
                        category_id INTEGER NOT NULL,
                        priority INTEGER DEFAULT 10,
                        is_active BOOLEAN DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_by INTEGER,
                        FOREIGN KEY (category_id) REFERENCES categories(id),
                        FOREIGN KEY (created_by) REFERENCES users(id)
                    )
                """)
            
            # ============ 3. ПОЛЬЗОВАТЕЛИ ============
            if 'users' not in existing_tables:
                logger.debug("📦 Создание таблицы users...")
                self.cursor.execute("""
                    CREATE TABLE users (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username VARCHAR(50) UNIQUE NOT NULL,
                        password_hash TEXT NOT NULL,
                        employee_id INTEGER,
                        email VARCHAR(100),
                        role VARCHAR(20) NOT NULL DEFAULT 'user',
                        is_active BOOLEAN DEFAULT 1,
                        last_login TIMESTAMP,
                        login_count INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (employee_id) REFERENCES employees(id)
                    )
                """)
            
            # ============ 4. ИСТОРИЯ ВХОДОВ ============
            if 'user_login_history' not in existing_tables:
                logger.debug("📦 Создание таблицы user_login_history...")
                self.cursor.execute("""
                    CREATE TABLE user_login_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER NOT NULL,
                        ip_address VARCHAR(45),
                        user_agent TEXT,
                        login_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        logout_time TIMESTAMP,
                        FOREIGN KEY (user_id) REFERENCES users(id)
                    )
                """)
            
            # ============ 5. ЛОГИ ДЕЙСТВИЙ ============
            if 'user_logs' not in existing_tables:
                logger.debug("📦 Создание таблицы user_logs...")
                self.cursor.execute("""
                    CREATE TABLE user_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER NOT NULL,
                        action TEXT NOT NULL,
                        entity_type VARCHAR(30),
                        entity_id INTEGER,
                        old_value TEXT,
                        new_value TEXT,
                        ip_address TEXT,
                        user_agent TEXT,
                        details TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (user_id) REFERENCES users(id)
                    )
                """)
            
            # ============ 6. ПЕРЕВОДЫ ПОЛЕЙ ============
            if 'field_translations' not in existing_tables:
                logger.debug("📦 Создание таблицы field_translations...")
                self.cursor.execute("""
                    CREATE TABLE field_translations (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        table_name VARCHAR(50) NOT NULL,
                        field_name VARCHAR(50) NOT NULL,
                        display_name VARCHAR(100) NOT NULL,
                        description TEXT,
                        import_enabled BOOLEAN DEFAULT 1,
                        export_enabled BOOLEAN DEFAULT 1,
                        display_order INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(table_name, field_name)
                    )
                """)
            
            # ============ 7. РЕЗЕРВЫ ============
            if 'reservations' not in existing_tables:
                logger.debug("📦 Создание таблицы reservations...")
                self.cursor.execute("""
                    CREATE TABLE reservations (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        reservation_number VARCHAR(50) UNIQUE NOT NULL,
                        nomenclature_id INTEGER NOT NULL,
                        quantity INTEGER NOT NULL,
                        warehouse_id INTEGER NOT NULL,
                        storage_bin_id INTEGER,
                        batch_id INTEGER,
                        reserved_by INTEGER NOT NULL,
                        employee_id INTEGER,
                        project_code VARCHAR(50),
                        need_by_date DATE NOT NULL,
                        status VARCHAR(20) NOT NULL DEFAULT 'active',
                        expires_at TIMESTAMP,
                        fulfilled_at TIMESTAMP,
                        fulfilled_by INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        notes TEXT,
                        FOREIGN KEY (nomenclature_id) REFERENCES nomenclatures(id),
                        FOREIGN KEY (warehouse_id) REFERENCES warehouses(id),
                        FOREIGN KEY (storage_bin_id) REFERENCES storage_bins(id),
                        FOREIGN KEY (batch_id) REFERENCES batches(id),
                        FOREIGN KEY (reserved_by) REFERENCES users(id),
                        FOREIGN KEY (employee_id) REFERENCES employees(id),
                        FOREIGN KEY (fulfilled_by) REFERENCES users(id)
                    )
                """)
            
            # ============ 8. СПЕЦИФИКАЦИИ КОМПЛЕКТОВ ============
            if 'kit_specifications' not in existing_tables:
                logger.debug("📦 Создание таблицы kit_specifications...")
                self.cursor.execute("""
                    CREATE TABLE kit_specifications (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        kit_nomenclature_id INTEGER NOT NULL,
                        component_nomenclature_id INTEGER NOT NULL,
                        quantity INTEGER NOT NULL DEFAULT 1,
                        is_optional BOOLEAN DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_by INTEGER,
                        FOREIGN KEY (kit_nomenclature_id) REFERENCES nomenclatures(id) ON DELETE CASCADE,
                        FOREIGN KEY (component_nomenclature_id) REFERENCES nomenclatures(id),
                        FOREIGN KEY (created_by) REFERENCES users(id),
                        UNIQUE(kit_nomenclature_id, component_nomenclature_id)
                    )
                """)
                
                # Создаем индексы для быстрого поиска
                self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_kit_specifications_kit ON kit_specifications(kit_nomenclature_id)")
                self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_kit_specifications_component ON kit_specifications(component_nomenclature_id)")
            
            # ============ 9. ЦЕЛИ РАСХОДОВАНИЯ ============
            if 'expense_purposes' not in existing_tables:
                logger.debug("📦 Создание таблицы expense_purposes...")
                self.cursor.execute("""
                    CREATE TABLE expense_purposes (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        code VARCHAR(20) UNIQUE NOT NULL,
                        name VARCHAR(100) NOT NULL,
                        description TEXT,
                        category VARCHAR(30) DEFAULT 'production',
                        is_active BOOLEAN DEFAULT 1,
                        sort_order INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_by INTEGER,
                        FOREIGN KEY (created_by) REFERENCES users(id)
                    )
                """)
                
                # Добавляем начальные данные
                default_purposes = [
                    ('PROD', 'Основное производство', 'Выпуск основной продукции', 'production', 10),
                    ('AUX', 'Вспомогательное производство', 'Обслуживание производства', 'production', 20),
                    ('DEV', 'Разработка нового оборудования', 'НИОКР, прототипы', 'development', 30),
                    ('RND', 'Эксперименты и исследования', 'Лабораторные работы', 'development', 40),
                    ('REPAIR', 'Ремонт оборудования', 'Текущий и капитальный ремонт', 'maintenance', 50),
                    ('MAINT', 'Обслуживание', 'Техническое обслуживание', 'maintenance', 60),
                    ('OWN', 'Собственные нужды', 'Хознужды, канцелярия', 'own_needs', 70),
                    ('ADMIN', 'Административные нужды', 'Управление', 'own_needs', 80),
                    ('OTHER', 'Прочее', 'Другие цели', 'other', 90)
                ]
                
                for code, name, desc, cat, sort in default_purposes:
                    self.cursor.execute("""
                        INSERT OR IGNORE INTO expense_purposes (code, name, description, category, sort_order)
                        VALUES (?, ?, ?, ?, ?)
                    """, (code, name, desc, cat, sort))
            
            # ============ 10. ПОПЫТКИ ВХОДА (ЗАЩИТА ОТ БРУТФОРСА) ============
            if 'login_attempts' not in existing_tables:
                logger.debug("📦 Создание таблицы login_attempts...")
                self.cursor.execute("""
                    CREATE TABLE login_attempts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ip VARCHAR(45) NOT NULL,
                        attempted_at REAL NOT NULL
                    )
                """)
                self.cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_login_attempts_ip ON login_attempts(ip)"
                )

            # ============ 11. ПРОВЕРКА И ДОБАВЛЕНИЕ ПОЛЕЙ В СУЩЕСТВУЮЩИЕ ТАБЛИЦЫ ============
            
            # Проверяем и добавляем новые поля в таблицу documents
            if 'documents' in existing_tables:
                logger.debug("📝 Проверка полей в таблице documents...")
                
                self.cursor.execute("PRAGMA table_info(documents)")
                document_columns = [col[1] for col in self.cursor.fetchall()]
                
                # Поле для получателя (сотрудник)
                if 'employee_id' not in document_columns:
                    try:
                        self.cursor.execute("ALTER TABLE documents ADD COLUMN employee_id INTEGER REFERENCES employees(id)")
                        logger.info("  ✅ Добавлено поле employee_id")
                    except Exception as e:
                        logger.warning(f"  ⚠️ Ошибка добавления employee_id: {e}")
                
                # Поле для типа выдачи
                if 'issuance_type' not in document_columns:
                    try:
                        self.cursor.execute("ALTER TABLE documents ADD COLUMN issuance_type VARCHAR(20)")
                        logger.info("  ✅ Добавлено поле issuance_type")
                    except Exception as e:
                        logger.warning(f"  ⚠️ Ошибка добавления issuance_type: {e}")
                
                # Поле для подразделения
                if 'department_id' not in document_columns:
                    try:
                        self.cursor.execute("ALTER TABLE documents ADD COLUMN department_id INTEGER REFERENCES departments(id)")
                        logger.info("  ✅ Добавлено поле department_id")
                    except Exception as e:
                        logger.warning(f"  ⚠️ Ошибка добавления department_id: {e}")
                
                # Поле для цели расходования
                if 'purpose_id' not in document_columns:
                    try:
                        self.cursor.execute("ALTER TABLE documents ADD COLUMN purpose_id INTEGER REFERENCES expense_purposes(id)")
                        logger.info("  ✅ Добавлено поле purpose_id")
                    except Exception as e:
                        logger.warning(f"  ⚠️ Ошибка добавления purpose_id: {e}")
                
                # Поле для комментария к цели
                if 'purpose_comment' not in document_columns:
                    try:
                        self.cursor.execute("ALTER TABLE documents ADD COLUMN purpose_comment TEXT")
                        logger.info("  ✅ Добавлено поле purpose_comment")
                    except Exception as e:
                        logger.warning(f"  ⚠️ Ошибка добавления purpose_comment: {e}")
                
                # Поле для центра затрат
                if 'cost_center_id' not in document_columns:
                    try:
                        self.cursor.execute("ALTER TABLE documents ADD COLUMN cost_center_id VARCHAR(50)")
                        logger.info("  ✅ Добавлено поле cost_center_id")
                    except Exception as e:
                        logger.warning(f"  ⚠️ Ошибка добавления cost_center_id: {e}")
            
            # Добавляем поля в таблицу document_items
            if 'document_items' in existing_tables:
                logger.debug("📝 Проверка полей в таблице document_items...")
                
                self.cursor.execute("PRAGMA table_info(document_items)")
                item_columns = [col[1] for col in self.cursor.fetchall()]
                
                # Добавляем поле purpose (цель использования для каждой позиции)
                if 'purpose' not in item_columns:
                    try:
                        self.cursor.execute("ALTER TABLE document_items ADD COLUMN purpose VARCHAR(30)")
                        logger.info("  ✅ Добавлено поле purpose")
                    except Exception as e:
                        logger.warning(f"  ⚠️ Ошибка добавления purpose: {e}")
                
                # Добавляем поле accounting_type (вид учета для каждой позиции)
                if 'accounting_type' not in item_columns:
                    try:
                        self.cursor.execute("ALTER TABLE document_items ADD COLUMN accounting_type VARCHAR(20) DEFAULT 'quantitative'")
                        logger.info("  ✅ Добавлено поле accounting_type")
                    except Exception as e:
                        logger.warning(f"  ⚠️ Ошибка добавления accounting_type: {e}")
                
                # Добавляем поле batch_number (номер партии)
                if 'batch_number' not in item_columns:
                    try:
                        self.cursor.execute("ALTER TABLE document_items ADD COLUMN batch_number VARCHAR(50)")
                        logger.info("  ✅ Добавлено поле batch_number")
                    except Exception as e:
                        logger.warning(f"  ⚠️ Ошибка добавления batch_number: {e}")
                
                # Добавляем поле expiry_date (срок годности)
                if 'expiry_date' not in item_columns:
                    try:
                        self.cursor.execute("ALTER TABLE document_items ADD COLUMN expiry_date DATE")
                        logger.info("  ✅ Добавлено поле expiry_date")
                    except Exception as e:
                        logger.warning(f"  ⚠️ Ошибка добавления expiry_date: {e}")
                
                # Добавляем поле serial_number (серийный номер)
                if 'serial_number' not in item_columns:
                    try:
                        self.cursor.execute("ALTER TABLE document_items ADD COLUMN serial_number VARCHAR(100)")
                        logger.info("  ✅ Добавлено поле serial_number")
                    except Exception as e:
                        logger.warning(f"  ⚠️ Ошибка добавления serial_number: {e}")
                
                # Добавляем поле inventory_number (инвентарный номер)
                if 'inventory_number' not in item_columns:
                    try:
                        self.cursor.execute("ALTER TABLE document_items ADD COLUMN inventory_number VARCHAR(50)")
                        logger.info("  ✅ Добавлено поле inventory_number")
                    except Exception as e:
                        logger.warning(f"  ⚠️ Ошибка добавления inventory_number: {e}")
            
            # Создаем индексы для новых полей
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_documents_employee ON documents(employee_id)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_documents_issuance_type ON documents(issuance_type)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_documents_department ON documents(department_id)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_documents_purpose ON documents(purpose_id)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_document_items_purpose ON document_items(purpose)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_document_items_accounting ON document_items(accounting_type)")
            
            # Создаем индексы (если их нет)
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_user ON user_logs(user_id)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_entity ON user_logs(entity_type, entity_id)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_date ON user_logs(created_at)")
            
            # Индексы для поиска по документам
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_documents_document_date ON documents(document_date)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_documents_status ON documents(status)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_documents_document_type ON documents(document_type)")

            # Индексы для поиска по экземплярам
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_instances_nomenclature ON instances(nomenclature_id)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_instances_status ON instances(status)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_instances_employee ON instances(employee_id)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_instances_location_id ON instances(location_id)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_instances_warehouse_id ON instances(warehouse_id)")

            # Индексы для остатков
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_stocks_nomenclature ON stocks(nomenclature_id)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_stocks_warehouse ON stocks(warehouse_id)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_stocks_batch ON stocks(batch_id)")

            # Составной индекс для частого запроса в post_document
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_stocks_lookup ON stocks(
                    nomenclature_id, warehouse_id, batch_id
                )
            """)

            # Составной индекс для поиска по статусу+дате документа (фильтры + сортировка)
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_documents_status_date
                ON documents(status, document_date DESC)
            """)
            # Составной индекс для документов по типу и дате (отчёты turnover, movement)
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_documents_type_date
                ON documents(document_type, document_date DESC)
            """)
            # Индекс для document_items по document_id (все JOIN-ы через документы)
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_document_items_document
                ON document_items(document_id)
            """)
            # Составной индекс для номенклатуры: is_active + name (список + поиск)
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_nomenclatures_active_name
                ON nomenclatures(is_active, name)
            """)
            # Индекс для instances по next_calibration (планировщик уведомлений)
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_instances_calibration
                ON instances(next_calibration)
                WHERE next_calibration IS NOT NULL
            """)
            # Индекс для batches по expiry_date (уведомления об истечении срока)
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_batches_expiry_active
                ON batches(expiry_date, is_active)
                WHERE expiry_date IS NOT NULL
            """)
            # Индекс для уведомлений по created_at (очистка старых)
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_notifications_created
                ON notifications(created_at)
            """)

            # ============ 11. ТАБЛИЦА ИЗОБРАЖЕНИЙ ============
            self.add_images_table()

            # ============ 12. СЧЁТЧИКИ НОМЕРОВ ДОКУМЕНТОВ ============
            if 'document_number_counters' not in existing_tables:
                logger.debug("📦 Создание таблицы document_number_counters...")
                self.cursor.execute("""
                    CREATE TABLE document_number_counters (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        counter_name TEXT UNIQUE,
                        last_number INTEGER DEFAULT 0,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

            # ============ 13. УВЕДОМЛЕНИЯ ============
            if 'notifications' not in existing_tables:
                logger.debug("📦 Создание таблицы notifications...")
                self.cursor.execute("""
                    CREATE TABLE notifications (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER,
                        type VARCHAR(30) NOT NULL,
                        title VARCHAR(255) NOT NULL,
                        message TEXT,
                        entity_type VARCHAR(30),
                        entity_id INTEGER,
                        expiry_date DATE,
                        is_read BOOLEAN DEFAULT 0,
                        read_at TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                    )
                """)
                self.cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_notifications_user_read ON notifications(user_id, is_read)"
                )

            # ============ 14. НАСТРОЙКИ МОДИФИКАЦИЙ КАТЕГОРИЙ ============
            if 'category_variation_settings' not in existing_tables:
                logger.debug("📦 Создание таблицы category_variation_settings...")
                self.cursor.execute("""
                    CREATE TABLE category_variation_settings (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        category_id INTEGER NOT NULL,
                        variation_type VARCHAR(50) NOT NULL,
                        field_name VARCHAR(50) NOT NULL,
                        display_name VARCHAR(100) NOT NULL,
                        is_required BOOLEAN DEFAULT 0,
                        sort_order INTEGER DEFAULT 0,
                        possible_values TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE,
                        UNIQUE(category_id, variation_type)
                    )
                """)

            # ============ 15. ИСПРАВЛЕНИЕ FK category_variation_settings ============
            self._fix_category_variation_settings_fk()

            # ============ 16. CORE TABLES (CREATE IF NOT EXISTS) ============
            self._ensure_core_tables()

            # ============ 17. АУДИТ-ПОЛЯ (ALTER TABLE) ============
            self._ensure_audit_fields()

            # ============ 18. ВЕРСИЯ СХЕМЫ ============
            self._ensure_schema_version()

            # ============ 19. ИНДЕКСЫ ДЛЯ ПОИСКА ============
            self._ensure_search_indexes()

            self.connection.commit()
            logger.info("✅ Проверка структуры базы данных завершена")

        except Exception as e:
            logger.error(f"❌ Ошибка при проверке структуры БД: {e}")
            traceback.print_exc()
            self.connection.rollback()

    def _ensure_search_indexes(self):
        """Создание индексов для ускорения поиска и JOIN-операций."""
        indexes = [
            # Поиск номенклатуры по имени и SKU
            ("idx_nomenclatures_name",    "nomenclatures(name)"),
            ("idx_nomenclatures_sku",     "nomenclatures(sku)"),
            ("idx_nomenclatures_cat",     "nomenclatures(category_id)"),
            ("idx_nomenclatures_active",  "nomenclatures(is_active, is_deleted)"),
            # Поиск экземпляров
            ("idx_instances_inv_num",     "instances(inventory_number)"),
            ("idx_instances_serial",      "instances(serial_number)"),
            ("idx_instances_status",      "instances(status)"),
            ("idx_instances_nomenclature","instances(nomenclature_id)"),
            ("idx_instances_employee",    "instances(employee_id)"),
            # Остатки
            ("idx_stocks_nomenclature",   "stocks(nomenclature_id)"),
            ("idx_stocks_warehouse",      "stocks(warehouse_id)"),
            # Документы
            ("idx_documents_type_status", "documents(document_type, status)"),
            ("idx_documents_date",        "documents(document_date)"),
            ("idx_documents_number",      "documents(document_number)"),
            # Уведомления
            ("idx_notifications_user",    "notifications(user_id, is_read)"),
            # Пользовательские логи
            ("idx_user_logs_user",        "user_logs(user_id)"),
            ("idx_user_logs_entity",      "user_logs(entity_type, entity_id)"),
            ("idx_user_logs_created",     "user_logs(created_at)"),
            # Партии
            ("idx_batches_nomenclature",  "batches(nomenclature_id)"),
            ("idx_batches_expiry",        "batches(expiry_date)"),
            # Сотрудники
            ("idx_employees_dept",        "employees(department_id)"),
        ]
        try:
            for idx_name, idx_expr in indexes:
                self.cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS {idx_name} ON {idx_expr}"
                )
            logger.debug(f"✅ Индексы поиска: {len(indexes)} проверено/создано")
        except Exception as e:
            logger.error(f"❌ Ошибка создания индексов: {e}")

    def _fix_category_variation_settings_fk(self):
        """Исправление сломанного FK в category_variation_settings.
        Таблица была создана с FK на categories_new(id), которой не существует.
        Пересоздаём с правильным FK на categories(id).
        """
        try:
            self.cursor.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='category_variation_settings'"
            )
            row = self.cursor.fetchone()
            if not row:
                return
            sql = row[0] if isinstance(row, (tuple, list)) else row['sql']
            if 'categories_new' not in sql:
                return  # FK уже правильный

            logger.info("🔧 Исправление FK в category_variation_settings (categories_new -> categories)...")

            self.cursor.execute("PRAGMA foreign_keys = OFF")
            self.cursor.execute("""
                CREATE TABLE category_variation_settings_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    category_id INTEGER NOT NULL,
                    variation_type VARCHAR(50) NOT NULL,
                    field_name VARCHAR(50) NOT NULL,
                    display_name VARCHAR(100) NOT NULL,
                    is_required BOOLEAN DEFAULT 0,
                    sort_order INTEGER DEFAULT 0,
                    possible_values TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE,
                    UNIQUE(category_id, variation_type)
                )
            """)
            self.cursor.execute("""
                INSERT INTO category_variation_settings_new
                SELECT id, category_id, variation_type, field_name, display_name,
                       is_required, sort_order, possible_values, created_at, updated_at
                FROM category_variation_settings
            """)
            self.cursor.execute("DROP TABLE category_variation_settings")
            self.cursor.execute(
                "ALTER TABLE category_variation_settings_new RENAME TO category_variation_settings"
            )
            self.cursor.execute("PRAGMA foreign_keys = ON")
            self.connection.commit()
            logger.info("✅ FK в category_variation_settings исправлен")

        except Exception as e:
            logger.error(f"❌ Ошибка исправления FK category_variation_settings: {e}")
            try:
                self.cursor.execute("PRAGMA foreign_keys = ON")
            except Exception:
                pass

    def _ensure_core_tables(self):
        """Создание основных таблиц если они отсутствуют.
        Гарантирует, что БД можно пересоздать с нуля из кода.
        """
        try:
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            existing = {row[0] for row in self.cursor.fetchall()}

            if 'categories' not in existing:
                logger.info("📦 Создание таблицы categories...")
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS categories (
                        id          INTEGER PRIMARY KEY AUTOINCREMENT,
                        code        VARCHAR(50) NOT NULL,
                        name_ru     VARCHAR(255) NOT NULL,
                        parent_id   INTEGER,
                        level       INTEGER NOT NULL DEFAULT 0,
                        type        VARCHAR(20) NOT NULL DEFAULT 'material'
                                    CHECK (type IN ('asset','tool','equipment','consumable','ppe','material')),
                        accounting_type VARCHAR(20) NOT NULL DEFAULT 'inventory'
                                    CHECK (accounting_type IN ('asset','inventory','service','intangible')),
                        account_method  VARCHAR(20) NOT NULL DEFAULT 'individual'
                                    CHECK (account_method IN ('individual','batch','quantitative','mixed')),
                        sort_order  INTEGER DEFAULT 500,
                        lft         INTEGER NOT NULL DEFAULT 0,
                        rgt         INTEGER NOT NULL DEFAULT 0,
                        path        TEXT,
                        description TEXT,
                        is_active   BOOLEAN DEFAULT 1,
                        created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE (code),
                        FOREIGN KEY (parent_id) REFERENCES categories(id) ON DELETE RESTRICT
                    )
                """)
                self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_cat_parent ON categories(parent_id)")
                self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_cat_active ON categories(is_active)")
                self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_cat_level ON categories(level)")
                self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_cat_type ON categories(type)")
                self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_cat_lft_rgt ON categories(lft, rgt)")
                self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_cat_path ON categories(path)")
            else:
                # Миграция: добавляем path если отсутствует
                if not self.column_exists('categories', 'path'):
                    logger.info("🔄 Добавляем колонку path в categories...")
                    self.cursor.execute("ALTER TABLE categories ADD COLUMN path TEXT")
                    self.connection.commit()
                # Добавляем недостающие индексы для categories
                self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_cat_level ON categories(level)")
                self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_cat_type ON categories(type)")
                self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_cat_lft_rgt ON categories(lft, rgt)")
                self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_cat_path ON categories(path)")

            if 'departments' not in existing:
                logger.info("📦 Создание таблицы departments...")
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS departments (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        code VARCHAR(20) UNIQUE NOT NULL,
                        name TEXT NOT NULL,
                        parent_id INTEGER,
                        manager_id INTEGER,
                        is_active BOOLEAN DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_by INTEGER,
                        updated_by INTEGER,
                        FOREIGN KEY (parent_id) REFERENCES departments(id)
                    )
                """)

            if 'locations' not in existing:
                logger.info("📦 Создание таблицы locations...")
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS locations (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        code VARCHAR(50) UNIQUE NOT NULL,
                        name TEXT NOT NULL,
                        type VARCHAR(30) NOT NULL DEFAULT 'office',
                        parent_id INTEGER,
                        department_id INTEGER,
                        responsible_id INTEGER,
                        building VARCHAR(100),
                        floor VARCHAR(10),
                        room VARCHAR(20),
                        is_active BOOLEAN DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_by INTEGER,
                        updated_by INTEGER,
                        FOREIGN KEY (parent_id) REFERENCES locations(id),
                        FOREIGN KEY (department_id) REFERENCES departments(id)
                    )
                """)

            if 'suppliers' not in existing:
                logger.info("📦 Создание таблицы suppliers...")
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS suppliers (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        code VARCHAR(50) UNIQUE NOT NULL,
                        name TEXT NOT NULL,
                        full_name TEXT,
                        inn VARCHAR(12),
                        kpp VARCHAR(9),
                        contact_person TEXT,
                        phone VARCHAR(20),
                        email VARCHAR(100),
                        address TEXT,
                        is_active BOOLEAN DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_by INTEGER,
                        updated_by INTEGER,
                        FOREIGN KEY (created_by) REFERENCES users(id),
                        FOREIGN KEY (updated_by) REFERENCES users(id)
                    )
                """)

            if 'warehouses' not in existing:
                logger.info("📦 Создание таблицы warehouses...")
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS warehouses (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        code VARCHAR(20) UNIQUE NOT NULL,
                        name TEXT NOT NULL,
                        location_id INTEGER,
                        manager_id INTEGER,
                        type VARCHAR(30) DEFAULT 'general',
                        is_active BOOLEAN DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_by INTEGER,
                        updated_by INTEGER,
                        FOREIGN KEY (location_id) REFERENCES locations(id)
                    )
                """)

            if 'storage_bins' not in existing:
                logger.info("📦 Создание таблицы storage_bins...")
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS storage_bins (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        warehouse_id INTEGER NOT NULL,
                        code VARCHAR(50) NOT NULL,
                        name TEXT,
                        zone VARCHAR(30),
                        rack VARCHAR(20),
                        shelf VARCHAR(20),
                        bin VARCHAR(20),
                        barcode VARCHAR(100),
                        capacity DECIMAL(10,2),
                        capacity_unit VARCHAR(10),
                        is_active BOOLEAN DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_by INTEGER,
                        updated_by INTEGER,
                        UNIQUE(warehouse_id, code),
                        FOREIGN KEY (warehouse_id) REFERENCES warehouses(id)
                    )
                """)

            if 'employees' not in existing:
                logger.info("📦 Создание таблицы employees...")
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS employees (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        employee_number VARCHAR(20) UNIQUE NOT NULL,
                        last_name TEXT NOT NULL,
                        first_name TEXT NOT NULL,
                        middle_name TEXT,
                        full_name TEXT GENERATED ALWAYS AS (
                            last_name || ' ' || first_name ||
                            CASE WHEN middle_name IS NOT NULL THEN ' ' || middle_name ELSE '' END
                        ) STORED,
                        department_id INTEGER,
                        position TEXT,
                        phone VARCHAR(20),
                        email VARCHAR(100),
                        hire_date DATE,
                        is_active BOOLEAN DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_by INTEGER,
                        updated_by INTEGER,
                        manager_id INTEGER REFERENCES employees(id),
                        FOREIGN KEY (department_id) REFERENCES departments(id)
                    )
                """)

            if 'nomenclatures' not in existing:
                logger.info("📦 Создание таблицы nomenclatures...")
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS nomenclatures (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        sku VARCHAR(50) UNIQUE NOT NULL,
                        barcode VARCHAR(100),
                        name TEXT NOT NULL,
                        description TEXT,
                        category_id INTEGER NOT NULL,
                        accounting_type VARCHAR(20) NOT NULL
                            CHECK (accounting_type IN ('individual','batch','quantitative','kit')),
                        unit VARCHAR(10) NOT NULL DEFAULT 'шт.',
                        manufacturer TEXT,
                        model TEXT,
                        brand VARCHAR(100),
                        country TEXT,
                        has_serial_numbers BOOLEAN DEFAULT 0,
                        has_expiry_dates BOOLEAN DEFAULT 0,
                        requires_calibration BOOLEAN DEFAULT 0,
                        requires_maintenance BOOLEAN DEFAULT 0,
                        min_stock INTEGER,
                        reorder_point INTEGER,
                        shelf_life_days INTEGER,
                        attributes TEXT,
                        is_active BOOLEAN DEFAULT 1,
                        is_deleted BOOLEAN DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_by INTEGER,
                        updated_by INTEGER,
                        siz_size TEXT, siz_height TEXT, siz_fullness TEXT, siz_color TEXT,
                        siz_material TEXT, siz_insulation TEXT, siz_temp TEXT, siz_gost TEXT,
                        siz_wear_period INTEGER, siz_previous_issue DATE, siz_next_replacement DATE,
                        FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE RESTRICT,
                        FOREIGN KEY (created_by) REFERENCES users(id),
                        FOREIGN KEY (updated_by) REFERENCES users(id)
                    )
                """)
                self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_nomenclatures_category ON nomenclatures(category_id)")
                self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_nomenclatures_active ON nomenclatures(is_active)")
                self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_nomenclatures_sku ON nomenclatures(sku)")

            if 'nomenclature_variations' not in existing:
                logger.info("📦 Создание таблицы nomenclature_variations...")
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS nomenclature_variations (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        nomenclature_id INTEGER NOT NULL,
                        sku VARCHAR(50) NOT NULL,
                        barcode VARCHAR(100),
                        size VARCHAR(20),
                        height VARCHAR(20),
                        fullness VARCHAR(20),
                        color VARCHAR(50),
                        additional_attributes TEXT,
                        is_active BOOLEAN DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_by INTEGER REFERENCES users(id),
                        updated_by INTEGER REFERENCES users(id),
                        FOREIGN KEY (nomenclature_id) REFERENCES nomenclatures(id) ON DELETE CASCADE,
                        UNIQUE(nomenclature_id, sku)
                    )
                """)

            if 'batches' not in existing:
                logger.info("📦 Создание таблицы batches...")
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS batches (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        nomenclature_id INTEGER NOT NULL,
                        batch_number VARCHAR(100) NOT NULL,
                        internal_batch_code VARCHAR(100),
                        supplier_id INTEGER,
                        invoice_number VARCHAR(50),
                        invoice_date DATE,
                        purchase_price DECIMAL(12,2),
                        purchase_date DATE,
                        production_date DATE,
                        expiry_date DATE,
                        quality_status VARCHAR(30) NOT NULL DEFAULT 'approved',
                        certificate TEXT,
                        is_active BOOLEAN DEFAULT 1,
                        closed_at TIMESTAMP,
                        closed_by INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_by INTEGER,
                        updated_by INTEGER,
                        FOREIGN KEY (nomenclature_id) REFERENCES nomenclatures(id),
                        FOREIGN KEY (supplier_id) REFERENCES suppliers(id),
                        FOREIGN KEY (closed_by) REFERENCES users(id),
                        FOREIGN KEY (created_by) REFERENCES users(id),
                        FOREIGN KEY (updated_by) REFERENCES users(id),
                        UNIQUE(nomenclature_id, batch_number)
                    )
                """)
                self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_batches_nomenclature ON batches(nomenclature_id)")
                self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_batches_expiry ON batches(expiry_date)")

            if 'instances' not in existing:
                logger.info("📦 Создание таблицы instances...")
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS instances (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        nomenclature_id INTEGER NOT NULL,
                        inventory_number VARCHAR(100) UNIQUE,
                        serial_number VARCHAR(100),
                        barcode VARCHAR(100),
                        batch_id INTEGER,
                        supplier_id INTEGER,
                        purchase_date DATE,
                        purchase_price DECIMAL(12,2),
                        warranty_until DATE,
                        status VARCHAR(30) NOT NULL DEFAULT 'in_stock',
                        condition VARCHAR(30) DEFAULT 'good',
                        location_id INTEGER,
                        warehouse_id INTEGER,
                        storage_bin_id INTEGER,
                        employee_id INTEGER,
                        last_calibration DATE,
                        next_calibration DATE,
                        calibration_interval INTEGER,
                        last_maintenance DATE,
                        next_maintenance DATE,
                        maintenance_interval INTEGER,
                        operating_hours INTEGER,
                        issued_date DATE,
                        expected_return_date DATE,
                        actual_return_date DATE,
                        parent_instance_id INTEGER,
                        old_inventory_number VARCHAR(100),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_by INTEGER,
                        updated_by INTEGER,
                        variation_id INTEGER REFERENCES nomenclature_variations(id),
                        siz_size TEXT, siz_height TEXT, siz_fullness TEXT, siz_color TEXT,
                        siz_material TEXT, siz_insulation TEXT, siz_temp TEXT, siz_gost TEXT,
                        siz_wear_period INTEGER, siz_previous_issue DATE, siz_next_replacement DATE,
                        FOREIGN KEY (nomenclature_id) REFERENCES nomenclatures(id),
                        FOREIGN KEY (batch_id) REFERENCES batches(id),
                        FOREIGN KEY (supplier_id) REFERENCES suppliers(id),
                        FOREIGN KEY (location_id) REFERENCES locations(id),
                        FOREIGN KEY (warehouse_id) REFERENCES warehouses(id),
                        FOREIGN KEY (storage_bin_id) REFERENCES storage_bins(id),
                        FOREIGN KEY (employee_id) REFERENCES employees(id),
                        FOREIGN KEY (parent_instance_id) REFERENCES instances(id),
                        FOREIGN KEY (created_by) REFERENCES users(id),
                        FOREIGN KEY (updated_by) REFERENCES users(id)
                    )
                """)

            if 'stocks' not in existing:
                logger.info("📦 Создание таблицы stocks...")
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS stocks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        nomenclature_id INTEGER NOT NULL,
                        warehouse_id INTEGER NOT NULL,
                        storage_bin_id INTEGER,
                        batch_id INTEGER,
                        quantity INTEGER NOT NULL DEFAULT 0,
                        reserved_quantity INTEGER DEFAULT 0,
                        available_quantity INTEGER GENERATED ALWAYS AS (quantity - reserved_quantity) STORED,
                        last_movement_at TIMESTAMP,
                        last_inventory_at TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        variation_id INTEGER REFERENCES nomenclature_variations(id),
                        FOREIGN KEY (nomenclature_id) REFERENCES nomenclatures(id),
                        FOREIGN KEY (warehouse_id) REFERENCES warehouses(id),
                        FOREIGN KEY (storage_bin_id) REFERENCES storage_bins(id),
                        FOREIGN KEY (batch_id) REFERENCES batches(id),
                        UNIQUE(nomenclature_id, warehouse_id, storage_bin_id, batch_id)
                    )
                """)

            if 'documents' not in existing:
                logger.info("📦 Создание таблицы documents...")
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS documents (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        document_type VARCHAR(30) NOT NULL,
                        document_number VARCHAR(50) NOT NULL,
                        document_date DATE NOT NULL,
                        status VARCHAR(20) NOT NULL DEFAULT 'draft',
                        supplier_id INTEGER,
                        customer_id INTEGER,
                        employee_id INTEGER,
                        from_warehouse_id INTEGER,
                        to_warehouse_id INTEGER,
                        from_location_id INTEGER,
                        to_location_id INTEGER,
                        base_document_type VARCHAR(30),
                        base_document_id INTEGER,
                        base_document_number VARCHAR(50),
                        reason TEXT,
                        notes TEXT,
                        total_amount DECIMAL(12,2),
                        posted_at TIMESTAMP,
                        posted_by INTEGER,
                        cancelled_at TIMESTAMP,
                        cancelled_by INTEGER,
                        cancel_reason TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_by INTEGER NOT NULL,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_by INTEGER,
                        issuance_type VARCHAR(20),
                        department_id INTEGER REFERENCES departments(id),
                        cost_center_id VARCHAR(50),
                        purpose_id INTEGER REFERENCES expense_purposes(id),
                        purpose_comment TEXT,
                        number_type VARCHAR(10) DEFAULT 'tn',
                        issuance_number VARCHAR(20),
                        FOREIGN KEY (supplier_id) REFERENCES suppliers(id),
                        FOREIGN KEY (employee_id) REFERENCES employees(id),
                        FOREIGN KEY (from_warehouse_id) REFERENCES warehouses(id),
                        FOREIGN KEY (to_warehouse_id) REFERENCES warehouses(id),
                        FOREIGN KEY (from_location_id) REFERENCES locations(id),
                        FOREIGN KEY (to_location_id) REFERENCES locations(id),
                        FOREIGN KEY (posted_by) REFERENCES users(id),
                        FOREIGN KEY (cancelled_by) REFERENCES users(id),
                        FOREIGN KEY (created_by) REFERENCES users(id),
                        FOREIGN KEY (updated_by) REFERENCES users(id),
                        UNIQUE(document_type, document_number)
                    )
                """)

            if 'document_items' not in existing:
                logger.info("📦 Создание таблицы document_items...")
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS document_items (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        document_id INTEGER NOT NULL,
                        nomenclature_id INTEGER NOT NULL,
                        batch_id INTEGER,
                        instance_id INTEGER,
                        quantity INTEGER NOT NULL,
                        price DECIMAL(12,2),
                        amount DECIMAL(12,2) GENERATED ALWAYS AS (quantity * price) STORED,
                        from_warehouse_id INTEGER,
                        to_warehouse_id INTEGER,
                        from_storage_bin_id INTEGER,
                        to_storage_bin_id INTEGER,
                        from_employee_id INTEGER,
                        to_employee_id INTEGER,
                        parent_item_id INTEGER,
                        return_condition VARCHAR(30),
                        return_notes TEXT,
                        notes TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_by INTEGER,
                        purpose VARCHAR(30),
                        accounting_type VARCHAR(20) DEFAULT 'quantitative',
                        batch_number VARCHAR(50),
                        expiry_date DATE,
                        serial_number VARCHAR(100),
                        inventory_number VARCHAR(50),
                        variation_id INTEGER REFERENCES nomenclature_variations(id),
                        FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE,
                        FOREIGN KEY (nomenclature_id) REFERENCES nomenclatures(id),
                        FOREIGN KEY (batch_id) REFERENCES batches(id),
                        FOREIGN KEY (instance_id) REFERENCES instances(id),
                        FOREIGN KEY (from_warehouse_id) REFERENCES warehouses(id),
                        FOREIGN KEY (to_warehouse_id) REFERENCES warehouses(id),
                        FOREIGN KEY (from_storage_bin_id) REFERENCES storage_bins(id),
                        FOREIGN KEY (to_storage_bin_id) REFERENCES storage_bins(id),
                        FOREIGN KEY (from_employee_id) REFERENCES employees(id),
                        FOREIGN KEY (to_employee_id) REFERENCES employees(id),
                        FOREIGN KEY (parent_item_id) REFERENCES document_items(id),
                        FOREIGN KEY (created_by) REFERENCES users(id)
                    )
                """)

            if 'inventories' not in existing:
                logger.info("📦 Создание таблицы inventories...")
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS inventories (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        inventory_number VARCHAR(50) UNIQUE NOT NULL,
                        inventory_date DATE NOT NULL,
                        warehouse_id INTEGER NOT NULL,
                        status VARCHAR(20) NOT NULL DEFAULT 'draft',
                        completed_at TIMESTAMP,
                        completed_by INTEGER,
                        notes TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_by INTEGER NOT NULL,
                        FOREIGN KEY (warehouse_id) REFERENCES warehouses(id),
                        FOREIGN KEY (completed_by) REFERENCES users(id),
                        FOREIGN KEY (created_by) REFERENCES users(id)
                    )
                """)

            if 'inventory_items' not in existing:
                logger.info("📦 Создание таблицы inventory_items...")
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS inventory_items (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        inventory_id INTEGER NOT NULL,
                        nomenclature_id INTEGER NOT NULL,
                        batch_id INTEGER,
                        storage_bin_id INTEGER,
                        expected_quantity INTEGER NOT NULL,
                        actual_quantity INTEGER NOT NULL,
                        variance INTEGER GENERATED ALWAYS AS (actual_quantity - expected_quantity) STORED,
                        notes TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_by INTEGER,
                        is_discrepancy BOOLEAN DEFAULT 0,
                        FOREIGN KEY (inventory_id) REFERENCES inventories(id) ON DELETE CASCADE,
                        FOREIGN KEY (nomenclature_id) REFERENCES nomenclatures(id),
                        FOREIGN KEY (batch_id) REFERENCES batches(id),
                        FOREIGN KEY (storage_bin_id) REFERENCES storage_bins(id),
                        FOREIGN KEY (created_by) REFERENCES users(id)
                    )
                """)

            self.connection.commit()
            logger.debug("✅ Проверка core-таблиц завершена")

        except Exception as e:
            logger.error(f"❌ Ошибка _ensure_core_tables: {e}")
            self.connection.rollback()

    def _ensure_audit_fields(self):
        """Добавление недостающих аудит-полей во все таблицы."""
        audit_additions = {
            'suppliers':          [
                'updated_at TIMESTAMP',
                'updated_by INTEGER',
                'created_by INTEGER',
            ],
            'kit_specifications': [
                'updated_at TIMESTAMP',
                'updated_by INTEGER',
            ],
            'batches': [
                'updated_at TIMESTAMP',
                'updated_by INTEGER',
            ],
            'locations': [
                'updated_at TIMESTAMP',
                'updated_by INTEGER',
                'created_by INTEGER',
            ],
            'storage_bins': [
                'created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                'updated_at TIMESTAMP',
                'created_by INTEGER',
                'updated_by INTEGER',
            ],
            'departments': [
                'updated_at TIMESTAMP',
                'updated_by INTEGER',
                'created_by INTEGER',
            ],
            'employees': [
                'updated_at TIMESTAMP',
                'updated_by INTEGER',
                'created_by INTEGER',
            ],
            'warehouses': [
                'updated_at TIMESTAMP',
                'updated_by INTEGER',
                'created_by INTEGER',
            ],
            'expense_purposes': [
                'updated_at TIMESTAMP',
                'updated_by INTEGER REFERENCES users(id)',
            ],
        }
        for table, columns in audit_additions.items():
            for col_def in columns:
                col_name = col_def.split()[0]
                if not self.column_exists(table, col_name):
                    try:
                        self.cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")
                        logger.info(f"  ✅ Добавлено поле {table}.{col_name}")
                    except Exception as e:
                        logger.warning(f"  ⚠️ Не удалось добавить {table}.{col_name}: {e}")
        try:
            self.connection.commit()
        except Exception:
            pass

    def _ensure_schema_version(self):
        """Создание таблицы версий схемы и регистрация выполненных миграций."""
        try:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER PRIMARY KEY,
                    description TEXT NOT NULL,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            migrations = [
                (1, 'Initial schema with all core tables'),
                (2, 'Added audit fields to sequences and document tables'),
                (3, 'Added nomenclature_variations and SIZ fields'),
                (4, 'Fixed category_variation_settings FK: categories_new -> categories'),
                (5, 'Added audit fields to suppliers, batches, locations, storage_bins, employees, warehouses, departments'),
                (6, 'Added CREATE TABLE definitions for all core tables in code'),
                (7, 'Added schema_version table for migration tracking'),
            ]
            for ver, desc in migrations:
                self.cursor.execute(
                    "INSERT OR IGNORE INTO schema_version (version, description) VALUES (?, ?)",
                    (ver, desc)
                )
            self.connection.commit()
            logger.debug("✅ Версия схемы обновлена")
        except Exception as e:
            logger.error(f"❌ Ошибка _ensure_schema_version: {e}")

    def get_next_sku(self, sequence_type='instance', prefix=None, year=None):
        """Генерация следующего инвентарного номера
        """
        try:
            import datetime
            if year is None:
                year = datetime.datetime.now().year
            
            # Для экземпляров используем формат без префикса
            if sequence_type == 'instance':
                if year is None:
                    year = datetime.now().year
                return self.get_next_inventory_number(year)
                # Проверяем, существует ли уже последовательность для этого года
                self.cursor.execute("""
                    SELECT id, last_number, format FROM sequences 
                    WHERE sequence_type = ? AND year = ?
                """, ('instance', year))
                
                seq = self.cursor.fetchone()
                
                if seq:
                    new_number = seq[1] + 1
                    self.cursor.execute("""
                        UPDATE sequences SET last_number = ? WHERE id = ?
                    """, (new_number, seq[0]))
                    format_str = seq[2] or '{YEAR}-{NUMBER:06d}'
                else:
                    new_number = 1
                    format_str = '{YEAR}-{NUMBER:06d}'
                    # Вставляем с пустым префиксом для экземпляров
                    self.cursor.execute("""
                        INSERT INTO sequences (sequence_type, prefix, year, last_number, format)
                        VALUES (?, ?, ?, ?, ?)
                    """, ('instance', '', year, new_number, format_str))
                
                self.connection.commit()
                
                # Форматируем номер по шаблону
                return format_str.replace('{YEAR}', str(year)).replace('{NUMBER:06d}', f"{new_number:06d}")
            
            # Для других типов (номенклатура, поставщики и т.д.) используем стандартный формат с префиксом
            else:
                # Определяем префикс по умолчанию
                prefixes = {
                    'nomenclature': 'NOM',
                    'batch': 'BCH',
                    'supplier': 'SUP',
                    'employee': 'EMP',
                    'kit': 'KIT',
                    'document': 'DOC'
                }
                if not prefix:
                    prefix = prefixes.get(sequence_type, sequence_type[:3].upper())
                
                # Проверяем, существует ли уже последовательность
                self.cursor.execute("""
                    SELECT id, last_number, format FROM sequences 
                    WHERE sequence_type = ? AND prefix = ? AND year = ?
                """, (sequence_type, prefix, year))
                
                seq = self.cursor.fetchone()
                
                if seq:
                    new_number = seq[1] + 1
                    self.cursor.execute("""
                        UPDATE sequences SET last_number = ? WHERE id = ?
                    """, (new_number, seq[0]))
                    format_str = seq[2] or '{PREFIX}-{YEAR}-{NUMBER:06d}'
                else:
                    new_number = 1
                    format_str = '{PREFIX}-{YEAR}-{NUMBER:06d}'
                    self.cursor.execute("""
                        INSERT INTO sequences (sequence_type, prefix, year, last_number, format)
                        VALUES (?, ?, ?, ?, ?)
                    """, (sequence_type, prefix, year, new_number, format_str))
                
                self.connection.commit()
                
                # Форматируем номер по шаблону
                return format_str.replace('{PREFIX}', prefix).replace('{YEAR}', str(year)).replace('{NUMBER:06d}', f"{new_number:06d}")
                
        except Exception as e:
            logger.error(f"Ошибка генерации номера: {e}")
            import traceback
            traceback.print_exc()
            # Fallback - используем timestamp
            if sequence_type == 'instance':
                import time
                return f"{datetime.datetime.now().year}-{int(time.time()) % 1000000:06d}"
            else:
                prefix = prefix or sequence_type[:3].upper()
                return f"{prefix}-{int(time.time())}"
                
    def initialize_dictionaries(self):
        """Инициализация справочников начальными данными."""
        try:
            logger.debug("🔄 Инициализация справочников...")
            
            # ============ ПОСЛЕДОВАТЕЛЬНОСТИ ============
            current_year = datetime.now().year
            
            # Проверяем структуру таблицы sequences
            self.cursor.execute("PRAGMA table_info(sequences)")
            columns = [col[1] for col in self.cursor.fetchall()]
            
            if 'year' in columns:
                # Инвентарные номера
                self.cursor.execute("""
                    INSERT OR IGNORE INTO sequences (sequence_type, prefix, year, last_number, format)
                    VALUES (?, ?, ?, ?, ?)
                """, ('inventory', 'INV', current_year, 0, 'INV-{YEAR}-{NUMBER:06d}'))
                
                # SKU
                self.cursor.execute("""
                    INSERT OR IGNORE INTO sequences (sequence_type, prefix, year, last_number, format)
                    VALUES (?, ?, ?, ?, ?)
                """, ('sku', 'NOM', current_year, 0, 'NOM-{NUMBER:05d}'))
                
                # Номера партий
                self.cursor.execute("""
                    INSERT OR IGNORE INTO sequences (sequence_type, prefix, year, last_number, format)
                    VALUES (?, ?, ?, ?, ?)
                """, ('batch', 'BATCH', current_year, 0, 'BATCH-{YEAR}-{NUMBER:05d}'))
                
                # Номера документов
                for doc_type in ['RECEIPT', 'WRITEOFF', 'TRANSFER', 'ISSUANCE', 'RETURN', 'ADJUSTMENT']:
                    self.cursor.execute("""
                        INSERT OR IGNORE INTO sequences (sequence_type, prefix, year, last_number, format)
                        VALUES (?, ?, ?, ?, ?)
                    """, ('document', doc_type, current_year, 0, '{PREFIX}-{YEAR}-{NUMBER:06d}'))
            else:
                logger.warning("⚠️ Таблица sequences имеет другую структуру, пропускаем инициализацию")
            
            # ============ ПОЛЬЗОВАТЕЛИ ПО УМОЛЧАНИЮ ============
            # Проверяем, есть ли админ
            self.cursor.execute("SELECT id FROM users WHERE username = 'admin'")
            if not self.cursor.fetchone():
                import bcrypt, secrets as _secrets
                admin_pw = os.environ.get('DEFAULT_ADMIN_PASSWORD') or _secrets.token_urlsafe(9)
                manager_pw = os.environ.get('DEFAULT_MANAGER_PASSWORD') or _secrets.token_urlsafe(9)
                user_pw = os.environ.get('DEFAULT_USER_PASSWORD') or _secrets.token_urlsafe(9)

                print("\n" + "="*60)
                print("ПЕРВЫЙ ЗАПУСК — СОЗДАНИЕ ПОЛЬЗОВАТЕЛЕЙ ПО УМОЛЧАНИЮ")
                print(f"  admin    / {admin_pw}")
                print(f"  manager  / {manager_pw}")
                print(f"  user     / {user_pw}")
                print("Сохраните эти пароли и смените при первом входе!")
                print("="*60 + "\n")
                logger.warning("Созданы пользователи по умолчанию: admin, manager, user. Смените пароли при первом входе!")

                password_hash = bcrypt.hashpw(admin_pw.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                self.cursor.execute("""
                    INSERT INTO users (username, password_hash, role, is_active, created_at)
                    VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP)
                """, ('admin', password_hash, 'admin'))

                password_hash = bcrypt.hashpw(manager_pw.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                self.cursor.execute("""
                    INSERT INTO users (username, password_hash, role, is_active, created_at)
                    VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP)
                """, ('manager', password_hash, 'manager'))

                password_hash = bcrypt.hashpw(user_pw.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                self.cursor.execute("""
                    INSERT INTO users (username, password_hash, role, is_active, created_at)
                    VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP)
                """, ('user', password_hash, 'viewer'))
            
            # ============ ПЕРЕВОДЫ ПОЛЕЙ ============
            if 'field_translations' in [row[0] for row in self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]:
                logger.debug("📝 Инициализация переводов полей...")
                translations = [
                    ('nomenclatures', 'sku', 'Артикул', 'Уникальный код номенклатуры'),
                    ('nomenclatures', 'name', 'Наименование', 'Полное наименование'),
                    ('nomenclatures', 'category_id', 'Категория', 'Категория номенклатуры'),
                    ('nomenclatures', 'model', 'Модель', 'Модель изделия'),
                    ('nomenclatures', 'manufacturer', 'Производитель', 'Компания-производитель'),
                    ('nomenclatures', 'unit', 'Ед. изм.', 'Единица измерения'),
                    ('nomenclatures', 'accounting_type', 'Тип учета', 'individual/batch/quantitative'),
                    ('nomenclatures', 'min_stock', 'Мин. запас', 'Минимальный запас на складе'),
                    
                    ('instances', 'inventory_number', 'Инв. номер', 'Уникальный инвентарный номер'),
                    ('instances', 'serial_number', 'Серийный номер', 'Заводской номер'),
                    ('instances', 'status', 'Статус', 'Текущий статус'),
                    ('instances', 'condition', 'Состояние', 'Техническое состояние'),
                    ('instances', 'location_id', 'Местоположение', 'Физическое расположение'),
                    ('instances', 'warehouse_id', 'Склад', 'Склад хранения'),
                    ('instances', 'employee_id', 'Сотрудник', 'Ответственный/держатель'),
                    ('instances', 'purchase_date', 'Дата покупки', 'Дата приобретения'),
                    ('instances', 'purchase_price', 'Цена', 'Цена приобретения'),
                    
                    ('batches', 'batch_number', 'Номер партии', 'Номер партии от поставщика'),
                    ('batches', 'expiry_date', 'Срок годности', 'Дата истечения срока'),
                    ('batches', 'quality_status', 'Статус качества', 'Карантин/Одобрено/Брак'),
                    
                    ('stocks', 'quantity', 'Количество', 'Текущее количество'),
                    ('stocks', 'reserved_quantity', 'Зарезервировано', 'Количество в резерве'),
                    ('stocks', 'available_quantity', 'Доступно', 'Доступное количество'),
                ]
                
                for table, field, display, desc in translations:
                    try:
                        self.cursor.execute("""
                            INSERT OR IGNORE INTO field_translations 
                                (table_name, field_name, display_name, description, import_enabled, export_enabled)
                            VALUES (?, ?, ?, ?, 1, 1)
                        """, (table, field, display, desc))
                    except Exception as e:
                        logger.warning(f"⚠️ Ошибка вставки перевода {table}.{field}: {e}")
            
            self.connection.commit()
            logger.info("✅ Справочники инициализированы")
            
        except Exception as e:
            logger.warning(f"⚠️ Ошибка инициализации справочников: {e}")
            # Не делаем rollback, чтобы не потерять другие данные
    
    def update_user_password(self, user_id: int, new_password: str) -> bool:
        """Обновление пароля пользователя"""
        try:
            import bcrypt
            password_hash = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            
            self.cursor.execute("""
                UPDATE users 
                SET password_hash = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (password_hash, user_id))
            self.connection.commit()
            return True
        except Exception as e:
            logger.debug(f"Ошибка update_user_password: {e}")
            return False
    
    # ============ ГЕНЕРАЦИЯ НОМЕРОВ ============

    def get_next_inventory_number(self, year=None):
        """Генерация следующего инвентарного номера для указанного года в формате ГГГГ-НННННН"""
        if year is None:
            year = datetime.now().year
        
        try:
            # Проверяем, существует ли уже последовательность для этого года
            self.cursor.execute("""
                SELECT id, last_number FROM sequences 
                WHERE sequence_type = 'instance' AND year = ?
            """, (year,))
            
            seq = self.cursor.fetchone()
            
            if seq:
                new_number = seq[1] + 1
                self.cursor.execute("""
                    UPDATE sequences SET last_number = ? WHERE id = ?
                """, (new_number, seq[0]))
            else:
                new_number = 1
                self.cursor.execute("""
                    INSERT INTO sequences (sequence_type, prefix, year, last_number, format)
                    VALUES (?, ?, ?, ?, ?)
                """, ('instance', '', year, new_number, '{YEAR}-{NUMBER:06d}'))
            
            self.connection.commit()
            
            # Форматируем номер: ГГГГ-НННННН
            return f"{year}-{new_number:06d}"
            
        except Exception as e:
            logger.error(f"Ошибка генерации инвентарного номера: {e}")
            # Fallback
            import time
            return f"{year}-{int(time.time()) % 1000000:06d}"
    
    def get_next_document_number_atomic(self, doc_type: str) -> str:
        """Атомарная генерация следующего номера документа без гонки за счётчиком.

        Использует единственную операцию INSERT...ON CONFLICT DO UPDATE...RETURNING,
        которая атомарна на уровне SQLite и не требует внешней блокировки.

        Args:
            doc_type: тип документа, например 'receipt', 'issuance', 'writeoff'

        Returns:
            Строка вида 'RECEIPT-2026-000001'
        """
        from datetime import datetime as _dt
        year = _dt.now().year
        counter_name = f"{doc_type}:{year}"
        try:
            with self.lock:
                row = self.connection.execute(
                    """
                    INSERT INTO document_number_counters (counter_name, last_number)
                    VALUES (?, 1)
                    ON CONFLICT (counter_name)
                    DO UPDATE SET last_number = last_number + 1
                    RETURNING last_number
                    """,
                    (counter_name,),
                ).fetchone()
                self.connection.commit()
            new_number = row[0] if row else 1
            prefix = doc_type.upper()[:8]
            return f"{prefix}-{year}-{new_number:06d}"
        except Exception as e:
            logger.error(f"Ошибка атомарной генерации номера документа: {e}")
            import time as _time
            return f"{doc_type.upper()[:8]}-{year}-{int(_time.time()) % 1000000:06d}"

    @staticmethod
    def generate_variation_sku(nomenclature_id, size=None, color=None, attributes=None):
        """
        Генерация уникального SKU для модификации
        """
        try:
            db = get_db()
            
            # Получаем базовый SKU номенклатуры
            nomen = db.execute_query(
                "SELECT sku FROM nomenclatures WHERE id = ?",
                (nomenclature_id,),
                fetch_all=False
            )
            
            if not nomen:
                return None
                
            base_sku = nomen['sku']
            
            # Формируем части SKU
            parts = [base_sku]
            
            if size:
                # Очищаем размер от небуквенных символов
                size_clean = ''.join(c for c in size if c.isalnum())
                parts.append(size_clean)
            
            if color:
                # Берем первые 3 буквы цвета
                color_part = color[:3].upper()
                parts.append(color_part)
            
            if attributes and isinstance(attributes, dict):
                # Добавляем основные атрибуты
                for key, value in attributes.items():
                    if key in ['power', 'volume', 'diameter', 'length']:
                        parts.append(str(value))
            
            # Базовый SKU
            sku = '-'.join(parts)
            
            # Проверяем уникальность
            counter = 1
            original_sku = sku
            
            while True:
                existing = db.execute_query(
                    "SELECT id FROM nomenclature_variations WHERE sku = ?",
                    (sku,),
                    fetch_all=False
                )
                
                if not existing:
                    break
                    
                # Если занято, добавляем счетчик
                sku = f"{original_sku}-{counter}"
                counter += 1
            
            return sku
            
        except Exception as e:
            logger.debug(f"Ошибка генерации SKU: {e}")
            return None

    def generate_number(self, sequence_type: str, prefix: str = None) -> str:
        """
        Генерация уникального номера.
        """
        try:
            current_year = datetime.now().year
            
            if not prefix:
                if sequence_type == 'inventory':
                    prefix = 'INV'
                elif sequence_type == 'sku':
                    prefix = 'NOM'
                elif sequence_type == 'batch':
                    prefix = 'BATCH'
                elif sequence_type == 'kit':
                    prefix = 'KIT'
                else:
                    prefix = sequence_type[:3].upper()
            
            # Проверяем структуру таблицы sequences
            self.cursor.execute("PRAGMA table_info(sequences)")
            columns = [col[1] for col in self.cursor.fetchall()]
            
            if 'year' in columns:
                # Полная структура с годом
                self.cursor.execute("""
                    SELECT id, last_number FROM sequences 
                    WHERE sequence_type = ? AND prefix = ? AND year = ?
                """, (sequence_type, prefix, current_year))
            else:
                # Упрощенная структура без года
                self.cursor.execute("""
                    SELECT id, last_number FROM sequences 
                    WHERE sequence_type = ? AND prefix = ?
                """, (sequence_type, prefix))
            
            seq = self.cursor.fetchone()
            
            if seq:
                new_number = seq['last_number'] + 1
                self.cursor.execute("""
                    UPDATE sequences SET last_number = ?
                    WHERE id = ?
                """, (new_number, seq['id']))
            else:
                new_number = 1
                if 'year' in columns:
                    self.cursor.execute("""
                        INSERT INTO sequences (sequence_type, prefix, year, last_number)
                        VALUES (?, ?, ?, 1)
                    """, (sequence_type, prefix, current_year))
                else:
                    self.cursor.execute("""
                        INSERT INTO sequences (sequence_type, prefix, last_number)
                        VALUES (?, ?, 1)
                    """, (sequence_type, prefix))
            
            self.connection.commit()
            
            # Форматируем номер
            if sequence_type == 'sku':
                return f"{prefix}-{new_number:05d}"
            elif sequence_type == 'kit':
                return f"{prefix}-{current_year}-{new_number:06d}"
            else:
                return f"{prefix}-{current_year}-{new_number:06d}"
            
        except Exception as e:
            logger.debug(f"Ошибка generate_number: {e}")
            timestamp = int(datetime.now().timestamp()) % 10000
            return f"{prefix}-{datetime.now().year}-{timestamp:04d}"
    
    # ============ НОМЕНКЛАТУРА ============
    
    def create_nomenclature(self, data: Dict[str, Any], user_id: int = None) -> Dict[str, Any]:
        """Создание новой позиции номенклатуры"""
        try:
            # Валидация обязательных полей
            name = data.get('name')
            if not name:
                return {'success': False, 'message': 'Отсутствует название'}
            
            category_id = data.get('category_id')
            if not category_id:
                return {'success': False, 'message': 'Отсутствует категория'}
            
            sku = data.get('sku')
            if not sku:
                # Генерируем SKU, если не указан
                sku = self.generate_number('sku')
                data['sku'] = sku
            
            logger.debug(f"Создание номенклатуры: {name}, SKU: {sku}")
            
            # Подготовка JSON атрибутов (если есть)
            attributes = data.get('attributes')
            attributes_json = json.dumps(attributes, ensure_ascii=False) if attributes else None
            
            # Вставка номенклатуры со всеми полями
            # Используем локальный курсор для thread-safe получения lastrowid
            _ins_cursor = self.connection.execute("""
                INSERT INTO nomenclatures (
                    sku, barcode, name, description, category_id,
                    accounting_type, unit,
                    manufacturer, model, brand, country,
                    has_serial_numbers, has_expiry_dates,
                    requires_calibration, requires_maintenance,
                    min_stock, reorder_point, shelf_life_days,
                    attributes, is_active,
                    created_by, updated_by,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, (
                data.get('sku'),
                data.get('barcode'),
                name,
                data.get('description'),
                category_id,
                data.get('accounting_type'),
                data.get('unit', 'шт.'),
                data.get('manufacturer'),
                data.get('model'),
                data.get('brand'),
                data.get('country'),
                1 if data.get('has_serial_numbers') else 0,
                1 if data.get('has_expiry_dates') else 0,
                1 if data.get('requires_calibration') else 0,
                1 if data.get('requires_maintenance') else 0,
                data.get('min_stock'),
                data.get('reorder_point'),
                data.get('shelf_life_days'),
                attributes_json,
                1 if data.get('is_active', True) else 0,
                user_id,
                user_id
            ))
            
            self.connection.commit()
            new_id = _ins_cursor.lastrowid
            _ins_cursor.close()

            # Логируем действие
            self.log_user_action(
                user_id=user_id,
                action='create',
                entity_type='nomenclature',
                entity_id=new_id,
                details=f'Создана номенклатура: {name} (SKU: {sku})'
            )
            
            return {
                'success': True,
                'nomenclature_id': new_id,
                'sku': sku,
                'message': f'Номенклатура {name} успешно создана'
            }
            
        except Exception as e:
            logger.debug(f"Ошибка create_nomenclature: {e}")
            import traceback
            traceback.print_exc()
            self.connection.rollback()
            return {'success': False, 'message': str(e)}
    
    def update_nomenclature(self, nomenclature_id: int, data: Dict[str, Any], user_id: int = None) -> Dict[str, Any]:
        """Обновление номенклатуры"""
        try:
            # Проверяем существование
            nomenclature = self.get_nomenclature_by_id(nomenclature_id)
            if not nomenclature:
                return {'success': False, 'message': 'Номенклатура не найдена'}
            
            # Разрешенные поля для обновления
            allowed_fields = [
                'name', 'description', 'category_id', 'model', 
                'manufacturer', 'brand', 'country', 'unit',
                'accounting_type',
                'min_stock', 'reorder_point', 'shelf_life_days',
                'has_serial_numbers', 'has_expiry_dates',
                'requires_calibration', 'requires_maintenance',
                'barcode', 'is_active', 'siz_size', 'siz_height',
                'siz_fullness', 'siz_color', 'siz_material', 'siz_insulation',
                'siz_temp', 'siz_gost', 'siz_wear_period',
                'siz_previous_issue', 'siz_next_replacement'
            ]
            
            # Формируем SET часть запроса
            set_fields = []
            values = []
            
            for field in allowed_fields:
                if field in data and data[field] is not None:
                    set_fields.append(f"{field} = ?")
                    values.append(data[field])
            
            if not set_fields:
                return {'success': False, 'message': 'Нет данных для обновления'}
            
            # Обновляем JSON атрибуты
            if 'attributes' in data and data['attributes']:
                set_fields.append("attributes = ?")
                values.append(json.dumps(data['attributes'], ensure_ascii=False))
            
            # Добавляем updated_by и id
            values.append(user_id)
            values.append(nomenclature_id)
            
            # Выполняем обновление
            query = f"""
                UPDATE nomenclatures 
                SET {', '.join(set_fields)}, updated_at = CURRENT_TIMESTAMP, updated_by = ?
                WHERE id = ?
            """
            
            self.cursor.execute(query, values)
            self.connection.commit()
            
            # Логируем действие
            self.log_user_action(
                user_id=user_id,
                action='update',
                entity_type='nomenclature',
                entity_id=nomenclature_id,
                details=f'Обновлена номенклатура ID {nomenclature_id}'
            )
            
            return {'success': True, 'message': 'Номенклатура обновлена'}
            
        except Exception as e:
            logger.debug(f"Ошибка update_nomenclature: {e}")
            self.connection.rollback()
            return {'success': False, 'message': str(e)}
    
    def get_nomenclature_by_id(self, nomenclature_id: int) -> Optional[Dict[str, Any]]:
        """
        Получение номенклатуры по ID.
        """
        try:
            logger.debug(f"Ищем номенклатуру с ID {nomenclature_id}")
            
            # Проверяем, есть ли колонка is_deleted
            has_deleted = self.column_exists('nomenclatures', 'is_deleted')
            
            query = """
                SELECT n.*, c.name_ru as category_name, c.type as item_type,
                    c.accounting_type as category_accounting_type
                FROM nomenclatures n
                LEFT JOIN categories c ON n.category_id = c.id
                WHERE n.id = ?
            """
            
            if has_deleted:
                query += " AND (n.is_deleted IS NULL OR n.is_deleted = 0)"
            
            self.cursor.execute(query, (nomenclature_id,))
            
            row = self.cursor.fetchone()
            if row:
                result = dict(row)
                # Парсим JSON атрибуты
                if result.get('attributes'):
                    try:
                        result['attributes'] = json.loads(result['attributes'])
                    except Exception:
                        result['attributes'] = {}
                return result
            return None
            
        except Exception as e:
            logger.debug(f"Ошибка get_nomenclature_by_id: {e}")
            return None
    
    def get_nomenclature_by_sku(self, sku: str) -> Optional[Dict[str, Any]]:
        """
        Получение номенклатуры по SKU.
        """
        try:
            self.cursor.execute("""
                SELECT n.*, c.name_ru as category_name, c.type as item_type
                FROM nomenclatures n
                LEFT JOIN categories c ON n.category_id = c.id
                WHERE n.sku = ? AND n.is_deleted = 0
            """, (sku,))
            
            row = self.cursor.fetchone()
            return dict(row) if row else None
            
        except Exception as e:
            logger.debug(f"Ошибка get_nomenclature_by_sku: {e}")
            return None
    
    def search_nomenclatures(self, query: str = None, category_id: int = None,
                        accounting_type: str = None, item_type: str = None,
                        limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Поиск номенклатуры по различным критериям.
        """
        try:
            sql = """
                SELECT n.*, c.name_ru as category_name, c.type as item_type,
                    (SELECT COUNT(*) FROM instances WHERE nomenclature_id = n.id) as instances_count,
                    (SELECT SUM(quantity) FROM stocks WHERE nomenclature_id = n.id) as total_stock
                FROM nomenclatures n
                LEFT JOIN categories c ON n.category_id = c.id
                WHERE 1=1
            """
            params = []
            
            if query:
                sql += _build_search_where(
                    ['LOWER(n.name)', 'LOWER(n.sku)', 'LOWER(n.model)',
                     'LOWER(n.barcode)', 'LOWER(n.siz_size)',
                     'LOWER(n.siz_color)', 'LOWER(n.siz_material)'],
                    query, params
                )
            
            if category_id:
                sql += " AND n.category_id = ?"
                params.append(category_id)
            
            if accounting_type:
                sql += " AND n.accounting_type = ?"
                params.append(accounting_type)
            
            if item_type:
                sql += " AND c.type = ?"
                params.append(item_type)
            
            sql += " ORDER BY n.created_at DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])
            
            self.cursor.execute(sql, params)
            rows = self.cursor.fetchall()
            
            results = []
            for row in rows:
                item = dict(row)
                if item.get('attributes'):
                    try:
                        item['attributes'] = json.loads(item['attributes'])
                    except Exception:
                        item['attributes'] = {}
                results.append(item)
            
            return results
            
        except Exception as e:
            logger.debug(f"Ошибка search_nomenclatures: {e}")
            traceback.print_exc()
            return []
    
    # ============ КАТЕГОРИИ С ПОДДЕРЖКОЙ NESTED SET ============

    def create_category(self, data: Dict[str, Any], user_id: int = None) -> Dict[str, Any]:
        """Создание категории с поддержкой Nested Set"""
        try:
            # Валидация
            name = data.get('name')
            if not name:
                return {'success': False, 'message': 'Отсутствует название'}
            
            code = data.get('code')
            if not code:
                code = name[:20].upper().replace(' ', '_')
            
            parent_id = data.get('parent_id')
            
            # Начинаем транзакцию
            self.connection.execute("BEGIN TRANSACTION")
            
            # Получаем информацию о родителе
            if parent_id:
                parent = self.execute_query(
                    "SELECT id, lft, rgt, level FROM categories WHERE id = ?",
                    (parent_id,), fetch_all=False
                )
                if not parent:
                    self.connection.rollback()
                    return {'success': False, 'message': 'Родительская категория не найдена'}
                
                # Сдвигаем все узлы справа от места вставки
                self.cursor.execute("""
                    UPDATE categories SET rgt = rgt + 2 
                    WHERE rgt >= ?
                """, (parent['rgt'],))
                
                self.cursor.execute("""
                    UPDATE categories SET lft = lft + 2 
                    WHERE lft > ?
                """, (parent['rgt'] - 1,))
                
                # Новые значения для вставляемой категории
                new_lft = parent['rgt']
                new_rgt = parent['rgt'] + 1
                new_level = parent['level'] + 1
            else:
                # Корневая категория - вставляем в конец
                max_rgt = self.execute_query(
                    "SELECT COALESCE(MAX(rgt), 0) as max_rgt FROM categories",
                    fetch_all=False
                )
                max_rgt_value = max_rgt['max_rgt'] if max_rgt else 0
                
                new_lft = max_rgt_value + 1
                new_rgt = max_rgt_value + 2
                new_level = 0
            
            # Вставка категории (локальный курсор для thread-safe lastrowid)
            _cat_cursor = self.connection.execute("""
                INSERT INTO categories (
                    code, name_ru, description, parent_id,
                    lft, rgt, level,
                    type, accounting_type, account_method,
                    sort_order, is_active,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, (
                code,
                name,
                data.get('description'),
                parent_id,
                new_lft,
                new_rgt,
                new_level,
                data.get('type', data.get('item_type', 'material')),
                data.get('accounting_type', 'inventory'),
                data.get('account_method', 'mixed'),
                data.get('sort_order', 500),
                1 if data.get('is_active', True) else 0
            ))

            self.connection.commit()
            category_id = _cat_cursor.lastrowid
            _cat_cursor.close()
            
            return {
                'success': True,
                'category_id': category_id,
                'code': code,
                'message': f'Категория {name} успешно создана'
            }
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Ошибка создания категории: {e}")
            return {'success': False, 'message': str(e)}

    def update_category(self, category_id: int, data: Dict[str, Any], user_id: int = None) -> Dict[str, Any]:
        """Обновление категории с поддержкой Nested Set"""
        try:
            # Проверяем существование
            category = self.get_category_by_id(category_id)
            if not category:
                return {'success': False, 'message': 'Категория не найдена'}
            
            new_parent_id = data.get('parent_id')
            
            # Если меняется родитель - нужно перестроить дерево
            if new_parent_id != category['parent_id']:
                return self._move_category(category_id, new_parent_id, user_id)
            
            # Разрешенные поля для обновления (без изменения структуры)
            allowed_fields = [
                'code', 'name_ru', 'description',
                'type', 'accounting_type', 'account_method',
                'sort_order', 'is_active'
            ]
            # Маппинг старых имён на новые (для совместимости с вызывающим кодом)
            field_aliases = {'name': 'name_ru', 'item_type': 'type'}
            for old, new in field_aliases.items():
                if old in data and new not in data:
                    data[new] = data[old]

            set_fields = []
            values = []

            for field in allowed_fields:
                if field in data and data[field] is not None:
                    set_fields.append(f"{field} = ?")
                    if field in ['is_active']:
                        values.append(1 if data[field] else 0)
                    else:
                        values.append(data[field])
            
            if not set_fields:
                return {'success': False, 'message': 'Нет данных для обновления'}
            
            values.append(category_id)
            
            query = f"""
                UPDATE categories 
                SET {', '.join(set_fields)}, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """
            
            self.cursor.execute(query, values)
            self.connection.commit()
            
            return {'success': True, 'message': 'Категория обновлена'}
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Ошибка обновления категории: {e}")
            return {'success': False, 'message': str(e)}

    def _move_category(self, category_id: int, new_parent_id: Optional[int], user_id: int = None) -> Dict[str, Any]:
        """Перемещение категории в другое место иерархии"""
        try:
            # Получаем информацию о перемещаемой категории
            category = self.execute_query("""
                SELECT id, lft, rgt, level, parent_id 
                FROM categories WHERE id = ?
            """, (category_id,), fetch_all=False)
            
            if not category:
                return {'success': False, 'message': 'Категория не найдена'}
            
            # Проверяем, не пытаемся ли переместить в себя или потомка
            if new_parent_id:
                parent = self.execute_query("""
                    SELECT id, lft, rgt FROM categories WHERE id = ?
                """, (new_parent_id,), fetch_all=False)
                
                if not parent:
                    return {'success': False, 'message': 'Родительская категория не найдена'}
                
                # Проверка на циклическую ссылку
                if parent['lft'] > category['lft'] and parent['rgt'] < category['rgt']:
                    return {'success': False, 'message': 'Нельзя переместить категорию в своего потомка'}
            
            # Начинаем транзакцию
            self.connection.execute("BEGIN TRANSACTION")
            
            # 1. Удаляем категорию из старого места
            tree_width = category['rgt'] - category['lft'] + 1
            
            # Сдвигаем все справа от удаляемого узла
            self.cursor.execute("""
                UPDATE categories SET lft = lft - ? WHERE lft > ?
            """, (tree_width, category['rgt']))
            
            self.cursor.execute("""
                UPDATE categories SET rgt = rgt - ? WHERE rgt > ?
            """, (tree_width, category['rgt']))
            
            # 2. Вставляем в новое место
            if new_parent_id:
                # Вставляем как дочерний
                parent = self.execute_query("""
                    SELECT lft, rgt, level FROM categories WHERE id = ?
                """, (new_parent_id,), fetch_all=False)
                
                # Сдвигаем узлы справа от места вставки
                self.cursor.execute("""
                    UPDATE categories SET rgt = rgt + ? WHERE rgt >= ?
                """, (tree_width, parent['rgt']))
                
                self.cursor.execute("""
                    UPDATE categories SET lft = lft + ? WHERE lft > ?
                """, (tree_width, parent['rgt'] - 1))
                
                # Новые координаты
                new_lft = parent['rgt']
                new_rgt = parent['rgt'] + tree_width - 1
                new_level = parent['level'] + 1
            else:
                # Вставляем как корневую (в конец)
                max_rgt = self.execute_query(
                    "SELECT COALESCE(MAX(rgt), 0) as max_rgt FROM categories",
                    fetch_all=False
                )
                max_rgt_value = max_rgt['max_rgt'] if max_rgt else 0
                
                new_lft = max_rgt_value + 1
                new_rgt = max_rgt_value + tree_width
                new_level = 0
            
            # Обновляем координаты перемещаемой категории и всех её потомков
            delta_lft = new_lft - category['lft']
            delta_level = new_level - category['level']
            
            self.cursor.execute("""
                UPDATE categories 
                SET lft = lft + ?,
                    rgt = rgt + ?,
                    level = level + ?,
                    parent_id = ?
                WHERE lft >= ? AND rgt <= ?
            """, (delta_lft, delta_lft, delta_level, new_parent_id, 
                category['lft'], category['rgt']))
            
            self.connection.commit()
            
            return {'success': True, 'message': 'Категория перемещена'}
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Ошибка перемещения категории: {e}")
            return {'success': False, 'message': str(e)}

    def delete_category(self, category_id: int, user_id: int = None) -> Dict[str, Any]:
        """Удаление категории с перестроением Nested Set"""
        try:
            # Получаем информацию о категории
            category = self.execute_query("""
                SELECT id, lft, rgt FROM categories WHERE id = ?
            """, (category_id,), fetch_all=False)
            
            if not category:
                return {'success': False, 'message': 'Категория не найдена'}
            
            # Проверяем, есть ли номенклатуры в этой категории
            nomenclatures = self.execute_query(
                "SELECT COUNT(*) as cnt FROM nomenclatures WHERE category_id = ?",
                (category_id,), fetch_all=False
            )
            
            if nomenclatures and nomenclatures['cnt'] > 0:
                return {'success': False, 'message': 'Нельзя удалить категорию, в которой есть номенклатура'}
            
            # Проверяем, есть ли подкатегории
            children = self.execute_query("""
                SELECT COUNT(*) as cnt FROM categories 
                WHERE lft > ? AND rgt < ?
            """, (category['lft'], category['rgt']), fetch_all=False)
            
            if children and children['cnt'] > 0:
                return {'success': False, 'message': 'Нельзя удалить категорию, у которой есть подкатегории'}
            
            # Начинаем транзакцию
            self.connection.execute("BEGIN TRANSACTION")
            
            # Удаляем категорию
            self.cursor.execute("DELETE FROM categories WHERE id = ?", (category_id,))
            
            # Сдвигаем все узлы справа от удаленного
            tree_width = category['rgt'] - category['lft'] + 1
            
            self.cursor.execute("""
                UPDATE categories SET lft = lft - ? WHERE lft > ?
            """, (tree_width, category['rgt']))
            
            self.cursor.execute("""
                UPDATE categories SET rgt = rgt - ? WHERE rgt > ?
            """, (tree_width, category['rgt']))
            
            self.connection.commit()
            
            return {'success': True, 'message': 'Категория удалена'}
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Ошибка удаления категории: {e}")
            return {'success': False, 'message': str(e)}

    def get_category_by_id(self, category_id: int) -> Optional[Dict[str, Any]]:
        """
        Получение категории по ID с дополнительной информацией.
        """
        try:
            logger.debug(f"Ищем категорию с ID {category_id}")

            # Получаем категорию
            self.cursor.execute("SELECT * FROM categories WHERE id = ?", (category_id,))
            row = self.cursor.fetchone()

            if not row:
                logger.debug(f"Категория с ID {category_id} не найдена")
                return None

            result = dict(row)
            logger.debug(f"Найдена категория: {result.get('name_ru')}")
            
            # Получаем родительскую категорию, если есть
            if result.get('parent_id'):
                parent = self.execute_query(
                    "SELECT id, name FROM categories WHERE id = ?",
                    (result['parent_id'],), fetch_all=False
                )
                if parent:
                    result['parent_name'] = parent['name']
            
            # Получаем количество дочерних категорий
            children_count = self.execute_query("""
                SELECT COUNT(*) as cnt FROM categories 
                WHERE lft > ? AND rgt < ?
            """, (result['lft'], result['rgt']), fetch_all=False)
            result['children_count'] = children_count['cnt'] if children_count else 0
            
            # Получаем количество номенклатур в этой категории
            nomenclatures_count = self.execute_query(
                "SELECT COUNT(*) as cnt FROM nomenclatures WHERE category_id = ?",
                (category_id,), fetch_all=False
            )
            result['nomenclatures_count'] = nomenclatures_count['cnt'] if nomenclatures_count else 0
            
            # Получаем полный путь к категории
            path = self.execute_query("""
                SELECT id, name FROM categories
                WHERE lft < ? AND rgt > ?
                ORDER BY lft
            """, (result['lft'], result['rgt']), fetch_all=True)
            result['path'] = [dict(p) for p in path] if path else []
            
            return result
            
        except Exception as e:
            logger.debug(f"Ошибка get_category_by_id: {e}")
            return None

    def get_all_categories(self, item_type: str = None, include_inactive: bool = False) -> List[Dict[str, Any]]:
        """Получение всех категорий в порядке дерева."""
        try:
            sql = """
                SELECT
                    id, code, name_ru as name, parent_id, level, lft, rgt,
                    type as item_type, accounting_type, account_method,
                    description, sort_order, is_active, path
                FROM categories
                WHERE 1=1
            """
            params = []

            if not include_inactive:
                sql += " AND is_active = 1"

            if item_type:
                sql += " AND type = ?"
                params.append(item_type)
            
            sql += " ORDER BY lft"
            
            self.cursor.execute(sql, params)
            rows = self.cursor.fetchall()
            
            categories = [dict(row) for row in rows]
            
            # Добавляем количество детей для каждой категории
            for cat in categories:
                children = [c for c in categories if c['parent_id'] == cat['id']]
                cat['children_count'] = len(children)
                cat['has_children'] = len(children) > 0
                
                # Вычисляем глубину для отображения
                cat['depth'] = cat['level']
                cat['padding'] = cat['level'] * 20  # для отступов в интерфейсе
            
            return categories
            
        except Exception as e:
            logger.debug(f"Ошибка get_all_categories: {e}")
            return []



    def _calculate_category_level(self, category_id, level=0, max_depth=10):
        """
        Устаревшая функция. Для Nested Set уровень уже хранится в БД.
        Оставлена для обратной совместимости.
        """
        try:
            # Просто получаем уровень из БД
            result = self.execute_query(
                "SELECT level FROM categories WHERE id = ?",
                (category_id,), fetch_all=False
            )
            return result['level'] if result else 0
        except Exception:
            # Если что-то пошло не так, используем старую логику
            if level >= max_depth:
                return level

            try:
                cursor = self.connection.cursor()
                cursor.execute("SELECT parent_id FROM categories WHERE id = ?", (category_id,))
                row = cursor.fetchone()
                cursor.close()

                if row and row[0]:
                    return self._calculate_category_level(row[0], level + 1, max_depth)
                else:
                    return level
            except Exception:
                return level

    def get_category_children(self, category_id: int, recursive: bool = False) -> List[Dict[str, Any]]:
        """Получение дочерних категорий."""
        try:
            # Получаем информацию о категории
            category = self.execute_query(
                "SELECT lft, rgt FROM categories WHERE id = ?",
                (category_id,), fetch_all=False
            )
            
            if not category:
                return []
            
            if recursive:
                # Все потомки (рекурсивно)
                query = """
                    SELECT * FROM categories 
                    WHERE lft > ? AND rgt < ?
                    ORDER BY lft
                """
                params = (category['lft'], category['rgt'])
            else:
                # Только прямые дети
                query = """
                    SELECT * FROM categories 
                    WHERE parent_id = ?
                    ORDER BY lft
                """
                params = (category_id,)
            
            children = self.execute_query(query, params, fetch_all=True)
            return [dict(child) for child in children] if children else []
            
        except Exception as e:
            logger.debug(f"Ошибка get_category_children: {e}")
            return []

    def get_category_path(self, category_id: int) -> List[Dict[str, Any]]:
        """Получение пути к категории (все родители)."""
        try:
            category = self.execute_query(
                "SELECT lft, rgt FROM categories WHERE id = ?",
                (category_id,), fetch_all=False
            )
            
            if not category:
                return []
            
            path = self.execute_query("""
                SELECT id, name, level FROM categories
                WHERE lft < ? AND rgt > ?
                ORDER BY lft
            """, (category['lft'], category['rgt']), fetch_all=True)
            
            return [dict(p) for p in path] if path else []
            
        except Exception as e:
            logger.debug(f"Ошибка get_category_path: {e}")
            return []

    def get_category_tree(self, parent_id: int = None) -> List[Dict[str, Any]]:
        """Получение дерева категорий для отображения в форме"""
        try:
            if parent_id:
                # Получаем поддерево от конкретной категории
                parent = self.execute_query(
                    "SELECT lft, rgt FROM categories WHERE id = ?",
                    (parent_id,), fetch_all=False
                )
                if not parent:
                    return []
                
                query = """
                    SELECT * FROM categories 
                    WHERE lft >= ? AND rgt <= ?
                    ORDER BY lft
                """
                params = (parent['lft'], parent['rgt'])
            else:
                # Все дерево - получаем все активные категории
                query = """
                    SELECT 
                        id, 
                        code, 
                        name_ru as name,  -- Переименовываем для удобства
                        parent_id, 
                        level, 
                        lft, 
                        rgt,
                        type as item_type, 
                        accounting_type, 
                        account_method,
                        description, 
                        sort_order, 
                        is_active, 
                        path
                    FROM categories 
                    WHERE is_active = 1
                    ORDER BY lft
                """
                params = []
            
            categories = self.execute_query(query, params, fetch_all=True)
            
            if not categories:
                logger.warning("Нет категорий в БД")
                return []
            
            # Преобразуем в список словарей
            categories_list = []
            for cat in categories:
                cat_dict = dict(cat)
                # Убеждаемся, что есть поле 'name'
                if 'name' not in cat_dict and 'name_ru' in cat_dict:
                    cat_dict['name'] = cat_dict['name_ru']
                categories_list.append(cat_dict)
            
            # Если запрошено дерево для конкретного родителя, возвращаем как есть
            if parent_id:
                return categories_list
            
            # Строим дерево для всех категорий
            root_categories = []
            categories_by_id = {}
            
            # Индексируем все категории
            for cat in categories_list:
                cat['children'] = []
                categories_by_id[cat['id']] = cat
            
            # Строим иерархию
            for cat in categories_list:
                if cat['parent_id'] and cat['parent_id'] in categories_by_id:
                    # Это дочерняя категория
                    categories_by_id[cat['parent_id']]['children'].append(cat)
                elif cat['level'] == 0:
                    # Это корневая категория
                    root_categories.append(cat)
            
            return root_categories
            
        except Exception as e:
            logger.error(f"Ошибка get_category_tree: {e}")
            import traceback
            traceback.print_exc()
            return []

    def get_all_categories_simple(self):
        """Простое получение всех категорий без построения дерева"""
        try:
            query = """
                SELECT 
                    id, 
                    code, 
                    name_ru as name,
                    parent_id, 
                    level
                FROM categories 
                WHERE is_active = 1
                ORDER BY lft
            """
            categories = self.execute_query(query, fetch_all=True)
            
            result = []
            for cat in categories or []:
                cat_dict = dict(cat)
                result.append(cat_dict)
            
            return result
        except Exception as e:
            logger.error(f"Ошибка get_all_categories_simple: {e}")
            return []
        
    # ============ ЭКЗЕМПЛЯРЫ ============
    
    def create_instance(self, nomenclature_id, data, user_id=None):
        """Создание нового экземпляра с поддержкой модификаций"""
        try:
            # Хелпер: пустая строка / 0 → None для FK-полей
            def _fk(val):
                if val is None or val == '' or val == '0':
                    return None
                try:
                    v = int(val)
                    return v if v else None
                except (ValueError, TypeError):
                    return None

            # ============ ПОЛУЧАЕМ VARIATION_ID ИЗ ДАННЫХ ============
            variation_id = _fk(data.get('variation_id'))
            size = None
            color = None

            # Если есть variation_id, получаем данные модификации
            if variation_id:
                variation = self.execute_query("""
                    SELECT size, color FROM nomenclature_variations WHERE id = ?
                """, (variation_id,), fetch_all=False)

                if variation:
                    size = variation.get('size')
                    color = variation.get('color')
                    logger.debug(f"📦 Используем модификацию ID {variation_id}: размер={size}, цвет={color}")
                else:
                    variation_id = None  # variation не найдена — не нарушаем FK
            
            # ============ ПРОВЕРКА НА ДУБЛИКАТ ============
            inventory_number = data.get('inventory_number')
            if inventory_number:
                existing = self.get_instance_by_inventory(inventory_number)
                if existing:
                    return {
                        'success': False, 
                        'error': 'duplicate',
                        'message': f'Экземпляр с инвентарным номером {inventory_number} уже существует',
                        'existing_id': existing['id']
                    }
            
            # ОПРЕДЕЛЯЕМ ГОД ИЗ ДАТЫ ПОКУПКИ
            purchase_year = None
            purchase_date = data.get('purchase_date')
            
            if purchase_date:
                try:
                    if isinstance(purchase_date, str):
                        for fmt in ('%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y', '%Y'):
                            try:
                                if fmt == '%Y':
                                    dt = datetime.strptime(purchase_date + '-01-01', '%Y-%m-%d')
                                else:
                                    dt = datetime.strptime(purchase_date, fmt)
                                purchase_year = dt.year
                                break
                            except Exception:
                                continue
                    elif hasattr(purchase_date, 'year'):
                        purchase_year = purchase_date.year
                except Exception as e:
                    logger.debug(f"Ошибка парсинга даты покупки: {e}")
            
            if not purchase_year:
                purchase_year = datetime.now().year
            
            # ОБРАБОТКА ИНВЕНТАРНОГО НОМЕРА
            final_inventory_number = None
            input_inventory_number = data.get('inventory_number')
            
            if not input_inventory_number:
                final_inventory_number = self.get_next_inventory_number(purchase_year)
            else:
                final_inventory_number = str(input_inventory_number).strip()
                if final_inventory_number.endswith('.0'):
                    final_inventory_number = final_inventory_number[:-2]
            
            # СОХРАНЯЕМ СТАРЫЙ ИНВЕНТАРНЫЙ НОМЕР
            old_inventory_number = data.get('old_inventory_number')
            if old_inventory_number:
                if isinstance(old_inventory_number, (int, float)):
                    old_inventory_number = str(int(old_inventory_number))
                elif isinstance(old_inventory_number, str) and old_inventory_number.endswith('.0'):
                    old_inventory_number = old_inventory_number[:-2]
            
            # ВСТАВКА В БД
            cursor = self.connection.execute("""
                INSERT INTO instances (
                    inventory_number, old_inventory_number, nomenclature_id, variation_id,
                    serial_number, barcode,
                    status, condition, location_id, warehouse_id, employee_id,
                    supplier_id, purchase_date, purchase_price, warranty_until,
                    last_calibration, calibration_interval, next_calibration,
                    last_maintenance, maintenance_interval, next_maintenance,
                    operating_hours, issued_date, expected_return_date,
                    actual_return_date, parent_instance_id, created_by, created_at,
                    siz_size, siz_height, siz_fullness, siz_color, siz_material,
                    siz_insulation, siz_temp, siz_gost, siz_wear_period,
                    siz_previous_issue, siz_next_replacement
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                final_inventory_number,
                old_inventory_number,
                nomenclature_id,
                variation_id,  # ВАЖНО: передаем variation_id
                data.get('serial_number'),
                data.get('barcode'),
                data.get('status', 'in_stock'),
                data.get('condition', 'good'),
                _fk(data.get('location_id')),
                _fk(data.get('warehouse_id')),
                _fk(data.get('employee_id')),
                _fk(data.get('supplier_id')),
                data.get('purchase_date'),
                data.get('purchase_price'),
                data.get('warranty_until'),
                data.get('last_calibration'),
                data.get('calibration_interval'),
                data.get('next_calibration'),
                data.get('last_maintenance'),
                data.get('maintenance_interval'),
                data.get('next_maintenance'),
                data.get('operating_hours', 0),
                data.get('issued_date'),
                data.get('expected_return_date'),
                data.get('actual_return_date'),
                _fk(data.get('parent_instance_id')),
                user_id,
                # Поля СИЗ - если есть данные из модификации, используем их
                size or data.get('siz_size'),
                data.get('siz_height'),
                data.get('siz_fullness'),
                color or data.get('siz_color'),
                data.get('siz_material'),
                data.get('siz_insulation'),
                data.get('siz_temp'),
                data.get('siz_gost'),
                data.get('siz_wear_period'),
                data.get('siz_previous_issue'),
                data.get('siz_next_replacement')
            ))
            
            self.connection.commit()
            instance_id = cursor.lastrowid
            
            return {
                'success': True, 
                'id': instance_id,
                'inventory_number': final_inventory_number,
                'variation_id': variation_id,
                'message': f'Экземпляр создан с инв. номером {final_inventory_number}'
            }
            
        except Exception as e:
            logger.error(f"Ошибка создания экземпляра: {e}")
            self.connection.rollback()
            import traceback
            traceback.print_exc()
            return {'success': False, 'error': str(e), 'message': 'Ошибка создания'}
        
    def create_variation(self, nomenclature_id, data, user_id=None):
        """Создание новой модификации для номенклатуры"""
        try:
            sku = data.get('sku')
            if not sku:
                return {'success': False, 'message': 'SKU обязателен'}
            
            # Проверяем уникальность SKU в рамках номенклатуры
            existing = self.execute_query("""
                SELECT id FROM nomenclature_variations 
                WHERE nomenclature_id = ? AND sku = ?
            """, (nomenclature_id, sku), fetch_all=False)
            
            if existing:
                return {'success': False, 'message': f'Модификация с SKU {sku} уже существует'}
            
            # Сохраняем дополнительные атрибуты в JSON
            additional_attributes = data.get('additional_attributes', {})
            if isinstance(additional_attributes, dict):
                additional_attributes = json.dumps(additional_attributes, ensure_ascii=False)
            
            _var_cursor = self.connection.execute("""
                INSERT INTO nomenclature_variations (
                    nomenclature_id, sku, barcode, size, height, fullness, color,
                    additional_attributes, is_active, created_by, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, CURRENT_TIMESTAMP)
            """, (
                nomenclature_id,
                sku,
                data.get('barcode'),
                data.get('size'),
                data.get('height'),
                data.get('fullness'),
                data.get('color'),
                additional_attributes,
                user_id
            ))

            self.connection.commit()
            variation_id = _var_cursor.lastrowid
            _var_cursor.close()
            
            return {
                'success': True,
                'id': variation_id,
                'sku': sku,
                'message': f'Модификация {sku} успешно создана'
            }
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Ошибка создания модификации: {e}")
            return {'success': False, 'message': str(e)}

    def get_variations(self, nomenclature_id, active_only=True):
        """Получение всех модификаций номенклатуры"""
        try:
            query = """
                SELECT * FROM nomenclature_variations
                WHERE nomenclature_id = ?
            """
            params = [nomenclature_id]
            
            if active_only:
                query += " AND is_active = 1"
            
            query += " ORDER BY size, color"
            
            variations = self.execute_query(query, params, fetch_all=True)
            return [dict(v) for v in variations] if variations else []
            
        except Exception as e:
            logger.error(f"Ошибка получения модификаций: {e}")
            return []

    def get_variation_by_sku(self, sku):
        """Поиск модификации по SKU"""
        try:
            variation = self.execute_query("""
                SELECT nv.*, n.name as model_name
                FROM nomenclature_variations nv
                JOIN nomenclatures n ON nv.nomenclature_id = n.id
                WHERE nv.sku = ? AND nv.is_active = 1
            """, (sku,), fetch_all=False)
            
            return dict(variation) if variation else None
            
        except Exception as e:
            logger.error(f"Ошибка поиска модификации: {e}")
            return None

    def update_variation(self, variation_id, data, user_id=None):
        """Обновление модификации"""
        try:
            allowed_fields = ['sku', 'barcode', 'size', 'height', 'fullness', 
                            'color', 'additional_attributes', 'is_active']
            
            set_fields = []
            values = []
            
            for field in allowed_fields:
                if field in data and data[field] is not None:
                    if field == 'additional_attributes':
                        set_fields.append(f"{field} = ?")
                        if isinstance(data[field], dict):
                            values.append(json.dumps(data[field], ensure_ascii=False))
                        else:
                            values.append(data[field])
                    else:
                        set_fields.append(f"{field} = ?")
                        values.append(data[field])
            
            if not set_fields:
                return {'success': False, 'message': 'Нет данных для обновления'}
            
            set_fields.append("updated_at = CURRENT_TIMESTAMP")
            values.append(variation_id)
            
            query = f"""
                UPDATE nomenclature_variations 
                SET {', '.join(set_fields)}
                WHERE id = ?
            """
            
            self.cursor.execute(query, values)
            self.connection.commit()
            
            return {'success': True, 'message': 'Модификация обновлена'}
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Ошибка обновления модификации: {e}")
            return {'success': False, 'message': str(e)}

    def delete_variation(self, variation_id, user_id=None):
        """Мягкое удаление модификации"""
        try:
            # Проверяем, есть ли экземпляры, использующие эту модификацию
            instances = self.execute_query(
                "SELECT COUNT(*) as cnt FROM instances WHERE variation_id = ?",
                (variation_id,), fetch_all=False
            )
            
            if instances and instances['cnt'] > 0:
                return {
                    'success': False, 
                    'message': f'Нельзя удалить модификацию, используемую в {instances["cnt"]} экземплярах'
                }
            
            # Мягкое удаление
            self.execute_query("""
                UPDATE nomenclature_variations 
                SET is_active = 0, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (variation_id,))
            
            self.connection.commit()
            
            return {'success': True, 'message': 'Модификация деактивирована'}
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Ошибка удаления модификации: {e}")
            return {'success': False, 'message': str(e)}    
               
    def update_instance(self, inventory_number: str, data: Dict[str, Any], user_id: int = None) -> Dict[str, Any]:
        """Обновление экземпляра"""
        try:
            # Получаем экземпляр
            instance = self.get_instance_by_inventory(inventory_number)
            if not instance:
                return {'success': False, 'message': 'Экземпляр не найден'}
            
            # Разрешенные поля для обновления
            allowed_fields = [
                'serial_number', 'barcode', 'status', 'condition',
                'location_id', 'warehouse_id', 'employee_id',
                'supplier_id', 'purchase_date', 'purchase_price',
                'warranty_until', 'last_calibration', 'calibration_interval',
                'last_maintenance', 'maintenance_interval',
                'operating_hours', 'issued_date', 'expected_return_date',
                'actual_return_date', 'notes', 'old_inventory_number'
            ]
            
            set_fields = []
            values = []
            
            for field in allowed_fields:
                if field in data and data[field] is not None:
                    # Для внешних ключей проверяем, что значение > 0
                    if field in ['location_id', 'warehouse_id', 'employee_id', 'supplier_id']:
                        try:
                            val = int(data[field]) if data[field] else None
                            if val and val > 0:
                                # Проверяем существование записи
                                if field == 'location_id':
                                    check = self.execute_query(
                                        "SELECT id FROM locations WHERE id = ?", 
                                        (val,), fetch_all=False
                                    )
                                elif field == 'warehouse_id':
                                    check = self.execute_query(
                                        "SELECT id FROM warehouses WHERE id = ?", 
                                        (val,), fetch_all=False
                                    )
                                elif field == 'employee_id':
                                    check = self.execute_query(
                                        "SELECT id FROM employees WHERE id = ?", 
                                        (val,), fetch_all=False
                                    )
                                elif field == 'supplier_id':
                                    check = self.execute_query(
                                        "SELECT id FROM suppliers WHERE id = ?", 
                                        (val,), fetch_all=False
                                    )
                                
                                if check:
                                    set_fields.append(f"{field} = ?")
                                    values.append(val)
                                else:
                                    logger.debug(f"Предупреждение: {field}={val} не найден, пропускаем")
                            else:
                                # Если значение 0 или None, устанавливаем NULL
                                set_fields.append(f"{field} = ?")
                                values.append(None)
                        except (ValueError, TypeError):
                            # Если не удалось преобразовать в int, устанавливаем NULL
                            set_fields.append(f"{field} = ?")
                            values.append(None)
                    
                    elif field in ['purchase_date', 'last_calibration', 'last_maintenance', 
                                'issued_date', 'expected_return_date', 'actual_return_date', 'warranty_until']:
                        # Для полей с датой
                        date_val = self._parse_date(data[field])
                        if date_val:
                            set_fields.append(f"{field} = ?")
                            values.append(date_val)
                        else:
                            set_fields.append(f"{field} = ?")
                            values.append(None)
                    
                    elif field in ['purchase_price']:
                        # Для числовых полей с плавающей точкой
                        price_val = self._parse_float(data[field])
                        set_fields.append(f"{field} = ?")
                        values.append(price_val)
                    
                    elif field in ['calibration_interval', 'maintenance_interval', 'operating_hours']:
                        # Для целочисленных полей
                        int_val = self._parse_int(data[field])
                        set_fields.append(f"{field} = ?")
                        values.append(int_val)
                    
                    else:
                        # Для остальных полей (строки)
                        set_fields.append(f"{field} = ?")
                        values.append(data[field])
            
            # Автоматический расчет следующих дат
            if 'last_calibration' in data and data['last_calibration']:
                cal_date = self._parse_date(data['last_calibration'])
                cal_interval = None
                
                # Проверяем, есть ли интервал в данных или берем из БД
                if 'calibration_interval' in data and data['calibration_interval']:
                    cal_interval = self._parse_int(data['calibration_interval'])
                elif instance.get('calibration_interval'):
                    cal_interval = instance['calibration_interval']
                
                if cal_date and cal_interval and cal_interval > 0:
                    try:
                        cal_date_obj = datetime.strptime(cal_date, '%Y-%m-%d')
                        next_date = cal_date_obj + timedelta(days=cal_interval)
                        set_fields.append("next_calibration = ?")
                        values.append(next_date.strftime('%Y-%m-%d'))
                    except Exception as e:
                        logger.debug(f"Ошибка расчета следующей поверки: {e}")
            
            if 'last_maintenance' in data and data['last_maintenance']:
                maint_date = self._parse_date(data['last_maintenance'])
                maint_interval = None
                
                if 'maintenance_interval' in data and data['maintenance_interval']:
                    maint_interval = self._parse_int(data['maintenance_interval'])
                elif instance.get('maintenance_interval'):
                    maint_interval = instance['maintenance_interval']
                
                if maint_date and maint_interval and maint_interval > 0:
                    try:
                        maint_date_obj = datetime.strptime(maint_date, '%Y-%m-%d')
                        next_date = maint_date_obj + timedelta(days=maint_interval)
                        set_fields.append("next_maintenance = ?")
                        values.append(next_date.strftime('%Y-%m-%d'))
                    except Exception as e:
                        logger.debug(f"Ошибка расчета следующего ТО: {e}")
            
            if not set_fields:
                return {'success': False, 'message': 'Нет данных для обновления'}
            
            # Добавляем updated_by и id
            values.append(user_id)
            values.append(instance['id'])
            
            # Формируем и выполняем запрос
            query = f"""
                UPDATE instances 
                SET {', '.join(set_fields)}, updated_at = CURRENT_TIMESTAMP, updated_by = ?
                WHERE id = ?
            """
            
            logger.debug(f"SQL: {query}")
            logger.debug(f"Values: {values}")
            
            self.cursor.execute(query, values)
            self.connection.commit()
            
            # Логируем действие
            self.log_user_action(
                user_id=user_id,
                action='update',
                entity_type='instance',
                entity_id=instance['id'],
                details=f'Обновлен экземпляр {inventory_number}'
            )
            
            return {'success': True, 'message': 'Экземпляр обновлен'}
            
        except Exception as e:
            logger.debug(f"Ошибка update_instance: {e}")
            import traceback
            traceback.print_exc()
            self.connection.rollback()
            return {'success': False, 'message': str(e)}
    
    def get_instance_by_inventory(self, inventory_number):
        """Получение экземпляра по инвентарному номеру (новому или старому)"""
        try:
            self.cursor.execute("""
                SELECT i.*, n.name as nomenclature_name, n.sku, n.model,
                    c.name_ru as category_name, c.type as item_type,
                    l.name as location_name, w.name as warehouse_name,
                    e.full_name as employee_name
                FROM instances i
                LEFT JOIN nomenclatures n ON i.nomenclature_id = n.id
                LEFT JOIN categories c ON n.category_id = c.id
                LEFT JOIN locations l ON i.location_id = l.id
                LEFT JOIN warehouses w ON i.warehouse_id = w.id
                LEFT JOIN employees e ON i.employee_id = e.id
                WHERE i.inventory_number = ? OR i.old_inventory_number = ?
            """, (inventory_number, inventory_number))
            
            row = self.cursor.fetchone()
            return dict(row) if row else None
            
        except Exception as e:
            logger.debug(f"Ошибка get_instance_by_inventory: {e}")
            return None
    
    def get_instance_by_id(self, instance_id: int) -> Optional[Dict[str, Any]]:
        """
        Получение экземпляра по ID.
        """
        try:
            self.cursor.execute("""
                SELECT i.*, n.name as nomenclature_name, n.sku, n.model,
                    c.name_ru as category_name, c.type as item_type,
                    l.name as location_name, w.name as warehouse_name,
                    e.full_name as employee_name, s.name as supplier_name
                FROM instances i
                LEFT JOIN nomenclatures n ON i.nomenclature_id = n.id
                LEFT JOIN categories c ON n.category_id = c.id
                LEFT JOIN locations l ON i.location_id = l.id
                LEFT JOIN warehouses w ON i.warehouse_id = w.id
                LEFT JOIN employees e ON i.employee_id = e.id
                LEFT JOIN suppliers s ON i.supplier_id = s.id
                WHERE i.id = ?
            """, (instance_id,))
            
            row = self.cursor.fetchone()
            return dict(row) if row else None
            
        except Exception as e:
            logger.debug(f"Ошибка get_instance_by_id: {e}")
            return None

    def search_instances(self, query=None, limit=50):
        """Поиск экземпляров по новому или старому инвентарному номеру"""
        try:
            sql = """
                SELECT i.*, n.name as nomenclature_name, n.sku
                FROM instances i
                LEFT JOIN nomenclatures n ON i.nomenclature_id = n.id
                WHERE 1=1
            """
            params = []
            
            if query:
                sql += """ AND (i.inventory_number LIKE ? OR i.old_inventory_number LIKE ? 
                            OR n.name LIKE ? OR n.sku LIKE ?)"""
                search_term = f"%{query}%"
                params.extend([search_term, search_term, search_term, search_term])
            
            sql += " ORDER BY i.created_at DESC LIMIT ?"
            params.append(limit)
            
            self.cursor.execute(sql, params)
            rows = self.cursor.fetchall()
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.debug(f"Ошибка search_instances: {e}")
            return []
        
    # ============ МЕТОДЫ ДЛЯ КОМПЛЕКТОВ ============

    def create_kit_instance(self, kit_nomenclature_id, inventory_number, location_id=None, user_id=None):
        """Создание экземпляра комплекта со всеми компонентами"""
        try:
            # Начинаем транзакцию
            self.connection.execute("BEGIN TRANSACTION")
            
            # 1. Создаем головной экземпляр
            cursor = self.connection.execute("""
                INSERT INTO instances (
                    inventory_number, nomenclature_id, status, location_id, created_by, created_at
                ) VALUES (?, ?, 'in_stock', ?, ?, CURRENT_TIMESTAMP)
            """, (inventory_number, kit_nomenclature_id, location_id, user_id))
            
            parent_id = cursor.lastrowid
            
            # 2. Получаем спецификацию комплекта
            components = self.execute_query("""
                SELECT ks.*, n.name, n.sku, n.unit
                FROM kit_specifications ks
                JOIN nomenclatures n ON ks.component_nomenclature_id = n.id
                WHERE ks.kit_nomenclature_id = ?
            """, (kit_nomenclature_id,), fetch_all=True)
            
            # 3. Создаем экземпляры компонентов
            for component in components or []:
                # Генерируем инвентарный номер для компонента
                component_inv = f"{inventory_number}-{component['id']}"
                
                self.connection.execute("""
                    INSERT INTO instances (
                        inventory_number, nomenclature_id, status, location_id, 
                        parent_instance_id, created_by, created_at
                    ) VALUES (?, ?, 'in_stock', ?, ?, ?, CURRENT_TIMESTAMP)
                """, (component_inv, component['component_nomenclature_id'], 
                    location_id, parent_id, user_id))
            
            self.connection.commit()
            
            # Логируем действие
            self.log_user_action(
                user_id=user_id,
                action='create',
                entity_type='kit_instance',
                entity_id=parent_id,
                details=f'Создан экземпляр комплекта: {inventory_number}'
            )
            
            return {
                'success': True,
                'message': f'Комплект создан, инв. номер: {inventory_number}',
                'parent_id': parent_id
            }
            
        except Exception as e:
            self.connection.rollback()
            logger.debug(f'Ошибка создания комплекта: {e}')
            traceback.print_exc()
            return {'success': False, 'error': str(e)}

    def get_kit_components(self, parent_instance_id):
        """Получение состава комплекта для отображения"""
        try:
            # Сначала получаем головной экземпляр
            parent = self.execute_query("""
                SELECT nomenclature_id FROM instances WHERE id = ?
            """, (parent_instance_id,), fetch_all=False)
            
            if not parent:
                return []
            
            query = """
                SELECT 
                    ks.id,
                    ks.quantity,
                    n.id as nomenclature_id,
                    n.name,
                    n.sku,
                    n.unit,
                    i.id as instance_id,
                    i.inventory_number,
                    i.status
                FROM kit_specifications ks
                JOIN nomenclatures n ON ks.component_nomenclature_id = n.id
                LEFT JOIN instances i ON i.parent_instance_id = ? 
                    AND i.nomenclature_id = n.id
                WHERE ks.kit_nomenclature_id = ?
                ORDER BY n.name
            """
            
            components = self.execute_query(query, (parent['nomenclature_id'], parent_instance_id), fetch_all=True)
            return [dict(c) for c in components] if components else []
            
        except Exception as e:
            logger.debug(f'Ошибка получения состава комплекта: {e}')
            traceback.print_exc()
            return []

    def get_all_kits(self):
        """Получение списка всех комплектов"""
        try:
            query = """
                SELECT n.*, 
                    (SELECT COUNT(*) FROM kit_specifications WHERE kit_nomenclature_id = n.id) as components_count,
                    (SELECT COUNT(*) FROM instances WHERE nomenclature_id = n.id) as instances_count,
                    c.name_ru as category_name
                FROM nomenclatures n
                LEFT JOIN categories c ON n.category_id = c.id
                WHERE n.accounting_type = 'kit' 
                OR n.id IN (SELECT DISTINCT kit_nomenclature_id FROM kit_specifications)
                ORDER BY n.name
            """
            
            kits = self.execute_query(query, fetch_all=True)
            return [dict(k) for k in kits] if kits else []
            
        except Exception as e:
            logger.debug(f'Ошибка получения списка комплектов: {e}')
            return []

    def get_kit_by_id(self, kit_id):
        """Получение комплекта по ID"""
        try:
            query = """
                SELECT n.*, c.name_ru as category_name
                FROM nomenclatures n
                LEFT JOIN categories c ON n.category_id = c.id
                WHERE n.id = ?
            """
            
            kit = self.execute_query(query, (kit_id,), fetch_all=False)
            return dict(kit) if kit else None
            
        except Exception as e:
            logger.debug(f'Ошибка получения комплекта: {e}')
            return None

    def get_kit_specification(self, kit_nomenclature_id):
        """Получение спецификации комплекта"""
        try:
            query = """
                SELECT ks.*, 
                    n.name as component_name,
                    n.sku as component_sku,
                    n.unit as component_unit,
                    c.name_ru as category_name
                FROM kit_specifications ks
                JOIN nomenclatures n ON ks.component_nomenclature_id = n.id
                LEFT JOIN categories c ON n.category_id = c.id
                WHERE ks.kit_nomenclature_id = ?
                ORDER BY n.name
            """
            
            components = self.execute_query(query, (kit_nomenclature_id,), fetch_all=True)
            return [dict(c) for c in components] if components else []
            
        except Exception as e:
            logger.debug(f'Ошибка получения спецификации комплекта: {e}')
            return []

    def add_component_to_kit(self, kit_nomenclature_id, component_nomenclature_id, quantity=1, user_id=None):
        """Добавление компонента в комплект"""
        try:
            # Проверяем существование
            existing = self.execute_query("""
                SELECT id FROM kit_specifications 
                WHERE kit_nomenclature_id = ? AND component_nomenclature_id = ?
            """, (kit_nomenclature_id, component_nomenclature_id), fetch_all=False)
            
            if existing:
                # Обновляем количество
                self.execute_query("""
                    UPDATE kit_specifications 
                    SET quantity = quantity + ? 
                    WHERE kit_nomenclature_id = ? AND component_nomenclature_id = ?
                """, (quantity, kit_nomenclature_id, component_nomenclature_id))
                message = 'Количество компонента увеличено'
            else:
                # Добавляем новый компонент (без created_by)
                self.execute_query("""
                    INSERT INTO kit_specifications 
                    (kit_nomenclature_id, component_nomenclature_id, quantity)
                    VALUES (?, ?, ?)
                """, (kit_nomenclature_id, component_nomenclature_id, quantity))
                message = 'Компонент добавлен'
            
            self.connection.commit()
            return {'success': True, 'message': message}
            
        except Exception as e:
            self.connection.rollback()
            return {'success': False, 'error': str(e)}

    def remove_component_from_kit(self, kit_nomenclature_id, component_nomenclature_id, user_id=None):
        """Удаление компонента из комплекта"""
        try:
            self.execute_query("""
                DELETE FROM kit_specifications 
                WHERE kit_nomenclature_id = ? AND component_nomenclature_id = ?
            """, (kit_nomenclature_id, component_nomenclature_id))
            
            self.connection.commit()
            
            self.log_user_action(
                user_id=user_id,
                action='delete',
                entity_type='kit_specification',
                entity_id=kit_nomenclature_id,
                details=f'Удален компонент {component_nomenclature_id} из комплекта {kit_nomenclature_id}'
            )
            
            return {'success': True, 'message': 'Компонент удален'}
            
        except Exception as e:
            self.connection.rollback()
            logger.debug(f'Ошибка удаления компонента: {e}')
            return {'success': False, 'error': str(e)}

    def update_component_quantity(self, kit_nomenclature_id, component_nomenclature_id, quantity, user_id=None):
        """Обновление количества компонента в комплекте"""
        try:
            if quantity <= 0:
                # Если количество 0 или меньше - удаляем компонент
                return self.remove_component_from_kit(kit_nomenclature_id, component_nomenclature_id, user_id)
            
            self.execute_query("""
                UPDATE kit_specifications 
                SET quantity = ? 
                WHERE kit_nomenclature_id = ? AND component_nomenclature_id = ?
            """, (quantity, kit_nomenclature_id, component_nomenclature_id))
            
            self.connection.commit()
            
            self.log_user_action(
                user_id=user_id,
                action='update',
                entity_type='kit_specification',
                entity_id=kit_nomenclature_id,
                details=f'Обновлено количество компонента {component_nomenclature_id} до {quantity}'
            )
            
            return {'success': True, 'message': 'Количество обновлено'}
            
        except Exception as e:
            self.connection.rollback()
            logger.debug(f'Ошибка обновления количества: {e}')
            return {'success': False, 'error': str(e)}

    def delete_kit(self, kit_nomenclature_id, user_id=None):
        """Удаление комплекта"""
        try:
            # Проверяем, есть ли созданные экземпляры
            instances = self.execute_query(
                "SELECT COUNT(*) as cnt FROM instances WHERE nomenclature_id = ?",
                (kit_nomenclature_id,), fetch_all=False
            )
            
            if instances and instances['cnt'] > 0:
                return {'success': False, 'message': 'Нельзя удалить комплект, по которому есть экземпляры'}
            
            # Удаляем спецификацию (каскадно)
            self.execute_query("DELETE FROM kit_specifications WHERE kit_nomenclature_id = ?", (kit_nomenclature_id,))
            
            # Удаляем номенклатуру
            self.execute_query("DELETE FROM nomenclatures WHERE id = ?", (kit_nomenclature_id,))
            
            self.connection.commit()
            
            self.log_user_action(
                user_id=user_id,
                action='delete',
                entity_type='kit',
                entity_id=kit_nomenclature_id,
                details=f'Удален комплект {kit_nomenclature_id}'
            )
            
            return {'success': True, 'message': 'Комплект удален'}
            
        except Exception as e:
            self.connection.rollback()
            logger.debug(f'Ошибка удаления комплекта: {e}')
            return {'success': False, 'error': str(e)}

    def find_instances_by_parent(self, parent_instance_id):
        """Поиск всех экземпляров, принадлежащих родительскому"""
        try:
            query = """
                SELECT i.*, n.name as nomenclature_name, n.sku
                FROM instances i
                JOIN nomenclatures n ON i.nomenclature_id = n.id
                WHERE i.parent_instance_id = ?
                ORDER BY i.inventory_number
            """
            
            instances = self.execute_query(query, (parent_instance_id,), fetch_all=True)
            return [dict(i) for i in instances] if instances else []
            
        except Exception as e:
            logger.debug(f'Ошибка поиска дочерних экземпляров: {e}')
            return []
    
    # ============ ПАРТИИ ============
    
    def create_batch(self, nomenclature_id: int, data: Dict[str, Any],
                    user_id: int = None) -> Dict[str, Any]:
        """
        Создание партии для номенклатуры.
        """
        try:
            # Проверяем номенклатуру
            nomenclature = self.get_nomenclature_by_id(nomenclature_id)
            if not nomenclature:
                return {'success': False, 'message': 'Номенклатура не найдена'}
            
            if nomenclature['accounting_type'] != 'batch':
                return {'success': False, 'message': 'Номенклатура не предназначена для партионного учета'}
            
            batch_number = data.get('batch_number')
            if not batch_number:
                return {'success': False, 'message': 'Отсутствует номер партии'}
            
            # Генерируем внутренний код
            internal_batch_code = self.generate_number('batch')
            
            supplier_id = self._get_or_create_supplier(data.get('supplier'))
            
            try:
                self.cursor.execute("""
                    INSERT INTO batches (
                        nomenclature_id, batch_number, internal_batch_code,
                        supplier_id, invoice_number, invoice_date,
                        purchase_price, purchase_date,
                        production_date, expiry_date,
                        quality_status, certificate,
                        is_active, created_by,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, CURRENT_TIMESTAMP)
                """, (
                    nomenclature_id,
                    batch_number,
                    internal_batch_code,
                    supplier_id,
                    data.get('invoice_number'),
                    self._parse_date(data.get('invoice_date')),
                    self._parse_float(data.get('purchase_price')),
                    self._parse_date(data.get('purchase_date')),
                    self._parse_date(data.get('production_date')),
                    self._parse_date(data.get('expiry_date')),
                    data.get('quality_status', 'approved'),
                    data.get('certificate'),
                    user_id
                ))
                
                self.connection.commit()
                
                # Получаем созданную партию
                self.cursor.execute("""
                    SELECT * FROM batches WHERE internal_batch_code = ?
                """, (internal_batch_code,))
                batch = self.cursor.fetchone()
                
                # Логируем действие
                self.log_user_action(
                    user_id=user_id,
                    action='create',
                    entity_type='batch',
                    entity_id=batch['id'],
                    details=f'Создана партия: {batch_number} для {nomenclature["name"]}'
                )
                
                return {
                    'success': True,
                    'batch_id': batch['id'],
                    'batch_number': batch_number,
                    'internal_code': internal_batch_code,
                    'message': f'Партия {batch_number} успешно создана'
                }
                
            except sqlite3.IntegrityError:
                return {'success': False, 'message': f'Партия {batch_number} уже существует'}
            
        except Exception as e:
            logger.debug(f"Ошибка create_batch: {e}")
            traceback.print_exc()
            self.connection.rollback()
            return {'success': False, 'message': str(e)}
    
    # ============ ЗАПАСЫ ============
    
    def update_stock(self, nomenclature_id: int, warehouse_name: str, 
                    quantity: int, batch_id: int = None, 
                    user_id: int = None) -> Dict[str, Any]:
        """
        Обновление количества на складе.
        """
        try:
            # Получаем или создаем склад
            warehouse_id = self._get_or_create_warehouse(warehouse_name)
            if not warehouse_id:
                return {'success': False, 'message': 'Склад не найден'}
            
            # Проверяем существующий остаток
            self.cursor.execute("""
                SELECT id, quantity FROM stocks 
                WHERE nomenclature_id = ? AND warehouse_id = ? 
                  AND (batch_id = ? OR (batch_id IS NULL AND ? IS NULL))
            """, (nomenclature_id, warehouse_id, batch_id, batch_id))
            
            existing = self.cursor.fetchone()
            
            if existing:
                new_quantity = existing['quantity'] + quantity
                self.cursor.execute("""
                    UPDATE stocks SET quantity = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (new_quantity, existing['id']))
                stock_id = existing['id']
            else:
                self.cursor.execute("""
                    INSERT INTO stocks (nomenclature_id, warehouse_id, batch_id, quantity, 
                                       created_at, updated_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """, (nomenclature_id, warehouse_id, batch_id, quantity))
                stock_id = self.cursor.lastrowid
            
            self.connection.commit()
            
            return {
                'success': True,
                'stock_id': stock_id,
                'quantity': new_quantity if existing else quantity,
                'message': 'Запасы обновлены'
            }
            
        except Exception as e:
            logger.debug(f"Ошибка update_stock: {e}")
            self.connection.rollback()
            return {'success': False, 'message': str(e)}
    
    # ============ ИНВЕНТАРИЗАЦИЯ ============
    
    def create_inventory(self, data: Dict[str, Any], user_id: int = None) -> Dict[str, Any]:
        """Создание инвентаризации"""
        try:
            # Валидация
            if not data.get('name'):
                return {'success': False, 'message': 'Отсутствует название'}
            
            inventory_type = data.get('inventory_type', 'full')
            
            # Генерация номера
            inventory_number = f"INV-{datetime.now().year}-{datetime.now().strftime('%m%d')}-{int(datetime.now().timestamp()) % 1000:03d}"
            
            # Создание инвентаризации
            self.cursor.execute("""
                INSERT INTO inventories (
                    inventory_number, name, inventory_type, status,
                    start_date, end_date, description,
                    warehouse_id, location_id,
                    created_by, created_at
                ) VALUES (?, ?, ?, 'planned', ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                inventory_number,
                data['name'],
                inventory_type,
                data.get('start_date'),
                data.get('end_date'),
                data.get('description', ''),
                data.get('warehouse_id'),
                data.get('location_id'),
                user_id
            ))
            
            inventory_id = self.cursor.lastrowid
            
            # Добавляем позиции
            self._add_inventory_items(inventory_id, data)
            
            self.connection.commit()
            
            self.log_user_action(
                user_id=user_id,
                action='create',
                entity_type='inventory',
                entity_id=inventory_id,
                details=f'Создана инвентаризация {inventory_number}'
            )
            
            return {
                'success': True,
                'inventory_id': inventory_id,
                'inventory_number': inventory_number,
                'message': f'Инвентаризация {inventory_number} успешно создана'
            }
            
        except Exception as e:
            logger.debug(f"Ошибка create_inventory: {e}")
            self.connection.rollback()
            return {'success': False, 'message': str(e)}
    
    def _add_inventory_items(self, inventory_id: int, data: Dict[str, Any]):
        """Добавление позиций в инвентаризацию"""
        try:
            inventory_type = data.get('inventory_type', 'full')
            warehouse_id = data.get('warehouse_id')
            location_id = data.get('location_id')
            
            # Получаем список номенклатуры для инвентаризации
            if inventory_type == 'full':
                # Все активные номенклатуры
                self.cursor.execute("""
                    SELECT id FROM nomenclatures 
                    WHERE is_active = 1 AND is_deleted = 0
                """)
                items = self.cursor.fetchall()
                
                for item in items:
                    # Получаем ожидаемое количество
                    self.cursor.execute("""
                        SELECT SUM(quantity) as total FROM stocks 
                        WHERE nomenclature_id = ?
                    """, (item['id'],))
                    qty = self.cursor.fetchone()
                    expected_qty = qty['total'] if qty and qty['total'] else 0
                    
                    self.cursor.execute("""
                        INSERT INTO inventory_items (
                            inventory_id, nomenclature_id,
                            expected_quantity,
                            verified
                        ) VALUES (?, ?, ?, 0)
                    """, (inventory_id, item['id'], expected_qty))
                    
            elif inventory_type == 'warehouse' and warehouse_id:
                # Только на конкретном складе
                self.cursor.execute("""
                    SELECT DISTINCT s.nomenclature_id, 
                           SUM(s.quantity) as total
                    FROM stocks s
                    WHERE s.warehouse_id = ? AND s.quantity > 0
                    GROUP BY s.nomenclature_id
                """, (warehouse_id,))
                items = self.cursor.fetchall()
                
                for item in items:
                    self.cursor.execute("""
                        INSERT INTO inventory_items (
                            inventory_id, nomenclature_id,
                            expected_quantity, expected_warehouse_id,
                            verified
                        ) VALUES (?, ?, ?, ?, 0)
                    """, (inventory_id, item['nomenclature_id'], 
                          item['total'], warehouse_id))
                    
            elif inventory_type == 'location' and location_id:
                # Только в конкретном местоположении
                self.cursor.execute("""
                    SELECT DISTINCT i.nomenclature_id,
                           COUNT(i.id) as total
                    FROM instances i
                    WHERE i.location_id = ?
                    GROUP BY i.nomenclature_id
                """, (location_id,))
                items = self.cursor.fetchall()
                
                for item in items:
                    self.cursor.execute("""
                        INSERT INTO inventory_items (
                            inventory_id, nomenclature_id,
                            expected_quantity, expected_location_id,
                            verified
                        ) VALUES (?, ?, ?, ?, 0)
                    """, (inventory_id, item['nomenclature_id'],
                          item['total'], location_id))
            
        except Exception as e:
            logger.debug(f"Ошибка _add_inventory_items: {e}")
            raise
    
    def _check_inventory_completion(self, inventory_id: int):
        """Проверка завершенности инвентаризации"""
        try:
            # Получаем статистику
            stats = self.execute_query("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN verified = 1 THEN 1 ELSE 0 END) as verified_count
                FROM inventory_items
                WHERE inventory_id = ?
            """, (inventory_id,), fetch_all=False)
            
            if stats and stats['total'] == stats['verified_count']:
                # Все позиции проверены - завершаем инвентаризацию
                self.cursor.execute("""
                    UPDATE inventories 
                    SET status = 'completed', completed_at = CURRENT_TIMESTAMP
                    WHERE id = ? AND status = 'in_progress'
                """, (inventory_id,))
                self.connection.commit()
                
        except Exception as e:
            logger.debug(f"Ошибка _check_inventory_completion: {e}")
    
    # ============ ПРАВИЛА КАТЕГОРИЙ ============
    
    def determine_category_by_rules(self, name: str, model: str = None,
                                   serial: str = None) -> Optional[Dict[str, Any]]:
        """
        Определение категории по правилам.
        """
        try:
            if not name:
                return None
            
            name_lower = name.lower()
            
            # 1. Точное совпадение
            self.cursor.execute("""
                SELECT cr.category_id, c.name_ru as category_name, c.type as item_type,
                       c.accounting_type, cr.priority
                FROM category_rules cr
                JOIN categories c ON cr.category_id = c.id
                WHERE cr.rule_type = 'name_contains'
                  AND LOWER(cr.rule_value) = ?
                  AND cr.is_active = 1 AND c.is_active = 1
                ORDER BY cr.priority DESC, cr.id
                LIMIT 1
            """, (name_lower,))
            
            rule = self.cursor.fetchone()
            if rule:
                return dict(rule)
            
            # 2. Содержит ключевое слово
            self.cursor.execute("""
                SELECT cr.category_id, c.name_ru as category_name, c.type as item_type,
                       c.accounting_type, cr.rule_value, cr.priority
                FROM category_rules cr
                JOIN categories c ON cr.category_id = c.id
                WHERE cr.rule_type = 'name_contains'
                  AND ? LIKE '%' || LOWER(cr.rule_value) || '%'
                  AND cr.is_active = 1 AND c.is_active = 1
                ORDER BY LENGTH(cr.rule_value) DESC, cr.priority DESC
                LIMIT 1
            """, (name_lower,))
            
            rule = self.cursor.fetchone()
            if rule:
                return dict(rule)
            
            # 3. Начинается с
            self.cursor.execute("""
                SELECT cr.category_id, c.name_ru as category_name, c.type as item_type,
                       c.accounting_type
                FROM category_rules cr
                JOIN categories c ON cr.category_id = c.id
                WHERE cr.rule_type = 'name_starts'
                  AND ? LIKE LOWER(cr.rule_value) || '%'
                  AND cr.is_active = 1 AND c.is_active = 1
                ORDER BY LENGTH(cr.rule_value) DESC, cr.priority DESC
                LIMIT 1
            """, (name_lower,))
            
            rule = self.cursor.fetchone()
            if rule:
                return dict(rule)
            
            # 4. По модели
            if model:
                model_lower = model.lower()
                self.cursor.execute("""
                    SELECT cr.category_id, c.name_ru as category_name, c.type as item_type,
                           c.accounting_type
                    FROM category_rules cr
                    JOIN categories c ON cr.category_id = c.id
                    WHERE cr.rule_type = 'model'
                      AND ? LIKE '%' || LOWER(cr.rule_value) || '%'
                      AND cr.is_active = 1 AND c.is_active = 1
                    ORDER BY LENGTH(cr.rule_value) DESC, cr.priority DESC
                    LIMIT 1
                """, (model_lower,))
                
                rule = self.cursor.fetchone()
                if rule:
                    return dict(rule)
            
            return None
            
        except Exception as e:
            logger.debug(f"Ошибка determine_category_by_rules: {e}")
            return None
    
    # ============ ПОЛЬЗОВАТЕЛИ ============
    
    def verify_user(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """
        Проверка учетных данных пользователя - поддерживает как bcrypt, так и старые SHA256 хеши
        """
        try:
            logger.debug(f"🔍 Проверка пользователя: {username}")
            
            self.cursor.execute("""
                SELECT * FROM users 
                WHERE username = ? AND is_active = 1
            """, (username,))
            
            user_data = self.cursor.fetchone()
            if not user_data:
                logger.error(f"❌ Пользователь {username} не найден")
                return None
            
            user_dict = dict(user_data)
            logger.info(f"✅ Пользователь найден: {user_dict['username']}, роль: {user_dict['role']}")
            
            stored_hash = user_dict.get('password_hash')
            if not stored_hash:
                logger.error(f"❌ У пользователя {username} нет пароля")
                return None
            
            # Пробуем проверить пароль разными способами
            password_valid = False
            
            # Способ 1: Проверка через bcrypt
            try:
                import bcrypt
                if bcrypt.checkpw(password.encode('utf-8'), stored_hash.encode('utf-8')):
                    password_valid = True
                    logger.info(f"✅ Пароль верный (bcrypt)")
            except Exception as e:
                logger.warning(f"⚠️ bcrypt проверка не удалась: {e}")
            
            # Способ 2: Проверка через старый SHA256 (если bcrypt не сработал)
            if not password_valid:
                try:
                    import hashlib
                    # Пробуем разные варианты SHA256
                    sha256_hash = hashlib.sha256(password.strip().encode('utf-8')).hexdigest().lower()
                    
                    # Проверяем прямой SHA256
                    if sha256_hash == stored_hash.lower():
                        password_valid = True
                        logger.info(f"✅ Пароль верный (SHA256 прямой)")
                    
                    # Проверяем старый формат (с солью)
                    if not password_valid and stored_hash.startswith('sha256$'):
                        # Формат: sha256$соль$хеш
                        parts = stored_hash.split('$')
                        if len(parts) == 3:
                            _, salt, hash_value = parts
                            test_hash = hashlib.sha256((salt + password).encode('utf-8')).hexdigest()
                            if test_hash == hash_value:
                                password_valid = True
                                logger.info(f"✅ Пароль верный (SHA256 с солью)")
                except Exception as e:
                    logger.warning(f"⚠️ SHA256 проверка не удалась: {e}")
            
            if password_valid:
                logger.info(f"✅ Аутентификация успешна для {username}")
                
                # Обновляем хеш на bcrypt для будущих входов
                try:
                    import bcrypt
                    new_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                    self.cursor.execute("""
                        UPDATE users 
                        SET password_hash = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (new_hash, user_dict['id']))
                    self.connection.commit()
                    logger.info(f"✅ Пароль обновлен на bcrypt")
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось обновить пароль на bcrypt: {e}")
                
                # Обновляем статистику входа
                self.cursor.execute("""
                    UPDATE users 
                    SET last_login = CURRENT_TIMESTAMP, 
                        login_count = COALESCE(login_count, 0) + 1 
                    WHERE id = ?
                """, (user_dict['id'],))
                self.connection.commit()
                
                # Логируем вход
                try:
                    self.cursor.execute("""
                        INSERT INTO user_login_history (user_id, ip_address, login_time) 
                        VALUES (?, ?, CURRENT_TIMESTAMP)
                    """, (user_dict['id'], None))
                    self.connection.commit()
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка логирования входа: {e}")
                
                # Удаляем хеш пароля из результата
                user_dict.pop('password_hash', None)
                return user_dict
            else:
                logger.error(f"❌ Неверный пароль для {username}")
                return None
            
        except Exception as e:
            logger.error(f"❌ Ошибка verify_user: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def get_user_by_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        """
        Получение пользователя по ID с данными из employees.
        """
        try:
            # Проверяем структуру таблицы users
            self.cursor.execute("PRAGMA table_info(users)")
            user_columns = [col[1] for col in self.cursor.fetchall()]
            
            # Базовый запрос с JOIN к employees
            query = """
                SELECT u.*, 
                    e.first_name, e.last_name, e.middle_name,
                    e.full_name as employee_full_name,
                    e.employee_number,
                    d.name as department_name
                FROM users u
                LEFT JOIN employees e ON u.employee_id = e.id
                LEFT JOIN departments d ON e.department_id = d.id
                WHERE u.id = ?
            """
            
            self.cursor.execute(query, (user_id,))
            user_data = self.cursor.fetchone()
            
            if user_data:
                user_dict = dict(user_data)
                # Удаляем хеш пароля
                user_dict.pop('password_hash', None)
                
                # Формируем полное имя для отображения
                if user_dict.get('employee_full_name'):
                    user_dict['full_name'] = user_dict['employee_full_name']
                elif user_dict.get('first_name') and user_dict.get('last_name'):
                    user_dict['full_name'] = f"{user_dict['last_name']} {user_dict['first_name']}".strip()
                    if user_dict.get('middle_name'):
                        user_dict['full_name'] += f" {user_dict['middle_name']}"
                else:
                    user_dict['full_name'] = user_dict.get('username', '')
                
                # Добавляем поля для обратной совместимости
                user_dict['first_name'] = user_dict.get('first_name', '')
                user_dict['last_name'] = user_dict.get('last_name', '')
                
                return user_dict
            return None
            
        except Exception as e:
            logger.debug(f"Ошибка get_user_by_id: {e}")
            # Возвращаем минимальные данные из сессии
            return {
                'id': user_id,
                'username': 'user',
                'full_name': 'Пользователь',
                'first_name': '',
                'last_name': '',
                'email': '',
                'role': 'user',
                'is_active': 1
            }
    
    def log_user_login(self, user_id: int, ip_address: str = None) -> bool:
        """
        Логирование входа пользователя.
        """
        try:
            self.cursor.execute("""
                INSERT INTO user_login_history (user_id, ip_address, login_time) 
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """, (user_id, ip_address))
            self.connection.commit()
            return True
        except Exception as e:
            logger.debug(f"Ошибка логирования входа: {e}")
            return False
    
    def log_user_action(self, user_id: int, action: str, entity_type: str = None,
                   entity_id: int = None, old_value: str = None,
                   new_value: str = None, details: str = None) -> bool:
        """Логирование действия пользователя"""
        try:
            # Проверяем существование таблицы
            self.cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='user_logs'
            """)
            if not self.cursor.fetchone():
                # Таблица не существует, создаем её
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS user_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER NOT NULL,
                        action TEXT NOT NULL,
                        entity_type VARCHAR(30),
                        entity_id INTEGER,
                        old_value TEXT,
                        new_value TEXT,
                        details TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (user_id) REFERENCES users(id)
                    )
                """)
                self.connection.commit()
            
            self.cursor.execute("""
                INSERT INTO user_logs (user_id, action, entity_type, entity_id,
                                    old_value, new_value, details, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (user_id, action, entity_type, entity_id, old_value, new_value, details))
            self.connection.commit()
            return True
        except Exception as e:
            logger.debug(f"Ошибка логирования действия: {e}")
            return False
    
    def get_user_login_history(self, user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Получение истории входов пользователя.
        """
        try:
            self.cursor.execute("""
                SELECT * FROM user_login_history 
                WHERE user_id = ? 
                ORDER BY login_time DESC 
                LIMIT ?
            """, (user_id, limit))
            rows = self.cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.debug(f"Ошибка get_user_login_history: {e}")
            return []
    
    # ============ ДОКУМЕНТЫ ============
    
    def create_document(self, doc_type: str, data: Dict[str, Any], user_id: int = None) -> Dict[str, Any]:
        """Создание документа"""
        try:
            # Валидация в зависимости от типа
            if doc_type in ['receipt', 'return'] and not data.get('to_warehouse_id'):
                return {'success': False, 'message': 'Для поступления/возврата необходимо указать склад получатель'}
            
            if doc_type in ['issuance', 'write_off'] and not data.get('from_warehouse_id'):
                return {'success': False, 'message': 'Для выдачи/списания необходимо указать склад отправитель'}
            
            if doc_type == 'transfer' and (not data.get('from_warehouse_id') or not data.get('to_warehouse_id')):
                return {'success': False, 'message': 'Для перемещения необходимо указать оба склада'}
            # Генерация номера
            doc_number = data.get('document_number')
            if not doc_number:
                doc_number = self.generate_number('document', doc_type.upper())
            
            # Вставка документа
            self.cursor.execute("""
                INSERT INTO documents (
                    document_type, document_number, document_date,
                    status, supplier_id, to_warehouse_id,
                    reason, accounting_type, created_by, created_at
                ) VALUES (?, ?, ?, 'draft', ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                doc_type,
                doc_number,
                data.get('document_date', datetime.now().strftime('%Y-%m-%d')),
                data.get('supplier_id'),
                data.get('to_warehouse_id'),
                data.get('reason'),
                data.get('accounting_type'),
                user_id
            ))
            
            document_id = self.cursor.lastrowid
            
            return {
                'success': True,
                'document_id': document_id,
                'document_number': doc_number,
                'message': f'Документ {doc_number} успешно создан'
            }
            
        except Exception as e:
            logger.debug(f"Ошибка create_document: {e}")
            self.connection.rollback()
            return {'success': False, 'message': str(e)}
        
    def _process_kit_receipt(self, document_id, item, user_id):
        """Обработка поступления комплекта"""
        try:
            # Создаем запись для комплекта
            self.cursor.execute("""
                INSERT INTO document_items (
                    document_id, nomenclature_id, quantity, price, notes, created_by
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                document_id,
                item['nomenclature_id'],
                item.get('quantity', 1),
                item.get('price', 0),
                'Комплект',
                user_id
            ))
            
            # Получаем состав комплекта
            components = self.execute_query("""
                SELECT component_nomenclature_id, quantity 
                FROM kit_specifications 
                WHERE kit_nomenclature_id = ?
            """, (item['nomenclature_id'],), fetch_all=True)
            
            # Создаем записи для компонентов
            for comp in components or []:
                self.cursor.execute("""
                    INSERT INTO document_items (
                        document_id, nomenclature_id, quantity, notes, created_by
                    ) VALUES (?, ?, ?, ?, ?)
                """, (
                    document_id,
                    comp['component_nomenclature_id'],
                    comp['quantity'] * item.get('quantity', 1),
                    'Компонент комплекта',
                    user_id
                ))
                
        except Exception as e:
            logger.debug(f"Ошибка обработки комплекта: {e}")
            raise
        
    def update_document(self, document_id: int, data: Dict[str, Any], user_id: int = None) -> Dict[str, Any]:
        """Обновление документа"""
        try:
            # Проверяем статус
            doc = self.execute_query(
                "SELECT status FROM documents WHERE id = ?",
                (document_id,), fetch_all=False
            )
            if not doc:
                return {'success': False, 'message': 'Документ не найден'}
            
            if doc['status'] != 'draft':
                return {'success': False, 'message': 'Можно редактировать только черновики'}
            
            # Обновляем основные поля (ДОБАВЛЕНЫ ПОЛЯ ДЛЯ ВЫДАЧИ)
            self.cursor.execute("""
                UPDATE documents 
                SET document_date = ?, supplier_id = ?, employee_id = ?,
                    from_warehouse_id = ?, to_warehouse_id = ?,
                    issuance_type = ?, department_id = ?, purpose_id = ?,
                    purpose_comment = ?, cost_center_id = ?,
                    reason = ?, notes = ?, updated_at = CURRENT_TIMESTAMP, updated_by = ?
                WHERE id = ?
            """, (
                data.get('document_date'),
                data.get('supplier_id'),
                data.get('employee_id'),
                data.get('from_warehouse_id'),
                data.get('to_warehouse_id'),
                data.get('issuance_type'),
                data.get('department_id'),
                data.get('purpose_id'),
                data.get('purpose_comment'),
                data.get('cost_center_id'),
                data.get('reason'),
                data.get('notes'),
                user_id,
                document_id
            ))
        
        # ... остальной код (удаление старых позиций и добавление новых)
            
            # Удаляем старые позиции
            self.cursor.execute("DELETE FROM document_items WHERE document_id = ?", (document_id,))
            
            # Добавляем новые
            total_amount = 0
            for item in data.get('items', []):
                amount = float(item.get('price', 0)) * int(item.get('quantity', 0))
                total_amount += amount
                
                self.cursor.execute("""
                    INSERT INTO document_items (
                        document_id, nomenclature_id, batch_id, instance_id,
                        quantity, price, from_warehouse_id, to_warehouse_id,
                        from_employee_id, to_employee_id, notes, created_by
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    document_id,
                    item['nomenclature_id'],
                    item.get('batch_id'),
                    item.get('instance_id'),
                    item.get('quantity', 0),
                    item.get('price', 0),
                    data.get('from_warehouse_id'),
                    data.get('to_warehouse_id'),
                    data.get('from_employee_id'),
                    data.get('to_employee_id'),
                    item.get('notes'),
                    user_id
                ))
            
            # Обновляем общую сумму
            self.cursor.execute("""
                UPDATE documents SET total_amount = ? WHERE id = ?
            """, (total_amount, document_id))
            
            self.connection.commit()
            
            return {'success': True, 'message': 'Документ обновлен'}
            
        except Exception as e:
            logger.debug(f"Ошибка update_document: {e}")
            self.connection.rollback()
            return {'success': False, 'message': str(e)}

    def post_document(self, document_id: int, user_id: int = None) -> Dict[str, Any]:
        """Проведение документа"""
        try:
            # Получаем документ
            doc = self.execute_query("SELECT * FROM documents WHERE id = ?", (document_id,), fetch_all=False)
            if not doc:
                return {'success': False, 'message': 'Документ не найден'}
            
            if doc['status'] != 'draft':
                return {'success': False, 'message': 'Документ уже проведен или отменен'}
            
            # Обновляем статус документа
            self.cursor.execute("""
                UPDATE documents 
                SET status = 'posted', posted_at = CURRENT_TIMESTAMP, posted_by = ?
                WHERE id = ?
            """, (user_id, document_id))
            
            self.connection.commit()
            
            return {'success': True, 'message': 'Документ проведен'}
            
        except Exception as e:
            logger.debug(f"Ошибка post_document: {e}")
            self.connection.rollback()
            return {'success': False, 'message': str(e)}
        
    def _get_or_create_employee(self, full_name: str) -> Optional[int]:
        """
        Получение или создание сотрудника по ФИО.
        """
        if not full_name:
            return None
        
        try:
            # Пытаемся найти существующего
            self.cursor.execute("""
                SELECT id FROM employees 
                WHERE full_name LIKE ? OR full_name LIKE ?
                LIMIT 1
            """, (f"%{full_name}%", f"%{full_name}%"))
            
            emp = self.cursor.fetchone()
            if emp:
                return emp[0]
            
            # Разбираем ФИО
            parts = full_name.strip().split()
            last_name = parts[0] if len(parts) > 0 else ''
            first_name = parts[1] if len(parts) > 1 else ''
            middle_name = parts[2] if len(parts) > 2 else ''
            
            # Генерируем табельный номер
            emp_number = f"EMP-{datetime.now().year}-{int(datetime.now().timestamp()) % 10000:04d}"
            
            self.cursor.execute("""
                INSERT INTO employees (employee_number, last_name, first_name, middle_name, is_active)
                VALUES (?, ?, ?, ?, 1)
            """, (emp_number, last_name, first_name, middle_name))
            
            self.connection.commit()
            return self.cursor.lastrowid
            
        except Exception as e:
            logger.debug(f"Ошибка _get_or_create_employee: {e}")
            return None
    
    def post_document(self, document_id: int, user_id: int = None) -> Dict[str, Any]:
        """Проведение документа с обновлением остатков"""
        try:
            # Получаем документ
            doc = self.execute_query("SELECT * FROM documents WHERE id = ?", (document_id,), fetch_all=False)
            if not doc:
                return {'success': False, 'message': 'Документ не найден'}
            
            if doc['status'] != 'draft':
                return {'success': False, 'message': 'Документ уже проведен или отменен'}
            
            # Получаем позиции документа
            items = self.execute_query("""
                SELECT * FROM document_items WHERE document_id = ?
            """, (document_id,), fetch_all=True)
            
            if not items:
                return {'success': False, 'message': 'Нет позиций для проведения'}
            
            # Начинаем транзакцию
            self.connection.execute("BEGIN TRANSACTION")
            
            try:
                # Обрабатываем в зависимости от типа документа
                if doc['document_type'] == 'receipt':
                    # Поступление - добавляем на склад
                    for item in items:
                        self._process_receipt(item, doc)
                        
                elif doc['document_type'] == 'issuance':
                    # Выдача - списываем со склада
                    for item in items:
                        self._process_issuance(item, doc)
                        
                elif doc['document_type'] == 'write_off':
                    # Списание - удаляем из остатков
                    for item in items:
                        self._process_write_off(item, doc)
                        
                elif doc['document_type'] == 'transfer':
                    # Перемещение - меняем склад
                    for item in items:
                        self._process_transfer(item, doc)
                        
                elif doc['document_type'] == 'adjustment':
                    # Корректировка - устанавливаем точное количество
                    for item in items:
                        self._process_adjustment(item, doc)
                        
                elif doc['document_type'] == 'return':
                    # Возврат - обратное поступление
                    for item in items:
                        self._process_return(item, doc)
                
                # Обновляем статус документа
                self.cursor.execute("""
                    UPDATE documents 
                    SET status = 'posted', posted_at = CURRENT_TIMESTAMP, posted_by = ?
                    WHERE id = ?
                """, (user_id, document_id))
                
                self.connection.commit()
                
                self.log_user_action(
                    user_id=user_id,
                    action='post',
                    entity_type='document',
                    entity_id=document_id,
                    details=f'Проведен документ {doc["document_number"]}'
                )
                
                return {'success': True, 'message': 'Документ проведен'}
                
            except Exception as e:
                self.connection.rollback()
                raise e
            
        except Exception as e:
            logger.debug(f"Ошибка post_document: {e}")
            import traceback
            traceback.print_exc()
            return {'success': False, 'message': str(e)}

    def _process_receipt(self, item, doc):
        """Обработка поступления"""
        try:
            # Получаем тип учета
            accounting_type = item.get('accounting_type')
            warehouse_id = doc.get('to_warehouse_id') or doc.get('warehouse_id')
            
            if not warehouse_id:
                raise Exception("Не указан склад получатель")
            
            # Для индивидуального учета - создаем экземпляры
            if accounting_type == 'individual':
                logger.debug(f"📦 Индивидуальный учет: создаем экземпляры для номенклатуры {item['nomenclature_id']}")
                
                # Получаем variation_id из item, если есть
                variation_id = item.get('variation_id')
                if variation_id:
                    logger.debug(f"   🔖 С модификацией ID: {variation_id}")
                
                for i in range(int(item['quantity'])):
                    # Генерируем инвентарный номер
                    year = datetime.now().year
                    inventory_number = self.get_next_inventory_number(year)
                    
                    # Создаем экземпляр с variation_id
                    self.cursor.execute("""
                        INSERT INTO instances (
                            inventory_number, nomenclature_id, variation_id, status,
                            warehouse_id, created_at
                        ) VALUES (?, ?, ?, 'in_stock', ?, CURRENT_TIMESTAMP)
                    """, (
                        inventory_number,
                        item['nomenclature_id'],
                        variation_id,  # ВАЖНО: передаем variation_id
                        warehouse_id
                    ))
                    logger.info(f"    ✅ Создан экземпляр {inventory_number} с модификацией {variation_id}")
                logger.info(f"✅ Создано {item['quantity']} экземпляров")
                return
            
            # Для количественного и партионного учета
            logger.debug("Тип: поступление (количественный/партионный)")
            
            # Проверяем существующий остаток
            existing = self.execute_query("""
                SELECT id, quantity FROM stocks 
                WHERE nomenclature_id = ? AND warehouse_id = ?
            """, (item['nomenclature_id'], warehouse_id), fetch_all=False)
            
            if existing:
                new_qty = existing['quantity'] + item['quantity']
                self.cursor.execute("""
                    UPDATE stocks SET quantity = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (new_qty, existing['id']))
                logger.debug(f"🔄 Обновлен остаток: +{item['quantity']} = {new_qty}")
            else:
                self.cursor.execute("""
                    INSERT INTO stocks (nomenclature_id, warehouse_id, quantity, created_at, updated_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """, (item['nomenclature_id'], warehouse_id, item['quantity']))
                logger.info(f"✅ Создан остаток: {item['quantity']}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка поступления: {e}")
            raise
    
    def _process_issuance(self, item, doc):
        """Обработка выдачи"""
        try:
            warehouse_id = doc.get('from_warehouse_id')
            if not warehouse_id:
                raise Exception("Не указан склад отправитель")
            
            accounting_type = item.get('accounting_type')
            
            if accounting_type == 'individual':
                logger.debug(f"📦 Индивидуальный учет: выдаем экземпляр")
                
                # Получаем ID конкретного экземпляра
                instance_id = item.get('instance_id')
                if not instance_id:
                    raise Exception("Не выбран экземпляр для выдачи")
                
                # Проверяем, что экземпляр доступен
                instance = self.execute_query("""
                    SELECT id, inventory_number, status FROM instances 
                    WHERE id = ? AND warehouse_id = ? AND status = 'in_stock'
                """, (instance_id, warehouse_id), fetch_all=False)
                
                if not instance:
                    raise Exception(f"Экземпляр {instance_id} не доступен для выдачи")
                
                # Обновляем статус экземпляра
                employee_id = doc.get('employee_id')
                self.cursor.execute("""
                    UPDATE instances 
                    SET status = 'in_use', 
                        employee_id = ?,
                        issued_date = CURRENT_DATE,
                        warehouse_id = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (employee_id, instance_id))
                
                logger.info(f"    ✅ Выдан экземпляр {instance['inventory_number']}")
                return
            
            # Для количественного учета - работаем со stocks
            logger.debug("Тип: выдача (количественный)")
            logger.debug(f"Склад ID: {warehouse_id}")
            
            existing = self.execute_query("""
                SELECT id, quantity FROM stocks 
                WHERE nomenclature_id = ? AND warehouse_id = ?
            """, (item['nomenclature_id'], warehouse_id), fetch_all=False)
            
            if not existing:
                raise Exception(f"Нет остатков для номенклатуры {item.get('nomenclature_id')} на складе {warehouse_id}")
            
            logger.debug(f"Текущий остаток: {existing['quantity']}")
            
            if existing['quantity'] < item['quantity']:
                raise Exception(f"Недостаточно товара. Доступно: {existing['quantity']}, требуется: {item['quantity']}")
            
            new_quantity = existing['quantity'] - item['quantity']
            if new_quantity == 0:
                logger.debug("Остаток стал нулевым, удаляем запись")
                self.cursor.execute("DELETE FROM stocks WHERE id = ?", (existing['id'],))
            else:
                logger.debug(f"Обновляем остаток: {new_quantity}")
                self.cursor.execute("UPDATE stocks SET quantity = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?", 
                                (new_quantity, existing['id']))
                
        except Exception as e:
            logger.error(f"❌ Ошибка выдачи: {e}")
            raise

    def _process_transfer(self, item, doc):
        """Обработка перемещения"""
        try:
            from_warehouse = doc['from_warehouse_id']
            to_warehouse = doc['to_warehouse_id']
            
            if not from_warehouse or not to_warehouse:
                raise Exception("Не указаны склады отправитель и получатель")
            
            # Уменьшаем на складе-отправителе
            from_stock = self.execute_query("""
                SELECT id, quantity FROM stocks 
                WHERE nomenclature_id = ? AND warehouse_id = ?
            """, (item['nomenclature_id'], from_warehouse), fetch_all=False)
            
            if not from_stock:
                raise Exception(f"Нет остатков на складе {from_warehouse}")
            
            if from_stock['quantity'] < item['quantity']:
                raise Exception(f"Недостаточно товара на складе {from_warehouse}")
            
            new_from_qty = from_stock['quantity'] - item['quantity']
            
            if new_from_qty == 0:
                self.cursor.execute("DELETE FROM stocks WHERE id = ?", (from_stock['id'],))
            else:
                self.cursor.execute("""
                    UPDATE stocks SET quantity = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (new_from_qty, from_stock['id']))
            
            # Увеличиваем на складе-получателе
            to_stock = self.execute_query("""
                SELECT id, quantity FROM stocks 
                WHERE nomenclature_id = ? AND warehouse_id = ?
            """, (item['nomenclature_id'], to_warehouse), fetch_all=False)
            
            if to_stock:
                new_to_qty = to_stock['quantity'] + item['quantity']
                self.cursor.execute("""
                    UPDATE stocks SET quantity = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (new_to_qty, to_stock['id']))
            else:
                self.cursor.execute("""
                    INSERT INTO stocks (nomenclature_id, warehouse_id, quantity, created_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """, (item['nomenclature_id'], to_warehouse, item['quantity']))
            
            logger.debug(f"🔄 Перемещено {item['quantity']} ед. со склада {from_warehouse} на склад {to_warehouse}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка перемещения: {e}")
            raise

    def _process_write_off(self, item, doc):
        """Обработка списания"""
        try:
            warehouse_id = doc.get('from_warehouse_id') or doc.get('warehouse_id')
            if not warehouse_id:
                raise Exception("Не указан склад")
            
            # Проверяем остаток
            stock = self.execute_query("""
                SELECT id, quantity FROM stocks 
                WHERE nomenclature_id = ? AND warehouse_id = ?
            """, (item['nomenclature_id'], warehouse_id), fetch_all=False)
            
            if not stock:
                raise Exception(f"Нет остатков на складе {warehouse_id}")
            
            if stock['quantity'] < item['quantity']:
                raise Exception(f"Недостаточно товара для списания: есть {stock['quantity']}, требуется {item['quantity']}")
            
            # Уменьшаем остаток
            new_qty = stock['quantity'] - item['quantity']
            
            if new_qty == 0:
                self.cursor.execute("DELETE FROM stocks WHERE id = ?", (stock['id'],))
                logger.debug(f"🗑️ Товар полностью списан")
            else:
                self.cursor.execute("""
                    UPDATE stocks SET quantity = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (new_qty, stock['id']))
                logger.debug(f"⬇️ Списано {item['quantity']}, остаток: {new_qty}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка списания: {e}")
            raise

    def _process_adjustment(self, item, doc):
        """Обработка корректировки"""
        try:
            warehouse_id = doc.get('warehouse_id') or doc.get('to_warehouse_id') or 1
            
            # Проверяем существующий остаток
            existing = self.execute_query("""
                SELECT id, quantity FROM stocks 
                WHERE nomenclature_id = ? AND warehouse_id = ?
            """, (item['nomenclature_id'], warehouse_id), fetch_all=False)
            
            if existing:
                # Обновляем существующий
                self.cursor.execute("""
                    UPDATE stocks SET quantity = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (item['quantity'], existing['id']))
                logger.debug(f"🔄 Скорректирован остаток: {existing['quantity']} -> {item['quantity']}")
            else:
                # Создаем новый
                self.cursor.execute("""
                    INSERT INTO stocks (nomenclature_id, warehouse_id, quantity, created_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """, (item['nomenclature_id'], warehouse_id, item['quantity']))
                logger.info(f"✅ Создан остаток: {item['quantity']}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка корректировки: {e}")
            raise

    def _process_return(self, item, doc):
        """Обработка возврата (аналогично поступлению)"""
        self._process_receipt(item, doc)
        
    # ============ ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ============
    
    def _get_or_create_location(self, name: str) -> Optional[int]:
        """Получение или создание местоположения."""
        if not name:
            return None
        
        try:
            self.cursor.execute("SELECT id FROM locations WHERE name = ?", (name,))
            loc = self.cursor.fetchone()
            if loc:
                return loc[0]
            
            code = name[:20].upper().replace(' ', '_')
            self.cursor.execute("""
                INSERT INTO locations (code, name, type, is_active)
                VALUES (?, ?, 'office', 1)
            """, (code, name))
            self.connection.commit()
            return self.cursor.lastrowid
        except Exception:
            return None

    def _get_or_create_warehouse(self, name: str) -> Optional[int]:
        """Получение или создание склада."""
        if not name:
            # Возвращаем склад по умолчанию
            self.cursor.execute("SELECT id FROM warehouses WHERE code = 'MAIN'")
            wh = self.cursor.fetchone()
            return wh[0] if wh else None
        
        try:
            self.cursor.execute("SELECT id FROM warehouses WHERE name = ?", (name,))
            wh = self.cursor.fetchone()
            if wh:
                return wh[0]
            
            code = name[:20].upper().replace(' ', '_')
            self.cursor.execute("""
                INSERT INTO warehouses (code, name, type, is_active)
                VALUES (?, ?, 'general', 1)
            """, (code, name))
            self.connection.commit()
            return self.cursor.lastrowid
        except Exception:
            # Возвращаем склад по умолчанию при ошибке
            self.cursor.execute("SELECT id FROM warehouses WHERE code = 'MAIN'")
            wh = self.cursor.fetchone()
            return wh[0] if wh else None
    
    def _get_or_create_supplier(self, name: str) -> Optional[int]:
        """Получение или создание поставщика."""
        if not name:
            return None
        
        try:
            self.cursor.execute("SELECT id FROM suppliers WHERE name = ?", (name,))
            sup = self.cursor.fetchone()
            if sup:
                return sup[0]
            
            code = name[:20].upper().replace(' ', '_')
            self.cursor.execute("""
                INSERT INTO suppliers (code, name, is_active)
                VALUES (?, ?, 1)
            """, (code, name))
            self.connection.commit()
            return self.cursor.lastrowid
        except Exception:
            return None

    def _parse_date(self, value) -> Optional[str]:
        """Парсинг строки даты."""
        if not value:
            return None
        try:
            if isinstance(value, str):
                for fmt in ('%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y', '%Y.%m.%d'):
                    try:
                        return datetime.strptime(value, fmt).strftime('%Y-%m-%d')
                    except Exception:
                        continue
            return None
        except Exception:
            return None
    
    def _parse_float(self, value) -> float:
        """Парсинг числа с плавающей точкой."""
        if not value:
            return 0.0
        try:
            return float(str(value).replace(',', '.').replace(' ', ''))
        except Exception:
            return 0.0
    
    def _parse_int(self, value, default=0) -> int:
        """Парсинг целого числа."""
        if not value:
            return default
        try:
            return int(float(str(value).replace(',', '.')))
        except Exception:
            return default
    
    # ============ НОВЫЕ МЕТОДЫ ============
    def get_departments(self):
        """Получение списка подразделений"""
        try:
            self.cursor.execute("""
                SELECT d.*, e.full_name as manager_name
                FROM departments d
                LEFT JOIN employees e ON d.manager_id = e.id
                WHERE d.is_active = 1
                ORDER BY d.name
            """)
            rows = self.cursor.fetchall()
            return [dict(row) for row in rows] if rows else []
        except Exception as e:
            logger.debug(f"Ошибка получения подразделений: {e}")
            return []

    def get_cost_centers(self):
        """Получение списка центров затрат"""
        # Можно либо хранить в отдельной таблице, либо возвращать статический список
        return [
            {'id': 'production_materials', 'name': 'Основное производство (материалы)'},
            {'id': 'auxiliary_materials', 'name': 'Вспомогательные материалы'},
            {'id': 'repair', 'name': 'Ремонт и обслуживание'},
            {'id': 'operating_expenses', 'name': 'Эксплуатационные расходы'},
            {'id': 'administration', 'name': 'Административные нужды'},
            {'id': 'rnd', 'name': 'НИОКР и эксперименты'},
        ]
    
    def get_employees(self, active_only=True):
        """Получение списка сотрудников"""
        try:
            query = """
                SELECT e.*, d.name as department_name
                FROM employees e
                LEFT JOIN departments d ON e.department_id = d.id
                WHERE 1=1
            """
            if active_only:
                query += " AND e.is_active = 1"
            query += " ORDER BY e.last_name, e.first_name"
            
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            return [dict(row) for row in rows] if rows else []
        except Exception as e:
            logger.debug(f"Ошибка получения сотрудников: {e}")
            return []

    def get_expense_purpose_by_id(self, purpose_id):
        """
        Получение цели расходования по ID
        """
        try:
            self.cursor.execute("""
                SELECT ep.*, 
                    u1.username as created_by_name,
                    u2.username as updated_by_name
                FROM expense_purposes ep
                LEFT JOIN users u1 ON ep.created_by = u1.id
                LEFT JOIN users u2 ON ep.updated_by = u2.id
                WHERE ep.id = ?
            """, (purpose_id,))
            row = self.cursor.fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.debug(f"Ошибка получения цели расходования: {e}")
            return None

    def create_expense_purpose(self, data, user_id=None):
        """
        Создание новой цели расходования
        """
        try:
            # Проверяем уникальность кода
            self.cursor.execute("SELECT id FROM expense_purposes WHERE code = ?", (data['code'],))
            if self.cursor.fetchone():
                return {'success': False, 'message': 'Цель с таким кодом уже существует'}
            
            self.cursor.execute("""
                INSERT INTO expense_purposes (
                    code, name, description, category, sort_order, 
                    is_active, created_by, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                data['code'],
                data['name'],
                data.get('description'),
                data.get('category', 'production'),
                data.get('sort_order', 0),
                1 if data.get('is_active', True) else 0,
                user_id
            ))
            self.connection.commit()
            new_id = self.cursor.lastrowid
            
            # Логируем действие
            self.log_user_action(
                user_id=user_id,
                action='create',
                entity_type='expense_purpose',
                entity_id=new_id,
                details=f'Создана цель расходования: {data["code"]} - {data["name"]}'
            )
            
            return {'success': True, 'id': new_id, 'message': 'Цель расходования создана'}
        except Exception as e:
            logger.debug(f"Ошибка создания цели расходования: {e}")
            self.connection.rollback()
            return {'success': False, 'message': str(e)}
        
    def create_expense_purposes_table(self):
        """Создание таблицы expense_purposes если её нет"""
        try:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS expense_purposes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code VARCHAR(20) UNIQUE NOT NULL,
                    name VARCHAR(100) NOT NULL,
                    description TEXT,
                    category VARCHAR(30) DEFAULT 'production',
                    is_active BOOLEAN DEFAULT 1,
                    sort_order INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_by INTEGER,
                    updated_at TIMESTAMP,
                    updated_by INTEGER,
                    FOREIGN KEY (created_by) REFERENCES users(id),
                    FOREIGN KEY (updated_by) REFERENCES users(id)
                )
            """)
            self.connection.commit()
            logger.info("✅ Таблица expense_purposes создана")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка создания таблицы: {e}")
            return False
    
    def add_updated_by_column(self):
        """Добавление колонки updated_by в таблицу expense_purposes (если нужно)"""
        try:
            # Проверяем, есть ли колонка updated_by
            self.cursor.execute("PRAGMA table_info(expense_purposes)")
            columns = [col[1] for col in self.cursor.fetchall()]
            
            if 'updated_by' not in columns:
                self.cursor.execute("ALTER TABLE expense_purposes ADD COLUMN updated_by INTEGER REFERENCES users(id)")
                self.connection.commit()
                logger.info("✅ Добавлена колонка updated_by")
            
            if 'updated_at' not in columns:
                self.cursor.execute("ALTER TABLE expense_purposes ADD COLUMN updated_at TIMESTAMP")
                self.connection.commit()
                logger.info("✅ Добавлена колонка updated_at")
                
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка добавления колонок: {e}")
            return False
    
    def add_images_table(self):
        """Добавление таблицы для хранения изображений номенклатуры"""
        try:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS nomenclature_images (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nomenclature_id INTEGER NOT NULL,
                    filename VARCHAR(255) NOT NULL,
                    original_filename VARCHAR(255) NOT NULL,
                    file_path VARCHAR(500) NOT NULL,
                    file_size INTEGER,
                    mime_type VARCHAR(100),
                    is_primary BOOLEAN DEFAULT 0,
                    sort_order INTEGER DEFAULT 0,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_by INTEGER,
                    FOREIGN KEY (nomenclature_id) REFERENCES nomenclatures(id) ON DELETE CASCADE,
                    FOREIGN KEY (created_by) REFERENCES users(id)
                )
            """)
            
            # Индекс для быстрого поиска
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_nomenclature_images_nomen 
                ON nomenclature_images(nomenclature_id)
            """)
            
            self.connection.commit()
            logger.info("✅ Таблица nomenclature_images создана")
        except Exception as e:
            logger.error(f"❌ Ошибка создания таблицы изображений: {e}")

    def update_expense_purpose(self, purpose_id, data, user_id=None):
        """
        Обновление цели расходования
        """
        try:
            # Проверяем уникальность кода (исключая текущую запись)
            self.cursor.execute("""
                SELECT id FROM expense_purposes 
                WHERE code = ? AND id != ?
            """, (data['code'], purpose_id))
            if self.cursor.fetchone():
                return {'success': False, 'message': 'Цель с таким кодом уже существует'}
            
            self.cursor.execute("""
                UPDATE expense_purposes
                SET code = ?,
                    name = ?,
                    description = ?,
                    category = ?,
                    sort_order = ?,
                    is_active = ?,
                    updated_at = CURRENT_TIMESTAMP,
                    updated_by = ?
                WHERE id = ?
            """, (
                data['code'],
                data['name'],
                data.get('description'),
                data.get('category'),
                int(data.get('sort_order', 0)),
                1 if data.get('is_active') else 0,
                user_id,
                purpose_id
            ))
            
            self.connection.commit()
            
            # Логируем действие
            self.log_user_action(
                user_id=user_id,
                action='update',
                entity_type='expense_purpose',
                entity_id=purpose_id,
                details=f'Обновлена цель расходования: {data["code"]} - {data["name"]}'
            )
            
            return {'success': True, 'message': 'Цель расходования обновлена'}
        except Exception as e:
            logger.debug(f"Ошибка обновления цели расходования: {e}")
            self.connection.rollback()
            return {'success': False, 'message': str(e)}
    
    def delete_expense_purpose(self, purpose_id, user_id=None):
        """
        Удаление цели расходования (мягкое удаление)
        """
        try:
            # Проверяем, используется ли цель в документах
            self.cursor.execute("""
                SELECT COUNT(*) as cnt FROM documents 
                WHERE purpose_id = ?
            """, (purpose_id,))
            result = self.cursor.fetchone()
            used = result[0] if result else 0
            
            if used > 0:
                return {'success': False, 'message': 'Цель используется в документах, удаление невозможно'}
            
            # Мягкое удаление (деактивация) - БЕЗ updated_by
            self.cursor.execute("""
                UPDATE expense_purposes 
                SET is_active = 0
                WHERE id = ?
            """, (purpose_id,))
            
            self.connection.commit()
            
            self.log_user_action(
                user_id=user_id,
                action='delete',
                entity_type='expense_purpose',
                entity_id=purpose_id,
                details=f'Деактивирована цель расходования ID: {purpose_id}'
            )
            
            return {'success': True, 'message': 'Цель расходования деактивирована'}
        except Exception as e:
            logger.debug(f"Ошибка удаления цели расходования: {e}")
            self.connection.rollback()
            return {'success': False, 'message': str(e)}
        
    def get_expense_purposes(self, category=None, active_only=True, search=None):
        """
        Получение списка целей расходования с фильтрацией
        """
        try:
            query = """
                SELECT ep.*, 
                    u.username as created_by_name
                FROM expense_purposes ep
                LEFT JOIN users u ON ep.created_by = u.id
                WHERE 1=1
            """
            params = []
            
            if category and category != '':
                query += " AND ep.category = ?"
                params.append(category)
            
            if active_only:
                query += " AND ep.is_active = 1"
            
            if search and search != '':
                query += " AND (ep.name LIKE ? OR ep.code LIKE ? OR ep.description LIKE ?)"
                search_term = f"%{search}%"
                params.extend([search_term, search_term, search_term])
            
            query += " ORDER BY ep.sort_order, ep.name"
            
            self.cursor.execute(query, params)
            rows = self.cursor.fetchall()
            
            result = []
            for row in rows:
                result.append(dict(row))
            
            return result
            
        except Exception as e:
            logger.debug(f"Ошибка получения целей расходования: {e}")
            return []  
         
    def get_expense_purposes_categories(self):
        """
        Получение списка категорий целей для фильтрации
        """
        return [
            {'id': 'production', 'name': 'Производство'},
            {'id': 'development', 'name': 'Разработка и НИОКР'},
            {'id': 'maintenance', 'name': 'Ремонт и обслуживание'},
            {'id': 'own_needs', 'name': 'Собственные нужды'},
            {'id': 'other', 'name': 'Прочее'}
        ]

    def toggle_expense_purpose(self, purpose_id, user_id=None):
        """
        Активация/деактивация цели
        """
        try:
            # Получаем текущий статус
            self.cursor.execute("SELECT is_active FROM expense_purposes WHERE id = ?", (purpose_id,))
            result = self.cursor.fetchone()
            if not result:
                return {'success': False, 'message': 'Цель не найдена'}
            
            current = result[0]
            new_status = 0 if current else 1
            
            # Обновляем статус
            self.cursor.execute("""
                UPDATE expense_purposes 
                SET is_active = ?
                WHERE id = ?
            """, (new_status, purpose_id))
            
            self.connection.commit()
            
            status_text = 'активирована' if new_status else 'деактивирована'
            
            self.log_user_action(
                user_id=user_id,
                action='toggle',
                entity_type='expense_purpose',
                entity_id=purpose_id,
                details=f'Цель расходования {status_text}'
            )
            
            return {'success': True, 'is_active': new_status, 'message': f'Цель {status_text}'}
        except Exception as e:
            logger.debug(f"Ошибка переключения статуса: {e}")
            return {'success': False, 'message': str(e)}
    
    def close(self):
        """Закрытие соединения с базой данных."""
        if self.connection:
            self.connection.close()
            logger.info("✅ Соединение с базой данных закрыто")

# Singleton instance
_db_instance = None

def get_db():
    """Получение экземпляра базы данных."""
    global _db_instance
    if _db_instance is None:
        _db_instance = Database()
    return _db_instance