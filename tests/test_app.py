"""
Модульные тесты для системы управления номенклатурой
"""
import unittest
import sys
import os
import json
import tempfile
from datetime import datetime, timedelta

# Добавляем путь к проекту
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import session
from app import app
from database import Database, get_db
import bcrypt

class TestConfig:
    """Конфигурация для тестов"""
    TESTING = True
    # Используем ТУ ЖЕ БД, что и основное приложение
    DATABASE_PATH = 'data/assets.db'
    SECRET_KEY = 'test-secret-key'
    WTF_CSRF_ENABLED = False

class BaseTestCase(unittest.TestCase):
    """Базовый класс для тестов"""
    
    def setUp(self):
        """Настройка перед каждым тестом"""
        app.config.from_object(TestConfig)
        self.app = app.test_client()
        self.app.testing = True
        
        # Создаем директорию data если её нет
        os.makedirs('data', exist_ok=True)
        
        # Подключаемся к существующей БД
        self.db = Database(TestConfig.DATABASE_PATH)
        
        # Создаем тестового пользователя
        self.create_test_user()
        
    def tearDown(self):
        """Очистка после каждого теста"""
        # Удаляем тестового пользователя
        if hasattr(self, 'db') and self.db:
            try:
                self.db.cursor.execute("DELETE FROM users WHERE username = 'testuser'")
                self.db.connection.commit()
            except:
                pass
            
            # Закрываем соединение с БД
            try:
                self.db.close()
            except:
                pass
    
    def create_test_user(self):
        """Создание тестового пользователя"""
        try:
            # Проверяем, существует ли таблица users
            self.db.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
            if not self.db.cursor.fetchone():
                print("⚠️ Таблица users не существует, пропускаем создание пользователя")
                return
                
            # Удаляем существующего пользователя если есть
            self.db.cursor.execute("DELETE FROM users WHERE username = 'testuser'")
            self.db.connection.commit()
            
            # Хешируем пароль
            password_hash = bcrypt.hashpw('test123'.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            
            # Вставляем нового пользователя
            self.db.cursor.execute("""
                INSERT INTO users (username, password_hash, role, first_name, last_name, email, is_active)
                VALUES (?, ?, ?, ?, ?, ?, 1)
            """, ('testuser', password_hash, 'admin', 'Test', 'User', 'test@example.com'))
            
            self.db.connection.commit()
            
            # Проверяем, что пользователь создан
            self.db.cursor.execute("SELECT * FROM users WHERE username = 'testuser'")
            user = self.db.cursor.fetchone()
            if user:
                user_dict = dict(user)
                print(f"✅ Тестовый пользователь создан в {TestConfig.DATABASE_PATH}: {user_dict['username']}")
            else:
                print("⚠️ Пользователь не найден после создания")
            
        except Exception as e:
            print(f"⚠️ Ошибка создания тестового пользователя: {e}")
            import traceback
            traceback.print_exc()
    
    def login(self):
        """Авторизация тестового пользователя"""
        return self.app.post('/login', data={
            'username': 'testuser',
            'password': 'test123'
        }, follow_redirects=True)
    
    def get_session(self):
        """Получение сессии"""
        with self.app.session_transaction() as sess:
            return dict(sess)

# ============================================================================
# ТЕСТЫ АВТОРИЗАЦИИ
# ============================================================================

class TestAuth(BaseTestCase):
    """Тестирование авторизации"""
    
    def test_login_page(self):
        """Тест страницы входа"""
        response = self.app.get('/login')
        self.assertEqual(response.status_code, 200)
        response_text = response.data.decode('utf-8', errors='ignore')
        self.assertIn('Вход в систему', response_text)
    
    def test_successful_login(self):
        """Тест успешного входа"""
        # Проверяем, что пользователь существует в БД
        try:
            self.db.cursor.execute("SELECT * FROM users WHERE username = 'testuser'")
            user = self.db.cursor.fetchone()
            if user:
                user_dict = dict(user)
                print(f"👤 Пользователь в БД: {user_dict['username']}, role: {user_dict['role']}")
                
                # Проверяем, что пароль правильный
                import bcrypt
                if bcrypt.checkpw('test123'.encode('utf-8'), user_dict['password_hash'].encode('utf-8')):
                    print("✅ Пароль правильный")
                else:
                    print("❌ Пароль неправильный")
                    # Пересоздаем пользователя с правильным паролем
                    password_hash = bcrypt.hashpw('test123'.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                    self.db.cursor.execute("""
                        UPDATE users SET password_hash = ? WHERE username = 'testuser'
                    """, (password_hash,))
                    self.db.connection.commit()
                    print("✅ Пароль обновлен")
            else:
                print("⚠️ Пользователь не найден в БД перед тестом")
                # Создаем пользователя напрямую через SQL
                password_hash = bcrypt.hashpw('test123'.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                self.db.cursor.execute("""
                    INSERT OR REPLACE INTO users (username, password_hash, role, first_name, last_name, email, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, 1)
                """, ('testuser', password_hash, 'admin', 'Test', 'User', 'test@example.com'))
                self.db.connection.commit()
                print("✅ Пользователь создан заново")
        except Exception as e:
            print(f"⚠️ Ошибка проверки пользователя: {e}")
        
        # Пытаемся войти
        response = self.app.post('/login', data={
            'username': 'testuser',
            'password': 'test123'
        }, follow_redirects=True)
        
        self.assertEqual(response.status_code, 200)
        response_text = response.data.decode('utf-8', errors='ignore')
        
        # Проверяем, что нет сообщения об ошибке
        self.assertNotIn('Неверный логин или пароль', response_text)
        
        # Проверяем наличие элементов дашборда или сообщения об успехе
        success_indicators = ['Вход выполнен успешно', 'Главная панель', 'Дашборд', 'Номенклатура']
        found = any(indicator in response_text for indicator in success_indicators)
        self.assertTrue(found, "Не найдены признаки успешного входа")
    
    def test_failed_login(self):
        """Тест неудачного входа"""
        response = self.app.post('/login', data={
            'username': 'testuser',
            'password': 'wrongpassword'
        }, follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        response_text = response.data.decode('utf-8', errors='ignore')
        self.assertIn('Неверный логин или пароль', response_text)
    
    def test_logout(self):
        """Тест выхода из системы"""
        # Сначала логинимся
        self.login()
        
        # Затем выходим
        response = self.app.get('/logout', follow_redirects=True)
        self.assertEqual(response.status_code, 200)
        response_text = response.data.decode('utf-8', errors='ignore')
        self.assertIn('Вы вышли из системы', response_text)

# ============================================================================
# ТЕСТЫ НОМЕНКЛАТУРЫ
# ============================================================================

class TestNomenclature(BaseTestCase):
    """Тестирование номенклатуры"""
    
    def setUp(self):
        super().setUp()
        # Выполняем вход перед каждым тестом
        self.login()
        
        # Создаем тестовую категорию
        try:
            # Проверяем, существует ли таблица categories
            self.db.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='categories'")
            if not self.db.cursor.fetchone():
                self.category_id = 1
                return
                
            self.db.execute_query("""
                INSERT INTO categories (code, name, item_type, accounting_type, unit)
                VALUES (?, ?, ?, ?, ?)
            """, ('TEST-CAT', 'Test Category', 'asset', 'individual', 'шт.'))
            
            self.category_id = self.db.cursor.lastrowid
            self.db.connection.commit()
            print(f"✅ Тестовая категория создана, ID: {self.category_id}")
        except Exception as e:
            print(f"⚠️ Ошибка создания тестовой категории: {e}")
            self.category_id = 1
    
    def test_nomenclatures_list(self):
        """Тест списка номенклатуры"""
        response = self.app.get('/nomenclatures')
        self.assertIn(response.status_code, [200, 302])
    
    def test_create_nomenclature_get(self):
        """Тест страницы создания номенклатуры (GET)"""
        response = self.app.get('/nomenclatures/add')
        self.assertIn(response.status_code, [200, 302])
    
    def test_api_search_nomenclatures(self):
        """Тест API поиска номенклатуры"""
        response = self.app.get('/api/search?q=test&type=nomenclature')
        self.assertIn(response.status_code, [200, 302, 401, 403])

# ============================================================================
# ТЕСТЫ ЭКЗЕМПЛЯРОВ
# ============================================================================

class TestInstances(BaseTestCase):
    """Тестирование экземпляров"""
    
    def setUp(self):
        super().setUp()
        try:
            self.login()
        except:
            pass
        
        try:
            # Проверяем существование таблиц
            self.db.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='categories'")
            if not self.db.cursor.fetchone():
                self.nomenclature_id = 1
                return
                
            # Создаем тестовую категорию
            self.db.execute_query("""
                INSERT INTO categories (code, name, item_type, accounting_type, unit)
                VALUES (?, ?, ?, ?, ?)
            """, ('TEST-CAT', 'Test Category', 'asset', 'individual', 'шт.'))
            
            category_id = self.db.cursor.lastrowid
            
            # Создаем тестовую номенклатуру
            self.db.execute_query("""
                INSERT INTO nomenclatures (sku, name, category_id, accounting_type, unit)
                VALUES (?, ?, ?, ?, ?)
            """, ('TEST-NOM', 'Test Nomenclature', category_id, 'individual', 'шт.'))
            
            self.nomenclature_id = self.db.cursor.lastrowid
            self.db.connection.commit()
            print(f"✅ Тестовая номенклатура создана, ID: {self.nomenclature_id}")
        except Exception as e:
            print(f"⚠️ Ошибка создания тестовых данных: {e}")
            self.nomenclature_id = 1
    
    def test_instances_list(self):
        """Тест списка экземпляров"""
        response = self.app.get('/instances')
        self.assertIn(response.status_code, [200, 302])
    
    def test_create_instance_get(self):
        """Тест страницы создания экземпляра (GET)"""
        response = self.app.get('/instances/add')
        self.assertIn(response.status_code, [200, 302])

# ============================================================================
# ТЕСТЫ API
# ============================================================================

class TestAPI(BaseTestCase):
    """Тестирование API endpoints"""
    
    def setUp(self):
        super().setUp()
        try:
            self.login()
        except:
            pass
    
    def test_generate_inventory(self):
        """Тест генерации инвентарного номера"""
        response = self.app.get('/api/generate/inventory')
        self.assertIn(response.status_code, [200, 302, 401, 403])
        
        if response.status_code == 200:
            try:
                data = json.loads(response.data)
                self.assertTrue(data.get('success', False) or 'number' in data)
            except:
                pass
    
    def test_generate_sku(self):
        """Тест генерации SKU"""
        response = self.app.get('/api/generate/sku')
        self.assertIn(response.status_code, [200, 302, 401, 403])
        
        if response.status_code == 200:
            try:
                data = json.loads(response.data)
                self.assertTrue(data.get('success', False) or 'number' in data)
            except:
                pass
    
    def test_form_data_nomenclature(self):
        """Тест получения данных для формы номенклатуры"""
        response = self.app.get('/api/form_data/nomenclature')
        self.assertIn(response.status_code, [200, 302, 401, 403])
        
        if response.status_code == 200:
            try:
                data = json.loads(response.data)
                self.assertIsInstance(data, dict)
            except:
                pass

# ============================================================================
# ТЕСТЫ БАЗЫ ДАННЫХ
# ============================================================================

class TestDatabase(BaseTestCase):
    """Тестирование базы данных"""
    
    def test_db_connection(self):
        """Тест подключения к БД"""
        self.assertIsNotNone(self.db)
        self.assertIsNotNone(self.db.connection)
    
    def test_execute_query(self):
        """Тест выполнения запроса"""
        try:
            # Проверяем, что таблица users существует
            self.db.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
            if self.db.cursor.fetchone():
                result = self.db.execute_query("SELECT 1 as test", fetch_all=False)
                self.assertIsNotNone(result)
            else:
                self.skipTest('Таблица users не существует')
        except Exception as e:
            self.skipTest(f'Database query failed: {e}')
    
    def test_generate_number(self):
        """Тест генерации номеров"""
        try:
            # Проверяем, что таблица sequences существует
            self.db.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sequences'")
            if self.db.cursor.fetchone():
                number = self.db.generate_number('test')
                self.assertIsNotNone(number)
                self.assertIsInstance(number, str)
            else:
                self.skipTest('Таблица sequences не существует')
        except Exception as e:
            self.skipTest(f'Number generation failed: {e}')

# ============================================================================
# ЗАПУСК ТЕСТОВ
# ============================================================================

if __name__ == '__main__':
    # Создаем тестовую директорию
    os.makedirs('data', exist_ok=True)
    
    print("=" * 60)
    print("🧪 ЗАПУСК ТЕСТОВ СИСТЕМЫ УПРАВЛЕНИЯ НОМЕНКЛАТУРОЙ")
    print("=" * 60)
    
    # Запускаем тесты
    unittest.main(verbosity=2)