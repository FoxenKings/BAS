"""
Тестовая инфраструктура для Inventory Bot.

Подход: используем реальную БД (read-only) и прямую установку сессии.
Smoke-тесты проверяют что маршруты возвращают корректные HTTP-ответы.
"""
import os
import sys
import pytest

# Добавляем корень проекта в sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

# Устанавливаем тестовый SECRET_KEY до импорта приложения
os.environ.setdefault('SECRET_KEY', 'test-secret-key-for-pytest-only')


@pytest.fixture(scope='session')
def app():
    """
    Flask-приложение в тестовом режиме.
    CSRF отключён, сессии включены.
    """
    import app as app_module
    flask_app = app_module.app
    flask_app.config.update({
        'TESTING': True,
        'WTF_CSRF_ENABLED': False,
        'SECRET_KEY': 'test-secret-key-for-pytest-only',
    })
    yield flask_app


@pytest.fixture(scope='session')
def client(app):
    """HTTP-клиент Flask без авторизации."""
    return app.test_client()


def _make_auth_client(app, role='admin', user_id=1, username='admin'):
    """
    Создаёт авторизованный тест-клиент через прямую установку сессии.
    Не зависит от реальных данных в БД.
    """
    c = app.test_client()
    with c.session_transaction() as sess:
        sess['user_id'] = user_id
        sess['username'] = username
        sess['role'] = role
        sess['full_name'] = username
    return c


@pytest.fixture()
def auth_client(app):
    """HTTP-клиент с активной сессией admin (новый на каждый тест)."""
    return _make_auth_client(app, role='admin', user_id=1, username='admin')


@pytest.fixture()
def viewer_client(app):
    """HTTP-клиент с ролью viewer (новый на каждый тест)."""
    return _make_auth_client(app, role='viewer', user_id=2, username='viewer')
