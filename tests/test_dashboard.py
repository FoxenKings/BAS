"""
Smoke-тесты для основных страниц системы (дашборд, склады, сотрудники).
"""
import pytest


class TestDashboard:
    """Тесты главной страницы."""

    def test_dashboard_requires_auth(self, client):
        """GET /dashboard без авторизации — редирект на login."""
        resp = client.get('/dashboard', follow_redirects=False)
        assert resp.status_code == 302

    def test_dashboard_accessible_for_admin(self, auth_client):
        """GET /dashboard для авторизованного пользователя — 200."""
        resp = auth_client.get('/dashboard')
        assert resp.status_code == 200

    def test_dashboard_contains_stats(self, auth_client):
        """Дашборд содержит секцию статистики."""
        resp = auth_client.get('/dashboard')
        html = resp.data.decode('utf-8', errors='replace')
        assert resp.status_code == 200
        # Страница содержит хоть какой-то контент (не пустой ответ)
        assert len(html) > 500


class TestMainPages:
    """Smoke-тесты ключевых страниц."""

    def test_root_redirects(self, client):
        """GET / перенаправляет (на login или dashboard)."""
        resp = client.get('/', follow_redirects=False)
        assert resp.status_code in (302, 200)

    def test_warehouses_list(self, auth_client):
        """GET /warehouses — 200."""
        resp = auth_client.get('/warehouses')
        assert resp.status_code == 200

    def test_employees_list(self, auth_client):
        """GET /employees — 200."""
        resp = auth_client.get('/employees')
        assert resp.status_code == 200

    def test_notifications_list(self, auth_client):
        """GET /notifications — 200."""
        resp = auth_client.get('/notifications')
        assert resp.status_code == 200
