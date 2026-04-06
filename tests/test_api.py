"""
Тесты API эндпоинтов.
"""
import pytest
import json


class TestNotificationsAPI:
    """Тесты API уведомлений."""

    def test_notifications_count_requires_auth(self, client):
        """GET /api/notifications/counts без авторизации — 302 или 401."""
        resp = client.get('/api/notifications/counts', follow_redirects=False)
        assert resp.status_code in (302, 401, 403)

    def test_notifications_count_returns_json(self, auth_client):
        """GET /api/notifications/counts — возвращает JSON."""
        resp = auth_client.get('/api/notifications/counts')
        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert isinstance(data, dict)


class TestBackupRoute:
    """Тесты маршрутов резервного копирования."""

    def test_backup_page_requires_admin(self, viewer_client):
        """GET /admin/backup для viewer — редирект (нет прав)."""
        resp = viewer_client.get('/admin/backup', follow_redirects=False)
        assert resp.status_code in (302, 403)

    def test_backup_page_accessible_for_admin(self, auth_client):
        """GET /admin/backup для admin — 200."""
        resp = auth_client.get('/admin/backup')
        assert resp.status_code == 200
