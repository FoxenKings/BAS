"""
Тесты безопасности: защита маршрутов, ролевая модель.
"""
import pytest


class TestUnauthenticatedAccess:
    """Все приватные маршруты требуют авторизации."""

    PROTECTED_ROUTES = [
        '/dashboard',
        '/nomenclatures',
        '/instances',
        '/documents',
        '/warehouses',
        '/employees',
        '/users',
        '/reports/stock-balance',
        '/notifications',
    ]

    @pytest.mark.parametrize('route', PROTECTED_ROUTES)
    def test_requires_auth(self, client, route):
        """Без авторизации — редирект на /login."""
        resp = client.get(route, follow_redirects=False)
        assert resp.status_code == 302, f'{route} должен требовать авторизации'
        location = resp.headers.get('Location', '')
        assert 'login' in location.lower(), f'{route} должен редиректить на /login'


class TestAdminOnlyRoutes:
    """Маршруты только для администраторов."""

    ADMIN_ROUTES = [
        '/users',
        '/admin/counters',
        '/admin/backup',
    ]

    @pytest.mark.parametrize('route', ADMIN_ROUTES)
    def test_admin_routes_blocked_for_viewer(self, viewer_client, route):
        """Пользователь с ролью viewer не должен получить доступ к admin-маршрутам."""
        resp = viewer_client.get(route, follow_redirects=False)
        assert resp.status_code in (302, 403), \
            f'{route} должен быть заблокирован для viewer'

    @pytest.mark.parametrize('route', ADMIN_ROUTES)
    def test_admin_routes_accessible_for_admin(self, auth_client, route):
        """Администратор имеет доступ ко всем admin-маршрутам."""
        resp = auth_client.get(route)
        assert resp.status_code == 200, \
            f'{route} должен быть доступен для admin'
