"""
Тесты авторизации и разграничения прав доступа (RBAC).
Проверяем: гость → редирект, viewer → ограниченный доступ, admin → полный доступ.
"""
import pytest
from tests.conftest import _make_auth_client


# ─── Вспомогательные маршруты для тестирования ───────────────────────────────

# Маршруты доступные только авторизованным (любая роль)
AUTH_REQUIRED_ROUTES = [
    '/dashboard',
    '/nomenclatures',
    '/instances',
    '/warehouses',
    '/documents',
    '/employees',
    '/reports',
    '/notifications',
]

# Маршруты только для администраторов
ADMIN_ONLY_ROUTES = [
    '/users',
    '/admin/backup',
    '/debug-routes',
]


# ─── Гость (неавторизованный) ─────────────────────────────────────────────────

class TestGuestAccess:
    @pytest.mark.parametrize('route', AUTH_REQUIRED_ROUTES)
    def test_redirects_to_login(self, client, route):
        resp = client.get(route, follow_redirects=False)
        assert resp.status_code == 302, f"Expected redirect for {route}"
        location = resp.headers.get('Location', '')
        assert 'login' in location.lower(), f"Expected redirect to login for {route}, got {location}"

    def test_login_page_accessible_to_guest(self, client):
        resp = client.get('/login')
        assert resp.status_code == 200

    def test_static_assets_accessible(self, client):
        """Статика не требует авторизации."""
        resp = client.get('/static/css/bootstrap.min.css')
        # 200 или 404 если файл не существует — но не 302
        assert resp.status_code != 302


# ─── Авторизованный пользователь (роль: viewer) ───────────────────────────────

class TestViewerAccess:
    def test_can_access_dashboard(self, viewer_client):
        resp = viewer_client.get('/dashboard')
        assert resp.status_code == 200

    def test_can_access_nomenclatures(self, viewer_client):
        resp = viewer_client.get('/nomenclatures')
        assert resp.status_code == 200

    def test_can_access_documents(self, viewer_client):
        resp = viewer_client.get('/documents')
        assert resp.status_code == 200

    @pytest.mark.parametrize('route', ADMIN_ONLY_ROUTES)
    def test_admin_only_routes_blocked(self, viewer_client, route):
        """Viewer не должен получить 200 на admin-маршруты."""
        resp = viewer_client.get(route, follow_redirects=False)
        # Должен быть либо 302 (редирект на dashboard/login) либо 403
        assert resp.status_code in (302, 403), \
            f"Viewer should not access {route}, got {resp.status_code}"


# ─── Администратор ────────────────────────────────────────────────────────────

class TestAdminAccess:
    @pytest.mark.parametrize('route', AUTH_REQUIRED_ROUTES)
    def test_can_access_all_routes(self, auth_client, route):
        resp = auth_client.get(route)
        assert resp.status_code == 200, f"Admin should access {route}, got {resp.status_code}"

    @pytest.mark.parametrize('route', ADMIN_ONLY_ROUTES)
    def test_can_access_admin_routes(self, auth_client, route):
        resp = auth_client.get(route)
        assert resp.status_code == 200, f"Admin should access {route}, got {resp.status_code}"

    def test_can_access_debug_routes_endpoint(self, auth_client):
        resp = auth_client.get('/debug-routes')
        assert resp.status_code == 200
        data = resp.get_json()
        assert isinstance(data, list)
        assert len(data) > 0


# ─── Сессия и выход ───────────────────────────────────────────────────────────

class TestSessionManagement:
    def test_logout_clears_session(self, app):
        """После выхода dashboard недоступен."""
        c = app.test_client()
        with c.session_transaction() as sess:
            sess['user_id'] = 1
            sess['username'] = 'admin'
            sess['role'] = 'admin'

        # Проверяем что до выхода dashboard доступен
        resp = c.get('/dashboard')
        assert resp.status_code == 200

        # Выходим
        c.get('/logout', follow_redirects=True)

        # После выхода — редирект
        resp = c.get('/dashboard', follow_redirects=False)
        assert resp.status_code == 302

    def test_session_role_required_for_admin(self, app):
        """Пользователь с role=viewer не может использовать admin-маршруты."""
        c = _make_auth_client(app, role='viewer', user_id=5, username='regular_user')
        resp = c.get('/users', follow_redirects=False)
        assert resp.status_code in (302, 403)

    def test_admin_role_allows_user_management(self, app):
        """Пользователь с role=admin может открыть /users."""
        c = _make_auth_client(app, role='admin', user_id=1, username='admin')
        resp = c.get('/users')
        assert resp.status_code == 200
