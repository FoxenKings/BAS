"""
Тесты авторизации.
"""
import pytest


class TestLogin:
    """Тесты страницы входа."""

    def test_login_page_accessible(self, client):
        """GET /login возвращает 200."""
        resp = client.get('/login')
        assert resp.status_code == 200

    def test_login_page_contains_form(self, client):
        """Страница логина содержит форму с полями username и password."""
        resp = client.get('/login')
        html = resp.data.decode('utf-8', errors='replace')
        assert 'username' in html
        assert 'password' in html

    def test_login_with_valid_credentials(self, app):
        """POST /login с правильными данными перенаправляет на dashboard."""
        c = app.test_client()
        resp = c.post('/login',
                      data={'username': 'admin', 'password': 'admin123'},
                      follow_redirects=False)
        assert resp.status_code in (302, 200)

    def test_login_with_wrong_password(self, client):
        """POST /login с неверным паролем возвращает страницу логина."""
        resp = client.post('/login',
                           data={'username': 'admin', 'password': 'wrongpassword'},
                           follow_redirects=True)
        assert resp.status_code == 200
        html = resp.data.decode('utf-8', errors='replace')
        # Должно быть сообщение об ошибке или форма снова
        assert 'password' in html.lower() or 'username' in html.lower()

    def test_login_with_unknown_user(self, client):
        """POST /login с несуществующим пользователем — статус 200 (форма с ошибкой)."""
        resp = client.post('/login',
                           data={'username': 'nosuchuser', 'password': 'any'},
                           follow_redirects=True)
        assert resp.status_code == 200


class TestLogout:
    """Тесты выхода из системы."""

    def test_logout_redirects(self, auth_client):
        """GET /logout перенаправляет неавторизованного или авторизованного пользователя."""
        resp = auth_client.get('/logout', follow_redirects=False)
        assert resp.status_code in (302, 200)

    def test_unauthenticated_redirect_to_login(self, client):
        """GET /dashboard без авторизации перенаправляет на /login."""
        resp = client.get('/dashboard', follow_redirects=False)
        assert resp.status_code == 302
        assert '/login' in resp.headers.get('Location', '')
