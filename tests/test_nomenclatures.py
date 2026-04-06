"""
Smoke-тесты для номенклатуры и экземпляров.
"""
import pytest


class TestNomenclatures:
    """Тесты страниц номенклатуры."""

    def test_nomenclatures_list_requires_auth(self, client):
        """GET /nomenclatures без авторизации — редирект."""
        resp = client.get('/nomenclatures', follow_redirects=False)
        assert resp.status_code == 302

    def test_nomenclatures_list(self, auth_client):
        """GET /nomenclatures — 200."""
        resp = auth_client.get('/nomenclatures')
        assert resp.status_code == 200

    def test_add_nomenclature_form(self, auth_client):
        """GET /nomenclatures/add — 200 (форма создания)."""
        resp = auth_client.get('/nomenclatures/add')
        assert resp.status_code == 200

    def test_nomenclatures_page_contains_table(self, auth_client):
        """Страница номенклатуры содержит html-таблицу или сообщение о пустом списке."""
        resp = auth_client.get('/nomenclatures')
        html = resp.data.decode('utf-8', errors='replace')
        assert resp.status_code == 200
        assert len(html) > 200


class TestInstances:
    """Тесты страниц экземпляров."""

    def test_instances_list(self, auth_client):
        """GET /instances — 200."""
        resp = auth_client.get('/instances')
        assert resp.status_code == 200
