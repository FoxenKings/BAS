"""
Smoke-тесты для документооборота и отчётов.
"""
import pytest


class TestDocuments:
    """Тесты страниц документов."""

    def test_documents_list_requires_auth(self, client):
        """GET /documents без авторизации — редирект."""
        resp = client.get('/documents', follow_redirects=False)
        assert resp.status_code == 302

    def test_documents_list(self, auth_client):
        """GET /documents — 200."""
        resp = auth_client.get('/documents')
        assert resp.status_code == 200

    def test_documents_list_with_filter(self, auth_client):
        """GET /documents?type=receipt — 200."""
        resp = auth_client.get('/documents?type=receipt')
        assert resp.status_code == 200


class TestReports:
    """Тесты страниц отчётов."""

    def test_stock_balance_report(self, auth_client):
        """GET /reports/stock-balance — 200."""
        resp = auth_client.get('/reports/stock-balance')
        assert resp.status_code == 200

    def test_stock_movement_report(self, auth_client):
        """GET /reports/stock-movement — 200."""
        resp = auth_client.get('/reports/stock-movement')
        assert resp.status_code in (200, 302)
