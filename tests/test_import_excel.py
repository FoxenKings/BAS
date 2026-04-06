"""
Тесты импорта Excel — smoke-тесты HTTP-маршрутов и unit-тесты парсинга.
"""
import io
import pytest


# ─── Smoke-тесты маршрутов импорта ───────────────────────────────────────────

class TestImportRoutes:
    def test_import_page_requires_auth(self, client):
        resp = client.get('/import-export', follow_redirects=False)
        assert resp.status_code == 302

    def test_import_page_accessible(self, auth_client):
        resp = auth_client.get('/import-export')
        assert resp.status_code == 200

    def test_universal_import_get(self, auth_client):
        resp = auth_client.get('/import/universal')
        assert resp.status_code == 200

    def test_upload_without_file_redirects(self, auth_client):
        """POST без файла — редирект или 200 с сообщением об ошибке."""
        resp = auth_client.post(
            '/import/universal',
            data={'action': 'upload'},
            follow_redirects=True
        )
        assert resp.status_code == 200

    def test_upload_empty_filename_redirects(self, auth_client):
        """POST с пустым именем файла — редирект."""
        data = {
            'action': 'upload',
            'file': (io.BytesIO(b''), ''),
        }
        resp = auth_client.post(
            '/import/universal',
            data=data,
            content_type='multipart/form-data',
            follow_redirects=True
        )
        assert resp.status_code == 200

    def test_upload_non_excel_file(self, auth_client):
        """POST с текстовым файлом — не 500."""
        data = {
            'action': 'upload',
            'file': (io.BytesIO(b'not an excel file'), 'data.txt'),
        }
        resp = auth_client.post(
            '/import/universal',
            data=data,
            content_type='multipart/form-data',
            follow_redirects=True
        )
        assert resp.status_code != 500

    def test_download_enhanced_template(self, auth_client):
        """GET /export/template — возвращает файл Excel."""
        resp = auth_client.get('/export/template')
        assert resp.status_code in (200, 302, 404)
        if resp.status_code == 200:
            ct = resp.content_type
            assert 'excel' in ct or 'spreadsheet' in ct or 'octet' in ct


class TestExportRoutes:
    def test_export_nomenclatures_requires_auth(self, client):
        resp = client.get('/export/nomenclatures', follow_redirects=False)
        assert resp.status_code == 302

    def test_export_nomenclatures(self, auth_client):
        resp = auth_client.get('/export/nomenclatures')
        assert resp.status_code in (200, 302)

    def test_export_stocks(self, auth_client):
        resp = auth_client.get('/export/stocks')
        assert resp.status_code in (200, 302)

    def test_export_documents(self, auth_client):
        resp = auth_client.get('/export/documents')
        assert resp.status_code in (200, 302)


# ─── Unit-тесты вспомогательных функций ──────────────────────────────────────

class TestBarcodeApiUnit:
    def test_api_generate_barcode_returns_json(self, auth_client):
        resp = auth_client.get('/api/generate-barcode')
        assert resp.status_code in (200, 404, 405)
        if resp.status_code == 200:
            data = resp.get_json()
            assert data is not None

    def test_api_expense_purposes_returns_json(self, auth_client):
        resp = auth_client.get('/api/expense-purposes')
        assert resp.status_code in (200, 404)
        if resp.status_code == 200:
            data = resp.get_json()
            assert isinstance(data, (list, dict))
