"""
Комплексное тестирование всех маршрутов, фильтров и поисковых полей.

Покрывает:
  - Smoke-тесты всех GET-маршрутов (200/302)
  - Все фильтры с корректными, пустыми и граничными значениями
  - Поисковые инпуты: кириллица, латиница, спецсимволы, SQL-инъекции
  - API-эндпоинты (JSON)
  - Пагинация
  - RBAC: доступ viewer vs admin
  - 404 / неверные ID
"""
import pytest
import json


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _ok(resp):
    """HTTP 200 или редирект — оба считаются «работает»."""
    assert resp.status_code in (200, 302), (
        f"Unexpected status {resp.status_code}: {resp.request.path}"
    )


def _ok200(resp):
    """Строго HTTP 200."""
    assert resp.status_code == 200, (
        f"Expected 200, got {resp.status_code}: {resp.request.path}"
    )


def _json_ok(resp):
    """HTTP 200 + валидный JSON."""
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
    data = json.loads(resp.data)
    assert isinstance(data, (dict, list))
    return data


# ---------------------------------------------------------------------------
# Маршруты дашборда и профиля
# ---------------------------------------------------------------------------

class TestDashboardRoutes:
    def test_dashboard(self, auth_client):
        _ok(auth_client.get('/dashboard'))

    def test_profile(self, auth_client):
        _ok(auth_client.get('/profile'))

    def test_root_redirect(self, auth_client):
        resp = auth_client.get('/')
        assert resp.status_code in (200, 302)

    def test_dashboard_no_auth_redirects(self, client):
        resp = client.get('/dashboard')
        assert resp.status_code == 302
        assert '/login' in resp.headers['Location']


# ---------------------------------------------------------------------------
# Номенклатура — маршруты и фильтры
# ---------------------------------------------------------------------------

class TestNomenclaturesRoutes:
    def test_list_plain(self, auth_client):
        _ok(auth_client.get('/nomenclatures'))

    def test_list_search_cyrillic(self, auth_client):
        _ok(auth_client.get('/nomenclatures?search=стол'))

    def test_list_search_latin(self, auth_client):
        _ok(auth_client.get('/nomenclatures?search=table'))

    def test_list_search_digits(self, auth_client):
        _ok(auth_client.get('/nomenclatures?search=12345'))

    def test_list_search_empty(self, auth_client):
        _ok(auth_client.get('/nomenclatures?search='))

    def test_list_search_special_chars(self, auth_client):
        _ok(auth_client.get("/nomenclatures?search=%25%26%40%21"))

    def test_list_search_sql_injection(self, auth_client):
        _ok(auth_client.get("/nomenclatures?search=' OR 1=1--"))

    def test_list_search_xss(self, auth_client):
        _ok(auth_client.get("/nomenclatures?search=<script>alert(1)</script>"))

    def test_list_search_long_string(self, auth_client):
        _ok(auth_client.get('/nomenclatures?search=' + 'а' * 300))

    def test_list_filter_category(self, auth_client):
        _ok(auth_client.get('/nomenclatures?category_id=1'))

    def test_list_filter_accounting_type_individual(self, auth_client):
        _ok(auth_client.get('/nomenclatures?accounting_type=individual'))

    def test_list_filter_accounting_type_batch(self, auth_client):
        _ok(auth_client.get('/nomenclatures?accounting_type=batch'))

    def test_list_filter_accounting_type_quantitative(self, auth_client):
        _ok(auth_client.get('/nomenclatures?accounting_type=quantitative'))

    def test_list_filter_combined(self, auth_client):
        _ok(auth_client.get('/nomenclatures?search=тест&category_id=1&accounting_type=individual'))

    def test_list_pagination_page1(self, auth_client):
        _ok(auth_client.get('/nomenclatures?page=1'))

    def test_list_pagination_page2(self, auth_client):
        _ok(auth_client.get('/nomenclatures?page=2'))

    def test_list_pagination_large(self, auth_client):
        _ok(auth_client.get('/nomenclatures?page=9999'))

    def test_list_pagination_zero(self, auth_client):
        _ok(auth_client.get('/nomenclatures?page=0'))

    def test_list_pagination_negative(self, auth_client):
        _ok(auth_client.get('/nomenclatures?page=-5'))

    def test_add_form(self, auth_client):
        _ok(auth_client.get('/nomenclatures/add'))

    def test_view_nonexistent(self, auth_client):
        resp = auth_client.get('/nomenclatures/999999')
        assert resp.status_code in (200, 302, 404)

    def test_edit_nonexistent(self, auth_client):
        resp = auth_client.get('/nomenclatures/999999/edit')
        assert resp.status_code in (200, 302, 404)


# ---------------------------------------------------------------------------
# Экземпляры — маршруты и фильтры
# ---------------------------------------------------------------------------

class TestInstancesRoutes:
    def test_list_plain(self, auth_client):
        _ok(auth_client.get('/instances'))

    def test_list_global_search_cyrillic(self, auth_client):
        _ok(auth_client.get('/instances?global_search=ноутбук'))

    def test_list_global_search_latin(self, auth_client):
        _ok(auth_client.get('/instances?global_search=laptop'))

    def test_list_global_search_inv_number(self, auth_client):
        _ok(auth_client.get('/instances?global_search=2025-000001'))

    def test_list_global_search_empty(self, auth_client):
        _ok(auth_client.get('/instances?global_search='))

    def test_list_global_search_sql(self, auth_client):
        _ok(auth_client.get("/instances?global_search='; DROP TABLE instances;--"))

    def test_list_barcode_search(self, auth_client):
        _ok(auth_client.get('/instances?barcode_search=1234567890'))

    def test_list_status_in_stock(self, auth_client):
        _ok(auth_client.get('/instances?status_filter=in_stock'))

    def test_list_status_in_use(self, auth_client):
        _ok(auth_client.get('/instances?status_filter=in_use'))

    def test_list_status_repair(self, auth_client):
        _ok(auth_client.get('/instances?status_filter=repair'))

    def test_list_status_written_off(self, auth_client):
        _ok(auth_client.get('/instances?status_filter=written_off'))

    def test_list_status_lost(self, auth_client):
        _ok(auth_client.get('/instances?status_filter=lost'))

    def test_list_status_invalid(self, auth_client):
        _ok(auth_client.get('/instances?status_filter=nonexistent_status'))

    def test_list_location_filter(self, auth_client):
        _ok(auth_client.get('/instances?location_filter=1'))

    def test_list_employee_filter(self, auth_client):
        _ok(auth_client.get('/instances?employee_filter=1'))

    def test_list_combined_filters(self, auth_client):
        _ok(auth_client.get('/instances?global_search=тест&status_filter=in_stock&page=1'))

    def test_list_pagination(self, auth_client):
        _ok(auth_client.get('/instances?page=1'))
        _ok(auth_client.get('/instances?page=99'))

    def test_add_form(self, auth_client):
        _ok(auth_client.get('/instances/add'))

    def test_api_check_inventory_number(self, auth_client):
        resp = auth_client.get('/api/instances/check?inventory_number=2025-999999')
        assert resp.status_code in (200, 302, 404)


# ---------------------------------------------------------------------------
# Категории
# ---------------------------------------------------------------------------

class TestCategoriesRoutes:
    def test_list(self, auth_client):
        _ok(auth_client.get('/categories'))

    def test_add_form(self, auth_client):
        _ok(auth_client.get('/categories/add'))

    def test_create_form(self, auth_client):
        _ok(auth_client.get('/categories/create'))

    def test_edit_nonexistent(self, auth_client):
        resp = auth_client.get('/categories/999999/edit')
        assert resp.status_code in (200, 302, 404)

    def test_category_rules(self, auth_client):
        _ok(auth_client.get('/category-rules'))


# ---------------------------------------------------------------------------
# Документы — маршруты и фильтры
# ---------------------------------------------------------------------------

class TestDocumentsRoutes:
    def test_list_plain(self, auth_client):
        _ok(auth_client.get('/documents'))

    def test_list_search_cyrillic(self, auth_client):
        _ok(auth_client.get('/documents?search=приходная'))

    def test_list_search_latin(self, auth_client):
        _ok(auth_client.get('/documents?search=DOC'))

    def test_list_search_number(self, auth_client):
        _ok(auth_client.get('/documents?search=ДОК-2025'))

    def test_list_filter_type_receipt(self, auth_client):
        _ok(auth_client.get('/documents?doc_type=receipt'))

    def test_list_filter_type_issuance(self, auth_client):
        _ok(auth_client.get('/documents?doc_type=issuance'))

    def test_list_filter_type_transfer(self, auth_client):
        _ok(auth_client.get('/documents?doc_type=transfer'))

    def test_list_filter_type_write_off(self, auth_client):
        _ok(auth_client.get('/documents?doc_type=write_off'))

    def test_list_filter_type_return(self, auth_client):
        _ok(auth_client.get('/documents?doc_type=return'))

    def test_list_filter_type_adjustment(self, auth_client):
        _ok(auth_client.get('/documents?doc_type=adjustment'))

    def test_list_filter_status_draft(self, auth_client):
        _ok(auth_client.get('/documents?status=draft'))

    def test_list_filter_status_posted(self, auth_client):
        _ok(auth_client.get('/documents?status=posted'))

    def test_list_filter_status_cancelled(self, auth_client):
        _ok(auth_client.get('/documents?status=cancelled'))

    def test_list_filter_date_range(self, auth_client):
        _ok(auth_client.get('/documents?date_from=2025-01-01&date_to=2025-12-31'))

    def test_list_filter_date_invalid(self, auth_client):
        _ok(auth_client.get('/documents?date_from=not-a-date'))

    def test_list_combined_filters(self, auth_client):
        _ok(auth_client.get('/documents?search=тест&doc_type=receipt&status=posted&page=1'))

    def test_list_pagination(self, auth_client):
        _ok(auth_client.get('/documents?page=1'))
        _ok(auth_client.get('/documents?page=2'))

    def test_issuance_create(self, auth_client):
        _ok(auth_client.get('/issuance/create'))

    def test_view_nonexistent(self, auth_client):
        resp = auth_client.get('/documents/999999')
        assert resp.status_code in (200, 302, 404)

    def test_expense_purposes_list(self, auth_client):
        _ok(auth_client.get('/expense-purposes'))

    def test_add_document_form(self, auth_client):
        # Маршрут требует тип документа: /documents/add/<doc_type>
        _ok(auth_client.get('/documents/add/receipt'))

    def test_add_document_form_issuance(self, auth_client):
        _ok(auth_client.get('/documents/add/issuance'))

    def test_add_document_form_transfer(self, auth_client):
        _ok(auth_client.get('/documents/add/transfer'))

    def test_add_document_form_write_off(self, auth_client):
        _ok(auth_client.get('/documents/add/write_off'))


# ---------------------------------------------------------------------------
# Склады и остатки
# ---------------------------------------------------------------------------

class TestWarehousesRoutes:
    def test_warehouses_list(self, auth_client):
        _ok(auth_client.get('/warehouses'))

    def test_add_warehouse_form(self, auth_client):
        _ok(auth_client.get('/warehouses/add'))

    def test_stocks_list(self, auth_client):
        _ok(auth_client.get('/stocks'))

    def test_stocks_search(self, auth_client):
        _ok(auth_client.get('/stocks?search=болт'))

    def test_stocks_filter_warehouse(self, auth_client):
        _ok(auth_client.get('/stocks?warehouse_id=1'))

    def test_stocks_filter_category(self, auth_client):
        _ok(auth_client.get('/stocks?category_id=1'))

    def test_stocks_filter_low(self, auth_client):
        _ok(auth_client.get('/stocks?show=low_stock'))

    def test_batches_list(self, auth_client):
        _ok(auth_client.get('/batches'))

    def test_batches_filter_active(self, auth_client):
        _ok(auth_client.get('/batches?status=active'))

    def test_batches_filter_expired(self, auth_client):
        _ok(auth_client.get('/batches?status=expired'))

    def test_batches_filter_expiring(self, auth_client):
        _ok(auth_client.get('/batches?status=expiring'))

    def test_add_batch_form(self, auth_client):
        _ok(auth_client.get('/batches/add'))

    def test_storage_bins_list(self, auth_client):
        _ok(auth_client.get('/storage-bins'))

    def test_add_storage_bin_form(self, auth_client):
        _ok(auth_client.get('/storage-bins/add'))

    def test_warehouse_view_nonexistent(self, auth_client):
        resp = auth_client.get('/warehouses/999999')
        assert resp.status_code in (200, 302, 404)

    def test_warehouse_stocks_nonexistent(self, auth_client):
        resp = auth_client.get('/warehouses/999999/stocks')
        assert resp.status_code in (200, 302, 404)


# ---------------------------------------------------------------------------
# Сотрудники
# ---------------------------------------------------------------------------

class TestEmployeesRoutes:
    def test_employees_list(self, auth_client):
        _ok(auth_client.get('/employees'))

    def test_employees_search_cyrillic(self, auth_client):
        _ok(auth_client.get('/employees?search=Иванов'))

    def test_employees_search_latin(self, auth_client):
        _ok(auth_client.get('/employees?search=Ivan'))

    def test_employees_search_empty(self, auth_client):
        _ok(auth_client.get('/employees?search='))

    def test_employees_filter_department(self, auth_client):
        _ok(auth_client.get('/employees?department_id=1'))

    def test_employees_filter_active(self, auth_client):
        _ok(auth_client.get('/employees?is_active=1'))

    def test_add_employee_form(self, auth_client):
        _ok(auth_client.get('/employees/add'))

    def test_view_employee_nonexistent(self, auth_client):
        resp = auth_client.get('/employees/999999/view')
        assert resp.status_code in (200, 302, 404)

    def test_departments_list(self, auth_client):
        _ok(auth_client.get('/departments'))

    def test_add_department_form(self, auth_client):
        _ok(auth_client.get('/departments/add'))

    def test_locations_list(self, auth_client):
        _ok(auth_client.get('/locations'))

    def test_add_location_form(self, auth_client):
        _ok(auth_client.get('/locations/add'))


# ---------------------------------------------------------------------------
# Поставщики
# ---------------------------------------------------------------------------

class TestSuppliersRoutes:
    def test_list(self, auth_client):
        _ok(auth_client.get('/suppliers'))

    def test_search_cyrillic(self, auth_client):
        _ok(auth_client.get('/suppliers?search=ООО'))

    def test_search_latin(self, auth_client):
        _ok(auth_client.get('/suppliers?search=Ltd'))

    def test_search_empty(self, auth_client):
        _ok(auth_client.get('/suppliers?search='))

    def test_search_special(self, auth_client):
        _ok(auth_client.get("/suppliers?search=% & + = ?"))

    def test_add_form(self, auth_client):
        _ok(auth_client.get('/suppliers/add'))

    def test_view_nonexistent(self, auth_client):
        resp = auth_client.get('/suppliers/999999/view')
        assert resp.status_code in (200, 302, 404)


# ---------------------------------------------------------------------------
# Комплекты
# ---------------------------------------------------------------------------

class TestKitsRoutes:
    def test_list(self, auth_client):
        _ok(auth_client.get('/kits'))

    def test_create_form(self, auth_client):
        _ok(auth_client.get('/kits/create'))

    def test_view_nonexistent(self, auth_client):
        resp = auth_client.get('/kits/999999')
        assert resp.status_code in (200, 302, 404)

    def test_edit_nonexistent(self, auth_client):
        resp = auth_client.get('/kits/999999/edit')
        assert resp.status_code in (200, 302, 404)


# ---------------------------------------------------------------------------
# Инвентаризация
# ---------------------------------------------------------------------------

class TestInventoryRoutes:
    def test_list(self, auth_client):
        _ok(auth_client.get('/inventory'))

    def test_list_status_draft(self, auth_client):
        _ok(auth_client.get('/inventory?status=draft'))

    def test_list_status_in_progress(self, auth_client):
        _ok(auth_client.get('/inventory?status=in_progress'))

    def test_list_status_completed(self, auth_client):
        _ok(auth_client.get('/inventory?status=completed'))

    def test_add_form(self, auth_client):
        _ok(auth_client.get('/inventory/add'))

    def test_view_nonexistent(self, auth_client):
        resp = auth_client.get('/inventory/999999')
        assert resp.status_code in (200, 302, 404)


# ---------------------------------------------------------------------------
# Уведомления
# ---------------------------------------------------------------------------

class TestNotificationsRoutes:
    def test_list(self, auth_client):
        _ok(auth_client.get('/notifications'))

    def test_list_page2(self, auth_client):
        _ok(auth_client.get('/notifications?page=2'))

    def test_list_filter_unread(self, auth_client):
        _ok(auth_client.get('/notifications?show=unread'))

    def test_list_filter_read(self, auth_client):
        _ok(auth_client.get('/notifications?show=read'))

    def test_list_filter_type_low_stock(self, auth_client):
        _ok(auth_client.get('/notifications?type=low_stock'))

    def test_list_filter_type_expiring(self, auth_client):
        _ok(auth_client.get('/notifications?type=expiring'))

    def test_list_filter_date_from(self, auth_client):
        _ok(auth_client.get('/notifications?date_from=2025-01-01'))

    def test_list_combined(self, auth_client):
        _ok(auth_client.get('/notifications?show=unread&type=low_stock&page=1'))


# ---------------------------------------------------------------------------
# Отчёты
# ---------------------------------------------------------------------------

class TestReportsRoutes:
    def test_reports_index(self, auth_client):
        _ok(auth_client.get('/reports'))

    def test_stock_balance(self, auth_client):
        _ok(auth_client.get('/reports/stock-balance'))

    def test_stock_balance_filter_warehouse(self, auth_client):
        _ok(auth_client.get('/reports/stock-balance?warehouse_id=1'))

    def test_stock_balance_filter_category(self, auth_client):
        _ok(auth_client.get('/reports/stock-balance?category_id=1'))

    def test_stock_balance_filter_date(self, auth_client):
        _ok(auth_client.get('/reports/stock-balance?date=2025-01-01'))

    def test_stock_movement(self, auth_client):
        _ok(auth_client.get('/reports/stock-movement'))

    def test_stock_movement_filter_dates(self, auth_client):
        _ok(auth_client.get('/reports/stock-movement?date_from=2025-01-01&date_to=2025-12-31'))

    def test_stock_movement_filter_doc_type(self, auth_client):
        _ok(auth_client.get('/reports/stock-movement?doc_type=receipt'))

    def test_low_stock(self, auth_client):
        _ok(auth_client.get('/reports/low-stock'))

    def test_expiring(self, auth_client):
        _ok(auth_client.get('/reports/expiring'))

    def test_documents_by_type(self, auth_client):
        _ok(auth_client.get('/reports/documents-by-type'))

    def test_documents_by_type_filter_dates(self, auth_client):
        _ok(auth_client.get('/reports/documents-by-type?date_from=2025-01-01&date_to=2025-12-31'))

    def test_documents_by_period(self, auth_client):
        _ok(auth_client.get('/reports/documents-by-period'))

    def test_documents_by_period_filter(self, auth_client):
        _ok(auth_client.get('/reports/documents-by-period?date_from=2025-01-01&date_to=2025-12-31'))

    def test_supplier_deliveries(self, auth_client):
        _ok(auth_client.get('/reports/supplier-deliveries'))

    def test_nomenclature_by_category(self, auth_client):
        _ok(auth_client.get('/reports/nomenclature-by-category'))

    def test_most_moved(self, auth_client):
        _ok(auth_client.get('/reports/most-moved'))

    def test_inactive(self, auth_client):
        _ok(auth_client.get('/reports/inactive'))

    def test_employee_issuance(self, auth_client):
        _ok(auth_client.get('/reports/employee-issuance'))

    def test_turnover(self, auth_client):
        _ok(auth_client.get('/reports/turnover'))

    def test_profit_loss(self, auth_client):
        _ok(auth_client.get('/reports/profit-loss'))


# ---------------------------------------------------------------------------
# Импорт / Экспорт / Логи
# ---------------------------------------------------------------------------

class TestImportExportRoutes:
    def test_import_export_index(self, auth_client):
        _ok(auth_client.get('/import-export'))

    def test_import_universal(self, auth_client):
        _ok(auth_client.get('/import/universal'))

    def test_logs_list(self, auth_client):
        _ok(auth_client.get('/logs'))

    def test_logs_search(self, auth_client):
        _ok(auth_client.get('/logs?search=admin'))

    def test_logs_filter_action(self, auth_client):
        _ok(auth_client.get('/logs?action=create'))

    def test_logs_filter_entity(self, auth_client):
        _ok(auth_client.get('/logs?entity_type=nomenclature'))

    def test_logs_filter_dates(self, auth_client):
        _ok(auth_client.get('/logs?date_from=2025-01-01&date_to=2025-12-31'))

    def test_logs_combined(self, auth_client):
        _ok(auth_client.get('/logs?search=admin&action=create&date_from=2025-01-01'))

    def test_translations_list(self, auth_client):
        _ok(auth_client.get('/translations'))

    def test_excel_import_page(self, auth_client):
        # Реальный маршрут: /admin/excel-import
        _ok(auth_client.get('/admin/excel-import'))


# ---------------------------------------------------------------------------
# Административный раздел
# ---------------------------------------------------------------------------

class TestAdminRoutes:
    def test_users_list(self, auth_client):
        _ok(auth_client.get('/users'))

    def test_add_user_form(self, auth_client):
        _ok(auth_client.get('/users/add'))

    def test_edit_user_nonexistent(self, auth_client):
        resp = auth_client.get('/users/999999/edit')
        assert resp.status_code in (200, 302, 404)

    def test_user_permissions(self, auth_client):
        _ok(auth_client.get('/users/permissions'))

    def test_counters(self, auth_client):
        _ok(auth_client.get('/admin/counters'))

    def test_backup(self, auth_client):
        _ok(auth_client.get('/admin/backup'))

    def test_debug_routes(self, auth_client):
        _ok(auth_client.get('/debug-routes'))

    def test_users_list_viewer_blocked(self, viewer_client):
        resp = viewer_client.get('/users')
        assert resp.status_code in (302, 403)

    def test_backup_viewer_blocked(self, viewer_client):
        resp = viewer_client.get('/admin/backup')
        assert resp.status_code in (302, 403)

    def test_counters_viewer_blocked(self, viewer_client):
        resp = viewer_client.get('/admin/counters')
        assert resp.status_code in (302, 403)


# ---------------------------------------------------------------------------
# API-эндпоинты (JSON)
# ---------------------------------------------------------------------------

class TestAPIEndpoints:
    def test_notifications_count(self, auth_client):
        resp = auth_client.get('/api/notifications/counts')
        assert resp.status_code in (200, 302)
        if resp.status_code == 200:
            data = json.loads(resp.data)
            assert 'count' in data or 'unread' in data or isinstance(data, dict)

    def test_employees_search_empty(self, auth_client):
        resp = auth_client.get('/api/employees/search?q=')
        assert resp.status_code in (200, 302)

    def test_employees_search_query(self, auth_client):
        resp = auth_client.get('/api/employees/search?q=Иванов')
        assert resp.status_code in (200, 302)
        if resp.status_code == 200:
            data = json.loads(resp.data)
            assert isinstance(data, (list, dict))

    def test_employees_list_api(self, auth_client):
        resp = auth_client.get('/api/employees/list')
        assert resp.status_code in (200, 302)

    def test_departments_list_api(self, auth_client):
        resp = auth_client.get('/api/departments/list')
        assert resp.status_code in (200, 302)

    def test_suppliers_search_empty(self, auth_client):
        resp = auth_client.get('/api/suppliers/search?q=')
        assert resp.status_code in (200, 302)

    def test_suppliers_search_query(self, auth_client):
        resp = auth_client.get('/api/suppliers/search?q=ООО')
        assert resp.status_code in (200, 302)

    def test_nomenclature_thumbnail_nonexistent(self, auth_client):
        resp = auth_client.get('/api/nomenclatures/999999/thumbnail')
        assert resp.status_code in (200, 302, 404)

    def test_nomenclature_variations_nonexistent(self, auth_client):
        resp = auth_client.get('/api/nomenclatures/999999/variations')
        assert resp.status_code in (200, 302, 404)

    def test_nomenclature_images_nonexistent(self, auth_client):
        resp = auth_client.get('/api/nomenclatures/999999/images')
        assert resp.status_code in (200, 302, 404)

    def test_kit_components_nonexistent(self, auth_client):
        resp = auth_client.get('/api/kits/999999/components')
        assert resp.status_code in (200, 302, 404)

    def test_check_instance_nonexistent_inv(self, auth_client):
        resp = auth_client.get('/api/instances/check?inventory_number=NONE-999999')
        assert resp.status_code in (200, 302, 404)

    def test_qr_document_nonexistent(self, auth_client):
        resp = auth_client.get('/api/qr/document/999999')
        assert resp.status_code in (200, 302, 404)

    def test_qr_instance_nonexistent(self, auth_client):
        resp = auth_client.get('/api/qr/instance/999999')
        assert resp.status_code in (200, 302, 404)

    def test_api_object_history(self, auth_client):
        resp = auth_client.get('/api/logs/nomenclature/1')
        assert resp.status_code in (200, 302, 404)

    def test_object_history_page(self, auth_client):
        resp = auth_client.get('/logs/object/nomenclature/1')
        assert resp.status_code in (200, 302, 404)


# ---------------------------------------------------------------------------
# Граничные случаи — неверные типы параметров
# ---------------------------------------------------------------------------

class TestEdgeCasesParams:
    def test_nomenclatures_page_string(self, auth_client):
        _ok(auth_client.get('/nomenclatures?page=abc'))

    def test_instances_page_float(self, auth_client):
        _ok(auth_client.get('/instances?page=1.5'))

    def test_documents_page_none(self, auth_client):
        _ok(auth_client.get('/documents?page='))

    def test_warehouses_id_string(self, auth_client):
        resp = auth_client.get('/warehouses/abc')
        assert resp.status_code in (200, 302, 404)

    def test_instances_unicode_search(self, auth_client):
        _ok(auth_client.get('/instances?global_search=测试テスト한국어'))

    def test_nomenclatures_unicode_search(self, auth_client):
        _ok(auth_client.get('/nomenclatures?search=测试テスト한국어'))

    def test_documents_future_date(self, auth_client):
        _ok(auth_client.get('/documents?date_from=2099-12-31'))

    def test_documents_past_date(self, auth_client):
        _ok(auth_client.get('/documents?date_from=1900-01-01'))

    def test_reports_invalid_warehouse(self, auth_client):
        _ok(auth_client.get('/reports/stock-balance?warehouse_id=abc'))

    def test_reports_negative_id(self, auth_client):
        _ok(auth_client.get('/reports/stock-balance?warehouse_id=-1'))

    def test_notifications_invalid_type(self, auth_client):
        _ok(auth_client.get('/notifications?type=nonexistent_type'))

    def test_inventory_invalid_status(self, auth_client):
        _ok(auth_client.get('/inventory?status=invalid_status'))


# ---------------------------------------------------------------------------
# Гость не имеет доступа ни к чему
# ---------------------------------------------------------------------------

class TestGuestBlocked:
    PROTECTED = [
        '/dashboard', '/nomenclatures', '/instances', '/categories',
        '/documents', '/warehouses', '/batches', '/storage-bins', '/stocks',
        '/employees', '/departments', '/locations', '/suppliers', '/kits',
        '/inventory', '/notifications', '/reports', '/reports/stock-balance',
        '/reports/stock-movement', '/reports/low-stock', '/reports/expiring',
        '/reports/documents-by-type', '/reports/documents-by-period',
        '/reports/supplier-deliveries', '/reports/nomenclature-by-category',
        '/reports/most-moved', '/reports/inactive', '/reports/employee-issuance',
        '/reports/turnover', '/reports/profit-loss',
        '/users', '/admin/counters', '/admin/backup',
        '/logs', '/import-export', '/import/universal',
        '/admin/excel-import',
    ]

    @pytest.mark.parametrize('url', PROTECTED)
    def test_guest_redirected(self, client, url):
        resp = client.get(url)
        assert resp.status_code == 302, f"Expected redirect for {url}, got {resp.status_code}"
        assert '/login' in resp.headers.get('Location', '')


# ---------------------------------------------------------------------------
# Viewer не может изменять данные (POST-запросы)
# ---------------------------------------------------------------------------

class TestViewerReadOnly:
    def test_viewer_can_read_nomenclatures(self, viewer_client):
        _ok(viewer_client.get('/nomenclatures'))

    def test_viewer_can_read_instances(self, viewer_client):
        _ok(viewer_client.get('/instances'))

    def test_viewer_can_read_documents(self, viewer_client):
        _ok(viewer_client.get('/documents'))

    def test_viewer_can_read_reports(self, viewer_client):
        _ok(viewer_client.get('/reports'))

    def test_viewer_cannot_access_users(self, viewer_client):
        resp = viewer_client.get('/users')
        assert resp.status_code in (302, 403)

    def test_viewer_cannot_access_admin_backup(self, viewer_client):
        resp = viewer_client.get('/admin/backup')
        assert resp.status_code in (302, 403)


# ---------------------------------------------------------------------------
# 404 для несуществующих страниц
# ---------------------------------------------------------------------------

class TestNotFound:
    def test_completely_unknown_route(self, auth_client):
        resp = auth_client.get('/this/does/not/exist/at/all/xyz')
        assert resp.status_code in (404, 302)

    def test_api_unknown(self, auth_client):
        resp = auth_client.get('/api/nonexistent_endpoint')
        assert resp.status_code in (404, 302)
