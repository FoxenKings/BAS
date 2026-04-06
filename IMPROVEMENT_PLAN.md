# Технический план улучшений inventory_bot_V12
_Последнее обновление: 2026-04-06 | Аудит v12_

---

## ДИАГНОСТИКА: Итоговые цифры

| Уровень     | Всего | Исправлено | Остаток |
|-------------|-------|-----------|---------|
| КРИТИЧЕСКИЙ | 4     | 4         | 0       |
| ВЫСОКИЙ     | 8     | 5         | 3       |
| СРЕДНИЙ     | 12    | 4         | 8       |
| НИЗКИЙ      | 9     | 2         | 7       |
| **Итого**   | **33**| **15**    | **18**  |

---

## СТАТУС КРИТИЧЕСКИХ ПРОБЛЕМ

### ✅ C-1. Debug-маршруты — ИСПРАВЛЕНО в V12
Все 10 debug-маршрутов имеют `@admin_required`:
- routes/documents.py: `/debug/fix-old-documents`, `/debug/fix-document/<id>`
- routes/import_export.py: 6 маршрутов `/debug/*`
- routes/kits.py: `/debug/kit/<id>`
- routes/notifications.py: `/debug-notifications`

### ✅ C-2. lastrowid race condition — ИСПРАВЛЕНО в V12
**Файлы:** database.py (create_nomenclature, create_category, create_variation)
**Решение:** Заменены обращения к `self.cursor.lastrowid` (разделяемый курсор) на
локальные курсоры через `self.connection.execute(...)` + `_cursor.lastrowid`.
`create_instance` исходно уже использовал локальный курсор — ✓.

### ✅ C-3. XSS через `|safe` — ИСПРАВЛЕНО в V12
Все Chart.js-шаблоны переведены на `|tojson`:
- templates/reports/*.html — `chart_labels|tojson`, `expense_data|tojson`
- templates/inventory/results.html — `chart_labels|tojson`
- templates/logs/list.html — `chart_labels|tojson`
- templates/kits/view.html — `kit.name|tojson|safe` (корректный паттерн)

### ✅ C-4. Session fixation — ИСПРАВЛЕНО в V12
routes/auth.py:97 — `session.clear()` перед присвоением данных пользователя.
Смена пароля также регенерирует сессию (строки 304-312).

---

## СТАТУС ВЫСОКИХ ПРОБЛЕМ

### ✅ H-1. CSRF в delete_modal.html — ИСПРАВЛЕНО в V12
`templates/modals/delete_modal.html` содержит:
```html
<input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
```

### ✅ H-2. Rate limiting только на /login — ЧАСТИЧНО ИСПРАВЛЕНО в V12
Добавлены ограничения на:
- `@limiter.limit("60 per minute")` → `/api/nomenclatures/search`
- `@limiter.limit("20 per minute")` → `/api/nomenclatures/quick-create`
- `@limiter.limit("10 per minute")` → `upload_nomenclature_image`, `add_user`, `export_nomenclatures`
- `@limiter.limit("5 per minute")` → `reset_user_password`
- `@limiter.limit("3 per minute")` → `export_full`
- Глобальный лимит: `200 per minute` (extensions.py)

**Остаток:** `/api/*` в documents.py, warehouses.py — без явных лимитов.

### ⬜ H-3. 49 мест cursor() вместо execute_query()
**Файлы:** routes/import_export.py (12 мест), routes/admin.py, routes/inventory.py
**Статус:** НЕ ИСПРАВЛЕНО. Большинство мест в import_export.py используют `BEGIN EXCLUSIVE`
транзакции, где прямой cursor необходим.

### ⬜ H-4. Монолиты: documents.py (1737 строк), import_export.py
**Статус:** НЕ ИСПРАВЛЕНО. Требует создания DocumentService, ImportService.
**Сложность:** Высокая, риск регрессий.

### ✅ H-5. N+1 запросы в reports.py — СНИЖЕНЫ индексами в V12
Добавлены составные индексы:
- `idx_documents_status_date ON documents(status, document_date DESC)` — фильтры отчётов
- `idx_documents_type_date ON documents(document_type, document_date DESC)` — turnover
- `idx_document_items_document ON document_items(document_id)` — JOIN-ы

**Остаток:** Сами N+1 циклы не переписаны в JOIN. Индексы смягчают деградацию.

### ⬜ H-6. fetch_all=True без LIMIT в 71 месте
**Статус:** НЕ ИСПРАВЛЕНО. Требует пагинации в UI и LIMIT в запросах.

### ✅ H-7. Отсутствует составной индекс на stocks — ИСПРАВЛЕНО ещё в V11
`idx_stocks_lookup ON stocks(nomenclature_id, warehouse_id, batch_id)` — существует.

### ⬜ H-8. import_export.py содержит несвязанные функции
**Статус:** НЕ ИСПРАВЛЕНО. logs.py и translations.py выделены в отдельные Blueprint-ы,
но barcodes, debug-маршруты остаются в import_export.py.

---

## СТАТУС СРЕДНИХ ПРОБЛЕМ

### ✅ M-1. Нет regenerate session ID при смене пароля — ИСПРАВЛЕНО в V11/V12
routes/auth.py:304-312 — полная регенерация сессии после смены пароля.

### ✅ M-2. Content-Security-Policy — УЛУЧШЕНО в V12
- Добавлен `upgrade-insecure-requests`
- Добавлена per-request nonce-инфраструктура (`g.csp_nonce`, передаётся в шаблоны)
- CSP-nonce доступен в шаблонах через `{{ csp_nonce }}`
- **Полный эффект** наступит после добавления `nonce="{{ csp_nonce }}"` в теги `<script>` шаблонов

### ⬜ M-3. Нет rollback транзакции после ошибки импорта
**Файл:** routes/import_export.py
**Статус:** НЕ ИСПРАВЛЕНО.

### ⬜ M-4. UnsavedChangesGuard не работает с AJAX-формами
**Статус:** НЕ ИСПРАВЛЕНО.

### ⬜ M-5. XMLHttpRequest вместо fetch API
**Статус:** НЕ ИСПРАВЛЕНО (не критично).

### ⬜ M-6. Chart.js без try/catch в 8 шаблонах
**Статус:** НЕ ИСПРАВЛЕНО.

### ✅ M-7. expense_data|safe → tojson — ИСПРАВЛЕНО в V12
templates/reports/profit_loss.html использует `{{ expense_data|tojson }}`.

### ✅ M-8. Валидация числовых полей на сервере — ДОБАВЛЕНА в V12
- routes/auth.py: проверка сложности пароля (заглавная + цифра + длина ≥ 8)
- routes/admin.py: та же проверка при создании пользователя
- constants.py: `Security.PASSWORD_REQUIRE_UPPERCASE`, `Security.PASSWORD_REQUIRE_DIGIT`

### ⬜ M-9. Debug-маршруты в kits.py и notifications.py без ограничений
**Статус:** Маршруты защищены `@admin_required` (C-1). Rate-limit не добавлен.

### ✅ M-10. X-Frame-Options — ИСПРАВЛЕНО в V11
Заголовок `X-Frame-Options: SAMEORIGIN` установлен в `set_security_headers`.

### ✅ M-11. Логи ротируются — ИСПРАВЛЕНО в V11
`RotatingFileHandler(maxBytes=10MB, backupCount=5)` в app.py.

### ✅ M-12. Healthcheck для scheduler — РЕАЛИЗОВАН в V12
`/health` проверяет scheduler через `gc.get_objects()` + `get_scheduler_health()`.

---

## СТАТУС НИЗКИХ ПРОБЛЕМ

### ⬜ L-1. Нет кэша для /api/nomenclatures/search
**Статус:** Кэш существует (`_search_cache_get`), TTL короткий. Rate-limit добавлен.

### ⬜ L-2. Нет gzip для статических файлов
**Статус:** `flask-compress` установлен и инициализирован в app.py. Работает для HTML/JSON.
Для статики нужен `COMPRESS_MIMETYPES` или CDN.

### ⬜ L-3. CSS: дублирование style.css и improvements.css
**Статус:** НЕ ИСПРАВЛЕНО.

### ⬜ L-4. Нет favicon для PWA
**Статус:** НЕ ИСПРАВЛЕНО.

### ✅ L-5. Jinja2 фильтр для дат
`app.py:389` — фильтр `|dt_fmt` реализован (`@app.template_filter('dt_fmt')`).

### ⬜ L-6. Нет skeleton loader
**Статус:** НЕ ИСПРАВЛЕНО.

### ⬜ L-7. Нет автоочистки sessions
**Статус:** Таблица `login_attempts` очищается планировщиком. Flask-session файлы не чистятся.

### ⬜ L-8. Тесты не запускались
**Статус:** НЕ ИСПРАВЛЕНО. 9 тест-файлов, coverage неизвестен.

### ⬜ L-9. requirements.txt без версий (==)
**Статус:** Все ключевые пакеты закреплены по версиям (`Flask==2.3.3` и т.д.).

---

## ДОПОЛНИТЕЛЬНЫЕ УЛУЧШЕНИЯ V12

### Новые индексы БД (database.py)
| Индекс | Таблица | Назначение |
|--------|---------|-----------|
| `idx_documents_status_date` | documents(status, document_date DESC) | Фильтрация по статусу + дата |
| `idx_documents_type_date` | documents(document_type, document_date DESC) | Отчёты по типу |
| `idx_document_items_document` | document_items(document_id) | JOIN документов |
| `idx_nomenclatures_active_name` | nomenclatures(is_active, name) | Поиск + список |
| `idx_instances_calibration` | instances(calibration_date) | Планировщик уведомлений |
| `idx_batches_expiry_active` | batches(expiry_date, is_active) | Уведомления истечения |
| `idx_notifications_created` | notifications(created_at) | Очистка старых |

### Безопасность
- `upgrade-insecure-requests` в CSP
- CSP nonce-инфраструктура (`g.csp_nonce`, передаётся в шаблоны)
- Password complexity: требование заглавной буквы + цифры

---

## СЛЕДУЮЩИЙ ЭТАП (приоритет)

### Этап 3 — Производительность
11. [H-6] Pagination где отсутствует — добавить LIMIT 500 как минимум
12. [H-5] Устранить N+1 в reports.py через JOIN
13. [H-3] cursor() → execute_query() в import_export.py

### Этап 4 — Рефакторинг
14. [H-4] Выделить DocumentService из documents.py
15. [H-8] Разбить import_export.py (barcodes → отдельный Blueprint)

### Этап 5 — Качество
16. Покрытие тестами: цель 40%+
17. Обновление Flask до 3.x
18. Type hints для публичных API database.py
