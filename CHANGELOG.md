# Changelog

Все значимые изменения в проекте фиксируются в этом файле.

Формат основан на [Keep a Changelog](https://keepachangelog.com/ru/1.0.0/).

---

## [11.0] — 2026-03-23

### Безопасность
- Удалены пароли из логов при инициализации пользователей по умолчанию (`database.py`)
- Добавлена обязательная авторизация `@admin_required` на маршрут `/check-template`
- Исправлен SQL с динамическим именем таблицы в PRAGMA — добавлена валидация через regex
- Добавлен белый список таблиц `ALLOWED_EXPORT_TABLES` для динамических SQL-запросов в экспорте
- Минимальная длина пароля увеличена с 6 до 8 символов
- Добавлен заголовок `Content-Security-Policy` ко всем ответам
- Добавлен `maxlength` на поля username/password в форме входа
- Маршрут `debug_fix_old_documents` защищён проверкой `DEBUG` режима

### Исправлено
- Ошибка `.seconds` vs `.total_seconds()` в TTL-проверке кэша переводов (приводила к сбросу кэша каждую минуту вместо каждого часа)
- 13 голых `except:` заменены на `except Exception:` в documents.py, import_export.py, nomenclatures.py
- Race condition при генерации номеров документов устранена через `BEGIN EXCLUSIVE`-транзакцию
- 5 ручных проверок роли `session.get('role') != 'admin'` заменены декоратором `@admin_required`
- Динамические f-строки с SQL в `api_update_profile` заменены параметризованными запросами

### Добавлено
- `constants.py` — централизованное хранилище констант (роли, статусы, типы, TTL, безопасность)
- Кэширование счётчика непрочитанных уведомлений с TTL 30 сек (вместо запроса на каждый рендер)
- Индексы: `idx_instances_location_id`, `idx_instances_warehouse_id`, `idx_login_attempts_ip_time`
- `backup_db.py`: WAL checkpoint + ANALYZE после создания резервной копии
- `DEBUG` параметр в `config.py` и `.env.example`
- Поле `DEFAULT_ADMIN_PASSWORD` в `.env.example`

### Рефакторинг
- `add_document()` (379 строк) разбит на 4 вспомогательные функции:
  `_parse_form_items()`, `_insert_document_record()`, `_save_document_items()`, `_load_document_form_data()`
- `edit_document()` POST переработан с использованием `_parse_form_items()`
- `app.py` и `auth.py` подключены к `constants.py` (убраны magic numbers)
- Удалены emoji из всех вызовов логгера (оставлены только в flash-сообщениях для UI)

---

## [10.x] — предыдущие версии

История изменений до v11.0 не задокументирована.
