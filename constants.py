"""
Константы приложения — централизованное хранилище magic strings и magic numbers.
Импортируйте нужные константы вместо использования строковых литералов напрямую.
"""


# ── Роли пользователей ────────────────────────────────────────────────────────
class Roles:
    ADMIN = 'admin'
    MANAGER = 'manager'
    VIEWER = 'viewer'

    ALL = (ADMIN, MANAGER, VIEWER)
    STAFF = (ADMIN, MANAGER)


# ── Статусы экземпляров ───────────────────────────────────────────────────────
class InstanceStatus:
    IN_STOCK = 'in_stock'
    IN_USE = 'in_use'
    UNDER_REPAIR = 'repair'
    WRITTEN_OFF = 'written_off'
    LOST = 'lost'

    ALL = (IN_STOCK, IN_USE, UNDER_REPAIR, WRITTEN_OFF, LOST)


# ── Типы учёта номенклатуры ───────────────────────────────────────────────────
class AccountingType:
    INDIVIDUAL = 'individual'
    BATCH = 'batch'
    QUANTITATIVE = 'quantitative'

    ALL = (INDIVIDUAL, BATCH, QUANTITATIVE)


# ── Типы документов ───────────────────────────────────────────────────────────
class DocumentType:
    RECEIPT = 'receipt'
    TRANSFER = 'transfer'
    ISSUANCE = 'issuance'
    WRITE_OFF = 'write_off'
    RETURN = 'return'
    ADJUSTMENT = 'adjustment'

    ALL = (RECEIPT, TRANSFER, ISSUANCE, WRITE_OFF, RETURN, ADJUSTMENT)


# ── Статусы документов ────────────────────────────────────────────────────────
class DocumentStatus:
    DRAFT = 'draft'
    POSTED = 'posted'
    CANCELLED = 'cancelled'

    ALL = (DRAFT, POSTED, CANCELLED)


# ── Типы локаций ─────────────────────────────────────────────────────────────
class LocationType:
    WAREHOUSE = 'warehouse'
    DEPARTMENT = 'department'
    PRODUCTION = 'production'
    OFFICE = 'office'

    ALL = (WAREHOUSE, DEPARTMENT, PRODUCTION, OFFICE)


# ── TTL кэшей (секунды) ───────────────────────────────────────────────────────
class CacheTTL:
    CATEGORIES = 600       # 10 минут
    TRANSLATIONS = 3600    # 1 час
    UNREAD_COUNT = 30      # 30 секунд


# ── Параметры безопасности ────────────────────────────────────────────────────
class Security:
    PASSWORD_MIN_LENGTH = 8
    PASSWORD_REQUIRE_UPPERCASE = True   # хотя бы одна заглавная буква
    PASSWORD_REQUIRE_DIGIT = True       # хотя бы одна цифра
    LOCKOUT_ATTEMPTS = 5
    LOCKOUT_WINDOW = 600   # 10 минут
    LOCKOUT_DURATION = 900 # 15 минут
    MAX_UPLOAD_MB = 16


# ── Допустимые таблицы для экспорта ──────────────────────────────────────────
ALLOWED_EXPORT_TABLES = frozenset({
    'nomenclatures', 'categories', 'instances', 'batches',
    'stocks', 'warehouses', 'storage_bins', 'suppliers',
    'employees', 'documents',
})
