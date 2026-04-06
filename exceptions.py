"""
Иерархия исключений приложения Inventory Bot.

Использование:
    from exceptions import NotFoundError, PermissionError, ValidationError

    raise NotFoundError("Номенклатура не найдена", entity="nomenclature", entity_id=42)
    raise ValidationError("Неверный формат даты", field="expiry_date")
    raise BusinessRuleError("Нельзя удалить склад с остатками")
"""


class AppError(Exception):
    """Базовый класс для всех прикладных ошибок."""

    http_status: int = 500
    default_message: str = "Внутренняя ошибка сервера"

    def __init__(self, message: str | None = None, **context):
        self.message = message or self.default_message
        self.context = context
        super().__init__(self.message)

    def to_dict(self) -> dict:
        return {"error": self.message, **self.context}


class NotFoundError(AppError):
    """Запрошенный объект не существует."""

    http_status = 404
    default_message = "Объект не найден"

    def __init__(self, message: str | None = None, *, entity: str = "", entity_id=None):
        super().__init__(message, entity=entity, entity_id=entity_id)


class PermissionError(AppError):
    """Недостаточно прав для выполнения операции."""

    http_status = 403
    default_message = "Доступ запрещён"

    def __init__(self, message: str | None = None, *, required_role: str = ""):
        super().__init__(message, required_role=required_role)


class ValidationError(AppError):
    """Ошибка валидации входных данных."""

    http_status = 400
    default_message = "Некорректные данные"

    def __init__(self, message: str | None = None, *, field: str = "", value=None):
        super().__init__(message, field=field, value=value)


class BusinessRuleError(AppError):
    """Нарушение бизнес-правила (операция допустима технически, но запрещена логикой)."""

    http_status = 409
    default_message = "Операция нарушает бизнес-правило"

    def __init__(self, message: str | None = None, *, rule: str = ""):
        super().__init__(message, rule=rule)


class DatabaseError(AppError):
    """Ошибка на уровне базы данных."""

    http_status = 500
    default_message = "Ошибка базы данных"
