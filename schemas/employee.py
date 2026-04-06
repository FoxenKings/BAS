"""
Marshmallow-схема для сотрудников.
"""
from marshmallow import Schema, fields, validate, validates, ValidationError


class EmployeeSchema(Schema):
    """Валидация данных сотрудника."""

    first_name = fields.Str(
        required=True,
        validate=validate.Length(min=1, max=100),
        error_messages={"required": "Имя обязательно"},
    )
    last_name = fields.Str(
        required=True,
        validate=validate.Length(min=1, max=100),
        error_messages={"required": "Фамилия обязательна"},
    )
    middle_name = fields.Str(load_default='', validate=validate.Length(max=100))
    employee_number = fields.Str(
        load_default=None,
        allow_none=True,
        validate=validate.Length(max=50),
    )
    department_id = fields.Int(load_default=None, allow_none=True)
    position = fields.Str(load_default='', validate=validate.Length(max=200))
    email = fields.Email(load_default=None, allow_none=True)
    phone = fields.Str(load_default='', validate=validate.Length(max=50))
    is_active = fields.Bool(load_default=True)

    @validates('first_name')
    def validate_first_name(self, value):
        if not value.strip():
            raise ValidationError("Имя не может быть пустым")

    @validates('last_name')
    def validate_last_name(self, value):
        if not value.strip():
            raise ValidationError("Фамилия не может быть пустой")
