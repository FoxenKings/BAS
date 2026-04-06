"""
Marshmallow-схемы для номенклатур и экземпляров.
"""
from marshmallow import Schema, fields, validate, validates, ValidationError, post_load

ACCOUNTING_TYPES = ['individual', 'batch', 'quantitative', 'kit']


class QuickCreateNomenclatureSchema(Schema):
    """Валидация данных быстрого создания номенклатуры (JSON API)."""

    name = fields.Str(
        required=True,
        validate=validate.Length(min=1, max=255),
        error_messages={"required": "Название обязательно"},
    )
    category_id = fields.Int(load_default=None, allow_none=True)
    unit = fields.Str(load_default='шт.', validate=validate.Length(max=20))
    accounting_type = fields.Str(
        load_default='quantitative',
        validate=validate.OneOf(ACCOUNTING_TYPES, error=f"Тип учёта: {', '.join(ACCOUNTING_TYPES)}"),
    )
    description = fields.Str(load_default='', validate=validate.Length(max=2000))
    create_variation = fields.Bool(load_default=False)
    variation_size = fields.Str(load_default=None, allow_none=True, validate=validate.Length(max=50))
    variation_color = fields.Str(load_default=None, allow_none=True, validate=validate.Length(max=50))

    @validates('name')
    def validate_name_not_blank(self, value):
        if not value.strip():
            raise ValidationError("Название не может быть пустым")


class VariationSchema(Schema):
    """Валидация данных модификации номенклатуры."""

    sku = fields.Str(
        required=True,
        validate=validate.Length(min=1, max=100),
        error_messages={"required": "SKU обязателен"},
    )
    size = fields.Str(load_default=None, allow_none=True, validate=validate.Length(max=50))
    color = fields.Str(load_default=None, allow_none=True, validate=validate.Length(max=50))
    additional_params = fields.Str(load_default=None, allow_none=True, validate=validate.Length(max=500))
    is_active = fields.Bool(load_default=True)

    @validates('sku')
    def validate_sku_not_blank(self, value):
        if not value.strip():
            raise ValidationError("SKU не может быть пустым")


class NomenclatureSchema(Schema):
    """Валидация данных номенклатуры."""

    name = fields.Str(
        required=True,
        validate=validate.Length(min=1, max=255, error="Название от 1 до 255 символов"),
        error_messages={"required": "Название обязательно"},
    )
    category_id = fields.Int(
        required=True,
        error_messages={"required": "Категория обязательна"},
    )
    unit = fields.Str(
        load_default='шт.',
        validate=validate.Length(max=20),
    )
    description = fields.Str(load_default='', validate=validate.Length(max=2000))
    min_stock = fields.Float(load_default=0.0, validate=validate.Range(min=0))
    is_serial = fields.Bool(load_default=False)
    accounting_type = fields.Str(
        load_default='quantitative',
        validate=validate.OneOf(
            ['individual', 'batch', 'quantitative', 'kit'],
            error="Тип учёта: individual, batch, quantitative или kit",
        ),
    )
    supplier_id = fields.Int(load_default=None, allow_none=True)
    barcode = fields.Str(load_default='', validate=validate.Length(max=100))

    @validates('name')
    def validate_name_not_blank(self, value):
        if not value.strip():
            raise ValidationError("Название не может быть пустым")


class InstanceSchema(Schema):
    """Валидация данных экземпляра (серийного актива)."""

    nomenclature_id = fields.Int(
        required=True,
        error_messages={"required": "Номенклатура обязательна"},
    )
    inventory_number = fields.Str(
        load_default=None,
        allow_none=True,
        validate=validate.Length(max=100),
    )
    serial_number = fields.Str(
        load_default='',
        validate=validate.Length(max=100),
    )
    status = fields.Str(
        load_default='in_stock',
        validate=validate.OneOf(
            ['in_stock', 'in_use', 'repair', 'written_off'],
            error="Статус: in_stock, in_use, repair или written_off",
        ),
    )
    warehouse_id = fields.Int(load_default=None, allow_none=True)
    location_id = fields.Int(load_default=None, allow_none=True)
    employee_id = fields.Int(load_default=None, allow_none=True)
    purchase_date = fields.Date(load_default=None, allow_none=True)
    warranty_expiry = fields.Date(load_default=None, allow_none=True)
    notes = fields.Str(load_default='', validate=validate.Length(max=2000))
