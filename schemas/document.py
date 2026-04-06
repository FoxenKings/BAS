"""
Marshmallow-схемы для документов и строк документов.
"""
from marshmallow import Schema, fields, validate, validates, ValidationError

DOCUMENT_TYPES = ['receipt', 'issuance', 'transfer', 'write-off', 'return']


class DocumentItemSchema(Schema):
    """Строка документа."""

    nomenclature_id = fields.Int(
        required=True,
        error_messages={"required": "Номенклатура в строке обязательна"},
    )
    quantity = fields.Float(
        required=True,
        validate=validate.Range(min=0.001, error="Количество должно быть больше 0"),
        error_messages={"required": "Количество обязательно"},
    )
    unit_price = fields.Float(load_default=0.0, validate=validate.Range(min=0))
    instance_id = fields.Int(load_default=None, allow_none=True)
    batch_id = fields.Int(load_default=None, allow_none=True)
    notes = fields.Str(load_default='', validate=validate.Length(max=500))


class DocumentSchema(Schema):
    """Заголовок документа."""

    doc_type = fields.Str(
        required=True,
        validate=validate.OneOf(DOCUMENT_TYPES, error=f"Тип: {', '.join(DOCUMENT_TYPES)}"),
        error_messages={"required": "Тип документа обязателен"},
    )
    warehouse_id = fields.Int(
        required=True,
        error_messages={"required": "Склад обязателен"},
    )
    document_date = fields.Date(load_default=None, allow_none=True)
    document_number = fields.Str(load_default='', validate=validate.Length(max=100))
    counterparty = fields.Str(load_default='', validate=validate.Length(max=255))
    responsible_id = fields.Int(load_default=None, allow_none=True)
    expense_purpose_id = fields.Int(load_default=None, allow_none=True)
    notes = fields.Str(load_default='', validate=validate.Length(max=2000))
    items = fields.List(
        fields.Nested(DocumentItemSchema),
        load_default=[],
    )

    @validates('items')
    def validate_items_not_empty(self, value):
        if not value:
            raise ValidationError("Документ должен содержать хотя бы одну строку")
