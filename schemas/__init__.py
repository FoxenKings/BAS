"""
Marshmallow-схемы для валидации входных данных Inventory Bot.

Использование:
    from schemas import NomenclatureSchema, DocumentSchema

    schema = NomenclatureSchema()
    data, errors = schema.load(request.json)   # marshmallow 2.x
    # или
    try:
        data = schema.load(request.json)        # marshmallow 3.x
    except ValidationError as e:
        return jsonify({'errors': e.messages}), 400
"""
from schemas.nomenclature import NomenclatureSchema, InstanceSchema, QuickCreateNomenclatureSchema, VariationSchema
from schemas.document import DocumentSchema, DocumentItemSchema
from schemas.employee import EmployeeSchema

__all__ = [
    'NomenclatureSchema',
    'InstanceSchema',
    'QuickCreateNomenclatureSchema',
    'VariationSchema',
    'DocumentSchema',
    'DocumentItemSchema',
    'EmployeeSchema',
]
