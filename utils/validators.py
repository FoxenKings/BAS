"""
utils/validators.py — вспомогательные функции для валидации через Marshmallow.

Использование:
    from utils.validators import validate_json, validate_form, api_validate

    # В API-маршруте:
    data, err = validate_json(NomenclatureSchema)
    if err:
        return err          # уже готовый jsonify-ответ 400

    # В form-маршруте:
    data, err = validate_form(EmployeeSchema)
    if err:
        flash(err, 'error')
        return render_template(...)
"""
import logging
from functools import wraps
from flask import request, jsonify
from marshmallow import ValidationError

logger = logging.getLogger('utils.validators')


def validate_json(schema_class, **schema_kwargs):
    """
    Валидирует request.json через указанную схему.
    Возвращает (data, None) при успехе или (None, response) при ошибке.
    """
    raw = request.get_json(silent=True)
    if raw is None:
        return None, (jsonify({'error': 'Требуется JSON тело запроса'}), 400)
    try:
        data = schema_class(**schema_kwargs).load(raw)
        return data, None
    except ValidationError as e:
        logger.debug(f"Validation error [{schema_class.__name__}]: {e.messages}")
        return None, (jsonify({'error': 'Ошибка валидации', 'details': e.messages}), 400)


def validate_form(schema_class, **schema_kwargs):
    """
    Валидирует request.form через указанную схему.
    Возвращает (data, error_message) при ошибке или (data, None) при успехе.
    """
    raw = request.form.to_dict(flat=True)
    # Checkbox поля (bool) — Flask отдаёт 'on'/'off', преобразуем
    for key, val in raw.items():
        if val in ('on', 'true', '1'):
            raw[key] = True
        elif val in ('off', 'false', '0'):
            raw[key] = False
    try:
        data = schema_class(**schema_kwargs).load(raw)
        return data, None
    except ValidationError as e:
        messages = e.messages
        # Собираем первую ошибку в читаемую строку
        first_error = next(
            (f"{field}: {msgs[0]}" if isinstance(msgs, list) else f"{field}: {msgs}"
             for field, msgs in messages.items()),
            'Ошибка валидации данных'
        )
        logger.debug(f"Form validation error [{schema_class.__name__}]: {messages}")
        return None, first_error


def api_validate(schema_class):
    """
    Декоратор для JSON API-маршрутов. Автоматически валидирует request.json
    и передаёт очищенные данные в функцию как именованный аргумент `validated`.

    Использование:
        @api_validate(NomenclatureSchema)
        def my_route(validated):
            name = validated['name']
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            data, err = validate_json(schema_class)
            if err:
                return err
            kwargs['validated'] = data
            return fn(*args, **kwargs)
        return wrapper
    return decorator
