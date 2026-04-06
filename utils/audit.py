"""
Декоратор аудита действий пользователей.

Использование:
    from utils.audit import audit_action

    @app.route('/documents/<int:id>/post', methods=['POST'])
    @login_required
    @audit_action('post_document', entity_type='document')
    def post_document(id):
        ...
"""
import logging
import functools
from flask import session, request

logger = logging.getLogger('audit')


def audit_action(action: str, entity_type: str = '', entity_id_arg: str = None):
    """Декоратор для записи действия пользователя в журнал user_logs.

    Args:
        action:         Строка действия, например 'create_document', 'delete_nomenclature'.
        entity_type:    Тип сущности, например 'document', 'nomenclature'.
        entity_id_arg:  Имя аргумента функции, содержащего ID сущности (например 'id').
                        Если None — первый int-аргумент маршрута.
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            result = fn(*args, **kwargs)

            try:
                from routes.common import get_db
                db = get_db()

                user_id = session.get('user_id')
                if not user_id:
                    return result

                # Определяем entity_id
                entity_id = None
                if entity_id_arg and entity_id_arg in kwargs:
                    entity_id = kwargs[entity_id_arg]
                elif kwargs:
                    for v in kwargs.values():
                        if isinstance(v, int):
                            entity_id = v
                            break

                extra = request.path

                db.execute_query(
                    """
                    INSERT INTO user_logs (user_id, action, entity_type, entity_id, details, created_at)
                    VALUES (?, ?, ?, ?, ?, datetime('now'))
                    """,
                    (user_id, action, entity_type, entity_id, extra),
                )
            except Exception as e:
                logger.error(f"audit_action({action!r}) error: {e}")

            return result
        return wrapper
    return decorator
