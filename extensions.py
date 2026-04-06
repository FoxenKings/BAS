"""
Flask-расширения, инициализируемые без привязки к конкретному приложению.

Паттерн application factory: объекты создаются здесь, а .init_app(app)
вызывается в app.py. Благодаря этому Blueprint-модули могут импортировать
csrf без циклических зависимостей.
"""
from flask_wtf.csrf import CSRFProtect
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_compress import Compress

csrf = CSRFProtect()
limiter = Limiter(key_func=get_remote_address, default_limits=["200 per minute"])
compress = Compress()


def register_url_compat(app):
    """
    После регистрации всех Blueprint добавляет «короткие» алиасы эндпоинтов.

    Например, если Blueprint 'suppliers' содержит endpoint 'suppliers.suppliers_list',
    функция добавляет алиас 'suppliers_list', чтобы все существующие вызовы
    url_for('suppliers_list') в шаблонах и Python-коде продолжали работать.

    Вызывать один раз после app.register_blueprint() всех модулей.
    """
    rules_to_add = []
    for rule in list(app.url_map.iter_rules()):
        bp_name, _, func_name = rule.endpoint.rpartition('.')
        if bp_name and func_name not in app.view_functions:
            rules_to_add.append((rule, func_name))

    for rule, alias in rules_to_add:
        app.view_functions[alias] = app.view_functions[rule.endpoint]
        app.add_url_rule(
            rule.rule,
            endpoint=alias,
            view_func=app.view_functions[rule.endpoint],
            methods=rule.methods,
        )
