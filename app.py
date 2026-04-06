"""
Flask Application for Assets Management System
"""
import os
import sys
import logging
import secrets
import threading
from datetime import datetime
from flask import Flask, render_template, request, jsonify, session, redirect, url_for, flash, g
from flask_wtf.csrf import CSRFError
from extensions import csrf, limiter, compress
from apscheduler.schedulers.background import BackgroundScheduler
from constants import CacheTTL
import atexit

# ============ ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ============
import_lock = threading.Lock()
import_counters = {}

_translations_cache = {}
_translations_cache_time = None
_translations_cache_lock = threading.Lock()

_categories_cache = []
_categories_cache_time = None
_categories_cache_lock = threading.Lock()
_CATEGORIES_TTL = CacheTTL.CATEGORIES

_unread_counts_cache = {}      # {user_id: count}
_unread_counts_cache_time = {} # {user_id: datetime}
_unread_counts_cache_lock = threading.Lock()
_UNREAD_TTL = CacheTTL.UNREAD_COUNT

# Список единиц измерения
UNITS = [
    {'value': 'шт.', 'label': 'штуки', 'category': 'common'},
    {'value': 'м', 'label': 'метры', 'category': 'length'},
    {'value': 'м²', 'label': 'квадратные метры', 'category': 'area'},
    {'value': 'м³', 'label': 'кубические метры', 'category': 'volume'},
    {'value': 'м.п', 'label': 'погонные метры', 'category': 'length'},
    {'value': 'л', 'label': 'литры', 'category': 'volume'},
    {'value': 'кг', 'label': 'килограммы', 'category': 'weight'},
    {'value': 'г', 'label': 'граммы', 'category': 'weight'},
    {'value': 'т', 'label': 'тонны', 'category': 'weight'},
    {'value': 'уп', 'label': 'упаковка', 'category': 'pack'},
    {'value': 'кор', 'label': 'коробка', 'category': 'pack'},
    {'value': 'компл', 'label': 'комплект', 'category': 'pack'},
    {'value': 'пара', 'label': 'пара', 'category': 'pair'},
]

# ============ ЛОГИРОВАНИЕ ============
from logging.handlers import RotatingFileHandler

_LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(_LOG_DIR, exist_ok=True)

_LOG_FORMAT = logging.Formatter(
    '%(asctime)s [%(levelname)s] %(name)s:%(lineno)d — %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
_file_handler = RotatingFileHandler(
    os.path.join(_LOG_DIR, 'app.log'),
    maxBytes=10 * 1024 * 1024,
    backupCount=5,
    encoding='utf-8'
)
_file_handler.setLevel(logging.INFO)
_file_handler.setFormatter(_LOG_FORMAT)

_error_handler = RotatingFileHandler(
    os.path.join(_LOG_DIR, 'errors.log'),
    maxBytes=5 * 1024 * 1024,
    backupCount=3,
    encoding='utf-8'
)
_error_handler.setLevel(logging.ERROR)
_error_handler.setFormatter(_LOG_FORMAT)

_console_handler = logging.StreamHandler()
_console_handler.setLevel(logging.WARNING)
_console_handler.setFormatter(_LOG_FORMAT)

logging.basicConfig(
    level=logging.INFO,
    handlers=[_file_handler, _error_handler, _console_handler]
)
logger = logging.getLogger(__name__)

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from config import Config
    from database import get_db
    logger.info("Модули импортированы")
except Exception as e:
    logger.error(f"Ошибка импорта: {e}")
    sys.exit(1)

# ============ ИНИЦИАЛИЗАЦИЯ FLASK ============
app = Flask(__name__)
app.config.from_object(Config)

_secret_key = app.config.get('SECRET_KEY', 'dev-secret-key')
if _secret_key == 'dev-secret-key':
    if not app.config.get('DEBUG', False):
        raise RuntimeError(
            "SECRET_KEY не задан в .env! "
            "Сгенерируйте ключ: python -c \"import secrets; print(secrets.token_hex(32))\" "
            "и добавьте в .env как SECRET_KEY=..."
        )
    else:
        logger.warning("ВНИМАНИЕ: используется дефолтный SECRET_KEY. Задайте SECRET_KEY в .env для production!")
app.secret_key = _secret_key
csrf.init_app(app)
limiter.init_app(app)
compress.init_app(app)

app.config['UPLOAD_FOLDER'] = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static/uploads')
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB
app.config['ALLOWED_EXTENSIONS'] = {'png', 'jpg', 'jpeg', 'gif', 'webp'}

DEFAULT_NO_IMAGE = os.path.join('static', 'img', 'no-image.png')
DEFAULT_NO_IMAGE_ABS = os.path.join(app.root_path, 'static', 'img', 'no-image.png')

if not os.path.exists(DEFAULT_NO_IMAGE_ABS):
    os.makedirs(os.path.dirname(DEFAULT_NO_IMAGE_ABS), exist_ok=True)
    default_svg = '''<svg xmlns="http://www.w3.org/2000/svg" width="40" height="40" viewBox="0 0 40 40">
        <rect width="40" height="40" fill="#f0f0f0"/>
        <text x="8" y="25" font-family="Arial" font-size="14" fill="#999">N/A</text>
    </svg>'''
    with open(DEFAULT_NO_IMAGE_ABS, 'w', encoding='utf-8') as f:
        f.write(default_svg)

@app.before_request
def generate_csp_nonce():
    """Генерирует уникальный nonce для CSP на каждый запрос."""
    g.csp_nonce = secrets.token_urlsafe(16)


@app.after_request
def set_security_headers(response):
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    # X-XSS-Protection устарел в современных браузерах, но не вреден в legacy
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    response.headers['Permissions-Policy'] = 'geolocation=(), microphone=(), camera=()'

    # Используем nonce из g (генерируется в before_request).
    # Браузеры с поддержкой nonce игнорируют 'unsafe-inline' при наличии nonce —
    # таким образом, добавление nonce уже является улучшением безопасности.
    # Для полного эффекта шаблоны должны добавить nonce="{{ csp_nonce }}"
    # на все теги <script>/<style>.
    nonce = getattr(g, 'csp_nonce', '')
    nonce_directive = f"'nonce-{nonce}'" if nonce else ''
    response.headers['Content-Security-Policy'] = (
        "default-src 'self'; "
        f"script-src 'self' {nonce_directive} 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data: blob:; "
        "font-src 'self'; "
        "connect-src 'self'; "
        "object-src 'none'; "
        "base-uri 'self'; "
        "frame-ancestors 'self'; "
        "upgrade-insecure-requests;"
    )
    if not app.debug:
        response.headers['Strict-Transport-Security'] = 'max-age=63072000; includeSubDomains; preload'
    # Убираем Server header для скрытия стека
    response.headers.pop('Server', None)
    return response


from routes.common import login_required, admin_required

# ============ КОНТЕКСТ-ПРОЦЕССОР ============

def _get_categories_cached():
    """Возвращает список категорий с кэшированием (TTL 10 минут). Thread-safe."""
    global _categories_cache, _categories_cache_time
    now = datetime.now()
    with _categories_cache_lock:
        if _categories_cache_time and (now - _categories_cache_time).total_seconds() < _CATEGORIES_TTL:
            return _categories_cache
        try:
            db = get_db()
            cats = db.execute_query(
                "SELECT id, name_ru as name FROM categories WHERE is_active = 1 ORDER BY name_ru LIMIT 10",
                fetch_all=True
            )
            _categories_cache = [dict(c) for c in cats] if cats else []
            _categories_cache_time = now
        except Exception as e:
            logger.error(f"Ошибка загрузки категорий: {e}")
        return _categories_cache


def invalidate_categories_cache():
    """Сбрасывает кэш категорий (вызывать при изменении категорий)."""
    global _categories_cache_time
    with _categories_cache_lock:
        _categories_cache_time = None


@app.context_processor
def inject_user():
    categories = []
    try:
        categories = _get_categories_cached()
    except Exception as e:
        logger.error(f"Ошибка получения категорий: {e}")

    unread_count = 0
    try:
        if 'user_id' in session:
            uid = session['user_id']
            now = datetime.now()
            with _unread_counts_cache_lock:
                cached_time = _unread_counts_cache_time.get(uid)
                if cached_time and (now - cached_time).total_seconds() < _UNREAD_TTL:
                    unread_count = _unread_counts_cache.get(uid, 0)
                else:
                    db = get_db()
                    result = db.execute_query(
                        "SELECT COUNT(*) as cnt FROM notifications WHERE (user_id = ? OR user_id IS NULL) AND is_read = 0",
                        (uid,),
                        fetch_all=False
                    )
                    unread_count = result['cnt'] if result else 0
                    _unread_counts_cache[uid] = unread_count
                    _unread_counts_cache_time[uid] = now
    except Exception as e:
        logger.error(f"Ошибка получения счетчика уведомлений: {e}")

    global _translations_cache, _translations_cache_time

    def get_translation(table, field, default=None, **kwargs):
        global _translations_cache, _translations_cache_time
        now = datetime.now()
        with _translations_cache_lock:
            if not _translations_cache_time or (now - _translations_cache_time).total_seconds() > CacheTTL.TRANSLATIONS:
                _translations_cache = {}
                try:
                    db = get_db()
                    trans = db.execute_query(
                        "SELECT table_name, field_name, display_name FROM field_translations",
                        fetch_all=True
                    )
                    for t in trans or []:
                        key = f"{t['table_name']}.{t['field_name']}"
                        _translations_cache[key] = t['display_name']
                    _translations_cache_time = now
                except Exception as e:
                    logger.error(f"Ошибка загрузки переводов: {e}")
            snapshot = dict(_translations_cache)
        key = f"{table}.{field}"
        result = snapshot.get(key, default or field)
        if kwargs and isinstance(result, str):
            try:
                result = result.format(**kwargs)
            except Exception:
                pass
        return result

    def t(text):
        if not text:
            return ''
        common_keys = ['title', 'code', 'name', 'description', 'status', 'type', 'actions',
                       'edit', 'delete', 'view', 'save', 'cancel', 'close', 'back', 'yes', 'no',
                       'all', 'none', 'total', 'amount', 'price', 'quantity', 'date', 'time',
                       'created_at', 'updated_at', 'created_by', 'updated_by', 'is_active']
        if text in common_keys:
            return get_translation('common', text, text.capitalize())
        return text.capitalize()

    def get_table_translations(table):
        result = {}
        prefix = f"{table}."
        for key, value in _translations_cache.items():
            if key.startswith(prefix):
                result[key[len(prefix):]] = value
        return result

    def format_datetime(dt, format='%d.%m.%Y %H:%M'):
        if not dt:
            return '—'
        try:
            if isinstance(dt, str):
                dt = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
            return dt.strftime(format)
        except Exception:
            return str(dt)

    def format_date(dt, format='%d.%m.%Y'):
        if not dt:
            return '—'
        try:
            if isinstance(dt, str):
                dt = datetime.strptime(dt, '%Y-%m-%d')
            return dt.strftime(format)
        except Exception:
            return str(dt)

    def format_currency(amount):
        if amount is None:
            return '0,00 ₽'
        try:
            return f"{float(amount):,.2f} ₽".replace(',', ' ')
        except Exception:
            return str(amount)

    def format_number(number, decimals=0):
        if number is None:
            return '0'
        try:
            return f"{float(number):,.{decimals}f}".replace(',', ' ')
        except Exception:
            return str(number)

    def get_status_color(status):
        colors = {
            'in_stock': 'success', 'available': 'success', 'in_use': 'primary',
            'under_repair': 'warning', 'repair': 'warning', 'written_off': 'secondary',
            'expired': 'danger', 'quarantine': 'warning', 'approved': 'success',
            'rejected': 'danger', 'draft': 'secondary', 'posted': 'success', 'cancelled': 'danger'
        }
        return colors.get(status, 'primary')

    def get_status_text(status):
        texts = {
            'in_stock': 'На складе', 'available': 'Доступно', 'in_use': 'В использовании',
            'under_repair': 'В ремонте', 'repair': 'В ремонте', 'written_off': 'Списано',
            'expired': 'Просрочено', 'quarantine': 'Карантин', 'approved': 'Одобрено',
            'rejected': 'Брак', 'draft': 'Черновик', 'posted': 'Проведен', 'cancelled': 'Отменен'
        }
        return texts.get(status, status)

    def get_document_type_text(doc_type):
        types = {
            'receipt': 'Поступление', 'transfer': 'Перемещение', 'issuance': 'Выдача',
            'write_off': 'Списание', 'return': 'Возврат', 'adjustment': 'Корректировка'
        }
        return types.get(doc_type, doc_type)

    def get_document_type_color(doc_type):
        colors = {
            'receipt': 'success', 'transfer': 'info', 'issuance': 'primary',
            'write_off': 'danger', 'return': 'warning', 'adjustment': 'secondary'
        }
        return colors.get(doc_type, 'secondary')

    def get_accounting_type_text(acc_type):
        types = {'individual': 'Индивидуальный', 'batch': 'Партионный', 'quantitative': 'Количественный'}
        return types.get(acc_type, acc_type)

    def yesno(value):
        return 'Да' if value else 'Нет'

    def get_purpose_color(category):
        colors = {
            'production': 'success', 'development': 'primary', 'maintenance': 'warning',
            'own_needs': 'info', 'other': 'secondary'
        }
        return colors.get(category, 'secondary')

    return {
        'current_user': {
            'id': session.get('user_id'),
            'username': session.get('username'),
            'role': session.get('role'),
            'full_name': session.get('full_name'),
            'is_authenticated': 'user_id' in session,
            'unread_count': unread_count
        },
        'csp_nonce': getattr(g, 'csp_nonce', ''),
        'now': datetime.now,
        'today': datetime.now().date,
        'app_version': '12.0',
        'categories': categories,
        '_': get_translation,
        'get_translation': get_translation,
        'get_table_translations': get_table_translations,
        't': t,
        'get_status_color': get_status_color,
        'get_status_text': get_status_text,
        'get_document_type_text': get_document_type_text,
        'get_document_type_color': get_document_type_color,
        'get_accounting_type_text': get_accounting_type_text,
        'get_purpose_color': get_purpose_color,
        'format_datetime': format_datetime,
        'format_date': format_date,
        'format_currency': format_currency,
        'format_number': format_number,
        'yesno': yesno,
        'range': range,
        'len': len,
        'int': int,
        'float': float,
        'str': str,
        'dict': dict,
        'list': list,
    }


@app.template_filter('dt_fmt')
def dt_fmt_filter(value, fmt='%d.%m.%Y %H:%M'):
    """Форматирование даты/времени. Пример: {{ row.created_at|dt_fmt }}"""
    if not value:
        return ''
    try:
        from datetime import datetime as _dt
        if isinstance(value, str):
            for pattern in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d'):
                try:
                    value = _dt.strptime(value, pattern)
                    break
                except ValueError:
                    continue
            else:
                return str(value)[:16]
        return value.strftime(fmt)
    except Exception:
        return str(value)[:16]


@app.template_filter('to_date')
def to_date_filter(date_string):
    if not date_string:
        return None
    try:
        return datetime.strptime(str(date_string), '%Y-%m-%d').date()
    except Exception:
        return None


# ============ ОБРАБОТКА ОШИБОК ============

@app.errorhandler(404)
def not_found_error(error):
    return render_template('404.html'), 404


@app.errorhandler(500)
def internal_error(error):
    import traceback as _tb
    logger.error(f"500 ERROR: {error}\n{_tb.format_exc()}")
    try:
        db = get_db()
        db.connection.rollback()
    except Exception:
        pass
    return render_template('500.html'), 500


@app.errorhandler(CSRFError)
def csrf_error(error):
    flash('Ошибка безопасности: токен CSRF недействителен. Пожалуйста, повторите действие.', 'error')
    return redirect(request.referrer or url_for('dashboard')), 400


# ============ СЛУЖЕБНЫЕ МАРШРУТЫ ============

@app.route('/health')
def health():
    checks = {}
    overall_ok = True

    # 1. Проверка базы данных
    try:
        db = get_db()
        row = db.execute_query("SELECT COUNT(*) as cnt FROM sqlite_master WHERE type='table'", fetch_all=False)
        checks['database'] = {'status': 'ok', 'tables': row['cnt'] if row else 0}
    except Exception as e:
        checks['database'] = {'status': 'error', 'detail': str(e)}
        overall_ok = False

    # 2. Проверка планировщика
    try:
        import gc
        from apscheduler.schedulers.background import BackgroundScheduler
        _sched = next(
            (obj for obj in gc.get_objects() if isinstance(obj, BackgroundScheduler)),
            None,
        )
        if _sched is not None:
            running = _sched.running
            checks['scheduler'] = {'status': 'ok' if running else 'stopped', 'running': running}
            if not running:
                overall_ok = False
            # Добавляем heartbeat-статус задач
            try:
                from services.scheduler_tasks import get_scheduler_health
                checks['scheduler']['jobs'] = get_scheduler_health()
                overdue = [j for j, v in checks['scheduler']['jobs'].items() if v['status'] == 'overdue']
                if overdue:
                    checks['scheduler']['overdue_jobs'] = overdue
            except Exception:
                pass
        else:
            checks['scheduler'] = {'status': 'not_found'}
    except Exception as e:
        checks['scheduler'] = {'status': 'error', 'detail': str(e)}

    # 3. Проверка дискового пространства (директория приложения)
    try:
        import shutil
        total, used, free = shutil.disk_usage(app.root_path)
        checks['disk'] = {
            'status': 'ok' if free > 100 * 1024 * 1024 else 'low',
            'free_mb': round(free / 1024 / 1024, 1),
            'total_mb': round(total / 1024 / 1024, 1),
        }
        if checks['disk']['status'] == 'low':
            overall_ok = False
    except Exception as e:
        checks['disk'] = {'status': 'error', 'detail': str(e)}

    status_code = 200 if overall_ok else 503
    return jsonify({
        'status': 'ok' if overall_ok else 'degraded',
        'time': str(datetime.now()),
        'checks': checks,
    }), status_code


# ============ ОТЛАДОЧНЫЕ МАРШРУТЫ ============

@app.route('/debug-routes')
@admin_required
def debug_routes():
    routes = []
    for rule in app.url_map.iter_rules():
        routes.append({
            'endpoint': rule.endpoint,
            'methods': list(rule.methods),
            'path': str(rule)
        })
    return jsonify(sorted(routes, key=lambda x: x['endpoint']))


@app.route('/test-base')
@admin_required
def test_base():
    try:
        return render_template('base.html')
    except Exception as e:
        logger.error(f"Ошибка в base.html: {e}")
        return f"Ошибка: {str(e)}"


@app.route('/test-generate-notifications')
@admin_required
def test_generate_notifications():
    try:
        db = get_db()
        test_notifications = [
            (None, 'expiry', 'Истекает срок годности', 'Партия ABC-123 истекает через 5 дней', 'batch', 1, '2026-02-22'),
            (None, 'expired', 'Партия просрочена', 'Партия XYZ-789 просрочена на 3 дня', 'batch', 2, '2026-02-10'),
            (None, 'low_stock', 'Малый остаток', 'Винты M4 - остаток 5 шт. при мин. запасе 20', 'nomenclature', 3, None),
            (None, 'calibration', 'Требуется поверка', 'Калибратор измерительный - поверка через 10 дней', 'instance', 4, '2026-02-27'),
            (session['user_id'], 'system', 'Системное уведомление', 'Обновление базы данных выполнено успешно', 'system', None, None)
        ]
        for notif in test_notifications:
            db.execute_query("""
                INSERT INTO notifications (user_id, type, title, message, entity_type, entity_id, expiry_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, notif)
        flash('Тестовые уведомления созданы', 'success')
    except Exception as e:
        flash(f'Ошибка: {e}', 'error')
    return redirect(url_for('notifications.notifications_list'))


@app.route('/check-template')
@admin_required
def check_template():
    import re
    template_path = os.path.join(app.root_path, 'templates', 'base.html')
    with open(template_path, 'r', encoding='utf-8') as f:
        content = f.read()
    matches = re.findall(r"url_for\(['\"]([^'\"]+)['\"]", content)
    existing_endpoints = [rule.endpoint for rule in app.url_map.iter_rules()]
    problems = [ep for ep in set(matches) if ep not in existing_endpoints]
    return jsonify({
        'used_endpoints': list(set(matches)),
        'existing_endpoints': existing_endpoints,
        'missing_endpoints': problems
    })


# ============ РЕГИСТРАЦИЯ BLUEPRINT ============

from routes.auth import auth_bp
from routes.notifications import notifications_bp
from routes.suppliers import suppliers_bp
from routes.categories import categories_bp
from routes.nomenclatures import nomenclatures_bp
from routes.instances import instances_bp
from routes.kits import kits_bp
from routes.warehouses import warehouses_bp
from routes.employees import employees_bp
from routes.reports import reports_bp
from routes.documents import documents_bp
from routes.admin import admin_bp
from routes.inventory import inventory_bp
from routes.import_export import import_export_bp
from routes.logs import logs_bp
from routes.translations import translations_bp
from routes.export_routes import export_bp
from routes.dashboard import dashboard_bp
from routes.excel_import_route import excel_import_bp

app.register_blueprint(auth_bp)
app.register_blueprint(notifications_bp)
app.register_blueprint(suppliers_bp)
app.register_blueprint(categories_bp)
app.register_blueprint(nomenclatures_bp)
app.register_blueprint(instances_bp)
app.register_blueprint(kits_bp)
app.register_blueprint(warehouses_bp)
app.register_blueprint(employees_bp)
app.register_blueprint(reports_bp)
app.register_blueprint(documents_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(inventory_bp)
app.register_blueprint(import_export_bp)
app.register_blueprint(logs_bp)
app.register_blueprint(translations_bp)
app.register_blueprint(export_bp)
app.register_blueprint(dashboard_bp)
app.register_blueprint(excel_import_bp)

from extensions import register_url_compat
register_url_compat(app)

# ============ ПЛАНИРОВЩИК УВЕДОМЛЕНИЙ ============

try:
    from services.scheduler_tasks import check_all_notifications, cleanup_login_attempts, cleanup_old_notifications
    if not app.debug:
        db = get_db()
        has_table = db.execute_query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='notifications'",
            fetch_all=False
        )
        if has_table:
            scheduler = BackgroundScheduler(timezone=app.config.get('SCHEDULER_TIMEZONE', 'Europe/Moscow'))
            scheduler.add_job(
                func=check_all_notifications,
                trigger='interval',
                hours=6,
                id='check_notifications_interval',
                coalesce=True,
                misfire_grace_time=600,
            )
            scheduler.add_job(
                func=check_all_notifications,
                trigger='cron',
                hour=8,
                minute=0,
                id='check_notifications_daily',
                coalesce=True,
                misfire_grace_time=3600,
            )
            scheduler.add_job(
                func=cleanup_login_attempts,
                trigger='interval',
                hours=1,
                id='cleanup_login_attempts',
                coalesce=True,
                misfire_grace_time=300,
            )
            scheduler.add_job(
                func=cleanup_old_notifications,
                trigger='cron',
                hour=3,
                minute=0,
                id='cleanup_old_notifications',
                coalesce=True,
                misfire_grace_time=3600,
            )
            scheduler.start()
            atexit.register(lambda: scheduler.shutdown())
            logger.info('Планировщик уведомлений запущен')
        else:
            logger.warning('Таблица notifications не найдена, планировщик не запущен')
    else:
        logger.info('Режим отладки: автоматические уведомления отключены')
except ImportError:
    logger.warning('APScheduler не установлен. Автоматические уведомления отключены.')
except Exception as e:
    logger.error(f'Ошибка запуска планировщика: {e}')


if __name__ == '__main__':
    try:
        app.run(debug=False, host='127.0.0.1', port=5000)
    except Exception as e:
        logger.error(f"Ошибка запуска: {e}")
        sys.exit(1)
