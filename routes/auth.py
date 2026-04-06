"""
Blueprint: авторизация и профиль пользователя.

Маршруты:
  /                         index
  /login                    login
  /logout                   logout
  /profile                  profile
  /api/profile              api_update_profile
  /api/profile/password     api_change_password
"""
import logging
import time as _time
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, flash
from extensions import limiter, csrf
from routes.common import login_required, get_db
from constants import Security

logger = logging.getLogger('routes.auth')

auth_bp = Blueprint('auth', __name__)

# ─── Account lockout (хранится в БД, не сбрасывается при рестарте) ──────────
_LOCKOUT_ATTEMPTS = Security.LOCKOUT_ATTEMPTS
_LOCKOUT_WINDOW = Security.LOCKOUT_WINDOW
_LOCKOUT_DURATION = Security.LOCKOUT_DURATION


def _check_lockout(ip: str) -> bool:
    """Returns True if IP is locked out (>= 5 attempts in last 10 min)."""
    now = _time.time()
    cutoff = now - _LOCKOUT_WINDOW
    try:
        db = get_db()
        row = db.execute_query(
            "SELECT COUNT(*) as cnt FROM login_attempts WHERE ip = ? AND attempted_at > ?",
            (ip, cutoff), fetch_all=False
        )
        return (row['cnt'] if row else 0) >= _LOCKOUT_ATTEMPTS
    except Exception as e:
        logger.warning(f"Ошибка проверки блокировки: {e}")
        return False


def _record_failed(ip: str):
    now = _time.time()
    cutoff = now - _LOCKOUT_WINDOW
    try:
        db = get_db()
        db.execute_query("DELETE FROM login_attempts WHERE ip = ? AND attempted_at <= ?", (ip, cutoff))
        db.execute_query("INSERT INTO login_attempts (ip, attempted_at) VALUES (?, ?)", (ip, now))
    except Exception as e:
        logger.warning(f"Ошибка записи неудачной попытки: {e}")


def _clear_attempts(ip: str):
    try:
        db = get_db()
        db.execute_query("DELETE FROM login_attempts WHERE ip = ?", (ip,))
    except Exception as e:
        logger.warning(f"Ошибка сброса попыток: {e}")


@auth_bp.route('/', endpoint='index')
def index():
    if 'user_id' in session:
        return redirect(url_for('dashboard'))
    return render_template('login.html')


@limiter.limit("10 per minute")
@auth_bp.route('/login', methods=['GET', 'POST'], endpoint='login')
def login():
    if 'user_id' in session:
        return redirect(url_for('dashboard'))

    if request.method == 'POST':
        ip_addr = request.headers.get('X-Forwarded-For', request.remote_addr)
        username = request.form.get('username')
        password = request.form.get('password')

        if _check_lockout(ip_addr):
            flash('Слишком много попыток входа. Подождите 15 минут.', 'error')
            return render_template('login.html')

        if not username or not password:
            flash('Введите имя пользователя и пароль', 'error')
            return render_template('login.html')

        try:
            db = get_db()
            user = db.verify_user(username, password)

            if user:
                _clear_attempts(ip_addr)
                # Session fixation protection: очищаем старую сессию и генерируем новый ID
                session.clear()
                session['user_id'] = user['id']
                session['username'] = user['username']
                session['role'] = user['role']
                session['full_name'] = user.get('full_name') or user['username']

                flash('Вход выполнен успешно', 'success')
                logger.info(f"Пользователь {username} вошел с IP {ip_addr}")
                return redirect(url_for('dashboard'))
            else:
                _record_failed(ip_addr)
                flash('Неверный логин или пароль', 'error')
                logger.warning(f"Неудачная попытка входа: {username} с IP {ip_addr}")
        except Exception as e:
            logger.error(f"Ошибка при входе: {e}")
            flash('Ошибка при входе в систему', 'error')

    return render_template('login.html')


@auth_bp.route('/logout', endpoint='logout')
def logout():
    username = session.get('username')
    session.clear()
    if username:
        logger.info(f"Пользователь {username} вышел из системы")
    flash('Вы вышли из системы', 'info')
    return redirect(url_for('auth.login'))


@auth_bp.route('/profile', endpoint='profile')
@login_required
def profile():
    """Профиль пользователя"""
    try:
        logger.debug(f"Загрузка профиля пользователя {session.get('username')}")
        db = get_db()

        user = db.get_user_by_id(session['user_id'])
        login_history = db.get_user_login_history(session['user_id'], limit=20)

        user_stats = {
            'nomenclatures_created': 0,
            'instances_created': 0,
            'documents_created': 0,
            'total_nomenclatures': 0
        }

        try:
            nomen_created = db.execute_query(
                "SELECT COUNT(*) as cnt FROM nomenclatures WHERE created_by = ?",
                (session['user_id'],), fetch_all=False
            )
            user_stats['nomenclatures_created'] = nomen_created['cnt'] if nomen_created else 0
        except Exception as e:
            logger.error(f"Ошибка получения статистики номенклатур: {e}")

        try:
            instances_created = db.execute_query(
                "SELECT COUNT(*) as cnt FROM instances WHERE created_by = ?",
                (session['user_id'],), fetch_all=False
            )
            user_stats['instances_created'] = instances_created['cnt'] if instances_created else 0
        except Exception as e:
            logger.error(f"Ошибка получения статистики экземпляров: {e}")

        try:
            docs_created = db.execute_query(
                "SELECT COUNT(*) as cnt FROM documents WHERE created_by = ?",
                (session['user_id'],), fetch_all=False
            )
            user_stats['documents_created'] = docs_created['cnt'] if docs_created else 0
        except Exception as e:
            logger.error(f"Ошибка получения статистики документов: {e}")

        try:
            total_nomen = db.execute_query(
                "SELECT COUNT(*) as cnt FROM nomenclatures",
                fetch_all=False
            )
            user_stats['total_nomenclatures'] = total_nomen['cnt'] if total_nomen else 0
        except Exception as e:
            logger.error(f"Ошибка получения общего количества: {e}")

        recent_activities = db.execute_query("""
            SELECT action, entity_type, entity_id, details, created_at
            FROM user_logs
            WHERE user_id = ?
            ORDER BY created_at DESC
            LIMIT 20
        """, (session['user_id'],), fetch_all=True)

        employees = []
        if session.get('role') == 'admin':
            employees = db.execute_query(
                "SELECT id, full_name, employee_number FROM employees WHERE is_active = 1 ORDER BY last_name",
                fetch_all=True
            )

        return render_template('profile.html',
                               user=user,
                               login_history=[dict(h) for h in login_history] if login_history else [],
                               user_stats=user_stats,
                               recent_activities=[dict(a) for a in recent_activities] if recent_activities else [],
                               employees=[dict(e) for e in employees] if employees else [])
    except Exception as e:
        logger.error(f'Ошибка загрузки профиля: {e}')
        flash('Ошибка загрузки профиля', 'error')
        return redirect(url_for('dashboard'))


@auth_bp.route('/api/profile', methods=['PUT'], endpoint='api_update_profile')
@login_required
@limiter.limit("20 per minute")
def api_update_profile():
    """Обновление профиля"""
    try:
        db = get_db()
        data = request.json

        if 'first_name' in data or 'last_name' in data:
            user = db.get_user_by_id(session['user_id'])

            if user and user.get('employee_id'):
                first_name = data.get('first_name')
                last_name = data.get('last_name')

                if first_name is not None and last_name is not None:
                    db.execute_query(
                        "UPDATE employees SET first_name = ?, last_name = ? WHERE id = ?",
                        (first_name, last_name, user['employee_id'])
                    )
                elif first_name is not None:
                    db.execute_query(
                        "UPDATE employees SET first_name = ? WHERE id = ?",
                        (first_name, user['employee_id'])
                    )
                elif last_name is not None:
                    db.execute_query(
                        "UPDATE employees SET last_name = ? WHERE id = ?",
                        (last_name, user['employee_id'])
                    )

                updated_employee = db.execute_query(
                    "SELECT first_name, last_name FROM employees WHERE id = ?",
                    (user['employee_id'],),
                    fetch_all=False
                )
                if updated_employee:
                    session['full_name'] = f"{updated_employee['last_name']} {updated_employee['first_name']}".strip()

        if 'email' in data:
            email_val = data['email']
            db.execute_query(
                "UPDATE users SET email = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (email_val, session['user_id'])
            )

        return jsonify({'success': True, 'message': 'Профиль обновлен'})

    except Exception as e:
        logger.error(f'Ошибка обновления профиля: {e}')
        return jsonify({'error': str(e)}), 500


@auth_bp.route('/api/profile/password', methods=['POST'], endpoint='api_change_password')
@login_required
@limiter.limit("5 per minute")
def api_change_password():
    """Смена пароля"""
    try:
        import bcrypt
        db = get_db()
        data = request.json

        current_password = data.get('current_password')
        new_password = data.get('new_password')

        if not current_password or not new_password:
            return jsonify({'error': 'Необходимо указать текущий и новый пароль'}), 400

        if len(new_password) < Security.PASSWORD_MIN_LENGTH:
            return jsonify({'error': f'Пароль должен быть не менее {Security.PASSWORD_MIN_LENGTH} символов'}), 400

        if Security.PASSWORD_REQUIRE_UPPERCASE and not any(c.isupper() for c in new_password):
            return jsonify({'error': 'Пароль должен содержать хотя бы одну заглавную букву'}), 400

        if Security.PASSWORD_REQUIRE_DIGIT and not any(c.isdigit() for c in new_password):
            return jsonify({'error': 'Пароль должен содержать хотя бы одну цифру'}), 400

        user = db.get_user_by_id(session['user_id'])
        stored_hash = user.get('password_hash')

        if not bcrypt.checkpw(current_password.encode('utf-8'), stored_hash.encode('utf-8')):
            return jsonify({'error': 'Неверный текущий пароль'}), 400

        new_hash = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

        db.execute_query("""
            UPDATE users
            SET password_hash = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (new_hash, session['user_id']))

        try:
            db.execute_query("""
                INSERT INTO user_logs (user_id, action, details, created_at)
                VALUES (?, 'change_password', 'Смена пароля', CURRENT_TIMESTAMP)
            """, (session['user_id'],))
        except Exception as _e:
            logger.debug(f"Ignored: {_e}")

        # Regenerate session after password change to invalidate old sessions
        user_id = session['user_id']
        username = session['username']
        role = session['role']
        full_name = session.get('full_name', username)
        session.clear()
        session['user_id'] = user_id
        session['username'] = username
        session['role'] = role
        session['full_name'] = full_name

        return jsonify({'success': True, 'message': 'Пароль успешно изменен'})

    except Exception as e:
        logger.error(f'Ошибка смены пароля: {e}')
        return jsonify({'error': str(e)}), 500
