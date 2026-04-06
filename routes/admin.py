"""
Blueprint: Администрирование.
Маршруты: /users, /admin/counters
"""
import logging
import time
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, flash
from extensions import csrf, limiter
from routes.common import login_required, admin_required, manager_required, get_db
from constants import Security

logger = logging.getLogger('routes.admin')

admin_bp = Blueprint('admin', __name__)

# ============ УПРАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯМИ ============

@admin_bp.route('/users', endpoint='users_list')
@login_required
def users_list():
    """Список пользователей (только для админа)"""
    if session.get('role') != 'admin':
        flash('Доступ запрещен', 'error')
        return redirect(url_for('dashboard'))

    try:
        db = get_db()
        users = db.execute_query("""
            SELECT u.*, e.full_name,
                   (SELECT COUNT(*) FROM user_login_history WHERE user_id = u.id) as login_count
            FROM users u
            LEFT JOIN employees e ON u.employee_id = e.id
            ORDER BY u.id
        """, fetch_all=True)

        return render_template('users/list.html', users=[dict(u) for u in users] if users else [])
    except Exception as e:
        logger.error(f'Ошибка загрузки пользователей: {e}')
        flash('Ошибка загрузки пользователей', 'error')
        return redirect(url_for('dashboard'))

@admin_bp.route('/users/add', methods=['GET', 'POST'], endpoint='add_user')
@limiter.limit("10 per minute")
@login_required
def add_user():
    """Создание нового пользователя (только для админа)"""
    if session.get('role') != 'admin':
        flash('Доступ запрещен', 'error')
        return redirect(url_for('dashboard'))

    db = get_db()

    if request.method == 'POST':
        try:
            username = request.form.get('username')
            password = request.form.get('password')
            confirm_password = request.form.get('confirm_password')
            role = request.form.get('role', 'user')
            email = request.form.get('email')
            employee_id = request.form.get('employee_id')
            is_active = 'is_active' in request.form

            if not username or not password:
                flash('Имя пользователя и пароль обязательны', 'error')
                return redirect(url_for('admin.add_user'))

            if password != confirm_password:
                flash('Пароли не совпадают', 'error')
                return redirect(url_for('admin.add_user'))

            if len(password) < Security.PASSWORD_MIN_LENGTH:
                flash(f'Пароль должен быть не менее {Security.PASSWORD_MIN_LENGTH} символов', 'error')
                return redirect(url_for('admin.add_user'))

            if Security.PASSWORD_REQUIRE_UPPERCASE and not any(c.isupper() for c in password):
                flash('Пароль должен содержать хотя бы одну заглавную букву', 'error')
                return redirect(url_for('admin.add_user'))

            if Security.PASSWORD_REQUIRE_DIGIT and not any(c.isdigit() for c in password):
                flash('Пароль должен содержать хотя бы одну цифру', 'error')
                return redirect(url_for('admin.add_user'))

            # Проверка уникальности
            existing = db.execute_query(
                "SELECT id FROM users WHERE username = ?",
                (username,),
                fetch_all=False
            )
            if existing:
                flash('Пользователь с таким именем уже существует', 'error')
                return redirect(url_for('admin.add_user'))

            # Хеширование пароля
            import bcrypt
            password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

            db.execute_query("""
                INSERT INTO users (username, password_hash, email, role, employee_id, is_active, created_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (username, password_hash, email, role, employee_id or None, 1 if is_active else 0))

            flash('Пользователь успешно создан', 'success')
            return redirect(url_for('admin.users_list'))

        except Exception as e:
            logger.error(f'Ошибка создания пользователя: {e}')
            flash('Ошибка создания пользователя', 'error')

    # Получаем список сотрудников для привязки
    employees = db.execute_query("""
        SELECT id, last_name || ' ' || first_name || COALESCE(' ' || middle_name, '') as full_name,
               employee_number
        FROM employees WHERE is_active = 1 ORDER BY last_name
    """, fetch_all=True) or []

    return render_template('users/form.html',
                         title='Новый пользователь',
                         user=None,
                         employees=[dict(e) for e in employees])

@admin_bp.route('/users/<int:id>/edit', methods=['GET', 'POST'], endpoint='edit_user')
@login_required
def edit_user(id):
    """Редактирование пользователя (только для админа)"""
    if session.get('role') != 'admin' and session.get('user_id') != id:
        flash('Доступ запрещен', 'error')
        return redirect(url_for('dashboard'))

    db = get_db()

    if request.method == 'POST':
        try:
            role = request.form.get('role')
            email = request.form.get('email')
            employee_id = request.form.get('employee_id')
            is_active = 'is_active' in request.form

            # Только админ может менять роль
            if session.get('role') == 'admin':
                db.execute_query("""
                    UPDATE users
                    SET email = ?, role = ?, employee_id = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (email, role, employee_id or None, 1 if is_active else 0, id))
            else:
                # Обычный пользователь может менять только email
                db.execute_query("""
                    UPDATE users
                    SET email = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (email, id))

            flash('Данные пользователя обновлены', 'success')
            return redirect(url_for('admin.users_list' if session.get('role') == 'admin' else 'auth.profile'))

        except Exception as e:
            logger.error(f'Ошибка обновления пользователя: {e}')
            flash('Ошибка обновления пользователя', 'error')

    user = db.execute_query("SELECT * FROM users WHERE id = ?", (id,), fetch_all=False)
    if not user:
        flash('Пользователь не найден', 'error')
        return redirect(url_for('admin.users_list'))

    employees = db.execute_query("""
        SELECT id, last_name || ' ' || first_name || COALESCE(' ' || middle_name, '') as full_name,
               employee_number
        FROM employees WHERE is_active = 1 ORDER BY last_name
    """, fetch_all=True) or []

    return render_template('users/form.html',
                         title='Редактирование пользователя',
                         user=dict(user),
                         employees=[dict(e) for e in employees])

@admin_bp.route('/users/<int:id>/toggle-status', methods=['POST'], endpoint='toggle_user_status')
@login_required
def toggle_user_status(id):
    """Блокировка/разблокировка пользователя (только для админа)"""
    if session.get('role') != 'admin':
        return jsonify({'success': False, 'error': 'Доступ запрещен'})

    try:
        db = get_db()

        # Нельзя заблокировать самого себя
        if id == session['user_id']:
            return jsonify({'success': False, 'error': 'Нельзя заблокировать самого себя'})

        user = db.execute_query("SELECT is_active FROM users WHERE id = ?", (id,), fetch_all=False)
        if not user:
            return jsonify({'success': False, 'error': 'Пользователь не найден'})

        new_status = 0 if user['is_active'] else 1
        action = 'заблокирован' if new_status == 0 else 'разблокирован'

        db.execute_query("UPDATE users SET is_active = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (new_status, id))

        return jsonify({'success': True, 'message': f'Пользователь {action}'})

    except Exception as e:
        logger.error(f'Ошибка изменения статуса пользователя: {e}')
        return jsonify({'success': False, 'error': str(e)})

@admin_bp.route('/users/<int:id>/delete', methods=['POST'], endpoint='delete_user')
@login_required
def delete_user(id):
    """Удаление пользователя (только для админа)"""
    if session.get('role') != 'admin':
        flash('Доступ запрещен', 'error')
        return redirect(url_for('dashboard'))

    try:
        db = get_db()

        # Нельзя удалить самого себя
        if id == session['user_id']:
            flash('Нельзя удалить самого себя', 'error')
            return redirect(url_for('admin.users_list'))

        # Проверяем, есть ли связанные документы
        docs = db.execute_query(
            "SELECT COUNT(*) as cnt FROM documents WHERE created_by = ?",
            (id,), fetch_all=False
        )

        if docs and docs['cnt'] > 0:
            flash('Нельзя удалить пользователя, который создавал документы', 'error')
            return redirect(url_for('admin.users_list'))

        db.execute_query("DELETE FROM users WHERE id = ?", (id,))
        flash('Пользователь удален', 'success')

    except Exception as e:
        logger.error(f'Ошибка удаления пользователя: {e}')
        flash('Ошибка удаления пользователя', 'error')

    return redirect(url_for('admin.users_list'))

@admin_bp.route('/users/permissions', endpoint='user_permissions')
@login_required
def user_permissions():
    """Просмотр прав доступа (только для админа)"""
    if session.get('role') != 'admin':
        flash('Доступ запрещен', 'error')
        return redirect(url_for('dashboard'))

    return render_template('users/permissions.html')

@admin_bp.route('/users/<int:id>/reset-password', methods=['POST'], endpoint='reset_user_password')
@login_required
@limiter.limit("5 per minute")
def reset_user_password(id):
    """Сброс пароля пользователя (только для админа)"""
    if session.get('role') != 'admin':
        return jsonify({'success': False, 'error': 'Доступ запрещен'})

    try:
        db = get_db()

        # Генерируем криптографически безопасный временный пароль
        import secrets
        import string
        alphabet = string.ascii_letters + string.digits
        temp_password = ''.join(secrets.choice(alphabet) for _ in range(12))

        import bcrypt
        password_hash = bcrypt.hashpw(temp_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

        db.execute_query("UPDATE users SET password_hash = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (password_hash, id))

        return jsonify({
            'success': True,
            'message': f'Пароль сброшен. Временный пароль: {temp_password}'
        })

    except Exception as e:
        logger.error(f'Ошибка сброса пароля: {e}')
        return jsonify({'success': False, 'error': str(e)})


# ============ СЧЁТЧИКИ ДОКУМЕНТОВ ============

@admin_bp.route('/admin/counters', endpoint='counters_list')
@admin_required
def counters_list():
    """Управление счетчиками документов (только для админа)"""
    if session.get('role') != 'admin':
        flash('Доступ запрещен', 'error')
        return redirect(url_for('dashboard'))

    db = get_db()

    # Проверяем существование таблицы
    table_exists = db.execute_query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='document_number_counters'",
        fetch_all=False
    )

    counters = []
    if table_exists:
        counters = db.execute_query("""
            SELECT * FROM document_number_counters
            ORDER BY counter_name
        """, fetch_all=True) or []

    return render_template('admin/counters.html',
                         counters=[dict(c) for c in counters])

@admin_bp.route('/test/sequence', endpoint='test_sequence')
@login_required
def test_sequence():
    """Тест генерации номеров последовательностей"""
    if session.get('role') != 'admin':
        return "Доступ запрещен"

    try:
        db = get_db()
        result = []

        # Проверяем текущие значения в таблице sequences
        sequences = db.execute_query("SELECT * FROM sequences ORDER BY sequence_type", fetch_all=True)
        result.append("=== ТЕКУЩИЕ ЗНАЧЕНИЯ В ТАБЛИЦЕ sequences ===")
        for seq in sequences or []:
            result.append(f"ID: {seq['id']}, type: {seq['sequence_type']}, prefix: {seq['prefix']}, year: {seq['year']}, last: {seq['last_number']}")

        # Тестируем генерацию новых номеров
        result.append("\n=== ТЕСТ ГЕНЕРАЦИИ НОВЫХ НОМЕРОВ ===")

        # Тест для issuance_m11
        m11_number = _get_next_sequence_number('issuance_m11')
        result.append(f"issuance_m11 -> {m11_number}")

        # Тест для issuance_tn
        tn_number = _get_next_sequence_number('issuance_tn')
        result.append(f"issuance_tn -> {tn_number}")

        # Проверяем обновленные значения
        sequences2 = db.execute_query("SELECT * FROM sequences ORDER BY sequence_type", fetch_all=True)
        result.append("\n=== ОБНОВЛЕННЫЕ ЗНАЧЕНИЯ ===")
        for seq in sequences2 or []:
            result.append(f"ID: {seq['id']}, type: {seq['sequence_type']}, last: {seq['last_number']}")

        return "<pre>" + "\n".join(result) + "</pre>"

    except Exception as e:
        return f"Ошибка: {str(e)}"

@admin_bp.route('/admin/counters/update', methods=['POST'], endpoint='update_counter')
@admin_required
def update_counter():
    """Обновление значения счетчика"""
    if session.get('role') != 'admin':
        return jsonify({'success': False, 'error': 'Доступ запрещен'})

    try:
        db = get_db()
        data = request.json
        counter_id = data.get('counter_id')
        new_value = data.get('new_value')

        if not counter_id or not new_value:
            return jsonify({'success': False, 'error': 'Не указаны параметры'})

        db.execute_query("""
            UPDATE document_number_counters
            SET last_number = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (new_value, counter_id))

        return jsonify({'success': True, 'message': 'Счетчик обновлен'})

    except Exception as e:
        logger.error(f'Ошибка обновления счетчика: {e}')
        return jsonify({'success': False, 'error': str(e)})

@admin_bp.route('/admin/reset-issuance-counters', methods=['POST'], endpoint='admin_reset_issuance_counters')
@admin_required
def admin_reset_issuance_counters():
    if session.get('role') != 'admin':
        return jsonify({'success': False, 'error': 'Доступ запрещён'})
    try:
        db = get_db()
        db.execute_query("""
            UPDATE document_number_counters
            SET last_number = 28
            WHERE counter_name = 'issuance_individual'
        """)
        db.execute_query("""
            UPDATE document_number_counters
            SET last_number = 180
            WHERE counter_name = 'issuance_quantitative'
        """)
        return jsonify({'success': True, 'message': 'Счётчики сброшены на 28 и 180'})
    except Exception as e:
        logger.error(f'Ошибка сброса счётчиков: {e}')
        return jsonify({'success': False, 'error': str(e)})

@admin_bp.route('/admin/fix-document/<int:id>', methods=['POST'], endpoint='admin_fix_document')
@admin_required
def admin_fix_document(id):
    """Исправление документа (только для админа)"""
    if session.get('role') != 'admin':
        return jsonify({'success': False, 'error': 'Доступ запрещен'})

    try:
        db = get_db()
        data = request.json or {}

        # Обновляем поля
        updates = []
        params = []

        if 'number_type' in data:
            updates.append("number_type = ?")
            params.append(data['number_type'])

        if 'issuance_number' in data:
            updates.append("issuance_number = ?")
            params.append(data['issuance_number'])

        if updates:
            params.append(id)
            db.execute_query(f"""
                UPDATE documents
                SET {', '.join(updates)}
                WHERE id = ?
            """, params)

            return jsonify({'success': True, 'message': 'Документ исправлен'})
        else:
            return jsonify({'success': False, 'error': 'Нет данных для обновления'})

    except Exception as e:
        logger.error(f'Ошибка исправления документа: {e}')
        return jsonify({'success': False, 'error': str(e)})


def _get_next_sequence_number(sequence_type, year=None):
    """
    Получение следующего номера из таблицы sequences
    (локальная копия для use в test_sequence)
    """
    from datetime import datetime as _datetime

    db = get_db()
    max_attempts = 3

    if year is None:
        year = _datetime.now().year

    for attempt in range(max_attempts):
        try:
            db.connection.execute("BEGIN IMMEDIATE")

            cursor = db.connection.execute("""
                SELECT id, last_number, format
                FROM sequences
                WHERE sequence_type = ? AND year = ?
            """, (sequence_type, year))

            row = cursor.fetchone()

            if row:
                next_number = row[1] + 1
                db.connection.execute("""
                    UPDATE sequences
                    SET last_number = ?
                    WHERE id = ?
                """, (next_number, row[0]))
            else:
                if sequence_type == 'issuance_m11':
                    next_number = 29
                elif sequence_type == 'issuance_tn':
                    next_number = 181
                else:
                    next_number = 1

                db.connection.execute("""
                    INSERT INTO sequences (sequence_type, prefix, year, last_number, format)
                    VALUES (?, ?, ?, ?, ?)
                """, (sequence_type, '', year, next_number, '{NUMBER}'))

            db.connection.commit()
            return str(next_number)

        except Exception as e:
            db.connection.rollback()
            if attempt < max_attempts - 1:
                time.sleep(0.1)
                continue

    fallback = str(int(time.time() * 1000))[-6:]
    return fallback


# ============ РЕЗЕРВНОЕ КОПИРОВАНИЕ БД ============

@admin_bp.route('/admin/backup', methods=['GET'], endpoint='admin_backup_list')
@admin_required
def admin_backup_list():
    """Страница управления резервными копиями БД."""
    try:
        import backup_db as bk
        backups = bk.list_backups()
        return render_template('admin/backup.html', backups=backups)
    except Exception as e:
        logger.error(f'Ошибка загрузки списка бэкапов: {e}')
        flash('Ошибка загрузки списка резервных копий', 'error')
        return redirect(url_for('dashboard'))


@admin_bp.route('/admin/backup/create', methods=['POST'], endpoint='admin_backup_create')
@admin_required
def admin_backup_create():
    """Создаёт резервную копию БД по требованию администратора."""
    try:
        import backup_db as bk
        success = bk.create_backup()
        if success:
            flash('Резервная копия успешно создана', 'success')
            logger.info(f'Резервная копия создана вручную пользователем {session.get("username")}')
        else:
            flash('Ошибка создания резервной копии. Подробности в logs/backup.log', 'error')
    except Exception as e:
        logger.error(f'Ошибка создания бэкапа: {e}')
        flash(f'Ошибка: {e}', 'error')
    return redirect(url_for('admin.admin_backup_list'))
