"""
Blueprint: employees
Маршруты для сотрудников, отделов и местоположений.
"""
import logging
from datetime import datetime
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, flash
from extensions import csrf
from routes.common import login_required, get_db
from utils.search import build_where
from utils.validators import validate_form
from schemas.employee import EmployeeSchema

logger = logging.getLogger('routes')

employees_bp = Blueprint('employees', __name__)

# ============ СОТРУДНИКИ ============

@employees_bp.route('/employees', endpoint='employees_list')
@login_required
def employees_list():
    """Список сотрудников с поиском"""
    try:
        db = get_db()
        search_query = request.args.get('search', '').strip()
        department_id = request.args.get('department_id', '')

        where = "WHERE e.is_active = 1"
        params: list = []

        if search_query:
            where += build_where(
                ['LOWER(e.last_name)', 'LOWER(e.first_name)',
                 'LOWER(e.middle_name)', 'LOWER(e.employee_number)',
                 'LOWER(e.position)'],
                search_query, params
            )

        if department_id and department_id.isdigit():
            where += " AND e.department_id = ?"
            params.append(int(department_id))

        employees = db.execute_query(f"""
            SELECT e.*, d.name as department_name
            FROM employees e
            LEFT JOIN departments d ON e.department_id = d.id
            {where}
            ORDER BY e.last_name, e.first_name
            LIMIT 500
        """, params, fetch_all=True)

        departments = db.execute_query(
            "SELECT id, name FROM departments ORDER BY name", fetch_all=True
        ) or []

        return render_template('employees/list.html',
                               employees=[dict(e) for e in employees] if employees else [],
                               departments=[dict(d) for d in departments],
                               search_query=search_query)
    except Exception as e:
        logger.error(f'Ошибка загрузки сотрудников: {e}')
        flash('Ошибка загрузки сотрудников', 'error')
        return redirect(url_for('dashboard'))

@employees_bp.route('/employees/add', methods=['GET', 'POST'], endpoint='add_employee')
@login_required
def add_employee():
    """Создание нового сотрудника"""
    db = get_db()

    if request.method == 'POST':
        try:
            # Валидация через Marshmallow
            validated, err_msg = validate_form(EmployeeSchema)
            if err_msg:
                flash(err_msg, 'error')
                departments = db.execute_query("SELECT id, name FROM departments WHERE is_active = 1", fetch_all=True) or []
                return render_template('employees/form.html', title='Новый сотрудник',
                                       employee=None, departments=[dict(d) for d in departments])

            last_name = validated['last_name']
            first_name = validated['first_name']

            # Генерация табельного номера
            employee_number = request.form.get('employee_number')
            if not employee_number:
                year = datetime.now().year
                # Получаем последний номер в этом году
                last = db.execute_query("""
                    SELECT employee_number FROM employees
                    WHERE employee_number LIKE ?
                    ORDER BY employee_number DESC LIMIT 1
                """, (f'EMP-{year}-%',), fetch_all=False)

                if last and last['employee_number']:
                    last_num = int(last['employee_number'].split('-')[-1])
                    new_num = last_num + 1
                else:
                    new_num = 1

                employee_number = f'EMP-{year}-{new_num:04d}'

            # Убираем manager_id из вставки
            db.execute_query("""
                INSERT INTO employees (
                    employee_number, last_name, first_name, middle_name,
                    department_id, position,
                    phone, email, birth_date,
                    hire_date, dismissal_date,
                    address, passport_data, inn,
                    notes, is_active, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                employee_number,
                last_name,
                first_name,
                validated.get('middle_name') or request.form.get('middle_name'),
                validated.get('department_id'),
                validated.get('position') or request.form.get('position'),
                validated.get('phone') or request.form.get('phone'),
                request.form.get('email'),
                request.form.get('birth_date'),
                request.form.get('hire_date') or datetime.now().strftime('%Y-%m-%d'),
                request.form.get('dismissal_date'),
                request.form.get('address'),
                request.form.get('passport_data'),
                request.form.get('inn'),
                request.form.get('notes'),
                1 if validated.get('is_active', True) else 0
            ))

            flash('Сотрудник успешно создан', 'success')
            return redirect(url_for('employees.employees_list'))

        except Exception as e:
            logger.error(f'Ошибка создания сотрудника: {e}')
            flash('Ошибка создания сотрудника', 'error')

    # Получаем данные для выпадающих списков
    departments = db.execute_query("SELECT id, name FROM departments WHERE is_active = 1", fetch_all=True) or []

    return render_template('employees/form.html',
                         title='Новый сотрудник',
                         employee=None,
                         departments=[dict(d) for d in departments],
                         managers=[])  # Пустой список для managers

@employees_bp.route('/employees/<int:id>/edit', methods=['GET', 'POST'], endpoint='edit_employee')
@login_required
def edit_employee(id):
    """Редактирование сотрудника"""
    db = get_db()

    if request.method == 'POST':
        try:
            # Убираем manager_id из обновления
            db.execute_query("""
                UPDATE employees
                SET employee_number = ?, last_name = ?, first_name = ?, middle_name = ?,
                    department_id = ?, position = ?,
                    phone = ?, email = ?, birth_date = ?,
                    hire_date = ?, dismissal_date = ?,
                    address = ?, passport_data = ?, inn = ?,
                    notes = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (
                request.form.get('employee_number'),
                request.form.get('last_name'),
                request.form.get('first_name'),
                request.form.get('middle_name'),
                request.form.get('department_id') or None,
                request.form.get('position'),
                request.form.get('phone'),
                request.form.get('email'),
                request.form.get('birth_date'),
                request.form.get('hire_date'),
                request.form.get('dismissal_date'),
                request.form.get('address'),
                request.form.get('passport_data'),
                request.form.get('inn'),
                request.form.get('notes'),
                1 if 'is_active' in request.form else 0,
                id
            ))

            flash('Сотрудник обновлен', 'success')
            return redirect(url_for('employees.employees_list'))

        except Exception as e:
            logger.error(f'Ошибка обновления сотрудника: {e}')
            flash('Ошибка обновления сотрудника', 'error')

    employee = db.execute_query("SELECT * FROM employees WHERE id = ?", (id,), fetch_all=False)
    if not employee:
        flash('Сотрудник не найден', 'error')
        return redirect(url_for('employees.employees_list'))

    departments = db.execute_query("SELECT id, name FROM departments WHERE is_active = 1", fetch_all=True) or []

    return render_template('employees/form.html',
                         title='Редактирование сотрудника',
                         employee=dict(employee),
                         departments=[dict(d) for d in departments],
                         managers=[])  # Пустой список для managers

@employees_bp.route('/employees/<int:id>/view', endpoint='view_employee')
@login_required
def employee_details(id):
    """Просмотр сотрудника"""
    try:
        db = get_db()

        # Убираем manager_name из запроса
        employee = db.execute_query("""
            SELECT e.*, d.name as department_name
            FROM employees e
            LEFT JOIN departments d ON e.department_id = d.id
            WHERE e.id = ?
        """, (id,), fetch_all=False)

        if not employee:
            flash('Сотрудник не найден', 'error')
            return redirect(url_for('employees.employees_list'))

        # Получаем выданное имущество
        issued_instances = db.execute_query("""
            SELECT i.*, n.name as nomenclature_name
            FROM instances i
            LEFT JOIN nomenclatures n ON i.nomenclature_id = n.id
            WHERE i.employee_id = ? AND i.status = 'in_use'
            ORDER BY i.issued_date DESC
        """, (id,), fetch_all=True) or []

        return render_template('employees/view.html',
                             employee=dict(employee),
                             issued_instances=[dict(i) for i in issued_instances])

    except Exception as e:
        logger.error(f'Ошибка просмотра сотрудника: {e}')
        flash('Ошибка просмотра сотрудника', 'error')
        return redirect(url_for('employees.employees_list'))

@employees_bp.route('/employees/<int:id>/delete', methods=['POST'], endpoint='delete_employee')
@login_required
def delete_employee(id):
    """Удаление сотрудника"""
    try:
        db = get_db()

        # Проверяем, есть ли выданное имущество
        instances = db.execute_query(
            "SELECT COUNT(*) as cnt FROM instances WHERE employee_id = ? AND status = 'in_use'",
            (id,), fetch_all=False
        )

        if instances and instances['cnt'] > 0:
            flash('Нельзя удалить сотрудника, у которого есть выданное имущество', 'error')
            return redirect(url_for('employees.employees_list'))

        # Проверяем, есть ли созданные документы
        docs = db.execute_query(
            "SELECT COUNT(*) as cnt FROM documents WHERE employee_id = ?",
            (id,), fetch_all=False
        )

        if docs and docs['cnt'] > 0:
            flash('Нельзя удалить сотрудника, по которому есть документы', 'error')
            return redirect(url_for('employees.employees_list'))

        # Мягкое удаление
        has_deleted = db.column_exists('employees', 'is_deleted')

        if has_deleted:
            db.execute_query(
                "UPDATE employees SET is_deleted = 1, deleted_at = CURRENT_TIMESTAMP WHERE id = ?",
                (id,)
            )
        else:
            db.execute_query("DELETE FROM employees WHERE id = ?", (id,))

        flash('Сотрудник удален', 'success')

    except Exception as e:
        logger.error(f'Ошибка удаления сотрудника: {e}')
        flash('Ошибка удаления сотрудника', 'error')

    return redirect(url_for('employees.employees_list'))

# API для поиска сотрудников

@employees_bp.route('/api/employees/search', endpoint='api_employees_search')
@login_required
def api_employees_search():
    """Поиск сотрудников (регистронезависимый)"""
    try:
        query = request.args.get('q', '')
        db = get_db()

        sql_params: list = []
        search_cond = build_where(
            ['LOWER(last_name)', 'LOWER(first_name)',
             'LOWER(middle_name)', 'LOWER(employee_number)', 'LOWER(position)'],
            query, sql_params
        )
        employees = db.execute_query(f"""
            SELECT id, employee_number,
                   last_name || ' ' || first_name || COALESCE(' ' || middle_name, '') as full_name,
                   position
            FROM employees
            WHERE is_active = 1
                {search_cond}
            ORDER BY last_name, first_name
            LIMIT 20
        """, sql_params, fetch_all=True)

        return jsonify([dict(e) for e in employees] if employees else [])
    except Exception as e:
        return jsonify([])

@employees_bp.route('/api/employees/<int:id>', endpoint='api_employee_details')
@login_required
def api_employee_details(id):
    """API для получения детальной информации о сотруднике"""
    try:
        db = get_db()
        # Убираем department_id из запроса, так как его нет в таблице
        employee = db.execute_query("""
            SELECT id, last_name, first_name, middle_name,
                   employee_number, position
            FROM employees
            WHERE id = ? AND is_active = 1
        """, (id,), fetch_all=False)

        if employee:
            return jsonify(dict(employee))
        return jsonify({'error': 'Сотрудник не найден'}), 404
    except Exception as e:
        logger.error(f'Ошибка получения данных сотрудника: {e}')
        return jsonify({'error': str(e)}), 500

@employees_bp.route('/api/employees/list', endpoint='api_employees_list')
@login_required
def api_employees_list():
    """API для получения списка сотрудников"""
    try:
        db = get_db()
        # Упрощенный запрос без department_id
        employees = db.execute_query("""
            SELECT id, last_name, first_name, middle_name, position, employee_number
            FROM employees
            WHERE is_active = 1
            ORDER BY last_name, first_name
        """, fetch_all=True)

        result = []
        for emp in employees:
            result.append(dict(emp))

        return jsonify(result)
    except Exception as e:
        logger.error(f'Ошибка получения сотрудников: {e}')
        return jsonify([])

# API для поиска отдела

@employees_bp.route('/api/departments/<int:id>', endpoint='api_department_details')
@login_required
def api_department_details(id):
    """API для получения детальной информации о подразделении"""
    try:
        db = get_db()
        department = db.execute_query("""
            SELECT d.*, e.full_name as manager_name, e.id as manager_id
            FROM departments d
            LEFT JOIN employees e ON d.manager_id = e.id
            WHERE d.id = ? AND d.is_active = 1
        """, (id,), fetch_all=False)

        if department:
            return jsonify(dict(department))
        return jsonify({'error': 'Подразделение не найдено'}), 404
    except Exception as e:
        logger.error(f'Ошибка получения данных подразделения: {e}')
        return jsonify({'error': str(e)}), 500

@employees_bp.route('/api/departments/list', endpoint='api_departments_list')
@login_required
def api_departments_list():
    """API для получения списка подразделений"""
    try:
        db = get_db()
        departments = db.execute_query("""
            SELECT d.*, e.full_name as manager_name
            FROM departments d
            LEFT JOIN employees e ON d.manager_id = e.id
            WHERE d.is_active = 1
            ORDER BY d.name
        """, fetch_all=True)

        result = []
        for dept in departments:
            result.append(dict(dept))

        return jsonify(result)
    except Exception as e:
        logger.error(f'Ошибка получения подразделений: {e}')
        return jsonify([])

# ============ ОТДЕЛЫ ============

@employees_bp.route('/departments', endpoint='departments_list')
@login_required
def departments_list():
    """Список отделов"""
    try:
        db = get_db()
        departments = db.execute_query("""
            SELECT d.*,
                   e.last_name || ' ' || e.first_name || COALESCE(' ' || e.middle_name, '') as manager_name,
                   (SELECT COUNT(*) FROM employees WHERE department_id = d.id AND is_active = 1) as employees_count
            FROM departments d
            LEFT JOIN employees e ON d.manager_id = e.id
            WHERE d.is_active = 1
            ORDER BY d.name
        """, fetch_all=True)

        return render_template('departments/list.html',
                             departments=[dict(d) for d in departments] if departments else [])
    except Exception as e:
        logger.error(f'Ошибка загрузки отделов: {e}')
        flash('Ошибка загрузки отделов', 'error')
        return redirect(url_for('dashboard'))

@employees_bp.route('/departments/<int:id>', endpoint='department_detail')
@login_required
def department_view(id):
    """Просмотр отдела"""
    try:
        db = get_db()

        department = db.execute_query("""
            SELECT d.*,
                   e.last_name || ' ' || e.first_name || COALESCE(' ' || e.middle_name, '') as manager_name
            FROM departments d
            LEFT JOIN employees e ON d.manager_id = e.id
            WHERE d.id = ? AND d.is_active = 1
        """, (id,), fetch_all=False)

        if not department:
            flash('Отдел не найден', 'error')
            return redirect(url_for('employees.departments_list'))

        # Сотрудники отдела
        employees = db.execute_query("""
            SELECT e.*,
                   (SELECT COUNT(*) FROM instances WHERE employee_id = e.id AND status = 'in_use') as issued_count
            FROM employees e
            WHERE e.department_id = ? AND e.is_active = 1
            ORDER BY e.last_name, e.first_name
        """, (id,), fetch_all=True)

        return render_template('departments/view.html',
                             department=dict(department),
                             employees=[dict(e) for e in employees] if employees else [])
    except Exception as e:
        logger.error(f'Ошибка просмотра отдела: {e}')
        flash('Ошибка просмотра отдела', 'error')
        return redirect(url_for('employees.departments_list'))

@employees_bp.route('/departments/add', methods=['GET', 'POST'], endpoint='add_department')
@login_required
def department_add():
    """Добавление отдела (только для админа)"""
    if session.get('role') != 'admin':
        flash('Доступ запрещен', 'error')
        return redirect(url_for('employees.departments_list'))

    db = get_db()

    if request.method == 'POST':
        try:
            code = request.form.get('code')
            name = request.form.get('name')
            parent_id = request.form.get('parent_id')
            manager_id = request.form.get('manager_id')
            is_active = 'is_active' in request.form

            if not code or not name:
                flash('Код и наименование обязательны', 'error')
                return redirect(url_for('employees.add_department'))

            db.execute_query("""
                INSERT INTO departments (code, name, parent_id, manager_id, is_active, created_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (code, name, parent_id or None, manager_id or None, 1 if is_active else 0))

            flash('Отдел успешно создан', 'success')
            return redirect(url_for('employees.departments_list'))

        except Exception as e:
            logger.error(f'Ошибка создания отдела: {e}')
            flash('Ошибка создания отдела', 'error')

    # Для выпадающих списков
    parent_departments = db.execute_query("SELECT id, name FROM departments WHERE is_active = 1 ORDER BY name", fetch_all=True) or []
    managers = db.execute_query("""
        SELECT e.id, e.last_name || ' ' || e.first_name || COALESCE(' ' || e.middle_name, '') as full_name
        FROM employees e
        WHERE e.is_active = 1
        ORDER BY e.last_name
    """, fetch_all=True) or []

    return render_template('departments/form.html',
                         title='Новый отдел',
                         department=None,
                         parent_departments=[dict(d) for d in parent_departments],
                         managers=[dict(m) for m in managers])

@employees_bp.route('/departments/<int:id>/edit', methods=['GET', 'POST'], endpoint='edit_department')
@login_required
def department_edit(id):
    """Редактирование отдела (только для админа)"""
    if session.get('role') != 'admin':
        flash('Доступ запрещен', 'error')
        return redirect(url_for('employees.departments_list'))

    db = get_db()

    if request.method == 'POST':
        try:
            code = request.form.get('code')
            name = request.form.get('name')
            parent_id = request.form.get('parent_id')
            manager_id = request.form.get('manager_id')
            is_active = 'is_active' in request.form

            if not code or not name:
                flash('Код и наименование обязательны', 'error')
                return redirect(url_for('employees.edit_department', id=id))

            # Нельзя сделать родителем самого себя
            if parent_id and int(parent_id) == id:
                flash('Отдел не может быть родителем самого себя', 'error')
                return redirect(url_for('employees.edit_department', id=id))

            db.execute_query("""
                UPDATE departments
                SET code = ?, name = ?, parent_id = ?, manager_id = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (code, name, parent_id or None, manager_id or None, 1 if is_active else 0, id))

            flash('Отдел обновлен', 'success')
            return redirect(url_for('employees.departments_list'))

        except Exception as e:
            logger.error(f'Ошибка обновления отдела: {e}')
            flash('Ошибка обновления отдела', 'error')

    department = db.execute_query("SELECT * FROM departments WHERE id = ?", (id,), fetch_all=False)
    if not department:
        flash('Отдел не найден', 'error')
        return redirect(url_for('employees.departments_list'))

    parent_departments = db.execute_query("SELECT id, name FROM departments WHERE is_active = 1 AND id != ? ORDER BY name", (id,), fetch_all=True) or []
    managers = db.execute_query("""
        SELECT e.id, e.last_name || ' ' || e.first_name || COALESCE(' ' || e.middle_name, '') as full_name
        FROM employees e
        WHERE e.is_active = 1
        ORDER BY e.last_name
    """, fetch_all=True) or []

    return render_template('departments/form.html',
                         title='Редактирование отдела',
                         department=dict(department),
                         parent_departments=[dict(d) for d in parent_departments],
                         managers=[dict(m) for m in managers])

@employees_bp.route('/departments/<int:id>/delete', methods=['POST'], endpoint='delete_department')
@login_required
def department_delete(id):
    """Удаление отдела (только для админа)"""
    if session.get('role') != 'admin':
        flash('Доступ запрещен', 'error')
        return redirect(url_for('employees.departments_list'))

    try:
        db = get_db()

        # Проверяем, есть ли сотрудники
        employees = db.execute_query(
            "SELECT COUNT(*) as cnt FROM employees WHERE department_id = ?",
            (id,), fetch_all=False
        )

        if employees and employees['cnt'] > 0:
            flash('Нельзя удалить отдел, в котором есть сотрудники', 'error')
            return redirect(url_for('employees.departments_list'))

        # Проверяем, есть ли дочерние отделы
        children = db.execute_query(
            "SELECT COUNT(*) as cnt FROM departments WHERE parent_id = ?",
            (id,), fetch_all=False
        )

        if children and children['cnt'] > 0:
            flash('Нельзя удалить отдел, у которого есть подотделы', 'error')
            return redirect(url_for('employees.departments_list'))

        db.execute_query("DELETE FROM departments WHERE id = ?", (id,))
        flash('Отдел удален', 'success')

    except Exception as e:
        logger.error(f'Ошибка удаления отдела: {e}')
        flash('Ошибка удаления отдела', 'error')

    return redirect(url_for('employees.departments_list'))

# ============ МЕСТОПОЛОЖЕНИЯ ============

@employees_bp.route('/locations', endpoint='locations_list')
@login_required
def locations_list():
    """Список местоположений"""
    try:
        db = get_db()
        locations = db.execute_query("""
            SELECT l.*,
                   p.name as parent_name,
                   d.name as department_name,
                   e.last_name || ' ' || e.first_name || COALESCE(' ' || e.middle_name, '') as responsible_name
            FROM locations l
            LEFT JOIN locations p ON l.parent_id = p.id
            LEFT JOIN departments d ON l.department_id = d.id
            LEFT JOIN employees e ON l.responsible_id = e.id
            WHERE l.is_active = 1
            ORDER BY l.name
        """, fetch_all=True)

        return render_template('locations/list.html',
                             locations=[dict(l) for l in locations] if locations else [])
    except Exception as e:
        logger.error(f'Ошибка загрузки местоположений: {e}')
        flash('Ошибка загрузки местоположений', 'error')
        return redirect(url_for('dashboard'))

@employees_bp.route('/locations/<int:id>', endpoint='location_detail')
@login_required
def location_view(id):
    """Просмотр местоположения"""
    try:
        db = get_db()

        location = db.execute_query("""
            SELECT l.*,
                   p.name as parent_name,
                   d.name as department_name,
                   e.last_name || ' ' || e.first_name || COALESCE(' ' || e.middle_name, '') as responsible_name
            FROM locations l
            LEFT JOIN locations p ON l.parent_id = p.id
            LEFT JOIN departments d ON l.department_id = d.id
            LEFT JOIN employees e ON l.responsible_id = e.id
            WHERE l.id = ? AND l.is_active = 1
        """, (id,), fetch_all=False)

        if not location:
            flash('Местоположение не найдено', 'error')
            return redirect(url_for('employees.locations_list'))

        # Дочерние местоположения
        children = db.execute_query("SELECT id, name, type FROM locations WHERE parent_id = ? AND is_active = 1", (id,), fetch_all=True)

        # Склады в этом местоположении
        warehouses = db.execute_query("SELECT id, code, name, type FROM warehouses WHERE location_id = ? AND is_active = 1", (id,), fetch_all=True)

        return render_template('locations/view.html',
                             location=dict(location),
                             children=[dict(c) for c in children] if children else [],
                             warehouses=[dict(w) for w in warehouses] if warehouses else [])
    except Exception as e:
        logger.error(f'Ошибка просмотра местоположения: {e}')
        flash('Ошибка просмотра местоположения', 'error')
        return redirect(url_for('employees.locations_list'))

@employees_bp.route('/locations/add', methods=['GET', 'POST'], endpoint='add_location')
@login_required
def location_add():
    """Добавление местоположения (только для админа)"""
    if session.get('role') != 'admin':
        flash('Доступ запрещен', 'error')
        return redirect(url_for('employees.locations_list'))

    db = get_db()

    if request.method == 'POST':
        try:
            code = request.form.get('code')
            name = request.form.get('name')
            type = request.form.get('type', 'office')
            parent_id = request.form.get('parent_id')
            department_id = request.form.get('department_id')
            responsible_id = request.form.get('responsible_id')
            building = request.form.get('building')
            floor = request.form.get('floor')
            room = request.form.get('room')
            is_active = 'is_active' in request.form

            if not code or not name:
                flash('Код и наименование обязательны', 'error')
                return redirect(url_for('employees.add_location'))

            # Убираем created_at, так как оно DEFAULT CURRENT_TIMESTAMP
            db.execute_query("""
                INSERT INTO locations (code, name, type, parent_id, department_id, responsible_id, building, floor, room, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (code, name, type, parent_id or None, department_id or None, responsible_id or None, building, floor, room, 1 if is_active else 0))

            flash('Местоположение успешно создано', 'success')
            return redirect(url_for('employees.locations_list'))

        except Exception as e:
            logger.error(f'Ошибка создания местоположения: {e}')
            flash('Ошибка создания местоположения', 'error')

    # Для выпадающих списков
    parent_locations = db.execute_query("SELECT id, name FROM locations WHERE is_active = 1 ORDER BY name", fetch_all=True) or []
    departments = db.execute_query("SELECT id, name FROM departments WHERE is_active = 1 ORDER BY name", fetch_all=True) or []
    employees = db.execute_query("""
        SELECT e.id, e.last_name || ' ' || e.first_name || COALESCE(' ' || e.middle_name, '') as full_name
        FROM employees e
        WHERE e.is_active = 1
        ORDER BY e.last_name
    """, fetch_all=True) or []

    return render_template('locations/form.html',
                         title='Новое местоположение',
                         location=None,
                         parent_locations=[dict(l) for l in parent_locations],
                         departments=[dict(d) for d in departments],
                         employees=[dict(e) for e in employees])

@employees_bp.route('/locations/<int:id>/edit', methods=['GET', 'POST'], endpoint='edit_location')
@login_required
def location_edit(id):
    """Редактирование местоположения (только для админа)"""
    if session.get('role') != 'admin':
        flash('Доступ запрещен', 'error')
        return redirect(url_for('employees.locations_list'))

    db = get_db()

    if request.method == 'POST':
        try:
            code = request.form.get('code')
            name = request.form.get('name')
            type = request.form.get('type')
            parent_id = request.form.get('parent_id')
            department_id = request.form.get('department_id')
            responsible_id = request.form.get('responsible_id')
            building = request.form.get('building')
            floor = request.form.get('floor')
            room = request.form.get('room')
            is_active = 'is_active' in request.form

            if not code or not name:
                flash('Код и наименование обязательны', 'error')
                return redirect(url_for('employees.edit_location', id=id))

            # Нельзя сделать родителем самого себя
            if parent_id and int(parent_id) == id:
                flash('Местоположение не может быть родителем самого себя', 'error')
                return redirect(url_for('employees.edit_location', id=id))

            # Убираем updated_at из запроса, так как его нет в таблице
            db.execute_query("""
                UPDATE locations
                SET code = ?, name = ?, type = ?, parent_id = ?, department_id = ?,
                    responsible_id = ?, building = ?, floor = ?, room = ?, is_active = ?
                WHERE id = ?
            """, (code, name, type, parent_id or None, department_id or None,
                  responsible_id or None, building, floor, room, 1 if is_active else 0, id))

            flash('Местоположение обновлено', 'success')
            return redirect(url_for('employees.locations_list'))

        except Exception as e:
            logger.error(f'Ошибка обновления местоположения: {e}')
            flash('Ошибка обновления местоположения', 'error')

    location = db.execute_query("SELECT * FROM locations WHERE id = ?", (id,), fetch_all=False)
    if not location:
        flash('Местоположение не найдено', 'error')
        return redirect(url_for('employees.locations_list'))

    parent_locations = db.execute_query("SELECT id, name FROM locations WHERE is_active = 1 AND id != ? ORDER BY name", (id,), fetch_all=True) or []
    departments = db.execute_query("SELECT id, name FROM departments WHERE is_active = 1 ORDER BY name", fetch_all=True) or []
    employees = db.execute_query("""
        SELECT e.id, e.last_name || ' ' || e.first_name || COALESCE(' ' || e.middle_name, '') as full_name
        FROM employees e
        WHERE e.is_active = 1
        ORDER BY e.last_name
    """, fetch_all=True) or []

    return render_template('locations/form.html',
                         title='Редактирование местоположения',
                         location=dict(location),
                         parent_locations=[dict(l) for l in parent_locations],
                         departments=[dict(d) for d in departments],
                         employees=[dict(e) for e in employees])

@employees_bp.route('/locations/<int:id>/delete', methods=['POST'], endpoint='delete_location')
@login_required
def location_delete(id):
    """Удаление местоположения (только для админа)"""
    if session.get('role') != 'admin':
        flash('Доступ запрещен', 'error')
        return redirect(url_for('employees.locations_list'))

    try:
        db = get_db()

        # Проверяем, есть ли дочерние местоположения
        children = db.execute_query(
            "SELECT COUNT(*) as cnt FROM locations WHERE parent_id = ?",
            (id,), fetch_all=False
        )

        if children and children['cnt'] > 0:
            flash('Нельзя удалить местоположение, у которого есть дочерние', 'error')
            return redirect(url_for('employees.locations_list'))

        # Проверяем, есть ли склады
        warehouses = db.execute_query(
            "SELECT COUNT(*) as cnt FROM warehouses WHERE location_id = ?",
            (id,), fetch_all=False
        )

        if warehouses and warehouses['cnt'] > 0:
            flash('Нельзя удалить местоположение, в котором есть склады', 'error')
            return redirect(url_for('employees.locations_list'))

        db.execute_query("DELETE FROM locations WHERE id = ?", (id,))
        flash('Местоположение удалено', 'success')

    except Exception as e:
        logger.error(f'Ошибка удаления местоположения: {e}')
        flash('Ошибка удаления местоположения', 'error')

    return redirect(url_for('employees.locations_list'))
