"""
Blueprint: instances
Маршруты для управления экземплярами (индивидуальный учёт).
"""
import logging
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, flash
from extensions import csrf
from routes.common import login_required, get_db
from utils.search import build_where

logger = logging.getLogger('routes')

instances_bp = Blueprint('instances', __name__)

# ============ ЭКЗЕМПЛЯРЫ ============

@instances_bp.route('/instances', endpoint='instances_list')
@login_required
def instances_list():
    """Список экземпляров с фильтрацией"""
    try:
        db = get_db()

        # Параметры пагинации
        page = request.args.get('page', 1, type=int)
        per_page = 50
        if page < 1:
            page = 1

        # Получаем параметры фильтрации из запроса
        global_search = request.args.get('global_search', '')  # объединенный поиск
        barcode_search = request.args.get('barcode_search', '')  # поиск по штрихкоду
        status_filter = request.args.get('status_filter', '')
        condition_filter = request.args.get('condition_filter', '')
        location_filter = request.args.get('location_filter', '')
        employee_filter = request.args.get('employee_filter', '')

        # Базовая WHERE-часть (общая для COUNT и SELECT)
        where = " WHERE 1=1"
        params = []

        # ОБЪЕДИНЕННЫЙ ПОИСК (инв. номер + название) с умным поиском
        if global_search:
            where += build_where(
                ['LOWER(i.inventory_number)', 'LOWER(i.old_inventory_number)', 'LOWER(n.name)'],
                global_search, params
            )

        # ПОИСК ПО ШТРИХКОДУ (точное совпадение)
        if barcode_search:
            where += """ AND i.barcode = ?"""
            params.append(barcode_search)

        # Фильтр по статусу
        if status_filter:
            where += """ AND i.status = ?"""
            params.append(status_filter)

        # Фильтр по состоянию
        if condition_filter:
            where += """ AND i.condition = ?"""
            params.append(condition_filter)

        # Фильтр по локации
        if location_filter and location_filter.isdigit():
            where += """ AND i.location_id = ?"""
            params.append(location_filter)

        # Фильтр по сотруднику
        if employee_filter and employee_filter.isdigit():
            where += """ AND i.employee_id = ?"""
            params.append(employee_filter)

        # JOIN-часть
        joins = """
            FROM instances i
            LEFT JOIN nomenclatures n ON i.nomenclature_id = n.id
            LEFT JOIN locations l ON i.location_id = l.id
            LEFT JOIN warehouses w ON i.warehouse_id = w.id
            LEFT JOIN employees e ON i.employee_id = e.id
        """

        # Подсчёт общего количества
        count_row = db.execute_query(
            f"SELECT COUNT(*) as total {joins}{where}",
            params=tuple(params), fetch_all=False
        )
        total = count_row['total'] if count_row else 0
        pages = max(1, (total + per_page - 1) // per_page)
        if page > pages:
            page = pages
        offset = (page - 1) * per_page

        # Основной запрос с пагинацией
        query = f"""
            SELECT i.*, n.name as nomenclature_name, n.sku,
                   l.name as location_name, w.name as warehouse_name,
                   e.full_name as employee_name
            {joins}{where}
            ORDER BY i.created_at DESC
            LIMIT ? OFFSET ?
        """
        instances = db.execute_query(
            query, params=tuple(params) + (per_page, offset), fetch_all=True
        )

        # Получаем списки для фильтров
        locations = db.execute_query("SELECT id, name FROM locations ORDER BY name", fetch_all=True)
        employees = db.execute_query("SELECT id, full_name FROM employees ORDER BY full_name", fetch_all=True)

        # Преобразуем в словари
        instances_list = [dict(i) for i in instances] if instances else []
        locations_list = [dict(l) for l in locations] if locations else []
        employees_list = [dict(e) for e in employees] if employees else []

        pagination = {
            'page': page,
            'per_page': per_page,
            'total': total,
            'pages': pages,
            'has_prev': page > 1,
            'has_next': page < pages,
            'prev_num': page - 1,
            'next_num': page + 1,
        }

        # Сводка статусов (глобальная, без текущих фильтров)
        status_summary = {'total': 0, 'in_stock': 0, 'in_use': 0, 'repair': 0, 'written_off': 0}
        try:
            rows = db.execute_query("""
                SELECT status, COUNT(*) as cnt FROM instances GROUP BY status
            """, fetch_all=True)
            for row in (rows or []):
                r = dict(row)
                s = r.get('status') or ''
                cnt = r.get('cnt', 0)
                status_summary['total'] += cnt
                if s == 'in_stock':
                    status_summary['in_stock'] = cnt
                elif s == 'in_use':
                    status_summary['in_use'] = cnt
                elif s in ('repair', 'under_repair'):
                    status_summary['repair'] += cnt
                elif s == 'written_off':
                    status_summary['written_off'] = cnt
        except Exception as e:
            logger.error(f'Ошибка сводки статусов: {e}')

        return render_template(
            'instances/list.html',
            instances=instances_list,
            locations=locations_list,
            employees=employees_list,
            pagination=pagination,
            status_summary=status_summary
        )

    except Exception as e:
        logger.error(f'Ошибка загрузки экземпляров: {e}')
        flash('Ошибка загрузки экземпляров', 'error')
        return redirect(url_for('dashboard'))

@instances_bp.route('/instances/add', methods=['GET', 'POST'], endpoint='add_instance')
@login_required
def add_instance():
    """Создание нового экземпляра с проверкой на дубликаты"""
    db = get_db()

    if request.method == 'POST':
        try:
            nomenclature_id = request.form.get('nomenclature_id')
            if not nomenclature_id:
                flash('Не выбрана номенклатура', 'error')
                return redirect(url_for('instances.add_instance'))

            # Проверяем, не нажал ли пользователь "Обновить"
            if 'update_existing' in request.form:
                existing_id = request.form.get('existing_id')
                if existing_id:
                    return redirect(url_for('instances.edit_instance', id=existing_id))

            data = {
                'inventory_number': request.form.get('inventory_number'),
                'old_inventory_number': request.form.get('old_inventory_number'),
                'serial_number': request.form.get('serial_number'),
                'barcode': request.form.get('barcode'),
                'status': request.form.get('status', 'in_stock'),
                'condition': request.form.get('condition', 'good'),
                'location_id': request.form.get('location_id'),
                'warehouse_id': request.form.get('warehouse_id'),
                'employee_id': request.form.get('employee_id'),
                'supplier_id': request.form.get('supplier_id'),
                'purchase_date': request.form.get('purchase_date'),
                'purchase_price': request.form.get('purchase_price'),
                'warranty_until': request.form.get('warranty_until'),
                'last_calibration': request.form.get('last_calibration'),
                'calibration_interval': request.form.get('calibration_interval'),
                'last_maintenance': request.form.get('last_maintenance'),
                'maintenance_interval': request.form.get('maintenance_interval'),
                'operating_hours': request.form.get('operating_hours', 0),
                'issued_date': request.form.get('issued_date'),
                'expected_return_date': request.form.get('expected_return_date')
            }

            result = db.create_instance(nomenclature_id, data, session['user_id'])

            if result['success']:
                flash(result['message'], 'success')
                return redirect(url_for('instances.instances_list'))
            elif result.get('error') == 'duplicate':
                # Показываем модальное окно с вопросом об обновлении
                existing_id = result.get('existing_id')
                existing_instance = db.get_instance_by_id(existing_id) if existing_id else None

                return render_template('instances/duplicate.html',
                                     title='Дубликат экземпляра',
                                     inventory_number=data['inventory_number'],
                                     existing_instance=existing_instance,
                                     form_data=data)
            else:
                flash(result['message'], 'error')

        except Exception as e:
            logger.error(f'Ошибка создания экземпляра: {e}')
            flash('Ошибка создания экземпляра', 'error')

    # Получаем данные для выпадающих списков
    nomenclatures = db.search_nomenclatures(limit=1000)
    locations = db.execute_query("SELECT id, name FROM locations WHERE is_active = 1", fetch_all=True) or []
    warehouses = db.execute_query("SELECT id, name FROM warehouses WHERE is_active = 1", fetch_all=True) or []
    employees = db.execute_query("SELECT id, full_name FROM employees WHERE is_active = 1", fetch_all=True) or []
    suppliers = db.execute_query("SELECT id, name FROM suppliers WHERE is_active = 1", fetch_all=True) or []

    return render_template('instances/form.html',
                         title='Новый экземпляр',
                         instance=None,
                         nomenclatures=nomenclatures,
                         locations=[dict(l) for l in locations],
                         warehouses=[dict(w) for w in warehouses],
                         employees=[dict(e) for e in employees],
                         suppliers=[dict(s) for s in suppliers])

@instances_bp.route('/instances/<int:id>/edit', methods=['GET', 'POST'], endpoint='edit_instance')
@login_required
def edit_instance(id):
    """Редактирование экземпляра"""
    db = get_db()

    if request.method == 'POST':
        try:
            # Собираем данные из формы, преобразуем пустые строки в None
            data = {}

            # Текстовые поля
            text_fields = ['serial_number', 'barcode', 'notes', 'old_inventory_number']
            for field in text_fields:
                val = request.form.get(field)
                data[field] = val if val and val.strip() else None

            # Статусы
            data['status'] = request.form.get('status', 'in_stock')
            data['condition'] = request.form.get('condition', 'good')

            # Внешние ключи - преобразуем пустые строки в None
            fk_fields = ['location_id', 'warehouse_id', 'employee_id', 'supplier_id']
            for field in fk_fields:
                val = request.form.get(field)
                data[field] = int(val) if val and val.strip() and val != '0' else None

            # Даты
            date_fields = ['purchase_date', 'warranty_until', 'last_calibration',
                          'last_maintenance', 'issued_date', 'expected_return_date',
                          'actual_return_date']
            for field in date_fields:
                val = request.form.get(field)
                data[field] = val if val and val.strip() else None

            # Числовые поля
            data['purchase_price'] = request.form.get('purchase_price')
            data['calibration_interval'] = request.form.get('calibration_interval')
            data['maintenance_interval'] = request.form.get('maintenance_interval')
            data['operating_hours'] = request.form.get('operating_hours', 0)

            # Получаем инвентарный номер
            instance = db.get_instance_by_id(id)
            if not instance:
                flash('Экземпляр не найден', 'error')
                return redirect(url_for('instances.instances_list'))

            result = db.update_instance(instance['inventory_number'], data, session['user_id'])

            if result['success']:
                flash(result['message'], 'success')
                return redirect(url_for('instances.instances_list'))
            else:
                flash(result['message'], 'error')

        except Exception as e:
            logger.error(f'Ошибка обновления экземпляра: {e}')
            flash('Ошибка обновления экземпляра', 'error')

    # Получаем данные экземпляра
    instance = db.get_instance_by_id(id)
    if not instance:
        flash('Экземпляр не найден', 'error')
        return redirect(url_for('instances.instances_list'))

    # Получаем данные для выпадающих списков
    nomenclatures = db.search_nomenclatures(limit=1000)

    locations = db.execute_query("SELECT id, name FROM locations WHERE is_active = 1", fetch_all=True) or []
    locations_list = [dict(l) for l in locations]

    warehouses = db.execute_query("SELECT id, name FROM warehouses WHERE is_active = 1", fetch_all=True) or []
    warehouses_list = [dict(w) for w in warehouses]

    employees = db.execute_query("SELECT id, full_name FROM employees WHERE is_active = 1", fetch_all=True) or []
    employees_list = [dict(e) for e in employees]

    suppliers = db.execute_query("SELECT id, name FROM suppliers WHERE is_active = 1", fetch_all=True) or []
    suppliers_list = [dict(s) for s in suppliers]

    return render_template('instances/form.html',
                         title='Редактирование экземпляра',
                         instance=instance,
                         nomenclatures=nomenclatures,
                         locations=locations_list,
                         warehouses=warehouses_list,
                         employees=employees_list,
                         suppliers=suppliers_list)

@instances_bp.route('/instances/<int:id>/delete', methods=['POST'], endpoint='delete_instance')
@login_required
def delete_instance(id):
    """Удаление экземпляра"""
    try:
        db = get_db()

        # Проверяем, есть ли связанные документы
        docs = db.execute_query(
            "SELECT COUNT(*) as cnt FROM document_items WHERE instance_id = ?",
            (id,),
            fetch_all=False
        )

        if docs and docs['cnt'] > 0:
            flash('Нельзя удалить экземпляр, по которому есть движения', 'error')
            return redirect(url_for('instances.instances_list'))

        db.execute_query("DELETE FROM instances WHERE id = ?", (id,))
        flash('Экземпляр удален', 'success')

    except Exception as e:
        logger.error(f'Ошибка удаления экземпляра: {e}')
        flash('Ошибка удаления экземпляра', 'error')

    return redirect(url_for('instances.instances_list'))

@instances_bp.route('/instances/<inventory_number>', endpoint='view_instance')
@login_required
def view_instance(inventory_number):
    """Просмотр экземпляра по инвентарному номеру"""
    try:
        db = get_db()
        instance = db.get_instance_by_inventory(inventory_number)

        if not instance:
            flash('Экземпляр не найден', 'error')
            return redirect(url_for('instances.instances_list'))

        # Получаем состав комплекта, если это родительский экземпляр
        kit_components = db.get_kit_components(instance['id'])

        # Получаем родительский экземпляр, если это компонент
        parent_instance = None
        if instance.get('parent_instance_id'):
            parent_instance = db.get_instance_by_id(instance['parent_instance_id'])

        return render_template('instances/view.html',
                             instance=instance,
                             kit_components=kit_components,
                             parent_instance=parent_instance)
    except Exception as e:
        logger.error(f'Ошибка просмотра экземпляра: {e}')
        flash('Ошибка просмотра экземпляра', 'error')
        return redirect(url_for('instances.instances_list'))

@instances_bp.route('/instances/<int:id>', endpoint='instance_detail')
@login_required
def instance_detail(id):
    """Просмотр экземпляра по ID (редирект на просмотр по инвентарному номеру)"""
    try:
        db = get_db()
        instance = db.get_instance_by_id(id)
        if instance and instance.get('inventory_number'):
            return redirect(url_for('instances.view_instance', inventory_number=instance['inventory_number']))
        else:
            flash('Экземпляр не найден', 'error')
            return redirect(url_for('instances.instances_list'))
    except Exception as e:
        logger.error(f'Ошибка просмотра экземпляра: {e}')
        flash('Ошибка просмотра экземпляра', 'error')
        return redirect(url_for('instances.instances_list'))

@instances_bp.route('/api/instances/check', endpoint='api_check_instance')
@login_required
def api_check_instance():
    """Проверка существования экземпляра по инвентарному номеру"""
    try:
        inventory_number = request.args.get('inventory_number')
        exclude_id = request.args.get('exclude_id')

        if not inventory_number:
            return jsonify({'exists': False})

        db = get_db()

        query = "SELECT i.*, n.name as nomenclature_name, l.name as location_name, e.full_name as employee_name FROM instances i LEFT JOIN nomenclatures n ON i.nomenclature_id = n.id LEFT JOIN locations l ON i.location_id = l.id LEFT JOIN employees e ON i.employee_id = e.id WHERE i.inventory_number = ?"
        params = [inventory_number]

        # Исключаем текущий экземпляр при редактировании
        if exclude_id and exclude_id.isdigit():
            query += " AND i.id != ?"
            params.append(int(exclude_id))

        instance = db.execute_query(query, params, fetch_all=False)

        if instance:
            return jsonify({
                'exists': True,
                'instance': {
                    'id': instance['id'],
                    'inventory_number': instance['inventory_number'],
                    'nomenclature_name': instance['nomenclature_name'],
                    'serial_number': instance['serial_number'],
                    'status': instance['status'],
                    'location_name': instance['location_name'],
                    'employee_name': instance['employee_name']
                }
            })
        else:
            return jsonify({'exists': False})

    except Exception as e:
        logger.error(f'Ошибка проверки экземпляра: {e}')
        return jsonify({'error': str(e)}), 500

@instances_bp.route('/api/instances/create_component', methods=['POST'], endpoint='api_create_component')
@login_required
def api_create_component():
    """Создание компонента комплекта"""
    try:
        db = get_db()
        data = request.json

        nomenclature_id = data.get('nomenclature_id')
        parent_instance_id = data.get('parent_instance_id')
        inventory_number = data.get('inventory_number')

        if not nomenclature_id or not parent_instance_id:
            return jsonify({'success': False, 'error': 'Не указаны обязательные параметры'})

        # Получаем информацию о родительском экземпляре
        parent = db.get_instance_by_id(parent_instance_id)
        if not parent:
            return jsonify({'success': False, 'error': 'Родительский экземпляр не найден'})

        # Создаем экземпляр компонента
        result = db.create_instance(nomenclature_id, {
            'inventory_number': inventory_number,
            'status': 'in_stock',
            'location_id': parent.get('location_id')
        }, session['user_id'])

        if result['success']:
            # Обновляем parent_instance_id
            db.execute_query("""
                UPDATE instances SET parent_instance_id = ? WHERE id = ?
            """, (parent_instance_id, result['id']))

            return jsonify({'success': True, 'message': 'Компонент создан'})
        else:
            return jsonify({'success': False, 'error': result.get('message', 'Ошибка создания')})

    except Exception as e:
        logger.error(f'Ошибка создания компонента: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500
