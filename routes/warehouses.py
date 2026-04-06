"""
Blueprint: warehouses
Маршруты для складов, партий, ячеек хранения, остатков.
"""
import time
import logging
from datetime import datetime
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, flash
from extensions import csrf
from routes.common import login_required, get_db
from utils.search import normalize, generate_variants

logger = logging.getLogger('routes')

warehouses_bp = Blueprint('warehouses', __name__)

# ============ СКЛАДЫ ============

@warehouses_bp.route('/warehouses', endpoint='warehouses_list')
@login_required
def warehouses_list():
    """Список складов"""
    try:
        db = get_db()
        warehouses = db.execute_query("""
            SELECT w.*, l.name as location_name, e.full_name as manager_name,
                   (SELECT COUNT(*) FROM stocks WHERE warehouse_id = w.id) as items_count
            FROM warehouses w
            LEFT JOIN locations l ON w.location_id = l.id
            LEFT JOIN employees e ON w.manager_id = e.id
            WHERE w.is_active = 1
            ORDER BY w.name
        """, fetch_all=True)

        return render_template('warehouses/list.html', warehouses=[dict(w) for w in warehouses] if warehouses else [])
    except Exception as e:
        logger.error(f'Ошибка загрузки складов: {e}')
        flash('Ошибка загрузки складов', 'error')
        return redirect(url_for('dashboard'))

@warehouses_bp.route('/warehouses/add', methods=['GET', 'POST'], endpoint='add_warehouse')
@login_required
def add_warehouse():
    """Создание нового склада"""
    db = get_db()

    if request.method == 'POST':
        try:
            code = request.form.get('code')
            name = request.form.get('name')

            if not code or not name:
                flash('Код и наименование обязательны', 'error')
                return redirect(url_for('warehouses.add_warehouse'))

            db.execute_query("""
                INSERT INTO warehouses (code, name, location_id, manager_id, type, is_active)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                code,
                name,
                request.form.get('location_id') or None,
                request.form.get('manager_id') or None,
                request.form.get('type', 'general'),
                1 if 'is_active' in request.form else 0
            ))

            flash('Склад успешно создан', 'success')
            return redirect(url_for('warehouses.warehouses_list'))

        except Exception as e:
            logger.error(f'Ошибка создания склада: {e}')
            flash('Ошибка создания склада', 'error')

    # Получаем данные для выпадающих списков
    locations = db.execute_query("SELECT id, name FROM locations WHERE is_active = 1", fetch_all=True) or []
    employees = db.execute_query("SELECT id, full_name FROM employees WHERE is_active = 1", fetch_all=True) or []

    return render_template('warehouses/form.html',
                         title='Новый склад',
                         warehouse=None,
                         locations=[dict(l) for l in locations],
                         employees=[dict(e) for e in employees])

@warehouses_bp.route('/warehouses/<int:id>/edit', methods=['GET', 'POST'], endpoint='edit_warehouse')
@login_required
def edit_warehouse(id):
    """Редактирование склада"""
    db = get_db()

    if request.method == 'POST':
        try:
            db.execute_query("""
                UPDATE warehouses
                SET code = ?, name = ?, location_id = ?, manager_id = ?,
                    type = ?, is_active = ?
                WHERE id = ?
            """, (
                request.form.get('code'),
                request.form.get('name'),
                request.form.get('location_id') or None,
                request.form.get('manager_id') or None,
                request.form.get('type', 'general'),
                1 if 'is_active' in request.form else 0,
                id
            ))

            flash('Склад обновлен', 'success')
            return redirect(url_for('warehouses.warehouses_list'))

        except Exception as e:
            logger.error(f'Ошибка обновления склада: {e}')
            flash('Ошибка обновления склада', 'error')

    warehouse = db.execute_query("SELECT * FROM warehouses WHERE id = ?", (id,), fetch_all=False)
    if not warehouse:
        flash('Склад не найден', 'error')
        return redirect(url_for('warehouses.warehouses_list'))

    locations = db.execute_query("SELECT id, name FROM locations WHERE is_active = 1", fetch_all=True) or []
    employees = db.execute_query("SELECT id, full_name FROM employees WHERE is_active = 1", fetch_all=True) or []

    return render_template('warehouses/form.html',
                         title='Редактирование склада',
                         warehouse=dict(warehouse),
                         locations=[dict(l) for l in locations],
                         employees=[dict(e) for e in employees])

@warehouses_bp.route('/warehouses/<int:id>/delete', methods=['POST'], endpoint='delete_warehouse')
@login_required
def delete_warehouse(id):
    """Удаление склада"""
    try:
        db = get_db()

        # Проверяем, есть ли остатки
        stocks = db.execute_query(
            "SELECT COUNT(*) as cnt FROM stocks WHERE warehouse_id = ? AND quantity > 0",
            (id,), fetch_all=False
        )

        if stocks and stocks['cnt'] > 0:
            flash('Нельзя удалить склад, на котором есть остатки', 'error')
            return redirect(url_for('warehouses.warehouses_list'))

        db.execute_query("DELETE FROM warehouses WHERE id = ?", (id,))
        flash('Склад удален', 'success')

    except Exception as e:
        logger.error(f'Ошибка удаления склада: {e}')
        flash('Ошибка удаления склада', 'error')

    return redirect(url_for('warehouses.warehouses_list'))

@warehouses_bp.route('/warehouses/<int:id>/stocks', endpoint='warehouse_stocks')
@login_required
def warehouse_stocks(id):
    """Остатки на складе"""
    try:
        db = get_db()

        # Информация о складе
        warehouse = db.execute_query("SELECT * FROM warehouses WHERE id = ?", (id,), fetch_all=False)
        if not warehouse:
            flash('Склад не найден', 'error')
            return redirect(url_for('warehouses.warehouses_list'))

        # Все остатки
        stocks = db.execute_query("""
            SELECT s.*, n.name as nomenclature_name, n.sku, n.min_stock,
                   c.name_ru as category_name, b.batch_number,
                   (s.quantity - s.reserved_quantity) as available_quantity
            FROM stocks s
            LEFT JOIN nomenclatures n ON s.nomenclature_id = n.id
            LEFT JOIN categories c ON n.category_id = c.id
            LEFT JOIN batches b ON s.batch_id = b.id
            WHERE s.warehouse_id = ?
            ORDER BY n.name
        """, (id,), fetch_all=True) or []

        # Мало на складе
        low_stocks = []
        for stock in stocks:
            stock_dict = dict(stock)
            if stock_dict.get('min_stock') and stock_dict['quantity'] <= stock_dict['min_stock']:
                low_stocks.append(stock_dict)

        # Нулевые остатки
        zero_stocks = [dict(s) for s in stocks if s['quantity'] == 0]

        return render_template('warehouses/stocks.html',
                             warehouse=dict(warehouse),
                             stocks=[dict(s) for s in stocks],
                             low_stocks=low_stocks,
                             zero_stocks=zero_stocks)

    except Exception as e:
        logger.error(f'Ошибка загрузки остатков: {e}')
        flash('Ошибка загрузки остатков', 'error')
        return redirect(url_for('warehouses.warehouses_list'))

# ============ ПАРТИИ ============

@warehouses_bp.route('/batches', endpoint='batches_list')
@login_required
def batches_list():
    """Список партий"""
    try:
        db = get_db()
        status = request.args.get('status')

        query = """
            SELECT b.*, n.name as nomenclature_name, n.unit, s.name as supplier_name,
                   (SELECT SUM(quantity) FROM stocks WHERE batch_id = b.id) as total_quantity
            FROM batches b
            LEFT JOIN nomenclatures n ON b.nomenclature_id = n.id
            LEFT JOIN suppliers s ON b.supplier_id = s.id
            WHERE 1=1
        """
        params = []

        if status == 'active':
            query += " AND b.is_active = 1"
        elif status == 'expiring':
            query += " AND b.expiry_date BETWEEN date('now') AND date('now', '+30 days')"
        elif status == 'expired':
            query += " AND b.expiry_date < date('now')"

        query += " ORDER BY b.expiry_date ASC, b.created_at DESC"

        batches = db.execute_query(query, params, fetch_all=True)

        return render_template('batches/list.html', batches=[dict(b) for b in batches] if batches else [])
    except Exception as e:
        logger.error(f'Ошибка загрузки партий: {e}')
        flash('Ошибка загрузки партий', 'error')
        return redirect(url_for('dashboard'))

@warehouses_bp.route('/batches/add', methods=['GET', 'POST'], endpoint='add_batch')
@login_required
def add_batch():
    """Создание новой партии"""
    db = get_db()

    if request.method == 'POST':
        try:
            nomenclature_id = request.form.get('nomenclature_id')
            batch_number = request.form.get('batch_number')

            if not nomenclature_id or not batch_number:
                flash('Номенклатура и номер партии обязательны', 'error')
                return redirect(url_for('warehouses.add_batch'))

            data = {
                'batch_number': batch_number,
                'internal_batch_code': request.form.get('internal_batch_code'),
                'supplier_id': request.form.get('supplier_id'),
                'invoice_number': request.form.get('invoice_number'),
                'invoice_date': request.form.get('invoice_date'),
                'purchase_price': request.form.get('purchase_price'),
                'purchase_date': request.form.get('purchase_date'),
                'production_date': request.form.get('production_date'),
                'expiry_date': request.form.get('expiry_date'),
                'quality_status': request.form.get('quality_status', 'approved'),
                'certificate': request.form.get('certificate')
            }

            result = db.create_batch(nomenclature_id, data, session['user_id'])

            if result['success']:
                flash(result['message'], 'success')
                return redirect(url_for('warehouses.batches_list'))
            else:
                flash(result['message'], 'error')

        except Exception as e:
            logger.error(f'Ошибка создания партии: {e}')
            flash('Ошибка создания партии', 'error')

    # Получаем номенклатуры с сортировкой по имени и фильтром по типу учета
    nomenclatures = db.search_nomenclatures(limit=1000)

    # Сортируем по имени (если search_nomenclatures не сортирует)
    nomenclatures.sort(key=lambda x: x.get('name', '').lower())

    # Получаем данные для выпадающих списков
    suppliers = db.execute_query("SELECT id, name FROM suppliers WHERE is_active = 1", fetch_all=True) or []

    return render_template('batches/form.html',
                         title='Новая партия',
                         batch=None,
                         nomenclatures=nomenclatures,
                         suppliers=[dict(s) for s in suppliers])

@warehouses_bp.route('/batches/<int:id>/edit', methods=['GET', 'POST'], endpoint='edit_batch')
@login_required
def edit_batch(id):
    """Редактирование партии"""
    db = get_db()

    if request.method == 'POST':
        try:
            data = {
                'batch_number': request.form.get('batch_number'),
                'internal_batch_code': request.form.get('internal_batch_code'),
                'supplier_id': request.form.get('supplier_id'),
                'invoice_number': request.form.get('invoice_number'),
                'invoice_date': request.form.get('invoice_date'),
                'purchase_price': request.form.get('purchase_price'),
                'purchase_date': request.form.get('purchase_date'),
                'production_date': request.form.get('production_date'),
                'expiry_date': request.form.get('expiry_date'),
                'quality_status': request.form.get('quality_status', 'approved'),
                'certificate': request.form.get('certificate'),
                'is_active': 'is_active' in request.form
            }

            db.execute_query("""
                UPDATE batches
                SET batch_number = ?, internal_batch_code = ?, supplier_id = ?,
                    invoice_number = ?, invoice_date = ?, purchase_price = ?,
                    purchase_date = ?, production_date = ?, expiry_date = ?,
                    quality_status = ?, certificate = ?, is_active = ?
                WHERE id = ?
            """, (
                data['batch_number'],
                data['internal_batch_code'],
                data['supplier_id'],
                data['invoice_number'],
                data['invoice_date'],
                data['purchase_price'],
                data['purchase_date'],
                data['production_date'],
                data['expiry_date'],
                data['quality_status'],
                data['certificate'],
                1 if data['is_active'] else 0,
                id
            ))

            flash('Партия обновлена', 'success')
            return redirect(url_for('warehouses.batches_list'))

        except Exception as e:
            logger.error(f'Ошибка обновления партии: {e}')
            flash('Ошибка обновления партии', 'error')

    batch = db.execute_query("""
        SELECT b.*, n.name as nomenclature_name
        FROM batches b
        LEFT JOIN nomenclatures n ON b.nomenclature_id = n.id
        WHERE b.id = ?
    """, (id,), fetch_all=False)

    if not batch:
        flash('Партия не найдена', 'error')
        return redirect(url_for('warehouses.batches_list'))

    nomenclatures = [n for n in db.search_nomenclatures(limit=1000) if n.get('accounting_type') == 'batch']
    suppliers = db.execute_query("SELECT id, name FROM suppliers WHERE is_active = 1", fetch_all=True) or []

    return render_template('batches/form.html',
                         title='Редактирование партии',
                         batch=dict(batch),
                         nomenclatures=nomenclatures,
                         suppliers=[dict(s) for s in suppliers])

@warehouses_bp.route('/batches/<int:id>/delete', methods=['POST'], endpoint='delete_batch')
@login_required
def delete_batch(id):
    """Удаление партии"""
    try:
        db = get_db()

        # Проверяем, есть ли остатки
        stocks = db.execute_query(
            "SELECT COUNT(*) as cnt FROM stocks WHERE batch_id = ? AND quantity > 0",
            (id,), fetch_all=False
        )

        if stocks and stocks['cnt'] > 0:
            flash('Нельзя удалить партию, по которой есть остатки', 'error')
            return redirect(url_for('warehouses.batches_list'))

        db.execute_query("DELETE FROM batches WHERE id = ?", (id,))
        flash('Партия удалена', 'success')

    except Exception as e:
        logger.error(f'Ошибка удаления партии: {e}')
        flash('Ошибка удаления партии', 'error')

    return redirect(url_for('warehouses.batches_list'))

@warehouses_bp.route('/batches/<int:id>/stocks', endpoint='batch_stocks')
@login_required
def batch_stocks(id):
    """Остатки по партии"""
    try:
        db = get_db()

        # Информация о партии
        batch = db.execute_query("""
            SELECT b.*, n.name as nomenclature_name, n.unit, s.name as supplier_name
            FROM batches b
            LEFT JOIN nomenclatures n ON b.nomenclature_id = n.id
            LEFT JOIN suppliers s ON b.supplier_id = s.id
            WHERE b.id = ?
        """, (id,), fetch_all=False)

        if not batch:
            flash('Партия не найдена', 'error')
            return redirect(url_for('warehouses.batches_list'))

        # Остатки по складам
        stocks = db.execute_query("""
            SELECT s.*, w.name as warehouse_name, sb.code as storage_bin_code,
                   (s.quantity - s.reserved_quantity) as available_quantity
            FROM stocks s
            LEFT JOIN warehouses w ON s.warehouse_id = w.id
            LEFT JOIN storage_bins sb ON s.storage_bin_id = sb.id
            WHERE s.batch_id = ?
            ORDER BY w.name, sb.code
        """, (id,), fetch_all=True) or []

        return render_template('batches/stocks.html',
                             batch=dict(batch),
                             stocks=[dict(s) for s in stocks])

    except Exception as e:
        logger.error(f'Ошибка загрузки остатков по партии: {e}')
        flash('Ошибка загрузки остатков', 'error')
        return redirect(url_for('warehouses.batches_list'))

# ============ ЯЧЕЙКИ СКЛАДА ============

@warehouses_bp.route('/storage-bins', endpoint='storage_bins_list')
@login_required
def storage_bins_list():
    """Список ячеек склада"""
    try:
        db = get_db()

        # Параметры фильтрации
        warehouse_id = request.args.get('warehouse_id')
        zone = request.args.get('zone')
        is_active = request.args.get('is_active')
        has_items = request.args.get('has_items')

        query = """
            SELECT sb.*, w.name as warehouse_name,
                   (SELECT COUNT(*) FROM stocks WHERE storage_bin_id = sb.id) as occupancy
            FROM storage_bins sb
            LEFT JOIN warehouses w ON sb.warehouse_id = w.id
            WHERE 1=1
        """
        params = []

        # Фильтр по складу
        if warehouse_id and warehouse_id.strip():
            try:
                params.append(int(warehouse_id))
                query += " AND sb.warehouse_id = ?"
            except ValueError:
                # Если ID склада не число, игнорируем фильтр
                flash('Некорректный ID склада', 'warning')

        # Фильтр по зоне
        if zone and zone.strip():
            params.append(zone)
            query += " AND sb.zone = ?"

        # Фильтр по активности
        if is_active and is_active.strip():
            try:
                is_active_val = int(is_active)
                params.append(is_active_val)
                query += " AND sb.is_active = ?"
            except ValueError:
                # Если значение не 0 или 1, игнорируем
                pass

        # Фильтр по наличию товаров
        if has_items == '1':
            query += " AND (SELECT COUNT(*) FROM stocks WHERE storage_bin_id = sb.id) > 0"
        elif has_items == '0':
            query += " AND (SELECT COUNT(*) FROM stocks WHERE storage_bin_id = sb.id) = 0"

        query += " ORDER BY w.name, sb.zone, sb.rack, sb.shelf, sb.bin"

        bins = db.execute_query(query, params, fetch_all=True) or []

        # Склады для фильтра
        warehouses = db.execute_query("SELECT id, name FROM warehouses WHERE is_active = 1", fetch_all=True) or []

        # Группировка по зонам для схемы
        zones = {}
        for bin in bins:
            bin_dict = dict(bin)
            zone_name = bin_dict['zone'] or 'Без зоны'
            if zone_name not in zones:
                zones[zone_name] = []
            zones[zone_name].append(bin_dict)

        return render_template('storage_bins/list.html',
                             bins=[dict(b) for b in bins],
                             warehouses=[dict(w) for w in warehouses],
                             zones=zones)

    except Exception as e:
        logger.error(f'Ошибка загрузки ячеек: {e}')
        flash('Ошибка загрузки ячеек', 'error')
        return redirect(url_for('dashboard'))

@warehouses_bp.route('/storage-bins/add', methods=['GET', 'POST'], endpoint='add_storage_bin')
@login_required
def add_storage_bin():
    """Создание новой ячейки"""
    db = get_db()

    if request.method == 'POST':
        try:
            warehouse_id = request.form.get('warehouse_id')
            code = request.form.get('code')

            if not warehouse_id or not code:
                flash('Склад и код ячейки обязательны', 'error')
                return redirect(url_for('warehouses.add_storage_bin'))

            # Проверка уникальности
            existing = db.execute_query(
                "SELECT id FROM storage_bins WHERE warehouse_id = ? AND code = ?",
                (warehouse_id, code), fetch_all=False
            )
            if existing:
                flash('Ячейка с таким кодом уже существует на этом складе', 'error')
                return redirect(url_for('warehouses.add_storage_bin'))

            db.execute_query("""
                INSERT INTO storage_bins (
                    warehouse_id, code, name, zone, rack, shelf, bin,
                    barcode, capacity, capacity_unit, is_active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                warehouse_id,
                code,
                request.form.get('name'),
                request.form.get('zone'),
                request.form.get('rack'),
                request.form.get('shelf'),
                request.form.get('bin'),
                request.form.get('barcode'),
                request.form.get('capacity') or None,
                request.form.get('capacity_unit'),
                1 if 'is_active' in request.form else 0
            ))

            flash('Ячейка успешно создана', 'success')
            return redirect(url_for('warehouses.storage_bins_list'))

        except Exception as e:
            logger.error(f'Ошибка создания ячейки: {e}')
            flash('Ошибка создания ячейки', 'error')

    warehouses = db.execute_query("SELECT id, name FROM warehouses WHERE is_active = 1", fetch_all=True) or []

    return render_template('storage_bins/form.html',
                         title='Новая ячейка',
                         bin=None,
                         warehouses=[dict(w) for w in warehouses])

@warehouses_bp.route('/storage-bins/<int:id>/edit', methods=['GET', 'POST'], endpoint='edit_storage_bin')
@login_required
def edit_storage_bin(id):
    """Редактирование ячейки"""
    db = get_db()

    if request.method == 'POST':
        try:
            warehouse_id = request.form.get('warehouse_id')
            code = request.form.get('code')

            if not warehouse_id or not code:
                flash('Склад и код ячейки обязательны', 'error')
                return redirect(url_for('warehouses.edit_storage_bin', id=id))

            # Проверка уникальности (исключая текущую)
            existing = db.execute_query(
                "SELECT id FROM storage_bins WHERE warehouse_id = ? AND code = ? AND id != ?",
                (warehouse_id, code, id), fetch_all=False
            )
            if existing:
                flash('Ячейка с таким кодом уже существует на этом складе', 'error')
                return redirect(url_for('warehouses.edit_storage_bin', id=id))

            db.execute_query("""
                UPDATE storage_bins
                SET warehouse_id = ?, code = ?, name = ?, zone = ?, rack = ?, shelf = ?, bin = ?,
                    barcode = ?, capacity = ?, capacity_unit = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (
                warehouse_id,
                code,
                request.form.get('name'),
                request.form.get('zone'),
                request.form.get('rack'),
                request.form.get('shelf'),
                request.form.get('bin'),
                request.form.get('barcode'),
                request.form.get('capacity') or None,
                request.form.get('capacity_unit'),
                1 if 'is_active' in request.form else 0,
                id
            ))

            flash('Ячейка обновлена', 'success')
            return redirect(url_for('warehouses.storage_bins_list'))

        except Exception as e:
            logger.error(f'Ошибка обновления ячейки: {e}')
            flash('Ошибка обновления ячейки', 'error')

    bin_data = db.execute_query("SELECT * FROM storage_bins WHERE id = ?", (id,), fetch_all=False)
    if not bin_data:
        flash('Ячейка не найдена', 'error')
        return redirect(url_for('warehouses.storage_bins_list'))

    warehouses = db.execute_query("SELECT id, name FROM warehouses WHERE is_active = 1", fetch_all=True) or []

    return render_template('storage_bins/form.html',
                         title='Редактирование ячейки',
                         bin=dict(bin_data),
                         warehouses=[dict(w) for w in warehouses])

@warehouses_bp.route('/storage-bins/<int:id>/view', endpoint='view_storage_bin')
@login_required
def view_storage_bin(id):
    """Просмотр ячейки"""
    try:
        db = get_db()

        bin_data = db.execute_query("""
            SELECT sb.*, w.name as warehouse_name,
                   (SELECT COUNT(*) FROM stocks WHERE storage_bin_id = sb.id) as occupancy,
                   CASE
                       WHEN sb.capacity > 0
                       THEN (SELECT COUNT(*) FROM stocks WHERE storage_bin_id = sb.id) * 100.0 / sb.capacity
                       ELSE 0
                   END as occupancy_percent
            FROM storage_bins sb
            LEFT JOIN warehouses w ON sb.warehouse_id = w.id
            WHERE sb.id = ?
        """, (id,), fetch_all=False)

        if not bin_data:
            flash('Ячейка не найдена', 'error')
            return redirect(url_for('warehouses.storage_bins_list'))

        # Остатки в ячейке
        stocks = db.execute_query("""
            SELECT s.*, n.name as nomenclature_name, n.sku, n.unit,
                   b.batch_number
            FROM stocks s
            LEFT JOIN nomenclatures n ON s.nomenclature_id = n.id
            LEFT JOIN batches b ON s.batch_id = b.id
            WHERE s.storage_bin_id = ?
            ORDER BY n.name
        """, (id,), fetch_all=True) or []

        # История перемещений
        movements = db.execute_query("""
            SELECT d.document_date, d.document_number, d.id as document_id,
                   d.document_type, n.name as nomenclature_name, di.quantity
            FROM document_items di
            JOIN documents d ON di.document_id = d.id
            JOIN nomenclatures n ON di.nomenclature_id = n.id
            WHERE di.to_storage_bin_id = ? OR di.from_storage_bin_id = ?
            ORDER BY d.document_date DESC
            LIMIT 20
        """, (id, id), fetch_all=True) or []

        return render_template('storage_bins/view.html',
                             bin=dict(bin_data),
                             stocks=[dict(s) for s in stocks],
                             movements=[dict(m) for m in movements])

    except Exception as e:
        logger.error(f'Ошибка просмотра ячейки: {e}')
        flash('Ошибка просмотра ячейки', 'error')
        return redirect(url_for('warehouses.storage_bins_list'))

@warehouses_bp.route('/storage-bins/<int:id>/stock', endpoint='storage_bin_stock')
@login_required
def bin_stock(id):
    """Остатки в ячейке"""
    try:
        db = get_db()

        bin_data = db.execute_query("SELECT * FROM storage_bins WHERE id = ?", (id,), fetch_all=False)
        if not bin_data:
            flash('Ячейка не найдена', 'error')
            return redirect(url_for('warehouses.storage_bins_list'))

        stocks = db.execute_query("""
            SELECT s.*, n.name as nomenclature_name, n.sku, n.unit,
                   b.batch_number, w.name as warehouse_name
            FROM stocks s
            LEFT JOIN nomenclatures n ON s.nomenclature_id = n.id
            LEFT JOIN batches b ON s.batch_id = b.id
            LEFT JOIN warehouses w ON s.warehouse_id = w.id
            WHERE s.storage_bin_id = ?
            ORDER BY n.name
        """, (id,), fetch_all=True) or []

        return render_template('storage_bins/stock.html',
                             bin=dict(bin_data),
                             stocks=[dict(s) for s in stocks])

    except Exception as e:
        logger.error(f'Ошибка загрузки остатков: {e}')
        flash('Ошибка загрузки остатков', 'error')
        return redirect(url_for('warehouses.storage_bins_list'))

@warehouses_bp.route('/storage-bins/<int:id>/delete', methods=['POST'], endpoint='delete_storage_bin')
@login_required
def delete_storage_bin(id):
    """Удаление ячейки"""
    try:
        db = get_db()

        # Проверяем, есть ли остатки
        stocks = db.execute_query(
            "SELECT COUNT(*) as cnt FROM stocks WHERE storage_bin_id = ?",
            (id,), fetch_all=False
        )

        if stocks and stocks['cnt'] > 0:
            flash('Нельзя удалить ячейку, в которой есть остатки', 'error')
            return redirect(url_for('warehouses.storage_bins_list'))

        db.execute_query("DELETE FROM storage_bins WHERE id = ?", (id,))
        flash('Ячейка удалена', 'success')

    except Exception as e:
        logger.error(f'Ошибка удаления ячейки: {e}')
        flash('Ошибка удаления ячейки', 'error')

    return redirect(url_for('warehouses.storage_bins_list'))

# ============ API ДЛЯ ЯЧЕЕК ============

@warehouses_bp.route('/api/stock/<int:stock_id>', endpoint='api_get_stock')
@login_required
def api_get_stock(stock_id):
    """Получение информации об остатке"""
    try:
        db = get_db()
        stock = db.execute_query("""
            SELECT s.*, n.name, n.unit
            FROM stocks s
            JOIN nomenclatures n ON s.nomenclature_id = n.id
            WHERE s.id = ?
        """, (stock_id,), fetch_all=False)

        if not stock:
            return jsonify({'error': 'Остаток не найден'}), 404

        return jsonify(dict(stock))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@warehouses_bp.route('/api/stock/move', methods=['POST'], endpoint='api_move_stock')
@login_required
def api_move_stock():
    """Перемещение товара между ячейками"""
    try:
        db = get_db()
        data = request.json

        stock_id = data.get('stock_id')
        target_bin_id = data.get('target_bin_id')
        quantity = int(data.get('quantity', 0))
        reason = data.get('reason', '')

        if not all([stock_id, target_bin_id, quantity]):
            return jsonify({'success': False, 'error': 'Не все данные заполнены'})

        # Получаем исходный остаток
        source_stock = db.execute_query("""
            SELECT s.*, s.warehouse_id, s.nomenclature_id, s.batch_id
            FROM stocks s
            WHERE s.id = ?
        """, (stock_id,), fetch_all=False)

        if not source_stock:
            return jsonify({'success': False, 'error': 'Остаток не найден'})

        if source_stock['quantity'] < quantity:
            return jsonify({'success': False, 'error': 'Недостаточно товара'})

        # Получаем информацию о целевой ячейке
        target_bin = db.execute_query("""
            SELECT warehouse_id FROM storage_bins WHERE id = ?
        """, (target_bin_id,), fetch_all=False)

        if not target_bin:
            return jsonify({'success': False, 'error': 'Целевая ячейка не найдена'})

        # Уменьшаем количество в исходной ячейке
        new_source_qty = source_stock['quantity'] - quantity
        if new_source_qty == 0:
            db.execute_query("DELETE FROM stocks WHERE id = ?", (stock_id,))
        else:
            db.execute_query("""
                UPDATE stocks SET quantity = ? WHERE id = ?
            """, (new_source_qty, stock_id))

        # Проверяем, есть ли уже такой же остаток в целевой ячейке
        target_stock = db.execute_query("""
            SELECT id FROM stocks
            WHERE warehouse_id = ? AND storage_bin_id = ?
                AND nomenclature_id = ?
                AND (batch_id = ? OR (batch_id IS NULL AND ? IS NULL))
        """, (target_bin['warehouse_id'], target_bin_id,
              source_stock['nomenclature_id'], source_stock['batch_id'],
              source_stock['batch_id']), fetch_all=False)

        if target_stock:
            # Обновляем существующий остаток
            db.execute_query("""
                UPDATE stocks SET quantity = quantity + ? WHERE id = ?
            """, (quantity, target_stock['id']))
        else:
            # Создаем новый остаток
            db.execute_query("""
                INSERT INTO stocks (warehouse_id, storage_bin_id, nomenclature_id, batch_id, quantity)
                VALUES (?, ?, ?, ?, ?)
            """, (target_bin['warehouse_id'], target_bin_id,
                  source_stock['nomenclature_id'], source_stock['batch_id'], quantity))

        # Создаем документ перемещения
        doc_number = f"MOVE-{datetime.now().strftime('%Y%m%d')}-{int(datetime.now().timestamp()) % 10000:04d}"

        doc_row = db.execute_query("""
            INSERT INTO documents (
                document_type, document_number, document_date, status,
                from_warehouse_id, to_warehouse_id, reason, notes, created_by, created_at
            ) VALUES (?, ?, ?, 'posted', ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            RETURNING id
        """, (
            'transfer',
            doc_number,
            datetime.now().strftime('%Y-%m-%d'),
            source_stock['warehouse_id'],
            target_bin['warehouse_id'],
            reason,
            f"Перемещение из ячейки {source_stock['storage_bin_id']} в ячейку {target_bin_id}",
            session['user_id']
        ), fetch_all=False)

        document_id = doc_row['id']

        # Добавляем позицию в документ
        db.execute_query("""
            INSERT INTO document_items (
                document_id, nomenclature_id, batch_id, quantity,
                from_storage_bin_id, to_storage_bin_id
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (document_id, source_stock['nomenclature_id'], source_stock['batch_id'],
              quantity, source_stock['storage_bin_id'], target_bin_id))

        db.connection.commit()

        return jsonify({'success': True, 'message': 'Товар перемещен'})

    except Exception as e:
        logger.error(f'Ошибка перемещения: {e}')
        return jsonify({'success': False, 'error': str(e)})

@warehouses_bp.route('/api/stock/adjust', methods=['POST'], endpoint='api_adjust_stock')
@login_required
def api_adjust_stock():
    """Корректировка остатка"""
    try:
        db = get_db()
        data = request.json

        stock_id = data.get('stock_id')
        new_quantity = int(data.get('new_quantity', 0))
        reason = data.get('reason', '')

        if not all([stock_id, reason]):
            return jsonify({'success': False, 'error': 'Не все данные заполнены'})

        # Получаем текущий остаток
        stock = db.execute_query("SELECT * FROM stocks WHERE id = ?", (stock_id,), fetch_all=False)

        if not stock:
            return jsonify({'success': False, 'error': 'Остаток не найден'})

        old_quantity = stock['quantity']

        if new_quantity == 0:
            db.execute_query("DELETE FROM stocks WHERE id = ?", (stock_id,))
        else:
            db.execute_query("UPDATE stocks SET quantity = ? WHERE id = ?", (new_quantity, stock_id))

        # Создаем документ корректировки
        doc_number = f"ADJ-{datetime.now().strftime('%Y%m%d')}-{int(datetime.now().timestamp()) % 10000:04d}"

        doc_row = db.execute_query("""
            INSERT INTO documents (
                document_type, document_number, document_date, status,
                reason, notes, created_by, created_at
            ) VALUES (?, ?, ?, 'posted', ?, ?, ?, CURRENT_TIMESTAMP)
            RETURNING id
        """, (
            'adjustment',
            doc_number,
            datetime.now().strftime('%Y-%m-%d'),
            reason,
            f"Корректировка: было {old_quantity}, стало {new_quantity}",
            session['user_id']
        ), fetch_all=False)

        document_id = doc_row['id']

        # Добавляем позицию в документ
        db.execute_query("""
            INSERT INTO document_items (
                document_id, nomenclature_id, batch_id, quantity,
                storage_bin_id
            ) VALUES (?, ?, ?, ?, ?)
        """, (document_id, stock['nomenclature_id'], stock['batch_id'],
              new_quantity - old_quantity, stock['storage_bin_id']))

        db.connection.commit()

        return jsonify({'success': True, 'message': 'Корректировка выполнена'})

    except Exception as e:
        logger.error(f'Ошибка корректировки: {e}')
        return jsonify({'success': False, 'error': str(e)})

@warehouses_bp.route('/api/warehouses/<int:warehouse_id>/bins', endpoint='api_get_warehouse_bins')
@login_required
def api_warehouse_bins(warehouse_id):
    """Получение ячеек склада"""
    try:
        db = get_db()
        bins = db.execute_query("""
            SELECT id, code, name, zone, rack, shelf, bin
            FROM storage_bins
            WHERE warehouse_id = ? AND is_active = 1
            ORDER BY zone, rack, shelf, bin
        """, (warehouse_id,), fetch_all=True)

        return jsonify([dict(b) for b in bins] if bins else [])
    except Exception as e:
        return jsonify([])

# ============ ОСТАТКИ (STOCKS) ============

@warehouses_bp.route('/stocks', endpoint='stocks_list')
@login_required
def stocks_list():
    """Список всех остатков на складах с поддержкой модификаций и фильтрацией"""
    try:
        db = get_db()

        # Получаем параметры фильтрации из GET запроса
        warehouse_id = request.args.get('warehouse_id')
        account_type = request.args.get('account_type')
        search = request.args.get('search', '')

        logger.debug(f"🔍 Фильтры: warehouse_id={warehouse_id}, account_type={account_type}, search={search}")

        # 1. Количественный учет (из таблицы stocks)
        stocks_query = """
            SELECT
                'quantitative' as account_type,
                s.id,
                s.warehouse_id,
                w.name as warehouse_name,
                s.nomenclature_id,
                n.name as nomenclature_name,
                n.sku,
                n.unit,
                s.quantity as total_quantity,
                s.reserved_quantity,
                (s.quantity - COALESCE(s.reserved_quantity, 0)) as available_quantity,
                NULL as instances_count,
                NULL as batches_count,
                b.id as batch_id,
                b.batch_number,
                sb.id as storage_bin_id,
                sb.code as storage_bin_code,
                NULL as inventory_number,
                NULL as serial_number,
                NULL as status,
                NULL as condition,
                NULL as employee_name,
                NULL as location_name,
                NULL as expiry_date,
                NULL as quality_status,
                NULL as purchase_date,
                NULL as size,
                NULL as color,
                NULL as variation_sku
            FROM stocks s
            LEFT JOIN nomenclatures n ON s.nomenclature_id = n.id
            LEFT JOIN warehouses w ON s.warehouse_id = w.id
            LEFT JOIN batches b ON s.batch_id = b.id
            LEFT JOIN storage_bins sb ON s.storage_bin_id = sb.id
            WHERE s.quantity > 0
        """

        # 2. Индивидуальный учет (экземпляры)
        instances_query = """
            SELECT
                'individual' as account_type,
                i.id,
                i.warehouse_id,
                w.name as warehouse_name,
                i.nomenclature_id,
                n.name as nomenclature_name,
                n.sku,
                n.unit,
                1 as total_quantity,
                0 as reserved_quantity,
                1 as available_quantity,
                1 as instances_count,
                NULL as batches_count,
                NULL as batch_id,
                NULL as batch_number,
                sb.id as storage_bin_id,
                sb.code as storage_bin_code,
                i.inventory_number,
                i.serial_number,
                i.status,
                i.condition,
                e.full_name as employee_name,
                l.name as location_name,
                NULL as expiry_date,
                NULL as quality_status,
                i.purchase_date,
                nv.size,
                nv.color,
                nv.sku as variation_sku
            FROM instances i
            LEFT JOIN nomenclatures n ON i.nomenclature_id = n.id
            LEFT JOIN warehouses w ON i.warehouse_id = w.id
            LEFT JOIN storage_bins sb ON i.storage_bin_id = sb.id
            LEFT JOIN employees e ON i.employee_id = e.id
            LEFT JOIN locations l ON i.location_id = l.id
            LEFT JOIN nomenclature_variations nv ON nv.id = i.variation_id
            WHERE i.status IN ('in_stock', 'available')
        """

        # 3. Партионный учет
        batches_query = """
            SELECT
                'batch' as account_type,
                b.id,
                NULL as warehouse_id,
                'Не привязан к складу' as warehouse_name,
                b.nomenclature_id,
                n.name as nomenclature_name,
                n.sku,
                n.unit,
                COALESCE(s.total_quantity, 0) as total_quantity,
                0 as reserved_quantity,
                COALESCE(s.total_quantity, 0) as available_quantity,
                NULL as instances_count,
                1 as batches_count,
                b.id as batch_id,
                b.batch_number,
                NULL as storage_bin_id,
                NULL as storage_bin_code,
                NULL as inventory_number,
                NULL as serial_number,
                NULL as status,
                NULL as condition,
                NULL as employee_name,
                NULL as location_name,
                b.expiry_date,
                b.quality_status,
                b.purchase_date,
                NULL as size,
                NULL as color,
                NULL as variation_sku
            FROM batches b
            LEFT JOIN nomenclatures n ON b.nomenclature_id = n.id
            LEFT JOIN (
                SELECT batch_id, SUM(quantity) as total_quantity
                FROM stocks
                WHERE batch_id IS NOT NULL
                GROUP BY batch_id
            ) s ON b.id = s.batch_id
            WHERE b.is_active = 1 AND COALESCE(s.total_quantity, 0) > 0
        """

        # Выполняем каждый запрос отдельно и потом объединяем в Python
        all_stocks = []

        # Добавляем количественные остатки
        stocks_result = db.execute_query(stocks_query, fetch_all=True)
        if stocks_result:
            all_stocks.extend([dict(row) for row in stocks_result])

        # Добавляем индивидуальные экземпляры
        instances_result = db.execute_query(instances_query, fetch_all=True)
        if instances_result:
            all_stocks.extend([dict(row) for row in instances_result])

        # Добавляем партии
        batches_result = db.execute_query(batches_query, fetch_all=True)
        if batches_result:
            all_stocks.extend([dict(row) for row in batches_result])

        # Применяем фильтры в Python
        filtered_stocks = []
        for stock in all_stocks:
            # Фильтр по складу
            if warehouse_id and warehouse_id.strip():
                try:
                    if stock.get('warehouse_id') != int(warehouse_id):
                        continue
                except (ValueError, TypeError):
                    pass

            # Фильтр по типу учета
            if account_type and account_type.strip():
                if stock.get('account_type') != account_type:
                    continue

            # Поиск по тексту (умный: регистр + транслитерация + токены)
            if search and search.strip():
                from utils.search import tokenize
                tokens = tokenize(search)
                if not tokens:
                    tokens = [normalize(search)]

                fields_to_search = [
                    (stock.get('nomenclature_name') or '').lower(),
                    (stock.get('sku') or '').lower(),
                    (stock.get('inventory_number') or '').lower(),
                    (stock.get('batch_number') or '').lower(),
                ]

                # Каждый токен должен встречаться хотя бы в одном поле
                all_tokens_found = True
                for token in tokens:
                    variants = generate_variants(token)
                    token_found = any(
                        any(v in field for v in variants)
                        for field in fields_to_search
                    )
                    if not token_found:
                        all_tokens_found = False
                        break

                if not all_tokens_found:
                    continue

            filtered_stocks.append(stock)

        # Сортируем по названию номенклатуры
        filtered_stocks.sort(key=lambda x: x.get('nomenclature_name', '') or '')

        # Получаем список складов для фильтра
        warehouses = db.execute_query("SELECT id, name FROM warehouses WHERE is_active = 1 ORDER BY name", fetch_all=True) or []
        warehouses_list = [dict(w) for w in warehouses] if warehouses else []

        logger.debug(f"📊 Найдено остатков: {len(filtered_stocks)}")

        return render_template('stocks/list.html',
                             stocks=filtered_stocks,
                             warehouses=warehouses_list)

    except Exception as e:
        logger.error(f'Ошибка загрузки остатков: {e}')
        import traceback
        traceback.print_exc()
        flash('Ошибка загрузки остатков', 'error')
        return redirect(url_for('dashboard'))

@warehouses_bp.route('/stocks/add', methods=['GET', 'POST'], endpoint='add_stock')
@login_required
def add_stock():
    """Добавление нового остатка на склад"""
    try:
        db = get_db()

        if request.method == 'POST':
            nomenclature_id = request.form.get('nomenclature_id')
            warehouse_id = request.form.get('warehouse_id')
            quantity = request.form.get('quantity', 0, type=float)
            batch_id = request.form.get('batch_id')
            storage_bin_id = request.form.get('storage_bin_id')
            reason = request.form.get('reason', 'Начальный остаток')

            if not nomenclature_id or not warehouse_id or quantity <= 0:
                flash('Заполните все обязательные поля', 'error')
                return redirect(request.url)

            # Проверяем существующий остаток
            existing = db.execute_query("""
                SELECT id, quantity FROM stocks
                WHERE nomenclature_id = ? AND warehouse_id = ?
                  AND (batch_id = ? OR (batch_id IS NULL AND ? IS NULL))
            """, (nomenclature_id, warehouse_id, batch_id, batch_id), fetch_all=False)

            # Начинаем транзакцию
            db.connection.execute("BEGIN TRANSACTION")

            if existing:
                # Обновляем существующий
                db.execute_query("""
                    UPDATE stocks
                    SET quantity = quantity + ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (quantity, existing['id']))
                stock_id = existing['id']
                message = f'Остаток обновлен: +{quantity}'
            else:
                # Создаем новый
                stock_row = db.execute_query("""
                    INSERT INTO stocks (nomenclature_id, warehouse_id, batch_id, storage_bin_id, quantity, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    RETURNING id
                """, (nomenclature_id, warehouse_id, batch_id, storage_bin_id, quantity), fetch_all=False)
                stock_id = stock_row['id']
                message = f'Остаток добавлен: {quantity}'

            # Создаем документ прихода для истории
            doc_number = f"REC-{datetime.now().strftime('%Y%m%d')}-{int(time.time()) % 10000:04d}"
            doc_row = db.execute_query("""
                INSERT INTO documents (
                    document_type, document_number, document_date, status,
                    to_warehouse_id, reason, created_by, created_at
                ) VALUES (?, ?, ?, 'posted', ?, ?, ?, CURRENT_TIMESTAMP)
                RETURNING id
            """, ('receipt', doc_number, datetime.now().strftime('%Y-%m-%d'),
                  warehouse_id, reason, session['user_id']), fetch_all=False)

            doc_id = doc_row['id']

            # Добавляем позицию в документ
            db.execute_query("""
                INSERT INTO document_items (document_id, nomenclature_id, batch_id, quantity, created_by)
                VALUES (?, ?, ?, ?, ?)
            """, (doc_id, nomenclature_id, batch_id, quantity, session['user_id']))

            db.connection.commit()

            flash(message, 'success')
            return redirect(url_for('warehouses.stocks_list'))

        # GET запрос - показываем форму
        # Получаем список номенклатуры с количественным учетом
        nomenclatures = db.execute_query("""
            SELECT n.id, n.name, n.sku, n.unit
            FROM nomenclatures n
            WHERE n.accounting_type = 'quantitative' AND n.is_active = 1
            ORDER BY n.name
        """, fetch_all=True)

        # Получаем список складов
        warehouses = db.execute_query(
            "SELECT id, name FROM warehouses WHERE is_active = 1 ORDER BY name",
            fetch_all=True
        )

        # Получаем список ячеек (если есть)
        storage_bins = db.execute_query("""
            SELECT sb.id, sb.code, w.name as warehouse_name
            FROM storage_bins sb
            JOIN warehouses w ON sb.warehouse_id = w.id
            WHERE sb.is_active = 1
            ORDER BY w.name, sb.code
        """, fetch_all=True)

        return render_template('stocks/add.html',
                             nomenclatures=[dict(n) for n in nomenclatures] if nomenclatures else [],
                             warehouses=[dict(w) for w in warehouses] if warehouses else [],
                             storage_bins=[dict(sb) for sb in storage_bins] if storage_bins else [])

    except Exception as e:
        db.connection.rollback()
        logger.debug(f"Ошибка добавления остатка: {e}")
        import traceback
        traceback.print_exc()
        flash(f'Ошибка добавления остатка: {str(e)}', 'error')
        return redirect(url_for('warehouses.stocks_list'))

@warehouses_bp.route('/stocks/manage/<int:nomenclature_id>', methods=['GET', 'POST'], endpoint='manage_stock')
@login_required
def manage_stock(nomenclature_id):
    """Управление остатками для количественного учета"""
    try:
        db = get_db()
        nomenclature = db.get_nomenclature_by_id(nomenclature_id)

        if not nomenclature:
            flash('Номенклатура не найдена', 'error')
            return redirect(url_for('nomenclatures.nomenclatures_list'))

        if request.method == 'POST':
            action = request.form.get('action')  # 'add', 'remove', 'set'
            warehouse_id = request.form.get('warehouse_id')
            quantity = request.form.get('quantity', 0, type=int)
            batch_id = request.form.get('batch_id')
            reason = request.form.get('reason', '')

            if not warehouse_id or quantity <= 0:
                flash('Заполните все поля', 'error')
                return redirect(request.url)

            if action == 'add':
                # Приход
                existing = db.execute_query("""
                    SELECT id, quantity FROM stocks
                    WHERE nomenclature_id = ? AND warehouse_id = ?
                      AND (batch_id = ? OR (batch_id IS NULL AND ? IS NULL))
                """, (nomenclature_id, warehouse_id, batch_id, batch_id), fetch_all=False)

                if existing:
                    db.execute_query("""
                        UPDATE stocks SET quantity = quantity + ? WHERE id = ?
                    """, (quantity, existing['id']))
                else:
                    db.execute_query("""
                        INSERT INTO stocks (nomenclature_id, warehouse_id, batch_id, quantity)
                        VALUES (?, ?, ?, ?)
                    """, (nomenclature_id, warehouse_id, batch_id, quantity))

                # Создаем документ прихода
                doc_number = f"REC-{datetime.now().strftime('%Y%m%d')}-{int(time.time()) % 10000:04d}"
                doc_row = db.execute_query("""
                    INSERT INTO documents (document_type, document_number, document_date, status,
                                          to_warehouse_id, reason, created_by, created_at)
                    VALUES (?, ?, ?, 'posted', ?, ?, ?, CURRENT_TIMESTAMP)
                    RETURNING id
                """, ('receipt', doc_number, datetime.now().strftime('%Y-%m-%d'),
                      warehouse_id, reason, session['user_id']), fetch_all=False)

                doc_id = doc_row['id']
                db.execute_query("""
                    INSERT INTO document_items (document_id, nomenclature_id, batch_id, quantity)
                    VALUES (?, ?, ?, ?)
                """, (doc_id, nomenclature_id, batch_id, quantity))

                flash(f'Приход {quantity} {nomenclature["unit"]} оформлен', 'success')

            elif action == 'remove':
                # Расход
                stock = db.execute_query("""
                    SELECT id, quantity FROM stocks
                    WHERE nomenclature_id = ? AND warehouse_id = ?
                      AND (batch_id = ? OR (batch_id IS NULL AND ? IS NULL))
                """, (nomenclature_id, warehouse_id, batch_id, batch_id), fetch_all=False)

                if not stock or stock['quantity'] < quantity:
                    flash('Недостаточно товара на складе', 'error')
                    return redirect(request.url)

                new_qty = stock['quantity'] - quantity
                if new_qty == 0:
                    db.execute_query("DELETE FROM stocks WHERE id = ?", (stock['id'],))
                else:
                    db.execute_query("UPDATE stocks SET quantity = ? WHERE id = ?", (new_qty, stock['id']))

                # Создаем документ расхода
                doc_number = f"ISS-{datetime.now().strftime('%Y%m%d')}-{int(time.time()) % 10000:04d}"
                doc_row = db.execute_query("""
                    INSERT INTO documents (document_type, document_number, document_date, status,
                                          from_warehouse_id, reason, created_by, created_at)
                    VALUES (?, ?, ?, 'posted', ?, ?, ?, CURRENT_TIMESTAMP)
                    RETURNING id
                """, ('issuance', doc_number, datetime.now().strftime('%Y-%m-%d'),
                      warehouse_id, reason, session['user_id']), fetch_all=False)

                doc_id = doc_row['id']
                db.execute_query("""
                    INSERT INTO document_items (document_id, nomenclature_id, batch_id, quantity)
                    VALUES (?, ?, ?, ?)
                """, (doc_id, nomenclature_id, batch_id, quantity))

                flash(f'Расход {quantity} {nomenclature["unit"]} оформлен', 'success')

            elif action == 'set':
                # Установка точного количества (корректировка)
                stock = db.execute_query("""
                    SELECT id, quantity FROM stocks
                    WHERE nomenclature_id = ? AND warehouse_id = ?
                      AND (batch_id = ? OR (batch_id IS NULL AND ? IS NULL))
                """, (nomenclature_id, warehouse_id, batch_id, batch_id), fetch_all=False)

                if stock:
                    old_qty = stock['quantity']
                    db.execute_query("UPDATE stocks SET quantity = ? WHERE id = ?", (quantity, stock['id']))
                else:
                    old_qty = 0
                    db.execute_query("""
                        INSERT INTO stocks (nomenclature_id, warehouse_id, batch_id, quantity)
                        VALUES (?, ?, ?, ?)
                    """, (nomenclature_id, warehouse_id, batch_id, quantity))

                # Создаем документ корректировки
                doc_number = f"ADJ-{datetime.now().strftime('%Y%m%d')}-{int(time.time()) % 10000:04d}"
                doc_row = db.execute_query("""
                    INSERT INTO documents (document_type, document_number, document_date, status,
                                          warehouse_id, reason, created_by, created_at)
                    VALUES (?, ?, ?, 'posted', ?, ?, ?, CURRENT_TIMESTAMP)
                    RETURNING id
                """, ('adjustment', doc_number, datetime.now().strftime('%Y-%m-%d'),
                      warehouse_id, f"Корректировка: было {old_qty}, стало {quantity}", session['user_id']), fetch_all=False)

                doc_id = doc_row['id']
                db.execute_query("""
                    INSERT INTO document_items (document_id, nomenclature_id, batch_id, quantity)
                    VALUES (?, ?, ?, ?)
                """, (doc_id, nomenclature_id, batch_id, quantity - old_qty))

                flash(f'Количество установлено: {quantity} {nomenclature["unit"]}', 'success')

            return redirect(url_for('warehouses.manage_stock', nomenclature_id=nomenclature_id))

        # GET запрос - показываем форму управления
        stocks = db.execute_query("""
            SELECT s.*, w.name as warehouse_name, sb.code as bin_code
            FROM stocks s
            LEFT JOIN warehouses w ON s.warehouse_id = w.id
            LEFT JOIN storage_bins sb ON s.storage_bin_id = sb.id
            WHERE s.nomenclature_id = ?
            ORDER BY w.name
        """, (nomenclature_id,), fetch_all=True)

        warehouses = db.execute_query("SELECT id, name FROM warehouses WHERE is_active = 1 ORDER BY name", fetch_all=True) or []

        # Получаем последние движения (добавляем document_id)
        movements = db.execute_query("""
            SELECT d.id as document_id, d.document_date, d.document_number, d.document_type,
                   di.quantity, d.reason
            FROM documents d
            JOIN document_items di ON d.id = di.document_id
            WHERE di.nomenclature_id = ? AND d.status = 'posted'
            ORDER BY d.document_date DESC
            LIMIT 10
        """, (nomenclature_id,), fetch_all=True)

        return render_template('stocks/manage.html',
                             nomenclature=nomenclature,
                             stocks=[dict(s) for s in stocks] if stocks else [],
                             warehouses=[dict(w) for w in warehouses],
                             movements=[dict(m) for m in movements] if movements else [])

    except Exception as e:
        logger.debug(f"Ошибка управления остатками: {e}")
        import traceback
        traceback.print_exc()
        flash('Ошибка управления остатками', 'error')
        return redirect(url_for('nomenclatures.nomenclatures_list'))

@warehouses_bp.route('/stocks/<int:id>/movements', endpoint='stock_movements')
@login_required
def stock_movements(id):
    """История движений по конкретному остатку"""
    try:
        db = get_db()

        # Получаем информацию об остатке
        stock = db.execute_query("""
            SELECT s.*, n.name as nomenclature_name, n.sku, n.unit,
                   w.name as warehouse_name
            FROM stocks s
            JOIN nomenclatures n ON s.nomenclature_id = n.id
            JOIN warehouses w ON s.warehouse_id = w.id
            WHERE s.id = ?
        """, (id,), fetch_all=False)

        if not stock:
            flash('Остаток не найден', 'error')
            return redirect(url_for('warehouses.stocks_list'))

        # Получаем движения по этому материалу
        movements = db.execute_query("""
            SELECT
                d.document_date,
                d.document_number,
                d.document_type,
                di.quantity,
                CASE
                    WHEN d.document_type = 'receipt' THEN '+' || di.quantity
                    WHEN d.document_type = 'issuance' THEN '-' || di.quantity
                    ELSE CAST(di.quantity as TEXT)
                END as quantity_change,
                d.reason,
                u.username as created_by_name
            FROM documents d
            JOIN document_items di ON d.id = di.document_id
            LEFT JOIN users u ON d.created_by = u.id
            WHERE di.nomenclature_id = ?
                AND d.status = 'posted'
            ORDER BY d.document_date DESC, d.created_at DESC
        """, (stock['nomenclature_id'],), fetch_all=True)

        return render_template('stocks/movements.html',
                             stock=dict(stock),
                             movements=[dict(m) for m in movements] if movements else [])

    except Exception as e:
        logger.debug(f"Ошибка загрузки движений: {e}")
        flash('Ошибка загрузки движений', 'error')
        return redirect(url_for('warehouses.stocks_list'))

@warehouses_bp.route('/stocks/import', methods=['GET', 'POST'], endpoint='import_stocks')
@login_required
def import_stocks():
    """Импорт остатков из Excel/CSV"""
    if request.method == 'POST':
        try:
            import pandas as pd
            from io import BytesIO

            if 'file' not in request.files:
                flash('Файл не загружен', 'error')
                return redirect(request.url)

            file = request.files['file']
            if file.filename == '':
                flash('Файл не выбран', 'error')
                return redirect(request.url)

            # Чтение файла
            if file.filename.endswith('.csv'):
                df = pd.read_csv(file)
            else:
                df = pd.read_excel(file)

            db = get_db()
            stats = {'added': 0, 'updated': 0, 'errors': 0}
            errors = []

            for idx, row in df.iterrows():
                try:
                    nomenclature_name = row.get('Номенклатура') or row.get('material')
                    warehouse_name = row.get('Склад') or row.get('warehouse')
                    quantity = float(row.get('Количество') or row.get('quantity', 0))

                    if not nomenclature_name or not warehouse_name or quantity <= 0:
                        stats['errors'] += 1
                        errors.append(f"Строка {idx+2}: Неполные данные")
                        continue

                    # Ищем номенклатуру
                    nomenclature = db.execute_query("""
                        SELECT id FROM nomenclatures
                        WHERE name LIKE ? AND accounting_type = 'quantitative'
                        LIMIT 1
                    """, (f'%{nomenclature_name}%',), fetch_all=False)

                    if not nomenclature:
                        stats['errors'] += 1
                        errors.append(f"Строка {idx+2}: Номенклатура '{nomenclature_name}' не найдена")
                        continue

                    # Ищем склад
                    warehouse = db.execute_query("""
                        SELECT id FROM warehouses WHERE name LIKE ? LIMIT 1
                    """, (f'%{warehouse_name}%',), fetch_all=False)

                    if not warehouse:
                        stats['errors'] += 1
                        errors.append(f"Строка {idx+2}: Склад '{warehouse_name}' не найден")
                        continue

                    # Проверяем существующий остаток
                    existing = db.execute_query("""
                        SELECT id, quantity FROM stocks
                        WHERE nomenclature_id = ? AND warehouse_id = ?
                    """, (nomenclature['id'], warehouse['id']), fetch_all=False)

                    if existing:
                        db.execute_query("""
                            UPDATE stocks SET quantity = ?, updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        """, (quantity, existing['id']))
                        stats['updated'] += 1
                    else:
                        db.execute_query("""
                            INSERT INTO stocks (nomenclature_id, warehouse_id, quantity, created_at, updated_at)
                            VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """, (nomenclature['id'], warehouse['id'], quantity))
                        stats['added'] += 1

                except Exception as e:
                    stats['errors'] += 1
                    errors.append(f"Строка {idx+2}: {str(e)}")

            message = f"Импорт завершен. Добавлено: {stats['added']}, Обновлено: {stats['updated']}, Ошибок: {stats['errors']}"
            if errors:
                flash(message + "\n" + "\n".join(errors[:5]), 'warning')
            else:
                flash(message, 'success')

        except Exception as e:
            flash(f'Ошибка импорта: {str(e)}', 'error')

        return redirect(url_for('warehouses.stocks_list'))

    # GET запрос - показываем форму импорта
    return render_template('stocks/import.html')
