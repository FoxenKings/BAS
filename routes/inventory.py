"""
Blueprint: Инвентаризация
"""
import json
import logging
from datetime import datetime, timedelta
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, flash, send_file
from extensions import csrf
from routes.common import login_required, admin_required, manager_required, get_db


logger = logging.getLogger('routes.inventory')

inventory_bp = Blueprint('inventory', __name__)

# ============ ИНВЕНТАРИЗАЦИЯ ============

@inventory_bp.route('/inventory', endpoint='inventory_list')
@login_required
def inventory_list():
    """Список инвентаризаций"""
    try:
        db = get_db()
        status = request.args.get('status')
        
        query = """
            SELECT i.*, w.name as warehouse_name, u.username as created_by_name,
                   (SELECT COUNT(*) FROM inventory_items WHERE inventory_id = i.id) as total_items,
                   (SELECT COUNT(*) FROM inventory_items WHERE inventory_id = i.id AND variance != 0) as discrepancies
            FROM inventories i
            LEFT JOIN warehouses w ON i.warehouse_id = w.id
            LEFT JOIN users u ON i.created_by = u.id
            WHERE 1=1
        """
        params = []
        
        if status:
            query += " AND i.status = ?"
            params.append(status)
        
        query += " ORDER BY i.created_at DESC"
        
        inventories = db.execute_query(query, params, fetch_all=True)
        
        return render_template('inventory/list.html', 
                             inventories=[dict(i) for i in inventories] if inventories else [])
    except Exception as e:
        logger.error(f'Ошибка загрузки инвентаризаций: {e}')
        flash('Ошибка загрузки инвентаризаций', 'error')
        return redirect(url_for('dashboard'))

@inventory_bp.route('/inventory/add', methods=['GET', 'POST'], endpoint='add_inventory')
@login_required
def add_inventory():
    """Создание новой инвентаризации"""
    db = get_db()
    
    if request.method == 'POST':
        try:
            inventory_number = request.form.get('inventory_number')
            if not inventory_number:
                # Генерация номера
                year = datetime.now().year
                month = datetime.now().strftime('%m')
                inventory_number = f"INV-{year}-{month}-{int(datetime.now().timestamp()) % 10000:04d}"
            
            inventory_date = request.form.get('inventory_date')
            warehouse_id = request.form.get('warehouse_id')
            inventory_type = request.form.get('inventory_type', 'full')
            responsible_id = request.form.get('responsible_id')
            notes = request.form.get('notes')
            
            if not warehouse_id:
                flash('Выберите склад', 'error')
                return redirect(url_for('inventory.add_inventory'))
            
            # Создание инвентаризации
            inv_row = db.execute_query("""
                INSERT INTO inventories (
                    inventory_number, inventory_date, warehouse_id, inventory_type,
                    responsible_id, notes, status, created_by, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'draft', ?, CURRENT_TIMESTAMP)
                RETURNING id
            """, (inventory_number, inventory_date, warehouse_id, inventory_type,
                  responsible_id or None, notes, session['user_id']), fetch_all=False)

            inventory_id = inv_row['id']
            
            # Добавление позиций в зависимости от типа
            if inventory_type == 'full':
                # Все позиции на складе
                db.execute_query("""
                    INSERT INTO inventory_items (inventory_id, nomenclature_id, batch_id, storage_bin_id, expected_quantity)
                    SELECT ?, s.nomenclature_id, s.batch_id, s.storage_bin_id, s.quantity
                    FROM stocks s
                    WHERE s.warehouse_id = ? AND s.quantity > 0
                """, (inventory_id, warehouse_id))
                
            elif inventory_type == 'partial':
                category_id = request.form.get('category_id')
                if category_id:
                    db.execute_query("""
                        INSERT INTO inventory_items (inventory_id, nomenclature_id, batch_id, storage_bin_id, expected_quantity)
                        SELECT ?, s.nomenclature_id, s.batch_id, s.storage_bin_id, s.quantity
                        FROM stocks s
                        JOIN nomenclatures n ON s.nomenclature_id = n.id
                        WHERE s.warehouse_id = ? AND n.category_id = ? AND s.quantity > 0
                    """, (inventory_id, warehouse_id, category_id))
            
            elif inventory_type == 'selective':
                selected_items = request.form.getlist('items[]')
                for item_id in selected_items:
                    db.execute_query("""
                        INSERT INTO inventory_items (inventory_id, nomenclature_id, expected_quantity)
                        SELECT ?, s.nomenclature_id, SUM(s.quantity)
                        FROM stocks s
                        WHERE s.warehouse_id = ? AND s.nomenclature_id = ?
                        GROUP BY s.nomenclature_id
                    """, (inventory_id, warehouse_id, item_id))
            
            flash('Инвентаризация успешно создана', 'success')
            return redirect(url_for('inventory.view_inventory', id=inventory_id))
            
        except Exception as e:
            logger.error(f'Ошибка создания инвентаризации: {e}')
            flash('Ошибка создания инвентаризации', 'error')
    
    # Данные для формы
    warehouses = db.execute_query("SELECT id, name FROM warehouses WHERE is_active = 1", fetch_all=True) or []
    employees = db.execute_query("""
        SELECT id, last_name || ' ' || first_name || COALESCE(' ' || middle_name, '') as full_name 
        FROM employees WHERE is_active = 1 ORDER BY last_name
    """, fetch_all=True) or []
    categories = db.get_all_categories()
    nomenclatures = db.search_nomenclatures(limit=1000)
    
    return render_template('inventory/form.html',
                         title='Новая инвентаризация',
                         inventory=None,
                         warehouses=[dict(w) for w in warehouses],
                         employees=[dict(e) for e in employees],
                         categories=categories,
                         nomenclatures=nomenclatures)

@inventory_bp.route('/inventory/<int:id>', endpoint='view_inventory')
@login_required
def view_inventory(id):
    """Просмотр инвентаризации"""
    try:
        db = get_db()
        
        inventory = db.execute_query("""
            SELECT i.*, w.name as warehouse_name, u.username as created_by_name,
                   u2.username as completed_by_name
            FROM inventories i
            LEFT JOIN warehouses w ON i.warehouse_id = w.id
            LEFT JOIN users u ON i.created_by = u.id
            LEFT JOIN users u2 ON i.completed_by = u2.id
            WHERE i.id = ?
        """, (id,), fetch_all=False)
        
        if not inventory:
            flash('Инвентаризация не найдена', 'error')
            return redirect(url_for('inventory.inventory_list'))
        
        # Получаем позиции
        items = db.execute_query("""
            SELECT ii.*, n.name as nomenclature_name, n.sku,
                   b.batch_number, sb.code as storage_bin_code,
                   (ii.actual_quantity - ii.expected_quantity) as variance
            FROM inventory_items ii
            LEFT JOIN nomenclatures n ON ii.nomenclature_id = n.id
            LEFT JOIN batches b ON ii.batch_id = b.id
            LEFT JOIN storage_bins sb ON ii.storage_bin_id = sb.id
            WHERE ii.inventory_id = ?
            ORDER BY n.name
        """, (id,), fetch_all=True) or []
        
        # Статистика
        total_items = len(items)
        checked_items = sum(1 for i in items if i['verified'])
        discrepancies = sum(1 for i in items if i['variance'] != 0)
        
        stats = {
            'total': total_items,
            'checked': checked_items,
            'discrepancies': discrepancies,
            'percentage': (checked_items / total_items * 100) if total_items > 0 else 0
        }
        
        if inventory['status'] == 'in_progress':
            template = 'inventory/count.html'
        else:
            template = 'inventory/results.html'
            
            # Данные для графиков
            matched = total_items - discrepancies
            chart_labels = json.dumps(['Совпало', 'Расхождений'])
            chart_data = json.dumps([matched, discrepancies])
            
            # Топ-10 расхождений
            top_items = sorted([i for i in items if i['variance'] != 0], 
                             key=lambda x: abs(x['variance']), reverse=True)[:10]
            top_labels = json.dumps([i['nomenclature_name'][:20] + '...' for i in top_items])
            top_data = json.dumps([i['variance'] for i in top_items])
            
            return render_template(template,
                                 inventory=dict(inventory),
                                 items=[dict(i) for i in items],
                                 stats=stats,
                                 chart_labels=chart_labels,
                                 chart_data=chart_data,
                                 top_labels=top_labels,
                                 top_data=top_data)
        
        return render_template(template,
                             inventory=dict(inventory),
                             items=[dict(i) for i in items],
                             stats=stats)
                             
    except Exception as e:
        logger.error(f'Ошибка просмотра инвентаризации: {e}')
        flash('Ошибка просмотра инвентаризации', 'error')
        return redirect(url_for('inventory.inventory_list'))

@inventory_bp.route('/inventory/<int:id>/start', methods=['POST'], endpoint='start_inventory')
@login_required
def start_inventory(id):
    """Начало инвентаризации"""
    try:
        db = get_db()
        
        db.execute_query("""
            UPDATE inventories 
            SET status = 'in_progress', started_at = CURRENT_TIMESTAMP
            WHERE id = ? AND status = 'draft'
        """, (id,))
        
        flash('Инвентаризация начата', 'success')
        return redirect(url_for('inventory.view_inventory', id=id))
        
    except Exception as e:
        logger.error(f'Ошибка начала инвентаризации: {e}')
        flash('Ошибка начала инвентаризации', 'error')
        return redirect(url_for('inventory.inventory_list'))

@inventory_bp.route('/inventory/<int:id>/complete', methods=['POST'], endpoint='complete_inventory')
@login_required
def complete_inventory(id):
    """Завершение инвентаризации"""
    try:
        db = get_db()
        
        # Проверяем, все ли позиции проверены
        unchecked = db.execute_query("""
            SELECT COUNT(*) as cnt FROM inventory_items 
            WHERE inventory_id = ? AND verified = 0
        """, (id,), fetch_all=False)
        
        if unchecked and unchecked['cnt'] > 0:
            return jsonify({'success': False, 'error': 'Не все позиции проверены'})
        
        db.execute_query("""
            UPDATE inventories 
            SET status = 'completed', completed_at = CURRENT_TIMESTAMP, completed_by = ?
            WHERE id = ?
        """, (session['user_id'], id))
        
        return jsonify({'success': True, 'message': 'Инвентаризация завершена'})
        
    except Exception as e:
        logger.error(f'Ошибка завершения инвентаризации: {e}')
        return jsonify({'success': False, 'error': str(e)})

@inventory_bp.route('/inventory/item/<int:item_id>/update', methods=['POST'], endpoint='update_inventory_item')
@login_required
def update_inventory_item(item_id):
    """Обновление фактического количества"""
    try:
        db = get_db()
        data = request.json
        actual_quantity = data.get('actual_quantity')

        if actual_quantity is None:
            return jsonify({'success': False, 'error': 'Не указано количество'})

        try:
            actual_quantity = float(actual_quantity)
            if actual_quantity < 0:
                return jsonify({'success': False, 'error': 'Количество не может быть отрицательным'})
        except (ValueError, TypeError):
            return jsonify({'success': False, 'error': 'Некорректное значение количества'})
        
        db.execute_query("""
            UPDATE inventory_items 
            SET actual_quantity = ?, variance = ? - expected_quantity
            WHERE id = ?
        """, (actual_quantity, actual_quantity, item_id))
        
        return jsonify({'success': True})
        
    except Exception as e:
        logger.error(f'Ошибка обновления позиции: {e}')
        return jsonify({'success': False, 'error': str(e)})

@inventory_bp.route('/inventory/item/<int:item_id>/verify', methods=['POST'], endpoint='verify_inventory_item')
@login_required
def verify_inventory_item(item_id):
    """Подтверждение проверки позиции"""
    try:
        db = get_db()
        
        db.execute_query("""
            UPDATE inventory_items 
            SET verified = 1
            WHERE id = ?
        """, (item_id,))
        
        return jsonify({'success': True})
        
    except Exception as e:
        logger.error(f'Ошибка подтверждения позиции: {e}')
        return jsonify({'success': False, 'error': str(e)})

@inventory_bp.route('/inventory/<int:id>/delete', methods=['POST'], endpoint='delete_inventory')
@login_required
def delete_inventory(id):
    """Удаление инвентаризации"""
    try:
        db = get_db()
        
        # Проверяем статус
        inv = db.execute_query("SELECT status FROM inventories WHERE id = ?", (id,), fetch_all=False)
        if not inv:
            flash('Инвентаризация не найдена', 'error')
            return redirect(url_for('inventory.inventory_list'))
        
        if inv['status'] == 'completed':
            flash('Нельзя удалить завершенную инвентаризацию', 'error')
            return redirect(url_for('inventory.inventory_list'))
        
        # Удаляем связанные позиции
        db.execute_query("DELETE FROM inventory_items WHERE inventory_id = ?", (id,))
        db.execute_query("DELETE FROM inventories WHERE id = ?", (id,))
        
        flash('Инвентаризация удалена', 'success')
        
    except Exception as e:
        logger.error(f'Ошибка удаления инвентаризации: {e}')
        flash('Ошибка удаления инвентаризации', 'error')
    
    return redirect(url_for('inventory.inventory_list'))

@inventory_bp.route('/inventory/<int:id>/adjustment', endpoint='create_adjustment_from_inventory')
@login_required
def create_adjustment_from_inventory(id):
    """Создание документа корректировки по результатам инвентаризации"""
    try:
        db = get_db()
        
        # Получаем расхождения
        discrepancies = db.execute_query("""
            SELECT ii.*, n.id as nomenclature_id, n.name, n.sku
            FROM inventory_items ii
            JOIN nomenclatures n ON ii.nomenclature_id = n.id
            WHERE ii.inventory_id = ? AND ii.variance != 0
        """, (id,), fetch_all=True)
        
        if not discrepancies:
            flash('Нет расхождений для корректировки', 'error')
            return redirect(url_for('inventory.view_inventory', id=id))
        
        # Создаем документ корректировки
        doc_number = f"ADJ-INV-{datetime.now().strftime('%Y%m%d')}-{id}"
        
        items = []
        for d in discrepancies:
            items.append({
                'nomenclature_id': d['nomenclature_id'],
                'quantity': abs(d['variance']),
                'price': 0,
                'notes': f"Корректировка по инвентаризации, расхождение: {d['variance']}"
            })
        
        data = {
            'document_number': doc_number,
            'document_date': datetime.now().strftime('%Y-%m-%d'),
            'reason': f'Корректировка по результатам инвентаризации #{id}',
            'notes': 'Автоматически создано из инвентаризации',
            'items': items
        }
        
        result = db.create_document('adjustment', data, session['user_id'])
        
        if result['success']:
            flash(f'Создан документ корректировки {result["document_number"]}', 'success')
        else:
            flash(result['message'], 'error')
        
        return redirect(url_for('inventory.view_inventory', id=id))
        
    except Exception as e:
        logger.error(f'Ошибка создания корректировки: {e}')
        flash('Ошибка создания корректировки', 'error')
        return redirect(url_for('inventory.view_inventory', id=id))
