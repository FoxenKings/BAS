import logging
import time
import traceback
from datetime import datetime

from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify

from routes.common import login_required, admin_required, get_db
from utils.search import build_where
from services.document_service import (
    get_next_counter_value,
    generate_unique_fallback_number,
    sync_document_counters,
    get_next_sequence_number,
    get_display_number,
    determine_accounting_type,
    parse_form_items as _parse_form_items,
    load_document_form_data as _load_document_form_data,
    insert_document_record as _insert_document_record,
    save_document_items as _save_document_items,
    get_document_for_print as _get_document_for_print,
)

logger = logging.getLogger(__name__)

documents_bp = Blueprint('documents', __name__)


# ============ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ (делегируют в services/document_service.py) ============
# Все хелперы импортированы выше из services.document_service.
# Локальные обёртки ниже не нужны — удалены.


# ============ ВЫДАЧА ИНСТРУМЕНТОВ И МАТЕРИАЛОВ С ПЕЧАТНОЙ ФОРМОЙ ============

@documents_bp.route('/issuance/create', methods=['GET', 'POST'], endpoint='issuance_create')
@login_required
def issuance_create():
    """Создание выдачи (инструменты + материалы)"""
    db = get_db()

    if request.method == 'POST':
        try:
            # ========== СБОР ДАННЫХ ИЗ ФОРМЫ ==========
            # Основная информация о выдаче
            employee_id = request.form.get('employee_id')
            from_warehouse = request.form.get('from_warehouse', 'Основной склад')
            purpose = request.form.get('purpose')
            reason = request.form.get('reason')

            # Сбор позиций
            nomenclature_ids = request.form.getlist('nomenclature_id[]')
            quantities = request.form.getlist('quantity[]')
            units = request.form.getlist('unit[]')
            accounting_types = request.form.getlist('accounting_type[]')

            # Для партионного учета
            batch_numbers = request.form.getlist('batch_number[]')
            expiry_dates = request.form.getlist('expiry_date[]')

            # Для индивидуального учета
            instance_ids = request.form.getlist('instance_id[]')
            inventory_numbers = request.form.getlist('inventory_number[]')

            # Валидация
            if not employee_id:
                flash('Выберите сотрудника', 'error')
                return redirect(request.url)

            if not nomenclature_ids or not any(nomenclature_ids):
                flash('Добавьте хотя бы одну позицию', 'error')
                return redirect(request.url)

            # ========== ОПРЕДЕЛЕНИЕ ТИПА УЧЕТА ==========
            primary_accounting_type = 'quantitative'
            for i in range(len(nomenclature_ids)):
                if i < len(accounting_types) and accounting_types[i] == 'individual':
                    primary_accounting_type = 'individual'
                    break

            # ========== ГЕНЕРАЦИЯ НОМЕРА ДОКУМЕНТА ==========
            number_type = None
            issuance_number = None

            if primary_accounting_type == 'individual':
                issuance_number = get_next_sequence_number('issuance_m11')
                number_type = 'm11'
                doc_number = f"М11-{issuance_number}"
            else:
                issuance_number = get_next_sequence_number('issuance_tn')
                number_type = 'tn'
                doc_number = f"ТН-{issuance_number}"

            # ========== ПОЛУЧАЕМ ID СКЛАДА ==========
            warehouse_id = None
            if from_warehouse:
                warehouse = db.execute_query(
                    "SELECT id FROM warehouses WHERE name = ?",
                    (from_warehouse,), fetch_all=False
                )
                if warehouse:
                    warehouse_id = warehouse['id']

            # ========== НАЧИНАЕМ ТРАНЗАКЦИЮ ==========
            db.connection.execute("BEGIN TRANSACTION")

            # ========== СОЗДАНИЕ ДОКУМЕНТА ==========
            doc_row = db.execute_query("""
                INSERT INTO documents (
                    document_type, document_number, document_date, status,
                    from_warehouse_id, employee_id, purpose, reason,
                    number_type, issuance_number,
                    created_by, created_at
                ) VALUES (?, ?, ?, 'posted', ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                RETURNING id
            """, (
                'issuance',
                doc_number,
                datetime.now().strftime('%Y-%m-%d'),
                warehouse_id,
                employee_id,
                purpose,
                reason,
                number_type,
                issuance_number,
                session['user_id']
            ), fetch_all=False)

            document_id = doc_row['id']
            logger.info(f"Документ создан: ID={document_id}, Номер={doc_number}")

            # ========== ДОБАВЛЕНИЕ ПОЗИЦИЙ ==========
            items_added = 0
            for i in range(len(nomenclature_ids)):
                if not nomenclature_ids[i] or not nomenclature_ids[i].strip():
                    continue

                nomenclature_id = int(nomenclature_ids[i].strip())
                quantity = float(quantities[i]) if i < len(quantities) and quantities[i] else 1
                accounting_type = accounting_types[i] if i < len(accounting_types) else 'quantitative'

                logger.debug(f"\n  Позиция {i+1}: номенклатура {nomenclature_id}, количество {quantity}, тип {accounting_type}")

                # Для количественного учета - списываем со склада
                if accounting_type == 'quantitative':
                    stock = db.execute_query("""
                        SELECT id, quantity FROM stocks
                        WHERE nomenclature_id = ? AND warehouse_id = ?
                    """, (nomenclature_id, warehouse_id), fetch_all=False)

                    if not stock:
                        raise Exception(f"Нет остатка для номенклатуры ID {nomenclature_id}")

                    if stock['quantity'] < quantity:
                        raise Exception(f"Недостаточно товара на складе. Требуется: {quantity}, доступно: {stock['quantity']}")

                    new_qty = stock['quantity'] - quantity
                    if new_qty == 0:
                        db.execute_query("DELETE FROM stocks WHERE id = ?", (stock['id'],))
                    else:
                        db.execute_query("UPDATE stocks SET quantity = ? WHERE id = ?", (new_qty, stock['id']))

                    db.execute_query("""
                        INSERT INTO document_items (
                            document_id, nomenclature_id, quantity, accounting_type
                        ) VALUES (?, ?, ?, ?)
                    """, (document_id, nomenclature_id, quantity, accounting_type))

                # Для партионного учета
                elif accounting_type == 'batch':
                    batch_number = batch_numbers[i] if i < len(batch_numbers) else None

                    if not batch_number:
                        raise Exception(f"Для партионного учета необходимо указать номер партии")

                    batch = db.execute_query("""
                        SELECT b.id FROM batches b
                        WHERE b.nomenclature_id = ? AND b.batch_number = ? AND b.is_active = 1
                    """, (nomenclature_id, batch_number), fetch_all=False)

                    if not batch:
                        raise Exception(f"Партия {batch_number} не найдена")

                    stock = db.execute_query("""
                        SELECT id, quantity FROM stocks
                        WHERE nomenclature_id = ? AND batch_id = ? AND warehouse_id = ?
                    """, (nomenclature_id, batch['id'], warehouse_id), fetch_all=False)

                    if not stock:
                        raise Exception(f"Нет остатка для партии {batch_number}")

                    if stock['quantity'] < quantity:
                        raise Exception(f"Недостаточно товара в партии {batch_number}")

                    new_qty = stock['quantity'] - quantity
                    if new_qty == 0:
                        db.execute_query("DELETE FROM stocks WHERE id = ?", (stock['id'],))
                    else:
                        db.execute_query("UPDATE stocks SET quantity = ? WHERE id = ?", (new_qty, stock['id']))

                    db.execute_query("""
                        INSERT INTO document_items (
                            document_id, nomenclature_id, quantity, accounting_type,
                            batch_id, batch_number
                        ) VALUES (?, ?, ?, ?, ?, ?)
                    """, (document_id, nomenclature_id, quantity, accounting_type, batch['id'], batch_number))

                # Для индивидуального учета (инструменты)
                elif accounting_type == 'individual':
                    instance_id = instance_ids[i] if i < len(instance_ids) else None

                    if not instance_id:
                        raise Exception(f"Для индивидуального учета необходимо выбрать экземпляр")

                    instance = db.execute_query("""
                        SELECT i.id, i.status FROM instances i
                        WHERE i.id = ? AND i.status = 'in_stock'
                    """, (instance_id,), fetch_all=False)

                    if not instance:
                        raise Exception(f"Экземпляр ID {instance_id} не доступен для выдачи")

                    db.execute_query("""
                        UPDATE instances
                        SET status = 'in_use',
                            employee_id = ?,
                            issued_date = CURRENT_DATE,
                            warehouse_id = NULL
                        WHERE id = ?
                    """, (employee_id, instance_id))

                    db.execute_query("""
                        INSERT INTO document_items (
                            document_id, nomenclature_id, instance_id, quantity, accounting_type
                        ) VALUES (?, ?, ?, ?, ?)
                    """, (document_id, nomenclature_id, instance_id, quantity, accounting_type))

                items_added += 1
                logger.info(f"Позиция {i+1} добавлена")

            # ========== ЗАВЕРШАЕМ ТРАНЗАКЦИЮ ==========
            db.connection.commit()

            flash(f'✅ Выдача оформлена. Документ № {doc_number}', 'success')

            return redirect(url_for('documents.documents_list'))

        except Exception as e:
            db.connection.rollback()
            logger.error(f'Ошибка оформления выдачи: {e}')
            traceback.print_exc()
            flash(f'Ошибка оформления выдачи: {str(e)}', 'error')
            return redirect(request.url)

    # ========== GET ЗАПРОС - ПОКАЗЫВАЕМ ФОРМУ ==========

    employees = db.execute_query("""
        SELECT id, last_name || ' ' || first_name || COALESCE(' ' || middle_name, '') as full_name,
               position, employee_number
        FROM employees
        WHERE is_active = 1
        ORDER BY last_name, first_name
    """, fetch_all=True) or []

    warehouses = db.execute_query("SELECT id, name FROM warehouses WHERE is_active = 1", fetch_all=True) or []

    quantitative_items = db.execute_query("""
        SELECT n.id, n.name, n.sku, n.unit,
               COALESCE(s.quantity, 0) as stock_quantity,
               c.name_ru as category_name
        FROM nomenclatures n
        LEFT JOIN stocks s ON n.id = s.nomenclature_id
        LEFT JOIN categories c ON n.category_id = c.id
        WHERE n.accounting_type = 'quantitative'
            AND n.is_active = 1
            AND COALESCE(s.quantity, 0) > 0
        ORDER BY n.name
        LIMIT 500
    """, fetch_all=True) or []

    batch_items = db.execute_query("""
        SELECT n.id, n.name, n.sku, n.unit,
               b.batch_number, b.expiry_date,
               COALESCE(s.quantity, 0) as stock_quantity,
               c.name_ru as category_name
        FROM nomenclatures n
        JOIN batches b ON n.id = b.nomenclature_id
        LEFT JOIN stocks s ON n.id = s.nomenclature_id AND s.batch_id = b.id
        LEFT JOIN categories c ON n.category_id = c.id
        WHERE n.accounting_type = 'batch'
            AND n.is_active = 1
            AND b.is_active = 1
            AND COALESCE(s.quantity, 0) > 0
        ORDER BY n.name, b.expiry_date
    """, fetch_all=True) or []

    individual_items = db.execute_query("""
        SELECT i.id, i.inventory_number, i.serial_number,
               n.id as nomenclature_id, n.name as nomenclature_name, n.sku, n.unit,
               w.name as warehouse_name
        FROM instances i
        JOIN nomenclatures n ON i.nomenclature_id = n.id
        LEFT JOIN warehouses w ON i.warehouse_id = w.id
        WHERE i.status = 'in_stock'
        ORDER BY n.name, i.inventory_number
        LIMIT 500
    """, fetch_all=True) or []

    tools_by_nomenclature = {}
    for tool in individual_items:
        tool_dict = dict(tool)
        nomen_id = tool_dict['nomenclature_id']
        if nomen_id not in tools_by_nomenclature:
            tools_by_nomenclature[nomen_id] = {
                'id': nomen_id,
                'name': tool_dict['nomenclature_name'],
                'sku': tool_dict['sku'],
                'unit': tool_dict['unit'],
                'instances': []
            }
        tools_by_nomenclature[nomen_id]['instances'].append(tool_dict)

    tools_list = sorted(tools_by_nomenclature.values(), key=lambda x: x['name'])

    employees_list = [dict(e) for e in employees]
    warehouses_list = [{'id': w['id'], 'name': w['name']} for w in warehouses]
    quantitative_list = [dict(i) for i in quantitative_items]
    batch_list = [dict(i) for i in batch_items]

    return render_template('issuance/create.html',
                         employees=employees_list,
                         warehouses=warehouses_list,
                         quantitative_items=quantitative_list,
                         batch_items=batch_list,
                         tools_list=tools_list)


@documents_bp.route('/issuance/receipt/<int:id>', endpoint='issuance_receipt')
@login_required
def issuance_receipt(id):
    """Печатная форма Требование-накладная"""
    try:
        db = get_db()

        document = db.execute_query("""
            SELECT d.*,
                   d.number_type, d.issuance_number,
                   u.username as created_by_name,
                   e.last_name || ' ' || e.first_name || COALESCE(' ' || e.middle_name, '') as employee_name,
                   e.position as employee_position,
                   e.employee_number,
                   dpt.name as department_name,
                   d.reason,
                   d.purpose_comment,
                   ep.name as purpose_name,
                   ep.code as purpose_code,
                   ep.category as purpose_category
            FROM documents d
            LEFT JOIN users u ON d.created_by = u.id
            LEFT JOIN employees e ON d.employee_id = e.id
            LEFT JOIN departments dpt ON e.department_id = dpt.id
            LEFT JOIN expense_purposes ep ON d.purpose_id = ep.id
            WHERE d.id = ? AND d.document_type = 'issuance'
        """, (id,), fetch_all=False)

        if not document:
            flash('Документ выдачи не найден', 'error')
            return redirect(url_for('documents.issuance_create'))

        document = dict(document)

        items = db.execute_query("""
            SELECT di.*,
                   n.name as nomenclature_name,
                   n.sku,
                   n.unit,
                   n.accounting_type,
                   i.inventory_number,
                   i.serial_number,
                   COALESCE(di.price, i.purchase_price, 0) as price,
                   b.batch_number,
                   b.expiry_date
            FROM document_items di
            JOIN nomenclatures n ON di.nomenclature_id = n.id
            LEFT JOIN instances i ON di.instance_id = i.id
            LEFT JOIN batches b ON di.batch_id = b.id
            WHERE di.document_id = ?
            ORDER BY di.id
        """, (id,), fetch_all=True) or []

        items_list = []
        has_individual = False

        for idx, item in enumerate(items, 1):
            item_dict = dict(item)
            item_dict['position'] = idx
            items_list.append(item_dict)

            if item_dict.get('accounting_type') == 'individual':
                has_individual = True

        display_number = get_display_number(document)

        issued_by_data = db.execute_query("""
            SELECT u.username,
                   COALESCE(e.last_name || ' ' || e.first_name, u.username) as full_name
            FROM users u
            LEFT JOIN employees e ON u.employee_id = e.id
            WHERE u.id = ?
        """, (document['created_by'],), fetch_all=False)

        if issued_by_data:
            issued_by = dict(issued_by_data)
        else:
            issued_by = {'full_name': 'Васецкий Г.О.', 'username': 'admin'}

        warehouse_name = "Основной склад"
        if document.get('from_warehouse_id'):
            warehouse = db.execute_query(
                "SELECT name FROM warehouses WHERE id = ?",
                (document['from_warehouse_id'],), fetch_all=False
            )
            if warehouse:
                warehouse_name = warehouse['name']

        if document.get('document_date'):
            try:
                doc_date_obj = datetime.strptime(document['document_date'], '%Y-%m-%d')
                doc_date_display = doc_date_obj.strftime('%d.%m.%Y')
            except Exception:
                doc_date_display = datetime.now().strftime('%d.%m.%Y')
        else:
            doc_date_display = datetime.now().strftime('%d.%m.%Y')

        purpose_text = ""
        if document.get('purpose_name'):
            purpose_text = document['purpose_name']
            if document.get('purpose_code'):
                purpose_text += f" ({document['purpose_code']})"

        if document.get('purpose_comment'):
            purpose_text = purpose_text + f": {document['purpose_comment']}" if purpose_text else document['purpose_comment']

        issuance_type_display = ""
        if document.get('issuance_type'):
            issuance_types = {
                'employee': 'Сотруднику',
                'department': 'В подразделение (начальнику цеха)',
                'production': 'На производство',
                'own_needs': 'На собственные нужды'
            }
            issuance_type_display = issuance_types.get(document['issuance_type'], document['issuance_type'])

        recipient_name = document.get('employee_name', '')
        if not recipient_name and document.get('employee_id'):
            emp = db.execute_query(
                "SELECT last_name || ' ' || first_name || COALESCE(' ' || middle_name, '') as full_name FROM employees WHERE id = ?",
                (document['employee_id'],), fetch_all=False
            )
            if emp:
                recipient_name = emp['full_name']

        receipt_data = {
            'document': document,
            'items': items_list,
            'issued_by': issued_by,
            'warehouse_name': warehouse_name,
            'doc_number': display_number,
            'doc_number_raw': document.get('issuance_number', document.get('document_number')),
            'doc_date': doc_date_display,
            'purpose_text': purpose_text,
            'issuance_type_display': issuance_type_display,
            'recipient_name': recipient_name,
            'employee_name': recipient_name,
            'has_individual': has_individual,
            'number_type': document.get('number_type'),
            'issuance_number': document.get('issuance_number')
        }

        if has_individual:
            template = 'issuance/receipt_individual.html'
        else:
            template = 'issuance/receipt_quantitative.html'

        return render_template(template, **receipt_data)

    except Exception as e:
        logger.error(f'Ошибка загрузки квитанции: {e}')
        traceback.print_exc()
        flash('Ошибка загрузки квитанции', 'error')
        return redirect(url_for('documents.issuance_create'))


# ============ ДОКУМЕНТЫ ============

@documents_bp.route('/documents', endpoint='documents_list')
@login_required
def documents_list():
    """Список документов с расширенной фильтрацией"""
    try:
        db = get_db()

        doc_type = request.args.get('type', '') or request.args.get('doc_type', '')
        doc_number = request.args.get('doc_number', '') or request.args.get('search', '')
        date_from = request.args.get('date_from', '')
        date_to = request.args.get('date_to', '')
        issuance_type = request.args.get('issuance_type', '')
        employee_id = request.args.get('employee_id', '')
        purpose_id = request.args.get('purpose_id', '')
        from_warehouse_id = request.args.get('from_warehouse_id', '')
        to_warehouse_id = request.args.get('to_warehouse_id', '')
        status = request.args.get('status', '')

        query = """
            SELECT
                d.*,
                u.username as created_by_name,
                w_from.name as from_warehouse_name,
                l_from.name as from_location_name,
                s.name as supplier_name,
                w_to.name as to_warehouse_name,
                l_to.name as to_location_name,
                e.id as employee_id,
                e.last_name || ' ' || e.first_name || COALESCE(' ' || e.middle_name, '') as employee_name,
                e.position as employee_position,
                dpt.name as department_name,
                dpt.id as department_id,
                emp.full_name as department_manager,
                ep.name as purpose_name,
                ep.code as purpose_code,
                ep.category as purpose_category,
                ep.description as purpose_description

            FROM documents d
            LEFT JOIN users u ON d.created_by = u.id
            LEFT JOIN warehouses w_from ON d.from_warehouse_id = w_from.id
            LEFT JOIN warehouses w_to ON d.to_warehouse_id = w_to.id
            LEFT JOIN locations l_from ON d.from_location_id = l_from.id
            LEFT JOIN locations l_to ON d.to_location_id = l_to.id
            LEFT JOIN suppliers s ON d.supplier_id = s.id
            LEFT JOIN employees e ON d.employee_id = e.id
            LEFT JOIN departments dpt ON d.department_id = dpt.id
            LEFT JOIN employees emp ON dpt.manager_id = emp.id
            LEFT JOIN expense_purposes ep ON d.purpose_id = ep.id
            WHERE 1=1
        """
        params = []

        if doc_type:
            query += " AND d.document_type = ?"
            params.append(doc_type)

        if doc_number:
            query += build_where(
                ['LOWER(d.document_number)'],
                doc_number, params
            )

        if date_from:
            query += " AND DATE(d.document_date) >= DATE(?)"
            params.append(date_from)

        if date_to:
            query += " AND DATE(d.document_date) <= DATE(?)"
            params.append(date_to)

        if issuance_type:
            query += " AND d.issuance_type = ?"
            params.append(issuance_type)

        if employee_id and employee_id.isdigit():
            query += " AND d.employee_id = ?"
            params.append(int(employee_id))

        if purpose_id and purpose_id.isdigit():
            query += " AND d.purpose_id = ?"
            params.append(int(purpose_id))

        if from_warehouse_id and from_warehouse_id.isdigit():
            query += " AND d.from_warehouse_id = ?"
            params.append(int(from_warehouse_id))

        if to_warehouse_id and to_warehouse_id.isdigit():
            query += " AND d.to_warehouse_id = ?"
            params.append(int(to_warehouse_id))

        if status:
            query += " AND d.status = ?"
            params.append(status)

        # Пагинация
        page = request.args.get('page', 1, type=int)
        per_page = 50
        if page < 1:
            page = 1

        count_query = "SELECT COUNT(*) as cnt FROM documents d WHERE 1=1" + query[query.find('WHERE 1=1') + len('WHERE 1=1'):]
        count_query = count_query[:count_query.find('ORDER BY')]
        total_row = db.execute_query(
            "SELECT COUNT(*) as cnt FROM (" + query + ") sub",
            params, fetch_all=False
        )
        total = total_row['cnt'] if total_row else 0
        pages = max(1, (total + per_page - 1) // per_page)
        if page > pages:
            page = pages
        offset = (page - 1) * per_page

        query += " ORDER BY d.document_date DESC, d.created_at DESC LIMIT ? OFFSET ?"

        documents = db.execute_query(query, params + [per_page, offset], fetch_all=True)

        documents_list_result = []
        if documents:
            for row in documents:
                doc_dict = dict(row)
                documents_list_result.append(doc_dict)

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

        logger.debug(f"Найдено документов: {total}, страница {page}/{pages}")

        employees = db.execute_query("""
            SELECT id, last_name || ' ' || first_name || COALESCE(' ' || middle_name, '') as full_name
            FROM employees WHERE is_active = 1 ORDER BY last_name
        """, fetch_all=True) or []

        purposes = db.execute_query("""
            SELECT id, name, code FROM expense_purposes WHERE is_active = 1 ORDER BY name
        """, fetch_all=True) or []

        warehouses = db.execute_query("""
            SELECT id, name FROM warehouses WHERE is_active = 1 ORDER BY name
        """, fetch_all=True) or []

        return render_template('documents/list.html',
                             documents=documents_list_result,
                             employees=[dict(e) for e in employees],
                             purposes=[dict(p) for p in purposes],
                             warehouses=[dict(w) for w in warehouses],
                             pagination=pagination,
                             current_filters={
                                 'type': doc_type,
                                 'doc_number': doc_number,
                                 'date_from': date_from,
                                 'date_to': date_to,
                                 'issuance_type': issuance_type,
                                 'employee_id': employee_id,
                                 'purpose_id': purpose_id,
                                 'from_warehouse_id': from_warehouse_id,
                                 'to_warehouse_id': to_warehouse_id,
                                 'status': status
                             })
    except Exception as e:
        logger.error(f'Ошибка загрузки документов: {e}')
        traceback.print_exc()
        flash('Ошибка загрузки документов', 'error')
        return redirect(url_for('dashboard'))


@documents_bp.route('/documents/add/<doc_type>', methods=['GET', 'POST'], endpoint='add_document')
@login_required
def add_document(doc_type):
    """Создание нового документа"""
    db = get_db()

    doc_types_rus = {
        'receipt': 'Поступление',
        'transfer': 'Перемещение',
        'issuance': 'Выдача',
        'return': 'Возврат',
        'write_off': 'Списание',
        'adjustment': 'Корректировка'
    }

    if request.method == 'POST':
        try:
            valid_items = _parse_form_items(request.form)

            if not valid_items:
                flash('Добавьте хотя бы одну позицию с количеством больше 0', 'error')
                return redirect(request.url)

            # Генерация номера документа
            number_type = None
            issuance_number = None
            document_number = request.form.get('document_number')
            document_date = request.form.get('document_date')

            if not document_number:
                if doc_type == 'issuance':
                    acc_type = determine_accounting_type(valid_items)
                    if acc_type == 'individual':
                        issuance_number = get_next_sequence_number('issuance_m11')
                        number_type = 'm11'
                        doc_number = f"М11-{issuance_number}"
                    else:
                        issuance_number = get_next_sequence_number('issuance_tn')
                        number_type = 'tn'
                        doc_number = f"ТН-{issuance_number}"
                else:
                    date_part = datetime.now().strftime('%Y%m%d')
                    time_part = int(time.time()) % 10000
                    doc_number = f"{doc_type.upper()}-{date_part}-{time_part:04d}"
            else:
                doc_number = document_number

            document_id = _insert_document_record(
                db, doc_type, doc_number, document_date, request.form,
                session['user_id'], number_type=number_type, issuance_number=issuance_number
            )
            logger.info(f"Документ создан: ID={document_id}, Номер={doc_number}")

            _save_document_items(db, document_id, valid_items)
            db.connection.commit()

            check_items = db.execute_query(
                "SELECT COUNT(*) as cnt FROM document_items WHERE document_id = ?",
                (document_id,), fetch_all=False
            )
            if check_items and check_items['cnt'] > 0:
                flash(f'Документ {doc_number} успешно создан с {check_items["cnt"]} позициями', 'success')
            else:
                flash('Документ создан, но позиции не сохранились!', 'error')

            if 'post' in request.form:
                try:
                    post_result = db.post_document(document_id, session['user_id'])
                    if post_result.get('success'):
                        flash('Документ проведен', 'success')
                    else:
                        flash(f'Ошибка при проведении: {post_result.get("message")}', 'warning')
                except Exception as e:
                    flash(f'Ошибка при проведении: {str(e)}', 'warning')

            return redirect(url_for('documents.documents_list'))

        except Exception as e:
            logger.error(f'Ошибка создания документа: {e}')
            traceback.print_exc()
            flash(f'Ошибка создания документа: {str(e)}', 'error')
            return redirect(url_for('documents.add_document', doc_type=doc_type))

    # GET запрос
    form_data = _load_document_form_data(db)
    return render_template('documents/form.html',
                         title=f'Новый документ: {doc_types_rus.get(doc_type, doc_type)}',
                         doc_type=doc_type,
                         doc_type_rus=doc_types_rus.get(doc_type, doc_type),
                         document=None,
                         document_items=[],
                         **form_data)


@documents_bp.route('/documents/edit_simple/<int:id>', methods=['GET', 'POST'], endpoint='edit_document_simple')
@login_required
def edit_document_simple(id):
    """Упрощенное редактирование документа"""
    db = get_db()

    doc = db.execute_query("SELECT * FROM documents WHERE id = ?", (id,), fetch_all=False)
    if not doc:
        flash('Документ не найден', 'error')
        return redirect(url_for('documents.documents_list'))

    doc_dict = dict(doc)

    if request.method == 'POST':
        try:
            db.execute_query("""
                UPDATE documents
                SET updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (id,))

            flash('Документ обновлен', 'success')
            return redirect(url_for('documents.documents_list'))
        except Exception as e:
            flash(f'Ошибка: {str(e)}', 'error')

    items = db.execute_query("""
        SELECT di.*, n.name as nomenclature_name
        FROM document_items di
        JOIN nomenclatures n ON di.nomenclature_id = n.id
        WHERE di.document_id = ?
    """, (id,), fetch_all=True)

    items_list = [dict(item) for item in items] if items else []

    return render_template('documents/form_simple.html',
                         title=f'Редактирование документа {doc_dict["document_number"]}',
                         document=doc_dict,
                         document_items=items_list)


@documents_bp.route('/documents/<int:id>/edit', methods=['GET', 'POST'], endpoint='edit_document')
@login_required
def edit_document(id):
    """Редактирование документа"""
    db = get_db()

    doc = db.execute_query("SELECT * FROM documents WHERE id = ?", (id,), fetch_all=False)
    if not doc:
        flash('Документ не найден', 'error')
        return redirect(url_for('documents.documents_list'))

    doc_dict = dict(doc)

    if doc_dict['status'] != 'draft':
        flash('Можно редактировать только черновики', 'error')
        return redirect(url_for('documents.documents_list'))

    if request.method == 'POST':
        try:
            items = _parse_form_items(request.form, include_prices=True)

            if not items:
                flash('Добавьте хотя бы одну позицию с количеством больше 0', 'error')
                return redirect(request.url)

            f = request.form
            data = {
                'document_date': f.get('document_date'),
                'supplier_id': f.get('supplier_id') or None,
                'employee_id': f.get('employee_id') or None,
                'department_id': f.get('department_id') or None,
                'from_warehouse_id': f.get('from_warehouse_id') or None,
                'to_warehouse_id': f.get('to_warehouse_id') or None,
                'issuance_type': f.get('issuance_type') or None,
                'purpose_id': f.get('purpose_id') or None,
                'purpose_comment': f.get('purpose_comment'),
                'cost_center_id': f.get('cost_center_id') or None,
                'reason': f.get('reason'),
                'notes': f.get('notes'),
                'items': items,
                'purposes': [item.get('purpose') for item in items]
            }

            result = db.update_document(id, data, session['user_id'])

            if result['success']:
                flash(result['message'], 'success')

                if 'post' in request.form:
                    post_result = db.post_document(id, session['user_id'])
                    if post_result['success']:
                        flash('Документ проведен', 'success')

                return redirect(url_for('documents.documents_list'))
            else:
                flash(result['message'], 'error')

        except Exception as e:
            logger.error(f'Ошибка обновления документа: {e}')
            traceback.print_exc()
            flash('Ошибка обновления документа', 'error')

    # ========== GET ЗАПРОС ==========

    items_rows = db.execute_query("""
        SELECT di.*, n.name as nomenclature_name, n.sku,
               b.batch_number, i.inventory_number,
               n.accounting_type, n.unit,
               di.batch_number as item_batch_number,
               di.expiry_date,
               di.serial_number,
               di.inventory_number as item_inventory_number,
               di.accounting_type as item_accounting_type
        FROM document_items di
        LEFT JOIN nomenclatures n ON di.nomenclature_id = n.id
        LEFT JOIN batches b ON di.batch_id = b.id
        LEFT JOIN instances i ON di.instance_id = i.id
        WHERE di.document_id = ?
    """, (id,), fetch_all=True)

    document_items = []
    if items_rows:
        for row in items_rows:
            item = dict(row)
            item['has_expiry'] = item.get('expiry_date') is not None
            document_items.append(item)

    nomenclatures_rows = db.search_nomenclatures(limit=1000)
    nomenclatures = []
    for n in nomenclatures_rows:
        if isinstance(n, dict):
            nomenclatures.append(n)
        else:
            nomenclatures.append(dict(n))

    warehouses_rows = db.execute_query("SELECT id, name FROM warehouses WHERE is_active = 1", fetch_all=True) or []
    warehouses = []
    for w in warehouses_rows:
        if isinstance(w, dict):
            warehouses.append(w)
        else:
            warehouses.append(dict(w))

    suppliers_rows = db.execute_query("SELECT id, name FROM suppliers WHERE is_active = 1", fetch_all=True) or []
    suppliers = []
    for s in suppliers_rows:
        if isinstance(s, dict):
            suppliers.append(s)
        else:
            suppliers.append(dict(s))

    employees_rows = db.execute_query("""
        SELECT id, last_name, first_name, middle_name,
               last_name || ' ' || first_name || COALESCE(' ' || middle_name, '') as full_name,
               position
        FROM employees
        WHERE is_active = 1
        ORDER BY last_name, first_name
    """, fetch_all=True) or []

    employees = []
    for e in employees_rows:
        if isinstance(e, dict):
            emp_dict = e
        else:
            emp_dict = dict(e)

        if not emp_dict.get('full_name'):
            parts = [emp_dict.get('last_name', ''), emp_dict.get('first_name', '')]
            if emp_dict.get('middle_name'):
                parts.append(emp_dict['middle_name'])
            emp_dict['full_name'] = ' '.join(parts).strip()

        employees.append(emp_dict)

    departments_rows = db.execute_query("""
        SELECT d.*, e.full_name as manager_name
        FROM departments d
        LEFT JOIN employees e ON d.manager_id = e.id
        WHERE d.is_active = 1
        ORDER BY d.name
    """, fetch_all=True) or []

    departments = []
    for d in departments_rows:
        if isinstance(d, dict):
            departments.append(d)
        else:
            departments.append(dict(d))

    purposes_rows = []
    if hasattr(db, 'get_expense_purposes'):
        purposes_data = db.get_expense_purposes(active_only=True)
        for p in purposes_data:
            if isinstance(p, dict):
                purposes_rows.append(p)
            else:
                purposes_rows.append(dict(p))

    purposes = purposes_rows

    cost_centers = [
        {'id': 'production_materials', 'name': 'Основное производство (материалы)'},
        {'id': 'auxiliary_materials', 'name': 'Вспомогательные материалы'},
        {'id': 'repair', 'name': 'Ремонт и обслуживание'},
        {'id': 'operating_expenses', 'name': 'Эксплуатационные расходы'},
        {'id': 'administration', 'name': 'Административные нужды'},
        {'id': 'rnd', 'name': 'НИОКР и эксперименты'},
    ]

    categories = db.get_all_categories()

    doc_types_rus = {
        'receipt': 'Поступление',
        'transfer': 'Перемещение',
        'issuance': 'Выдача',
        'write_off': 'Списание',
        'return': 'Возврат',
        'adjustment': 'Корректировка'
    }

    return render_template('documents/form.html',
                         title=f'Редактирование документа: {doc_dict["document_number"]}',
                         doc_type=doc_dict['document_type'],
                         doc_type_rus=doc_types_rus.get(doc_dict['document_type'], doc_dict['document_type']),
                         document=doc_dict,
                         document_items=document_items,
                         nomenclatures=nomenclatures,
                         categories=categories,
                         warehouses=warehouses,
                         suppliers=suppliers,
                         employees=employees,
                         departments=departments,
                         purposes=purposes,
                         cost_centers=cost_centers)


@documents_bp.route('/documents/<int:id>/view', endpoint='view_document')
@login_required
def view_document(id):
    """Просмотр документа"""
    try:
        db = get_db()

        doc = db.execute_query("""
            SELECT d.*,
                   u.username as created_by_name,
                   u2.username as posted_by_name,
                   w_from.name as from_warehouse_name,
                   w_to.name as to_warehouse_name,
                   l_from.name as from_location_name,
                   l_to.name as to_location_name,
                   s.name as supplier_name,
                   e.last_name || ' ' || e.first_name || COALESCE(' ' || e.middle_name, '') as employee_name,
                   e.position as employee_position,
                   dep.name as employee_department,
                   dpt.name as department_name,
                   emp.full_name as department_manager,
                   ep.name as purpose_name,
                   ep.code as purpose_code,
                   ep.description as purpose_description,
                   ep.category as purpose_category
            FROM documents d
            LEFT JOIN users u ON d.created_by = u.id
            LEFT JOIN users u2 ON d.posted_by = u2.id
            LEFT JOIN warehouses w_from ON d.from_warehouse_id = w_from.id
            LEFT JOIN warehouses w_to ON d.to_warehouse_id = w_to.id
            LEFT JOIN locations l_from ON d.from_location_id = l_from.id
            LEFT JOIN locations l_to ON d.to_location_id = l_to.id
            LEFT JOIN suppliers s ON d.supplier_id = s.id
            LEFT JOIN employees e ON d.employee_id = e.id
            LEFT JOIN departments dep ON e.department_id = dep.id
            LEFT JOIN departments dpt ON d.department_id = dpt.id
            LEFT JOIN employees emp ON dpt.manager_id = emp.id
            LEFT JOIN expense_purposes ep ON d.purpose_id = ep.id
            WHERE d.id = ?
        """, (id,), fetch_all=False)

        if not doc:
            flash('Документ не найден', 'error')
            return redirect(url_for('documents.documents_list'))

        document = dict(doc)

        items_rows = db.execute_query("""
            SELECT
                di.*,
                n.name as nomenclature_name,
                n.sku,
                n.unit,
                b.batch_number,
                i.inventory_number,
                i.serial_number,
                di.purpose,
                di.price,
                di.quantity,
                nv.id as variation_id,
                nv.size,
                nv.color,
                nv.sku as variation_sku
            FROM document_items di
            LEFT JOIN nomenclatures n ON di.nomenclature_id = n.id
            LEFT JOIN batches b ON di.batch_id = b.id
            LEFT JOIN instances i ON di.instance_id = i.id
            LEFT JOIN nomenclature_variations nv ON nv.id = di.variation_id
            WHERE di.document_id = ?
            ORDER BY di.id
        """, (id,), fetch_all=True)

        items = []
        total_sum = 0

        if items_rows:
            for row in items_rows:
                item = dict(row)
                if item.get('price') is None:
                    item['price'] = 0
                total_sum += float(item['price']) * float(item['quantity'])
                items.append(item)

        summary_by_purpose = {}
        for item in items:
            purpose = item.get('purpose')
            if purpose:
                quantity = item.get('quantity', 0)
                if purpose not in summary_by_purpose:
                    summary_by_purpose[purpose] = 0
                summary_by_purpose[purpose] += quantity

        return render_template('documents/view.html',
                             document=document,
                             items=items,
                             doc_type=document['document_type'],
                             summary_by_purpose=summary_by_purpose,
                             total_sum=total_sum)

    except Exception as e:
        logger.error(f'Ошибка просмотра документа: {e}')
        traceback.print_exc()
        flash('Ошибка просмотра документа', 'error')
        return redirect(url_for('documents.documents_list'))


@documents_bp.route('/documents/<int:id>/print/issuance-act', endpoint='print_issuance_act')
@login_required
def print_issuance_act(id):
    """Печать акта выдачи инструмента (индивидуальный учёт)"""
    try:
        db = get_db()
        document, items = _get_document_for_print(db, id)
        if not document:
            flash('Документ не найден', 'error')
            return redirect(url_for('documents.documents_list'))
        return render_template('documents/print_issuance_act.html',
                               document=document, items=items)
    except Exception as e:
        logger.error(f'Ошибка печати акта выдачи: {e}')
        flash('Ошибка формирования акта выдачи', 'error')
        return redirect(url_for('documents.view_document', id=id))


@documents_bp.route('/documents/<int:id>/print/demand-invoice', endpoint='print_demand_invoice')
@login_required
def print_demand_invoice(id):
    """Требование-накладная (для количественного и партионного учёта)"""
    try:
        db = get_db()
        document, items = _get_document_for_print(db, id)
        if not document:
            flash('Документ не найден', 'error')
            return redirect(url_for('documents.documents_list'))
        return render_template('documents/print_demand_invoice.html',
                               document=document, items=items)
    except Exception as e:
        logger.error(f'Ошибка печати требования-накладной: {e}')
        flash('Ошибка формирования требования-накладной', 'error')
        return redirect(url_for('documents.view_document', id=id))


@documents_bp.route('/documents/<int:id>/print/delivery-receipt', endpoint='print_delivery_receipt')
@login_required
def print_delivery_receipt(id):
    """Квитанция на доставку"""
    try:
        db = get_db()
        document, items = _get_document_for_print(db, id)
        if not document:
            flash('Документ не найден', 'error')
            return redirect(url_for('documents.documents_list'))
        return render_template('documents/print_delivery_receipt.html',
                               document=document, items=items)
    except Exception as e:
        logger.error(f'Ошибка печати квитанции: {e}')
        flash('Ошибка формирования квитанции на доставку', 'error')
        return redirect(url_for('documents.view_document', id=id))


@documents_bp.route('/documents/<int:id>/post', methods=['POST'], endpoint='post_document')
@login_required
def post_document(id):
    """Проведение документа с обновлением остатков"""
    try:
        db = get_db()
        logger.debug(f"=== ПРОВЕДЕНИЕ ДОКУМЕНТА ID {id} ===")

        doc = db.execute_query("SELECT * FROM documents WHERE id = ?", (id,), fetch_all=False)
        if not doc:
            return jsonify({'success': False, 'error': 'Документ не найден'})

        if doc['status'] != 'draft':
            return jsonify({'success': False, 'error': 'Документ уже проведен'})

        items = db.execute_query("""
            SELECT di.*, n.accounting_type, n.name as nomenclature_name, n.unit
            FROM document_items di
            JOIN nomenclatures n ON di.nomenclature_id = n.id
            WHERE di.document_id = ?
        """, (id,), fetch_all=True)

        if not items:
            return jsonify({'success': False, 'error': 'Нет позиций для проведения'})

        db.connection.execute("BEGIN TRANSACTION")

        try:
            for item in items:
                if doc['document_type'] == 'receipt':
                    warehouse_id = doc['to_warehouse_id']
                    if not warehouse_id:
                        raise Exception("Не указан склад получатель")

                    if item['accounting_type'] == 'quantitative':
                        existing = db.execute_query("""
                            SELECT id, quantity FROM stocks
                            WHERE nomenclature_id = ? AND warehouse_id = ?
                            AND storage_bin_id IS NULL AND batch_id IS NULL
                        """, (item['nomenclature_id'], warehouse_id), fetch_all=False)
                    else:
                        existing = db.execute_query("""
                            SELECT id, quantity FROM stocks
                            WHERE nomenclature_id = ? AND warehouse_id = ?
                            AND (batch_id = ? OR (batch_id IS NULL AND ? IS NULL))
                        """, (item['nomenclature_id'], warehouse_id, item['batch_id'], item['batch_id']), fetch_all=False)

                    if existing:
                        new_quantity = existing['quantity'] + item['quantity']
                        db.execute_query("""
                            UPDATE stocks SET quantity = ?, updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        """, (new_quantity, existing['id']))
                    else:
                        db.execute_query("""
                            INSERT INTO stocks (nomenclature_id, warehouse_id, quantity, created_at, updated_at)
                            VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """, (item['nomenclature_id'], warehouse_id, item['quantity']))

                elif doc['document_type'] == 'issuance':
                    warehouse_id = doc['from_warehouse_id']
                    if not warehouse_id:
                        raise Exception("Не указан склад отправитель")

                    existing = db.execute_query("""
                        SELECT id, quantity FROM stocks
                        WHERE nomenclature_id = ? AND warehouse_id = ?
                          AND (batch_id IS NULL AND ? IS NULL)
                    """, (item['nomenclature_id'], warehouse_id, None), fetch_all=False)

                    if not existing:
                        raise Exception(f"Нет остатка для номенклатуры {item['nomenclature_name']}")

                    if existing['quantity'] < item['quantity']:
                        raise Exception(f"Недостаточно товара. Доступно: {existing['quantity']}, требуется: {item['quantity']}")

                    new_quantity = existing['quantity'] - item['quantity']
                    if new_quantity == 0:
                        db.execute_query("DELETE FROM stocks WHERE id = ?", (existing['id'],))
                    else:
                        db.execute_query("UPDATE stocks SET quantity = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                                        (new_quantity, existing['id']))

                elif doc['document_type'] == 'transfer':
                    from_warehouse = doc['from_warehouse_id']
                    to_warehouse = doc['to_warehouse_id']

                    if not from_warehouse or not to_warehouse:
                        raise Exception("Не указаны склады отправитель и получатель")

                    from_stock = db.execute_query("""
                        SELECT id, quantity FROM stocks
                        WHERE nomenclature_id = ? AND warehouse_id = ?
                          AND (batch_id IS NULL AND ? IS NULL)
                    """, (item['nomenclature_id'], from_warehouse, None), fetch_all=False)

                    if not from_stock or from_stock['quantity'] < item['quantity']:
                        raise Exception(f"Недостаточно товара на складе отправителе")

                    new_from_qty = from_stock['quantity'] - item['quantity']
                    if new_from_qty == 0:
                        db.execute_query("DELETE FROM stocks WHERE id = ?", (from_stock['id'],))
                    else:
                        db.execute_query("UPDATE stocks SET quantity = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                                        (new_from_qty, from_stock['id']))

                    to_stock = db.execute_query("""
                        SELECT id, quantity FROM stocks
                        WHERE nomenclature_id = ? AND warehouse_id = ?
                          AND (batch_id IS NULL AND ? IS NULL)
                    """, (item['nomenclature_id'], to_warehouse, None), fetch_all=False)

                    if to_stock:
                        db.execute_query("UPDATE stocks SET quantity = quantity + ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                                        (item['quantity'], to_stock['id']))
                    else:
                        db.execute_query("""
                            INSERT INTO stocks (nomenclature_id, warehouse_id, quantity, created_at, updated_at)
                            VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """, (item['nomenclature_id'], to_warehouse, item['quantity']))

            db.execute_query("""
                UPDATE documents
                SET status = 'posted', posted_at = CURRENT_TIMESTAMP, posted_by = ?
                WHERE id = ?
            """, (session['user_id'], id))

            db.connection.commit()

            return jsonify({'success': True, 'message': 'Документ проведен, остатки обновлены'})

        except Exception as e:
            db.connection.rollback()
            traceback.print_exc()
            return jsonify({'success': False, 'error': str(e)})

    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)})


# ============ УПРАВЛЕНИЕ ЦЕЛЯМИ РАСХОДОВАНИЯ ============

@documents_bp.route('/expense-purposes', endpoint='expense_purposes_list')
@admin_required
def expense_purposes_list():
    """Список целей расходования (только для админа)"""

    try:
        db = get_db()

        category = request.args.get('category', '')
        active = request.args.get('active', 'true')
        search = request.args.get('search', '')

        active_only = active == 'true'

        purposes = db.get_expense_purposes(
            category=category if category else None,
            active_only=active_only,
            search=search if search else None
        )

        categories = [
            {'id': 'production', 'name': 'Производство'},
            {'id': 'development', 'name': 'Разработка и НИОКР'},
            {'id': 'maintenance', 'name': 'Ремонт и обслуживание'},
            {'id': 'own_needs', 'name': 'Собственные нужды'},
            {'id': 'other', 'name': 'Прочее'}
        ]

        stats = {
            'total': len(purposes),
            'active': sum(1 for p in purposes if p.get('is_active', False)),
            'inactive': sum(1 for p in purposes if not p.get('is_active', False))
        }

        by_category = {}
        for cat in categories:
            cat_id = cat['id']
            by_category[cat_id] = {
                'name': cat['name'],
                'count': sum(1 for p in purposes if p.get('category') == cat_id and p.get('is_active', False))
            }

        return render_template('expense_purposes/list.html',
                             purposes=purposes,
                             categories=categories,
                             stats=stats,
                             by_category=by_category,
                             current_category=category,
                             current_active=active,
                             search=search)

    except Exception as e:
        logger.error(f'Ошибка загрузки целей расходования: {e}')
        traceback.print_exc()
        flash(f'Ошибка загрузки целей расходования: {str(e)}', 'error')
        return redirect(url_for('dashboard'))


@documents_bp.route('/expense-purposes/add', methods=['GET', 'POST'], endpoint='expense_purpose_add')
@admin_required
def expense_purpose_add():
    """Добавление новой цели расходования"""

    db = get_db()

    if request.method == 'POST':
        try:
            data = {
                'code': request.form.get('code'),
                'name': request.form.get('name'),
                'description': request.form.get('description'),
                'category': request.form.get('category'),
                'sort_order': request.form.get('sort_order', 0),
                'is_active': 'is_active' in request.form
            }

            if not data['code'] or not data['name']:
                flash('Код и наименование обязательны', 'error')
                return redirect(url_for('documents.expense_purpose_add'))

            result = db.create_expense_purpose(data, session['user_id'])

            if result['success']:
                flash(result['message'], 'success')
                return redirect(url_for('documents.expense_purposes_list'))
            else:
                flash(result['message'], 'error')

        except Exception as e:
            logger.error(f'Ошибка создания цели: {e}')
            flash('Ошибка создания цели', 'error')

    categories = [
        {'id': 'production', 'name': 'Производство'},
        {'id': 'development', 'name': 'Разработка и НИОКР'},
        {'id': 'maintenance', 'name': 'Ремонт и обслуживание'},
        {'id': 'own_needs', 'name': 'Собственные нужды'},
        {'id': 'other', 'name': 'Прочее'}
    ]

    try:
        result = db.execute_query("SELECT MAX(sort_order) as max_sort FROM expense_purposes", fetch_all=False)
        next_sort = (result['max_sort'] or 0) + 10 if result else 10
    except Exception:
        next_sort = 10

    return render_template('expense_purposes/form.html',
                         title='Новая цель расходования',
                         purpose=None,
                         categories=categories,
                         next_sort=next_sort)


@documents_bp.route('/expense-purposes/<int:id>/edit', methods=['GET', 'POST'], endpoint='expense_purpose_edit')
@admin_required
def expense_purpose_edit(id):
    """Редактирование цели расходования"""

    db = get_db()

    if request.method == 'POST':
        try:
            data = {
                'code': request.form.get('code'),
                'name': request.form.get('name'),
                'description': request.form.get('description'),
                'category': request.form.get('category'),
                'sort_order': request.form.get('sort_order', 0),
                'is_active': 'is_active' in request.form
            }

            if not data['code'] or not data['name']:
                flash('Код и наименование обязательны', 'error')
                return redirect(url_for('documents.expense_purpose_edit', id=id))

            result = db.update_expense_purpose(id, data, session['user_id'])

            if result['success']:
                flash(result['message'], 'success')
                return redirect(url_for('documents.expense_purposes_list'))
            else:
                flash(result['message'], 'error')

        except Exception as e:
            logger.error(f'Ошибка обновления цели: {e}')
            flash('Ошибка обновления цели', 'error')

    purpose = db.get_expense_purpose_by_id(id)
    if not purpose:
        flash('Цель не найдена', 'error')
        return redirect(url_for('documents.expense_purposes_list'))

    categories = [
        {'id': 'production', 'name': 'Производство'},
        {'id': 'development', 'name': 'Разработка и НИОКР'},
        {'id': 'maintenance', 'name': 'Ремонт и обслуживание'},
        {'id': 'own_needs', 'name': 'Собственные нужды'},
        {'id': 'other', 'name': 'Прочее'}
    ]

    return render_template('expense_purposes/form.html',
                         title='Редактирование цели расходования',
                         purpose=purpose,
                         categories=categories)


@documents_bp.route('/expense-purposes/<int:id>/toggle', methods=['POST'], endpoint='expense_purpose_toggle')
@admin_required
def expense_purpose_toggle(id):
    """Активация/деактивация цели"""

    try:
        db = get_db()

        result = db.execute_query("SELECT is_active FROM expense_purposes WHERE id = ?", (id,), fetch_all=False)
        if not result:
            return jsonify({'success': False, 'error': 'Цель не найдена'})

        current = result['is_active']
        new_status = 0 if current else 1

        db.execute_query("UPDATE expense_purposes SET is_active = ? WHERE id = ?", (new_status, id))

        status_text = 'активирована' if new_status else 'деактивирована'

        db.log_user_action(
            user_id=session['user_id'],
            action='toggle',
            entity_type='expense_purpose',
            entity_id=id,
            details=f'Цель расходования {status_text}'
        )

        return jsonify({
            'success': True,
            'is_active': bool(new_status),
            'message': f'Цель {status_text}'
        })

    except Exception as e:
        logger.error(f'Ошибка изменения статуса: {e}')
        return jsonify({'success': False, 'error': str(e)})


@documents_bp.route('/expense-purposes/<int:id>/delete', methods=['POST'], endpoint='expense_purpose_delete')
@admin_required
def expense_purpose_delete(id):
    """Удаление (деактивация) цели расходования"""

    try:
        db = get_db()

        result = db.execute_query("SELECT COUNT(*) as cnt FROM documents WHERE purpose_id = ?", (id,), fetch_all=False)
        used = result['cnt'] if result else 0

        if used > 0:
            flash('Цель используется в документах, удаление невозможно', 'error')
            return redirect(url_for('documents.expense_purposes_list'))

        db.execute_query("UPDATE expense_purposes SET is_active = 0 WHERE id = ?", (id,))

        db.log_user_action(
            user_id=session['user_id'],
            action='delete',
            entity_type='expense_purpose',
            entity_id=id,
            details=f'Удалена цель расходования ID: {id}'
        )

        flash('Цель расходования удалена', 'success')

    except Exception as e:
        logger.error(f'Ошибка удаления цели: {e}')
        flash('Ошибка удаления цели', 'error')

    return redirect(url_for('documents.expense_purposes_list'))


@documents_bp.route('/debug/fix-old-documents', endpoint='debug_fix_old_documents')
@admin_required
def debug_fix_old_documents():
    """Обновление старых документов - добавление модификаций (только в режиме отладки)"""
    from flask import current_app
    if not current_app.debug:
        return jsonify({'error': 'Доступен только в режиме DEBUG'}), 403
    try:
        db = get_db()
        results = []

        old_documents = db.execute_query("""
            SELECT d.id, d.document_number, d.document_date
            FROM documents d
            WHERE d.id <= 62
            ORDER BY d.id
        """, fetch_all=True)

        results.append(f"📋 Найдено старых документов: {len(old_documents)}")

        for doc in old_documents:
            doc_id = doc['id']
            doc_number = doc['document_number']

            items = db.execute_query("""
                SELECT di.id, di.nomenclature_id, di.quantity,
                       n.name as nomenclature_name
                FROM document_items di
                JOIN nomenclatures n ON di.nomenclature_id = n.id
                WHERE di.document_id = ? AND n.id = 900
                ORDER BY di.id
            """, (doc_id,), fetch_all=True)

            if items:
                results.append(f"\n📄 Документ {doc_number} (ID: {doc_id}):")

                variations = db.execute_query("""
                    SELECT id, size FROM nomenclature_variations
                    WHERE nomenclature_id = 900
                    ORDER BY size
                """, fetch_all=True)

                if not variations:
                    results.append(f"  ⚠️ Нет модификаций для номенклатуры 900")
                    continue

                for i, item in enumerate(items):
                    var_index = i % len(variations)
                    variation = variations[var_index]

                    db.execute_query("""
                        UPDATE document_items
                        SET variation_id = ?
                        WHERE id = ?
                    """, (variation['id'], item['id']))

                    results.append(f"  ✅ Позиция {item['id']} ({item['nomenclature_name']}, {item['quantity']} шт.) → размер {variation['size']}")

        results.append(f"\n📦 Обновление экземпляров:")

        instances = db.execute_query("""
            SELECT i.id, i.inventory_number
            FROM instances i
            JOIN nomenclatures n ON i.nomenclature_id = n.id
            WHERE n.id = 900 AND i.variation_id IS NULL
            ORDER BY i.id
        """, fetch_all=True)

        if instances:
            variations = db.execute_query("""
                SELECT id, size FROM nomenclature_variations
                WHERE nomenclature_id = 900
                ORDER BY size
            """, fetch_all=True)

            for i, inst in enumerate(instances):
                var_index = i % len(variations)
                variation = variations[var_index]

                db.execute_query("""
                    UPDATE instances
                    SET variation_id = ?,
                        siz_size = ?,
                        siz_color = 'черный'
                    WHERE id = ?
                """, (variation['id'], variation['size'], inst['id']))

                results.append(f"  ✅ Экземпляр {inst['inventory_number']} → размер {variation['size']}")
        else:
            results.append(f"  ✅ Нет экземпляров без модификаций")

        db.connection.commit()

        html = "<h1>Обновление старых документов и экземпляров</h1>"
        html += "<pre>"
        for r in results:
            html += r + "\n"
        html += "</pre>"
        html += '<p><a href="/documents">Перейти к списку документов</a></p>'

        return html

    except Exception as e:
        return f"❌ Ошибка: {str(e)}"


@documents_bp.route('/debug/fix-document/<int:document_id>', endpoint='debug_fix_specific_document')
@admin_required
def debug_fix_specific_document(document_id):
    """Обновление конкретного документа"""
    from flask import current_app
    if not current_app.debug:
        return jsonify({'error': 'Debug-only endpoint'}), 403
    try:
        db = get_db()
        results = []

        doc = db.execute_query("""
            SELECT id, document_number FROM documents WHERE id = ?
        """, (document_id,), fetch_all=False)

        if not doc:
            return f"Документ {document_id} не найден"

        results.append(f"📄 Документ {doc['document_number']} (ID: {document_id})")

        items = db.execute_query("""
            SELECT di.id, di.nomenclature_id, di.quantity,
                   n.name as nomenclature_name
            FROM document_items di
            JOIN nomenclatures n ON di.nomenclature_id = n.id
            WHERE di.document_id = ? AND n.id = 900
            ORDER BY di.id
        """, (document_id,), fetch_all=True)

        if not items:
            return f"В документе {document_id} нет позиций с номенклатурой 900"

        variations = db.execute_query("""
            SELECT id, size FROM nomenclature_variations
            WHERE nomenclature_id = 900
            ORDER BY size
        """, fetch_all=True)

        for i, item in enumerate(items):
            var_index = i % len(variations)
            variation = variations[var_index]

            db.execute_query("""
                UPDATE document_items
                SET variation_id = ?
                WHERE id = ?
            """, (variation['id'], item['id']))

            results.append(f"  ✅ Позиция {item['id']} ({item['nomenclature_name']}, {item['quantity']} шт.) → размер {variation['size']}")

        db.connection.commit()

        html = f"<h1>Обновление документа {doc['document_number']}</h1>"
        html += "<ul>"
        for r in results[1:]:
            html += f"<li>{r}</li>"
        html += "</ul>"
        html += f'<p><a href="/documents/{document_id}/view">Просмотреть документ</a></p>'

        return html

    except Exception as e:
        return f"❌ Ошибка: {str(e)}"


@documents_bp.route('/documents/<int:id>/delete', methods=['POST'], endpoint='delete_document')
@login_required
def delete_document(id):
    """Удаление документа"""
    try:
        db = get_db()

        doc = db.execute_query("SELECT status FROM documents WHERE id = ?", (id,), fetch_all=False)
        if not doc:
            flash('Документ не найден', 'error')
            return redirect(url_for('documents.documents_list'))

        if doc['status'] == 'posted':
            flash('Нельзя удалить проведенный документ', 'error')
            return redirect(url_for('documents.documents_list'))

        db.execute_query("DELETE FROM documents WHERE id = ?", (id,))
        flash('Документ удален', 'success')

    except Exception as e:
        logger.error(f'Ошибка удаления документа: {e}')
        flash('Ошибка удаления документа', 'error')

    return redirect(url_for('documents.documents_list'))
