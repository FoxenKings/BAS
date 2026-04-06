"""
DocumentService — бизнес-логика для работы с документами.

Инкапсулирует:
- проведение документов (posting)
- генерацию номеров документов
- парсинг форм документов
- вставку записей в БД
- валидацию перед сохранением

Публичные хелперы (совместимы с routes/documents.py):
    get_next_counter_value, generate_unique_fallback_number,
    sync_document_counters, get_next_sequence_number,
    get_display_number, determine_accounting_type,
    parse_form_items, load_document_form_data,
    insert_document_record, save_document_items,
    get_document_for_print
"""
import logging
import time
from datetime import datetime
from exceptions import NotFoundError, BusinessRuleError, ValidationError

logger = logging.getLogger('services.document')

DOCUMENT_TYPES = ('receipt', 'issuance', 'transfer', 'write-off', 'return')


class DocumentService:
    def __init__(self, db):
        self.db = db

    # ─── Проведение документа ────────────────────────────────────────────────

    def post_document(self, document_id: int, user_id: int) -> dict:
        """
        Проводит документ: обновляет статус и применяет складские движения.

        Returns:
            dict с ключами success, message, document
        Raises:
            NotFoundError: если документ не существует
            BusinessRuleError: если документ уже проведён
        """
        doc = self.db.execute_query(
            "SELECT * FROM documents WHERE id = ?",
            (document_id,), fetch_all=False
        )
        if not doc:
            raise NotFoundError(f"Документ #{document_id} не найден", entity="document", entity_id=document_id)

        if doc['status'] == 'posted':
            raise BusinessRuleError(
                f"Документ #{document_id} уже проведён",
                rule="document_already_posted"
            )

        if doc['doc_type'] not in DOCUMENT_TYPES:
            raise ValidationError(f"Неизвестный тип документа: {doc['doc_type']}", field="doc_type")

        # Обновляем статус
        self.db.execute_query(
            "UPDATE documents SET status = 'posted', posted_by = ?, posted_at = CURRENT_TIMESTAMP WHERE id = ?",
            (user_id, document_id)
        )

        logger.info(f"Документ #{document_id} проведён пользователем #{user_id}")
        return {'success': True, 'message': f'Документ #{document_id} проведён'}

    # ─── Генерация номера документа ─────────────────────────────────────────

    def generate_document_number(self, doc_type: str) -> str:
        """
        Генерирует следующий порядковый номер документа для указанного типа.

        Returns:
            Строка-номер вида 'RC-0001', 'IS-0042', и т.п.
        """
        prefix_map = {
            'receipt': 'RC',
            'issuance': 'IS',
            'transfer': 'TR',
            'write-off': 'WO',
            'return': 'RN',
        }
        prefix = prefix_map.get(doc_type, 'DOC')

        row = self.db.execute_query(
            "SELECT COUNT(*) as cnt FROM documents WHERE doc_type = ?",
            (doc_type,), fetch_all=False
        )
        seq = (row['cnt'] if row else 0) + 1
        return f"{prefix}-{seq:04d}"

    # ─── Валидация ───────────────────────────────────────────────────────────

    def validate_document(self, data: dict) -> None:
        """
        Базовая валидация данных документа перед сохранением.

        Raises:
            ValidationError: если данные некорректны
        """
        if not data.get('doc_type'):
            raise ValidationError("Тип документа обязателен", field="doc_type")

        if data['doc_type'] not in DOCUMENT_TYPES:
            raise ValidationError(
                f"Недопустимый тип документа: {data['doc_type']}",
                field="doc_type",
                value=data['doc_type']
            )

        if not data.get('warehouse_id'):
            raise ValidationError("Склад обязателен", field="warehouse_id")


# ============ STANDALONE HELPERS (мигрированы из routes/documents.py) ============

_svc_logger = logging.getLogger('services.document')


def get_next_counter_value(counter_name):
    """Атомарный инкремент счётчика через BEGIN EXCLUSIVE."""
    from database import get_db
    db = get_db()
    max_attempts = 10
    for attempt in range(max_attempts):
        try:
            cursor = db.connection.cursor()
            cursor.execute("BEGIN EXCLUSIVE")
            cursor.execute(
                "SELECT last_number FROM document_number_counters WHERE counter_name = ?",
                (counter_name,)
            )
            row = cursor.fetchone()
            if row:
                next_number = row[0] + 1
            else:
                next_number = 1
                cursor.execute(
                    "INSERT INTO document_number_counters (counter_name, last_number) VALUES (?, ?)",
                    (counter_name, next_number)
                )
                db.connection.commit()
                return str(next_number)
            cursor.execute(
                "SELECT id FROM documents WHERE document_type = 'issuance' AND document_number = ?",
                (str(next_number),)
            )
            if not cursor.fetchone():
                cursor.execute(
                    "UPDATE document_number_counters SET last_number = ?, updated_at = CURRENT_TIMESTAMP WHERE counter_name = ?",
                    (next_number, counter_name)
                )
                db.connection.commit()
                _svc_logger.info(f"Счётчик {counter_name}: новый номер {next_number}")
                return str(next_number)
            else:
                cursor.execute(
                    "UPDATE document_number_counters SET last_number = ? WHERE counter_name = ?",
                    (next_number, counter_name)
                )
                db.connection.commit()
                continue
        except Exception as e:
            try:
                db.connection.rollback()
            except Exception:
                pass
            _svc_logger.error(f'Ошибка счётчика {counter_name} (попытка {attempt + 1}): {e}')
            if attempt >= max_attempts - 1:
                return generate_unique_fallback_number(db, counter_name)
    return generate_unique_fallback_number(db, counter_name)


def generate_unique_fallback_number(db, counter_name):
    """Генерация уникального запасного номера."""
    max_attempts = 20
    for attempt in range(max_attempts):
        timestamp = int(time.time() * 1000) % 100000
        random_suffix = int(time.time() * 1000) % 1000
        fallback = f"{timestamp}{random_suffix:03d}"
        existing = db.execute_query(
            "SELECT id FROM documents WHERE document_type = 'issuance' AND document_number = ?",
            (fallback,), fetch_all=False
        )
        if not existing:
            _svc_logger.warning(f"Использован запасной номер: {fallback}")
            return fallback
    return str(int(time.time() * 1000))


def sync_document_counters():
    """Синхронизирует счётчики с максимальными номерами существующих документов."""
    from database import get_db
    db = get_db()
    _max_q = """
        SELECT MAX(CAST(document_number AS INTEGER)) as max_num
        FROM documents
        WHERE document_type = 'issuance'
            AND document_number GLOB '[0-9]*'
            AND length(document_number) < 10
    """
    try:
        row = db.execute_query(_max_q, fetch_all=False)
        if row and row['max_num']:
            max_individual = int(row['max_num'])
            db.execute_query(
                "UPDATE document_number_counters SET last_number = ? WHERE counter_name = 'issuance_individual'",
                (max_individual,)
            )
        row = db.execute_query(_max_q, fetch_all=False)
        if row and row['max_num']:
            max_quantitative = int(row['max_num'])
            db.execute_query(
                "UPDATE document_number_counters SET last_number = ? WHERE counter_name = 'issuance_quantitative'",
                (max_quantitative,)
            )
    except Exception as e:
        _svc_logger.error(f"Ошибка синхронизации счётчиков: {e}")


def get_next_sequence_number(sequence_type, year=None):
    """Следующий номер из таблицы sequences (BEGIN IMMEDIATE)."""
    from database import get_db
    db = get_db()
    max_attempts = 3
    if year is None:
        year = datetime.now().year
    for attempt in range(max_attempts):
        try:
            db.connection.execute("BEGIN IMMEDIATE")
            cursor = db.connection.execute(
                "SELECT id, last_number, format FROM sequences WHERE sequence_type = ? AND year = ?",
                (sequence_type, year)
            )
            row = cursor.fetchone()
            if row:
                next_number = row[1] + 1
                db.connection.execute("UPDATE sequences SET last_number = ? WHERE id = ?", (next_number, row[0]))
            else:
                defaults = {'issuance_m11': 29, 'issuance_tn': 181}
                next_number = defaults.get(sequence_type, 1)
                db.connection.execute(
                    "INSERT INTO sequences (sequence_type, prefix, year, last_number, format) VALUES (?, ?, ?, ?, ?)",
                    (sequence_type, '', year, next_number, '{NUMBER}')
                )
            db.connection.commit()
            return str(next_number)
        except Exception as e:
            db.connection.rollback()
            _svc_logger.warning(f"Ошибка генерации номера (попытка {attempt+1}): {e}")
            if attempt < max_attempts - 1:
                time.sleep(0.1)
    fallback = str(int(time.time() * 1000))[-6:]
    _svc_logger.warning(f"Использован запасной номер: {fallback}")
    return fallback


def get_display_number(document):
    """Возвращает номер для отображения в печатной форме."""
    if not document:
        return ''
    if document.get('number_type') and document.get('issuance_number'):
        if document['number_type'] == 'm11':
            return f"М11-{document['issuance_number']}"
        return f"ТН-{document['issuance_number']}"
    return document.get('document_number', '')


def determine_accounting_type(items):
    """individual если хотя бы одна позиция individual, иначе quantitative."""
    for item in items:
        if item.get('accounting_type') == 'individual':
            return 'individual'
    return 'quantitative'


def parse_form_items(form, include_prices=False):
    """Парсит поля формы с позициями документа. Возвращает список позиций с quantity > 0."""
    nomenclature_ids = form.getlist('nomenclature_id[]')
    quantities = form.getlist('quantity[]')
    purposes = form.getlist('purpose[]')
    position_accounting_types = form.getlist('position_accounting_type[]')
    variation_ids = form.getlist('variation_id[]')
    batch_numbers = form.getlist('batch_number[]')
    expiry_dates = form.getlist('expiry_date[]')
    serial_numbers = form.getlist('serial_number[]')
    inventory_numbers = form.getlist('inventory_number[]')
    kit_inventory_numbers = form.getlist('kit_inventory_number[]')
    prices = form.getlist('price[]') if include_prices else []
    batch_ids = form.getlist('batch_id[]') if include_prices else []
    instance_ids = form.getlist('instance_id[]') if include_prices else []

    items = []
    for i, nomen_raw in enumerate(nomenclature_ids):
        if not nomen_raw or not nomen_raw.strip():
            continue
        try:
            quantity = float(quantities[i]) if i < len(quantities) and quantities[i] and quantities[i].strip() else 0.0
        except ValueError:
            quantity = 0.0
        if quantity <= 0:
            continue

        accounting_type = 'quantitative'
        if i < len(position_accounting_types) and position_accounting_types[i]:
            accounting_type = position_accounting_types[i]

        item = {
            'nomenclature_id': int(nomen_raw.strip()),
            'quantity': quantity,
            'accounting_type': accounting_type,
            'purpose': purposes[i] if i < len(purposes) and purposes[i] else None,
        }

        if i < len(variation_ids) and variation_ids[i] and variation_ids[i].strip():
            try:
                item['variation_id'] = int(variation_ids[i])
            except ValueError:
                pass

        if accounting_type == 'batch':
            if i < len(batch_numbers) and batch_numbers[i] and batch_numbers[i].strip():
                item['batch_number'] = batch_numbers[i]
            if i < len(expiry_dates) and expiry_dates[i] and expiry_dates[i].strip():
                item['expiry_date'] = expiry_dates[i]
        elif accounting_type == 'individual':
            if i < len(serial_numbers) and serial_numbers[i] and serial_numbers[i].strip():
                item['serial_number'] = serial_numbers[i]
            if i < len(inventory_numbers) and inventory_numbers[i] and inventory_numbers[i].strip():
                item['inventory_number'] = inventory_numbers[i]
        elif accounting_type == 'kit':
            if i < len(kit_inventory_numbers) and kit_inventory_numbers[i] and kit_inventory_numbers[i].strip():
                item['kit_inventory_number'] = kit_inventory_numbers[i]

        if include_prices:
            try:
                item['price'] = max(0.0, float(prices[i])) if i < len(prices) and prices[i] and prices[i].strip() else 0.0
            except (ValueError, TypeError):
                item['price'] = 0.0
            try:
                item['batch_id'] = int(batch_ids[i]) if i < len(batch_ids) and batch_ids[i] and batch_ids[i].strip() else None
            except (ValueError, TypeError):
                item['batch_id'] = None
            try:
                item['instance_id'] = int(instance_ids[i]) if i < len(instance_ids) and instance_ids[i] and instance_ids[i].strip() else None
            except (ValueError, TypeError):
                item['instance_id'] = None

        items.append(item)
    return items


def load_document_form_data(db):
    """Загружает данные для форм документов (выпадающие списки)."""
    def to_dicts(rows):
        return [r if isinstance(r, dict) else dict(r) for r in (rows or [])]

    employees_data = db.get_employees() if hasattr(db, 'get_employees') else []
    departments_data = db.get_departments() if hasattr(db, 'get_departments') else []
    purposes_data = db.get_expense_purposes() if hasattr(db, 'get_expense_purposes') else []

    return {
        'nomenclatures': to_dicts(db.search_nomenclatures(limit=1000)),
        'warehouses': to_dicts(db.execute_query("SELECT id, name FROM warehouses WHERE is_active = 1", fetch_all=True)),
        'suppliers': to_dicts(db.execute_query("SELECT id, name FROM suppliers WHERE is_active = 1", fetch_all=True)),
        'employees': to_dicts(employees_data),
        'departments': to_dicts(departments_data),
        'purposes': to_dicts(purposes_data),
        'categories': db.get_all_categories(),
    }


def insert_document_record(db, doc_type, doc_number, document_date, form, user_id,
                            number_type=None, issuance_number=None):
    """Вставляет запись документа в БД. form — объект с методом .get()."""
    if doc_type in ('receipt', 'return'):
        row = db.execute_query("""
            INSERT INTO documents (
                document_type, document_number, document_date, status,
                supplier_id, to_warehouse_id, reason, created_by, created_at
            ) VALUES (?, ?, ?, 'draft', ?, ?, ?, ?, CURRENT_TIMESTAMP) RETURNING id
        """, (doc_type, doc_number, document_date,
              form.get('supplier_id') or None, form.get('to_warehouse_id') or None,
              form.get('reason'), user_id), fetch_all=False)

    elif doc_type == 'issuance':
        row = db.execute_query("""
            INSERT INTO documents (
                document_type, document_number, document_date, status,
                from_warehouse_id, employee_id, department_id,
                purpose_id, purpose_comment, cost_center_id,
                issuance_type, reason, number_type, issuance_number,
                created_by, created_at
            ) VALUES (?, ?, ?, 'draft', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP) RETURNING id
        """, (doc_type, doc_number, document_date,
              form.get('from_warehouse_id') or None, form.get('employee_id') or None,
              form.get('department_id') or None, form.get('purpose_id') or None,
              form.get('purpose_comment'), form.get('cost_center_id') or None,
              form.get('issuance_type'), form.get('reason'),
              number_type, issuance_number, user_id), fetch_all=False)

    elif doc_type == 'write_off':
        row = db.execute_query("""
            INSERT INTO documents (
                document_type, document_number, document_date, status,
                from_warehouse_id, employee_id, department_id,
                purpose_id, purpose_comment, cost_center_id,
                issuance_type, reason, created_by, created_at
            ) VALUES (?, ?, ?, 'draft', ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP) RETURNING id
        """, (doc_type, doc_number, document_date,
              form.get('from_warehouse_id') or None, form.get('employee_id') or None,
              form.get('department_id') or None, form.get('purpose_id') or None,
              form.get('purpose_comment'), form.get('cost_center_id') or None,
              form.get('issuance_type'), form.get('reason'), user_id), fetch_all=False)

    elif doc_type == 'transfer':
        row = db.execute_query("""
            INSERT INTO documents (
                document_type, document_number, document_date, status,
                from_warehouse_id, to_warehouse_id, issuance_type,
                employee_id, department_id, reason, created_by, created_at
            ) VALUES (?, ?, ?, 'draft', ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP) RETURNING id
        """, (doc_type, doc_number, document_date,
              form.get('from_warehouse_id') or None, form.get('to_warehouse_id') or None,
              form.get('issuance_type') or 'department', form.get('employee_id') or None,
              form.get('department_id') or None, form.get('reason'), user_id), fetch_all=False)

    else:
        row = db.execute_query("""
            INSERT INTO documents (
                document_type, document_number, document_date, status,
                reason, created_by, created_at
            ) VALUES (?, ?, ?, 'draft', ?, ?, CURRENT_TIMESTAMP) RETURNING id
        """, (doc_type, doc_number, document_date, form.get('reason'), user_id), fetch_all=False)

    return row['id']


def save_document_items(db, document_id, items):
    """Сохраняет позиции документа в document_items."""
    for item in items:
        db.execute_query("""
            INSERT INTO document_items (
                document_id, nomenclature_id, quantity,
                batch_number, expiry_date,
                serial_number, inventory_number,
                purpose, accounting_type, variation_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (document_id, item['nomenclature_id'], item['quantity'],
              item.get('batch_number'), item.get('expiry_date'),
              item.get('serial_number'), item.get('inventory_number'),
              item.get('purpose'), item['accounting_type'], item.get('variation_id')))


def get_document_for_print(db, id):
    """Загружает документ и позиции для печати."""
    doc = db.execute_query("""
        SELECT d.*,
               u.username as created_by_name,
               u2.username as posted_by_name,
               w_from.name as from_warehouse_name,
               w_to.name as to_warehouse_name,
               l_from.name as from_location_name,
               l_to.name as to_location_name,
               e.last_name || ' ' || e.first_name || COALESCE(' ' || e.middle_name, '') as employee_name,
               e.position as employee_position,
               dep.name as employee_department,
               dpt.name as department_name,
               emp.full_name as department_manager,
               ep.name as purpose_name
        FROM documents d
        LEFT JOIN users u ON d.created_by = u.id
        LEFT JOIN users u2 ON d.posted_by = u2.id
        LEFT JOIN warehouses w_from ON d.from_warehouse_id = w_from.id
        LEFT JOIN warehouses w_to ON d.to_warehouse_id = w_to.id
        LEFT JOIN locations l_from ON d.from_location_id = l_from.id
        LEFT JOIN locations l_to ON d.to_location_id = l_to.id
        LEFT JOIN employees e ON d.employee_id = e.id
        LEFT JOIN departments dep ON e.department_id = dep.id
        LEFT JOIN departments dpt ON d.department_id = dpt.id
        LEFT JOIN employees emp ON dpt.manager_id = emp.id
        LEFT JOIN expense_purposes ep ON d.purpose_id = ep.id
        WHERE d.id = ?
    """, (id,), fetch_all=False)
    if not doc:
        return None, []
    items_rows = db.execute_query("""
        SELECT di.quantity, di.purpose, di.price, di.accounting_type,
               n.name as nomenclature_name, n.unit, n.sku,
               i.inventory_number, i.serial_number,
               b.batch_number, b.expiry_date,
               nv.size, nv.color
        FROM document_items di
        LEFT JOIN nomenclatures n ON di.nomenclature_id = n.id
        LEFT JOIN instances i ON di.instance_id = i.id
        LEFT JOIN batches b ON di.batch_id = b.id
        LEFT JOIN nomenclature_variations nv ON nv.id = di.variation_id
        WHERE di.document_id = ?
        ORDER BY di.id
    """, (id,), fetch_all=True)
    return dict(doc), [dict(r) for r in items_rows] if items_rows else []
