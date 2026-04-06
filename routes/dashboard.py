"""
Blueprint: Дашборд.
"""
import logging
from flask import Blueprint, render_template, session, request, jsonify
from database import get_db
from routes.common import login_required

logger = logging.getLogger('routes.dashboard')

dashboard_bp = Blueprint('dashboard', __name__)


@dashboard_bp.route('/dashboard')
@login_required
def dashboard():
    try:
        logger.debug(f"Загрузка дашборда для пользователя {session.get('username')}")
        db = get_db()

        stats = {
            'nomenclatures': {'total': 0, 'new': 0},
            'instances': {'total': 0, 'in_stock': 0, 'in_use': 0, 'repair': 0, 'written_off': 0},
            'stocks': {'total_quantity': 0, 'total_items': 0, 'low_stock': 0},
            'batches': {'total': 0, 'expiring_soon': 0, 'expired': 0},
            'documents': {'total': 0, 'today': 0, 'pending': 0},
            'attention': {'total': 0, 'expiring': 0, 'low_stock': 0, 'uncompleted': 0},
            'employees': {'total': 0, 'active': 0},
            'warehouses': {'total': 0},
            'categories': {'total': 0},
            'suppliers': {'total': 0},
            'locations': {'total': 0},
            'departments': {'total': 0}
        }

        chart_movement = {'labels': [], 'receipts': [], 'issuances': []}
        chart_top5 = {'labels': [], 'data': []}
        chart_instances = {
            'labels': ['На складе', 'В использовании', 'Ремонт', 'Списан'],
            'data': [0, 0, 0, 0]
        }
        low_stock_items = []
        draft_documents = []
        expiring_batches = []

        # Один агрегированный запрос вместо 11 отдельных COUNT
        try:
            row = db.execute_query("""
                SELECT
                    (SELECT COUNT(*) FROM nomenclatures)                              AS nom_total,
                    (SELECT COUNT(*) FROM instances)                                  AS inst_total,
                    (SELECT COUNT(*) FROM instances WHERE status = 'in_stock')        AS inst_in_stock,
                    (SELECT COUNT(*) FROM instances WHERE status = 'in_use')          AS inst_in_use,
                    (SELECT COUNT(*) FROM instances WHERE status = 'repair')          AS inst_repair,
                    (SELECT COUNT(*) FROM instances WHERE status = 'written_off')     AS inst_written_off,
                    (SELECT COUNT(*) FROM documents)                                  AS doc_total,
                    (SELECT COUNT(*) FROM categories)                                 AS cat_total,
                    (SELECT COUNT(*) FROM warehouses)                                 AS wh_total,
                    (SELECT COUNT(*) FROM employees)                                  AS emp_total,
                    (SELECT COUNT(*) FROM suppliers)                                  AS sup_total,
                    (SELECT COUNT(*) FROM documents WHERE document_date >= date('now', '-7 days')) AS doc_week,
                    (SELECT COUNT(*) FROM documents WHERE document_date >= date('now', '-14 days')
                        AND document_date < date('now', '-7 days'))                   AS doc_prev_week,
                    (SELECT COUNT(*) FROM instances WHERE created_at >= date('now', '-7 days')) AS inst_new_week,
                    (SELECT COUNT(*) FROM nomenclatures WHERE created_at >= date('now', '-7 days')) AS nom_new_week
            """, fetch_all=False)
            if row:
                stats['nomenclatures']['total'] = row['nom_total']
                stats['instances']['total']      = row['inst_total']
                stats['instances']['in_stock']   = row['inst_in_stock']
                stats['instances']['in_use']     = row['inst_in_use']
                stats['instances']['repair']     = row['inst_repair']
                stats['instances']['written_off']= row['inst_written_off']
                stats['documents']['total']      = row['doc_total']
                stats['categories']['total']     = row['cat_total']
                stats['warehouses']['total']     = row['wh_total']
                stats['employees']['total']      = row['emp_total']
                stats['suppliers']['total']      = row['sup_total']
                # Тренды за 7 дней
                doc_week = row['doc_week'] or 0
                doc_prev = row['doc_prev_week'] or 0
                stats['trends'] = {
                    'doc_week': doc_week,
                    'doc_trend': 'up' if doc_week > doc_prev else ('down' if doc_week < doc_prev else 'flat'),
                    'doc_delta': doc_week - doc_prev,
                    'inst_new_week': row['inst_new_week'] or 0,
                    'nom_new_week': row['nom_new_week'] or 0,
                }
        except Exception as e:
            logger.error(f"Ошибка агрегированных счётчиков: {e}")
            stats['trends'] = {'doc_week': 0, 'doc_trend': 'flat', 'doc_delta': 0, 'inst_new_week': 0, 'nom_new_week': 0}

        try:
            rows = db.execute_query("""
                SELECT date(document_date) as day,
                       SUM(CASE WHEN document_type='receipt' THEN 1 ELSE 0 END) as receipts,
                       SUM(CASE WHEN document_type='issuance' THEN 1 ELSE 0 END) as issuances
                FROM documents
                WHERE document_date >= date('now', '-30 days') AND status = 'posted'
                GROUP BY day ORDER BY day
            """, fetch_all=True) or []
            for row in rows:
                chart_movement['labels'].append(row['day'])
                chart_movement['receipts'].append(row['receipts'] or 0)
                chart_movement['issuances'].append(row['issuances'] or 0)
        except Exception as e:
            logger.error(f"Ошибка движения документов: {e}")

        try:
            rows = db.execute_query("""
                SELECT n.name, COALESCE(SUM(s.quantity), 0) as total
                FROM nomenclatures n
                LEFT JOIN stocks s ON s.nomenclature_id = n.id
                GROUP BY n.id, n.name HAVING total > 0
                ORDER BY total DESC LIMIT 5
            """, fetch_all=True) or []
            for row in rows:
                name = row['name']
                chart_top5['labels'].append(name[:25] + '…' if len(name) > 25 else name)
                chart_top5['data'].append(float(row['total']))
        except Exception as e:
            logger.error(f"Ошибка топ-5: {e}")

        chart_instances['data'] = [
            stats['instances'].get('in_stock', 0),
            stats['instances'].get('in_use', 0),
            stats['instances'].get('repair', 0),
            stats['instances'].get('written_off', 0),
        ]

        try:
            rows = db.execute_query("""
                SELECT n.name, n.sku, n.min_stock,
                       COALESCE(SUM(s.quantity), 0) as current_stock
                FROM nomenclatures n
                LEFT JOIN stocks s ON s.nomenclature_id = n.id
                WHERE n.min_stock IS NOT NULL AND n.min_stock > 0
                GROUP BY n.id, n.name, n.sku, n.min_stock
                HAVING current_stock <= n.min_stock
                ORDER BY (current_stock * 1.0 / n.min_stock) ASC LIMIT 10
            """, fetch_all=True) or []
            for row in rows:
                low_stock_items.append({
                    'name': row['name'],
                    'sku': row['sku'],
                    'min_stock': row['min_stock'],
                    'current_stock': row['current_stock'],
                })
        except Exception as e:
            logger.error(f"Ошибка критических остатков: {e}")

        try:
            rows = db.execute_query("""
                SELECT d.id, d.document_number, d.document_date, d.document_type,
                       u.username as created_by_name
                FROM documents d LEFT JOIN users u ON d.created_by = u.id
                WHERE d.status = 'draft' ORDER BY d.created_at DESC LIMIT 5
            """, fetch_all=True) or []
            for row in rows:
                draft_documents.append({
                    'id': row['id'],
                    'document_number': row['document_number'],
                    'document_date': row['document_date'],
                    'document_type': row['document_type'],
                    'created_by_name': row['created_by_name'],
                })
        except Exception as e:
            logger.error(f"Ошибка черновиков: {e}")

        # Истекающие партии (срок годности ≤30 дней)
        try:
            rows = db.execute_query("""
                SELECT b.batch_number, b.expiry_date, n.name as nomenclature_name,
                       CAST(julianday(b.expiry_date) - julianday('now') AS INTEGER) as days_left
                FROM batches b
                JOIN nomenclatures n ON b.nomenclature_id = n.id
                WHERE b.expiry_date IS NOT NULL
                  AND b.expiry_date BETWEEN date('now', '-1 day') AND date('now', '+30 days')
                  AND b.is_active = 1
                ORDER BY b.expiry_date ASC LIMIT 10
            """, fetch_all=True) or []
            for row in rows:
                expiring_batches.append({
                    'batch_number': row['batch_number'],
                    'expiry_date': row['expiry_date'],
                    'nomenclature_name': row['nomenclature_name'],
                    'days_left': row['days_left'],
                })
        except Exception as e:
            logger.error(f"Ошибка истекающих партий: {e}")

        # Распределение документов по типам
        chart_doctypes = {'labels': [], 'data': []}
        try:
            rows = db.execute_query("""
                SELECT document_type,
                       COUNT(*) as cnt
                FROM documents
                WHERE status = 'posted'
                GROUP BY document_type
                ORDER BY cnt DESC
            """, fetch_all=True) or []
            type_labels = {
                'receipt': 'Поступление', 'issuance': 'Выдача',
                'write_off': 'Списание', 'transfer': 'Перемещение',
                'return': 'Возврат', 'inventory': 'Инвентаризация',
            }
            for row in rows:
                chart_doctypes['labels'].append(type_labels.get(row['document_type'], row['document_type']))
                chart_doctypes['data'].append(row['cnt'])
        except Exception as e:
            logger.error(f"Ошибка chart_doctypes: {e}")

        # Остатки по складам
        chart_warehouse_stock = {'labels': [], 'data': []}
        try:
            rows = db.execute_query("""
                SELECT w.name,
                       COALESCE(SUM(s.quantity), 0) as total_qty
                FROM warehouses w
                LEFT JOIN stocks s ON s.warehouse_id = w.id
                GROUP BY w.id, w.name
                ORDER BY total_qty DESC
                LIMIT 10
            """, fetch_all=True) or []
            for row in rows:
                name = row['name']
                chart_warehouse_stock['labels'].append(name[:20] + '…' if len(name) > 20 else name)
                chart_warehouse_stock['data'].append(float(row['total_qty']))
        except Exception as e:
            logger.error(f"Ошибка chart_warehouse_stock: {e}")

        # Лента активности — последние 8 действий из журнала
        recent_activity = []
        try:
            rows = db.execute_query("""
                SELECT ul.action, ul.entity_type, ul.entity_id, ul.created_at,
                       u.username
                FROM user_logs ul
                LEFT JOIN users u ON ul.user_id = u.id
                ORDER BY ul.created_at DESC LIMIT 8
            """, fetch_all=True) or []
            for row in rows:
                recent_activity.append({
                    'action': row['action'],
                    'entity_type': row['entity_type'],
                    'entity_id': row['entity_id'],
                    'created_at': row['created_at'],
                    'username': row['username'],
                })
        except Exception as e:
            logger.error(f"Ошибка ленты активности: {e}")

        legacy_counts = {
            'total': stats['nomenclatures']['total'],
            'active': stats['nomenclatures']['total'],
            'in_stock': stats['instances']['in_stock']
        }

        return render_template('dashboard.html',
                               stats=stats,
                               legacy_counts=legacy_counts,
                               expiring_batches=expiring_batches,
                               low_stock_items=low_stock_items,
                               draft_documents=draft_documents,
                               chart_movement=chart_movement,
                               chart_top5=chart_top5,
                               chart_instances=chart_instances,
                               chart_doctypes=chart_doctypes,
                               chart_warehouse_stock=chart_warehouse_stock,
                               recent_activity=recent_activity,
                               trends=stats.get('trends', {}),
                               migration_progress=100)

    except Exception as e:
        logger.error(f'Ошибка дашборда: {e}')
        import traceback
        traceback.print_exc()

        empty_stats = {
            'nomenclatures': {'total': 0, 'new': 0},
            'instances': {'total': 0, 'in_stock': 0, 'in_use': 0, 'repair': 0, 'written_off': 0},
            'stocks': {'total_quantity': 0, 'total_items': 0, 'low_stock': 0},
            'batches': {'total': 0, 'expiring_soon': 0, 'expired': 0},
            'documents': {'total': 0, 'today': 0, 'pending': 0},
            'attention': {'total': 0, 'expiring': 0, 'low_stock': 0, 'uncompleted': 0},
            'employees': {'total': 0, 'active': 0},
            'warehouses': {'total': 0},
            'categories': {'total': 0},
            'suppliers': {'total': 0},
            'locations': {'total': 0},
            'departments': {'total': 0}
        }
        return render_template('dashboard.html',
                               stats=empty_stats,
                               legacy_counts={'total': 0, 'active': 0, 'in_stock': 0},
                               trends={},
                               expiring_batches=[],
                               low_stock_items=[],
                               draft_documents=[],
                               recent_activity=[],
                               chart_movement={'labels': [], 'receipts': [], 'issuances': []},
                               chart_top5={'labels': [], 'data': []},
                               chart_instances={'labels': ['На складе', 'В использовании', 'Ремонт', 'Списан'],
                                               'data': [0, 0, 0, 0]},
                               chart_doctypes={'labels': [], 'data': []},
                               chart_warehouse_stock={'labels': [], 'data': []},
                               migration_progress=0)


@dashboard_bp.route('/api/dashboard/config', methods=['GET', 'POST'])
@login_required
def api_dashboard_config():
    """Сохранение / загрузка конфигурации виджетов дашборда (в сессии)."""
    if request.method == 'POST':
        data = request.get_json(silent=True) or {}
        session['dashboard_config'] = data
        session.modified = True
        return jsonify({'ok': True})
    return jsonify(session.get('dashboard_config', {}))
