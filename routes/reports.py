from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from datetime import datetime, timedelta
from collections import defaultdict
import json
import logging

from routes.common import login_required, get_db
from extensions import limiter

logger = logging.getLogger(__name__)

reports_bp = Blueprint('reports', __name__)


@reports_bp.route('/reports', endpoint='reports')
@login_required
def reports():
    """Главная страница отчетов"""
    return render_template('reports/index.html')


@reports_bp.route('/reports/stock-balance', endpoint='report_stock_balance')
@login_required
def report_stock_balance():
    """Отчет по остаткам"""
    try:
        db = get_db()

        # Получаем параметры фильтрации
        warehouse_id = request.args.get('warehouse_id')
        category_id = request.args.get('category_id')
        stock_type = request.args.get('stock_type', 'all')
        date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))

        # Базовый запрос
        query = """
            SELECT
                s.*,
                n.name as nomenclature_name,
                n.sku,
                n.min_stock,
                c.name_ru as category_name,
                w.name as warehouse_name,
                b.batch_number,
                (s.quantity - s.reserved_quantity) as available_quantity,
                s.quantity * COALESCE(b.purchase_price, 0) as amount
            FROM stocks s
            JOIN nomenclatures n ON s.nomenclature_id = n.id
            LEFT JOIN categories c ON n.category_id = c.id
            LEFT JOIN warehouses w ON s.warehouse_id = w.id
            LEFT JOIN batches b ON s.batch_id = b.id
            WHERE 1=1
        """
        params = []

        if warehouse_id:
            query += " AND s.warehouse_id = ?"
            params.append(warehouse_id)

        if category_id:
            query += " AND n.category_id = ?"
            params.append(category_id)

        if stock_type == 'positive':
            query += " AND s.quantity > 0"
        elif stock_type == 'zero':
            query += " AND s.quantity = 0"
        elif stock_type == 'negative':
            query += " AND s.quantity < 0"

        query += " ORDER BY n.name, w.name LIMIT 5000"

        stocks = db.execute_query(query, params, fetch_all=True) or []

        # Сводка
        summary = {
            'total_items': len(stocks),
            'total_quantity': sum(s['quantity'] for s in stocks),
            'total_amount': sum(s['amount'] or 0 for s in stocks),
            'low_stock': sum(1 for s in stocks if s['min_stock'] and s['quantity'] <= s['min_stock'])
        }

        # Для фильтров
        warehouses = db.execute_query("SELECT id, name FROM warehouses WHERE is_active = 1", fetch_all=True) or []
        categories = db.get_all_categories()

        return render_template('reports/stock_balance.html',
                             stocks=[dict(s) for s in stocks],
                             summary=summary,
                             warehouses=[dict(w) for w in warehouses],
                             categories=categories)

    except Exception as e:
        logger.error(f'Ошибка формирования отчета по остаткам: {e}')
        flash('Ошибка формирования отчета', 'error')
        return redirect(url_for('reports.reports'))


@reports_bp.route('/reports/stock-movement', endpoint='report_stock_movement')
@login_required
def report_stock_movement():
    """Отчет по движению товаров"""
    try:
        db = get_db()

        # Параметры фильтрации
        date_from = request.args.get('date_from', datetime.now().replace(day=1).strftime('%Y-%m-%d'))
        date_to = request.args.get('date_to', datetime.now().strftime('%Y-%m-%d'))
        doc_type = request.args.get('doc_type')
        nomenclature_id = request.args.get('nomenclature_id')

        # Получаем движения
        query = """
            SELECT
                d.document_date,
                d.document_number,
                d.document_type,
                d.id as document_id,
                n.name as nomenclature_name,
                di.quantity,
                di.price,
                di.amount,
                COALESCE(w_from.name, l_from.name, s.name) as from_location,
                COALESCE(w_to.name, l_to.name, e.full_name) as to_location,
                u.username as created_by_name
            FROM document_items di
            JOIN documents d ON di.document_id = d.id
            JOIN nomenclatures n ON di.nomenclature_id = n.id
            LEFT JOIN users u ON d.created_by = u.id
            LEFT JOIN warehouses w_from ON d.from_warehouse_id = w_from.id
            LEFT JOIN warehouses w_to ON d.to_warehouse_id = w_to.id
            LEFT JOIN locations l_from ON d.from_location_id = l_from.id
            LEFT JOIN locations l_to ON d.to_location_id = l_to.id
            LEFT JOIN suppliers s ON d.supplier_id = s.id
            LEFT JOIN employees e ON d.employee_id = e.id
            WHERE d.document_date BETWEEN ? AND ?
            AND d.status = 'posted'
        """
        params = [date_from, date_to]

        if doc_type:
            query += " AND d.document_type = ?"
            params.append(doc_type)

        if nomenclature_id:
            query += " AND di.nomenclature_id = ?"
            params.append(nomenclature_id)

        query += " ORDER BY d.document_date DESC, d.id DESC LIMIT 5000"

        movements = db.execute_query(query, params, fetch_all=True) or []

        # Данные для графика
        dates = []
        receipts_data = []
        issuances_data = []

        # Группируем по датам
        daily_stats = defaultdict(lambda: {'receipts': 0, 'issuances': 0})

        for move in movements:
            date = move['document_date']
            if move['document_type'] in ['receipt', 'return']:
                daily_stats[date]['receipts'] += move['quantity']
            elif move['document_type'] in ['issuance', 'write_off']:
                daily_stats[date]['issuances'] += move['quantity']

        # Сортируем по датам
        for date in sorted(daily_stats.keys()):
            dates.append(date)
            receipts_data.append(daily_stats[date]['receipts'])
            issuances_data.append(daily_stats[date]['issuances'])

        # Для фильтров
        nomenclatures = db.search_nomenclatures(limit=1000)

        return render_template('reports/stock_movement.html',
                             movements=[dict(m) for m in movements],
                             nomenclatures=nomenclatures,
                             chart_labels=json.dumps(dates),
                             chart_receipts=json.dumps(receipts_data),
                             chart_issuances=json.dumps(issuances_data))

    except Exception as e:
        logger.error(f'Ошибка формирования отчета по движению: {e}')
        flash('Ошибка формирования отчета', 'error')
        return redirect(url_for('reports.reports'))


@reports_bp.route('/reports/low-stock', endpoint='report_low_stock')
@login_required
def report_low_stock():
    """Отчет по позициям с малым остатком"""
    try:
        db = get_db()

        low_stock = db.execute_query("""
            SELECT
                n.id,
                n.name as nomenclature_name,
                n.sku,
                n.min_stock,
                c.name_ru as category_name,
                SUM(s.quantity) as total_quantity,
                COUNT(DISTINCT s.warehouse_id) as warehouses_count
            FROM nomenclatures n
            LEFT JOIN categories c ON n.category_id = c.id
            LEFT JOIN stocks s ON n.id = s.nomenclature_id
            WHERE n.min_stock > 0 AND n.is_active = 1
            GROUP BY n.id
            HAVING total_quantity <= n.min_stock
            ORDER BY (n.min_stock - total_quantity) DESC
        """, fetch_all=True) or []

        return render_template('reports/low_stock.html', low_stock=[dict(l) for l in low_stock])

    except Exception as e:
        logger.error(f'Ошибка формирования отчета по малым остаткам: {e}')
        flash('Ошибка формирования отчета', 'error')
        return redirect(url_for('reports.reports'))


@reports_bp.route('/reports/expiring', endpoint='report_expiring')
@login_required
def report_expiring():
    """Отчет по истекающим срокам годности"""
    try:
        db = get_db()

        today = datetime.now().date()
        expiring_date = (today + timedelta(days=30)).strftime('%Y-%m-%d')

        expiring = db.execute_query("""
            SELECT
                b.*,
                n.name as nomenclature_name,
                n.sku,
                s.name as supplier_name,
                (SELECT SUM(quantity) FROM stocks WHERE batch_id = b.id) as total_quantity,
                julianday(b.expiry_date) - julianday('now') as days_left
            FROM batches b
            JOIN nomenclatures n ON b.nomenclature_id = n.id
            LEFT JOIN suppliers s ON b.supplier_id = s.id
            WHERE b.expiry_date IS NOT NULL
                AND b.expiry_date <= date('now', '+30 days')
                AND b.expiry_date >= date('now')
                AND b.is_active = 1
            ORDER BY b.expiry_date ASC
        """, fetch_all=True) or []

        expired = db.execute_query("""
            SELECT
                b.*,
                n.name as nomenclature_name,
                n.sku,
                s.name as supplier_name,
                (SELECT SUM(quantity) FROM stocks WHERE batch_id = b.id) as total_quantity,
                julianday('now') - julianday(b.expiry_date) as days_overdue
            FROM batches b
            JOIN nomenclatures n ON b.nomenclature_id = n.id
            LEFT JOIN suppliers s ON b.supplier_id = s.id
            WHERE b.expiry_date IS NOT NULL
                AND b.expiry_date < date('now')
                AND (b.quality_status != 'expired' OR b.quality_status IS NULL)
            ORDER BY b.expiry_date ASC
        """, fetch_all=True) or []

        return render_template('reports/expiring.html',
                             expiring=[dict(e) for e in expiring],
                             expired=[dict(e) for e in expired])

    except Exception as e:
        logger.error(f'Ошибка формирования отчета по срокам: {e}')
        flash('Ошибка формирования отчета', 'error')
        return redirect(url_for('reports.reports'))


@reports_bp.route('/reports/documents-by-type', endpoint='report_documents_by_type')
@login_required
def report_documents_by_type():
    """Отчет по документам по типам"""
    try:
        db = get_db()
        date_from = request.args.get('date_from', (datetime.now().replace(day=1)).strftime('%Y-%m-%d'))
        date_to = request.args.get('date_to', datetime.now().strftime('%Y-%m-%d'))

        # Статистика по типам
        by_type = db.execute_query("""
            SELECT
                document_type,
                COUNT(*) as count,
                SUM(COALESCE(total_amount, 0)) as amount,
                SUM(CASE WHEN status = 'posted' THEN 1 ELSE 0 END) as posted,
                SUM(CASE WHEN status = 'draft' THEN 1 ELSE 0 END) as draft
            FROM documents
            WHERE document_date BETWEEN ? AND ?
            GROUP BY document_type
        """, (date_from, date_to), fetch_all=True)

        if not by_type:
            by_type = []

        # Общая статистика
        summary_row = db.execute_query("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'posted' THEN 1 ELSE 0 END) as posted,
                SUM(CASE WHEN status = 'draft' THEN 1 ELSE 0 END) as draft,
                SUM(COALESCE(total_amount, 0)) as total_amount
            FROM documents
            WHERE document_date BETWEEN ? AND ?
        """, (date_from, date_to), fetch_all=False)

        summary = {
            'total': summary_row['total'] if summary_row else 0,
            'posted': summary_row['posted'] if summary_row else 0,
            'draft': summary_row['draft'] if summary_row else 0,
            'total_amount': summary_row['total_amount'] if summary_row and summary_row['total_amount'] else 0
        }

        # Динамика по месяцам
        timeline = db.execute_query("""
            SELECT
                strftime('%Y-%m', document_date) as month,
                SUM(CASE WHEN document_type = 'receipt' THEN 1 ELSE 0 END) as receipts,
                SUM(CASE WHEN document_type = 'issuance' THEN 1 ELSE 0 END) as issuances,
                SUM(CASE WHEN document_type = 'write_off' THEN 1 ELSE 0 END) as writeoffs
            FROM documents
            WHERE document_date BETWEEN ? AND ?
            GROUP BY strftime('%Y-%m', document_date)
            ORDER BY month
        """, (date_from, date_to), fetch_all=True)

        if not timeline:
            timeline = []

        # Преобразуем by_type в список словарей и считаем проценты
        by_type_list = []
        for t in by_type:
            t_dict = dict(t)
            t_dict['count'] = t_dict['count'] or 0
            t_dict['amount'] = t_dict['amount'] or 0
            t_dict['posted'] = t_dict['posted'] or 0
            t_dict['draft'] = t_dict['draft'] or 0
            t_dict['percentage'] = (t_dict['count'] / summary['total'] * 100) if summary['total'] > 0 else 0
            by_type_list.append(t_dict)

        # Подготовка данных для графиков
        doc_types_rus = {
            'receipt': 'Поступления',
            'transfer': 'Перемещения',
            'issuance': 'Выдачи',
            'write_off': 'Списания',
            'return': 'Возвраты',
            'adjustment': 'Корректировки'
        }

        chart_labels = [doc_types_rus.get(t['document_type'], t['document_type']) for t in by_type_list]
        chart_data = [t['count'] for t in by_type_list]

        timeline_labels = [t['month'] for t in timeline]
        timeline_receipts = [t['receipts'] or 0 for t in timeline]
        timeline_issuances = [t['issuances'] or 0 for t in timeline]
        timeline_writeoffs = [t['writeoffs'] or 0 for t in timeline]

        return render_template('reports/documents_by_type.html',
                             by_type=by_type_list,
                             summary=summary,
                             chart_labels=json.dumps(chart_labels),
                             chart_data=json.dumps(chart_data),
                             timeline_labels=json.dumps(timeline_labels),
                             timeline_receipts=json.dumps(timeline_receipts),
                             timeline_issuances=json.dumps(timeline_issuances),
                             timeline_writeoffs=json.dumps(timeline_writeoffs))

    except Exception as e:
        logger.error(f'Ошибка формирования отчета по типам документов: {e}')
        flash('Ошибка формирования отчета', 'error')
        return redirect(url_for('reports.reports'))


@reports_bp.route('/reports/documents-by-period', endpoint='report_documents_by_period')
@login_required
def report_documents_by_period():
    """Отчет по документам за период"""
    try:
        db = get_db()
        date_from = request.args.get('date_from', (datetime.now().replace(day=1)).strftime('%Y-%m-%d'))
        date_to = request.args.get('date_to', datetime.now().strftime('%Y-%m-%d'))
        status = request.args.get('status')

        query = """
            SELECT d.*,
                   u.username as created_by_name,
                   COALESCE(w_from.name, l_from.name, s.name) as from_location,
                   COALESCE(w_to.name, l_to.name, e.full_name) as to_location
            FROM documents d
            LEFT JOIN users u ON d.created_by = u.id
            LEFT JOIN warehouses w_from ON d.from_warehouse_id = w_from.id
            LEFT JOIN warehouses w_to ON d.to_warehouse_id = w_to.id
            LEFT JOIN locations l_from ON d.from_location_id = l_from.id
            LEFT JOIN locations l_to ON d.to_location_id = l_to.id
            LEFT JOIN suppliers s ON d.supplier_id = s.id
            LEFT JOIN employees e ON d.employee_id = e.id
            WHERE d.document_date BETWEEN ? AND ?
        """
        params = [date_from, date_to]

        if status:
            query += " AND d.status = ?"
            params.append(status)

        query += " ORDER BY d.document_date DESC"

        documents = db.execute_query(query, params, fetch_all=True) or []

        # Общая сумма
        total_amount = sum(d['total_amount'] or 0 for d in documents)

        return render_template('reports/documents_by_period.html',
                             documents=[dict(d) for d in documents],
                             total_amount=total_amount)

    except Exception as e:
        logger.error(f'Ошибка формирования отчета по документам за период: {e}')
        flash('Ошибка формирования отчета', 'error')
        return redirect(url_for('reports.reports'))


@reports_bp.route('/reports/supplier-deliveries', endpoint='report_supplier_deliveries')
@login_required
def report_supplier_deliveries():
    """Отчет по поставкам поставщиков"""
    try:
        db = get_db()
        date_from = request.args.get('date_from', (datetime.now().replace(day=1)).strftime('%Y-%m-%d'))
        date_to = request.args.get('date_to', datetime.now().strftime('%Y-%m-%d'))
        supplier_id = request.args.get('supplier_id')

        # Список поставщиков для фильтра
        suppliers = db.execute_query("SELECT id, name FROM suppliers WHERE is_active = 1 ORDER BY name", fetch_all=True) or []

        # Статистика по поставщикам
        supplier_stats_query = """
            SELECT
                s.id,
                s.name,
                COUNT(DISTINCT d.id) as doc_count,
                SUM(di.quantity) as total_quantity,
                SUM(di.amount) as total_amount,
                AVG(di.amount) as avg_amount
            FROM suppliers s
            LEFT JOIN documents d ON s.id = d.supplier_id AND d.document_type = 'receipt' AND d.status = 'posted'
            LEFT JOIN document_items di ON d.id = di.document_id
            WHERE d.document_date BETWEEN ? AND ?
        """
        params = [date_from, date_to]

        if supplier_id:
            supplier_stats_query += " AND s.id = ?"
            params.append(supplier_id)

        supplier_stats_query += " GROUP BY s.id, s.name ORDER BY total_amount DESC"

        supplier_stats = db.execute_query(supplier_stats_query, params, fetch_all=True) or []

        # Детализация поставок
        deliveries_query = """
            SELECT
                d.document_date,
                d.document_number,
                d.id as document_id,
                s.name as supplier_name,
                n.name as nomenclature_name,
                di.quantity,
                di.price,
                di.amount,
                b.batch_number
            FROM documents d
            JOIN suppliers s ON d.supplier_id = s.id
            JOIN document_items di ON d.id = di.document_id
            JOIN nomenclatures n ON di.nomenclature_id = n.id
            LEFT JOIN batches b ON di.batch_id = b.id
            WHERE d.document_type = 'receipt'
                AND d.status = 'posted'
                AND d.document_date BETWEEN ? AND ?
        """
        deliveries_params = [date_from, date_to]

        if supplier_id:
            deliveries_query += " AND d.supplier_id = ?"
            deliveries_params.append(supplier_id)

        deliveries_query += " ORDER BY d.document_date DESC"

        deliveries = db.execute_query(deliveries_query, deliveries_params, fetch_all=True) or []

        return render_template('reports/supplier_deliveries.html',
                             suppliers=[dict(s) for s in suppliers],
                             supplier_stats=[dict(s) for s in supplier_stats],
                             deliveries=[dict(d) for d in deliveries])

    except Exception as e:
        logger.error(f'Ошибка формирования отчета по поставкам: {e}')
        flash('Ошибка формирования отчета', 'error')
        return redirect(url_for('reports.reports'))


@reports_bp.route('/reports/nomenclature-by-category', endpoint='report_nomenclature_by_category')
@login_required
def report_nomenclature_by_category():
    """Отчет по номенклатуре по категориям"""
    try:
        db = get_db()

        # Статистика по категориям
        categories = db.execute_query("""
            SELECT
                c.id,
                c.name_ru as name,
                c.type as item_type,
                COUNT(DISTINCT n.id) as total_items,
                SUM(CASE WHEN n.is_active = 1 THEN 1 ELSE 0 END) as active_items,
                COUNT(DISTINCT CASE WHEN s.quantity > 0 THEN n.id END) as with_stock,
                COALESCE(SUM(s.quantity), 0) as total_quantity,
                COALESCE(SUM(s.quantity * COALESCE(b.purchase_price, 0)), 0) as total_amount
            FROM categories c
            LEFT JOIN nomenclatures n ON c.id = n.category_id
            LEFT JOIN stocks s ON n.id = s.nomenclature_id
            LEFT JOIN batches b ON s.batch_id = b.id
            WHERE c.is_active = 1
            GROUP BY c.id, c.name_ru, c.type
            ORDER BY total_items DESC
        """, fetch_all=True)

        if not categories:
            categories = []

        # Общая сводка
        summary = db.execute_query("""
            SELECT
                COUNT(DISTINCT n.id) as total,
                SUM(CASE WHEN n.is_active = 1 THEN 1 ELSE 0 END) as active,
                COUNT(DISTINCT c.id) as categories,
                COUNT(DISTINCT CASE WHEN s.quantity > 0 THEN n.id END) as with_stock
            FROM nomenclatures n
            LEFT JOIN categories c ON n.category_id = c.id
            LEFT JOIN stocks s ON n.id = s.nomenclature_id
        """, fetch_all=False)

        if not summary:
            summary = {'total': 0, 'active': 0, 'categories': 0, 'with_stock': 0}
        else:
            summary = dict(summary)

        # Данные для графиков
        categories_list = [dict(c) for c in categories]
        chart_labels = [c['name'] for c in categories_list[:10]]  # Топ-10 категорий
        chart_data = [c['total_items'] for c in categories_list[:10]]

        return render_template('reports/nomenclature_by_category.html',
                             categories=categories_list,
                             summary=summary,
                             chart_labels=json.dumps(chart_labels),
                             chart_data=json.dumps(chart_data))

    except Exception as e:
        logger.error(f'Ошибка формирования отчета по категориям: {e}')
        flash('Ошибка формирования отчета', 'error')
        return redirect(url_for('reports.reports'))


@reports_bp.route('/reports/most-moved', endpoint='report_most_moved')
@login_required
def report_most_moved():
    """Отчет по наиболее активным позициям"""
    try:
        db = get_db()
        date_from = request.args.get('date_from', (datetime.now().replace(day=1)).strftime('%Y-%m-%d'))
        date_to = request.args.get('date_to', datetime.now().strftime('%Y-%m-%d'))

        # Топ по поступлениям
        top_receipts = db.execute_query("""
            SELECT
                n.id,
                n.name,
                n.sku,
                c.name_ru as category_name,
                SUM(di.quantity) as total_quantity,
                SUM(di.amount) as total_amount
            FROM nomenclatures n
            JOIN document_items di ON n.id = di.nomenclature_id
            JOIN documents d ON di.document_id = d.id
            LEFT JOIN categories c ON n.category_id = c.id
            WHERE d.document_type = 'receipt'
                AND d.status = 'posted'
                AND d.document_date BETWEEN ? AND ?
            GROUP BY n.id, n.name, n.sku, c.name_ru
            ORDER BY total_quantity DESC
            LIMIT 10
        """, (date_from, date_to), fetch_all=True) or []

        # Топ по выдачам
        top_issuances = db.execute_query("""
            SELECT
                n.id,
                n.name,
                n.sku,
                c.name_ru as category_name,
                SUM(di.quantity) as total_quantity,
                SUM(di.amount) as total_amount
            FROM nomenclatures n
            JOIN document_items di ON n.id = di.nomenclature_id
            JOIN documents d ON di.document_id = d.id
            LEFT JOIN categories c ON n.category_id = c.id
            WHERE d.document_type IN ('issuance', 'write_off')
                AND d.status = 'posted'
                AND d.document_date BETWEEN ? AND ?
            GROUP BY n.id, n.name, n.sku, c.name_ru
            ORDER BY total_quantity DESC
            LIMIT 10
        """, (date_from, date_to), fetch_all=True) or []

        # Общий рейтинг активности
        rating = db.execute_query("""
            SELECT
                n.id,
                n.name,
                n.sku,
                c.name_ru as category_name,
                COUNT(DISTINCT d.id) as total_movements,
                SUM(CASE WHEN d.document_type = 'receipt' THEN di.quantity ELSE 0 END) as total_receipts,
                SUM(CASE WHEN d.document_type IN ('issuance', 'write_off') THEN di.quantity ELSE 0 END) as total_issuances,
                COALESCE((SELECT SUM(quantity) FROM stocks WHERE nomenclature_id = n.id), 0) as current_stock,
                julianday(?) - julianday(?) as days_period,
                CASE
                    WHEN SUM(CASE WHEN d.document_type IN ('issuance', 'write_off') THEN di.quantity ELSE 0 END) > 0
                    THEN (julianday(?) - julianday(?)) * COALESCE((SELECT AVG(quantity) FROM stocks WHERE nomenclature_id = n.id), 1)
                         / SUM(CASE WHEN d.document_type IN ('issuance', 'write_off') THEN di.quantity ELSE 0 END)
                    ELSE 999
                END as turnover
            FROM nomenclatures n
            LEFT JOIN document_items di ON n.id = di.nomenclature_id
            LEFT JOIN documents d ON di.document_id = d.id AND d.status = 'posted'
            LEFT JOIN categories c ON n.category_id = c.id
            WHERE d.document_date BETWEEN ? AND ?
            GROUP BY n.id, n.name, n.sku, c.name_ru
            HAVING total_movements > 0
            ORDER BY total_movements DESC
            LIMIT 50
        """, (date_to, date_from, date_to, date_from, date_from, date_to), fetch_all=True) or []

        return render_template('reports/most_moved.html',
                             top_receipts=[dict(t) for t in top_receipts],
                             top_issuances=[dict(t) for t in top_issuances],
                             rating=[dict(r) for r in rating])

    except Exception as e:
        logger.error(f'Ошибка формирования отчета по активности: {e}')
        flash('Ошибка формирования отчета', 'error')
        return redirect(url_for('reports.reports'))


@reports_bp.route('/reports/inactive', endpoint='report_inactive')
@login_required
def report_inactive():
    """Отчет по неактивным позициям"""
    try:
        db = get_db()
        days = int(request.args.get('days', 90))
        category_id = request.args.get('category_id')

        # Категории для фильтра
        categories_result = db.get_all_categories()
        categories = []
        if categories_result:
            for c in categories_result:
                if isinstance(c, dict):
                    categories.append(c)
                else:
                    categories.append(dict(c))

        # Неактивные позиции
        query = """
            WITH last_movements AS (
                SELECT
                    di.nomenclature_id,
                    MAX(d.document_date) as last_date,
                    d.document_type as last_type
                FROM document_items di
                JOIN documents d ON di.document_id = d.id
                WHERE d.status = 'posted'
                GROUP BY di.nomenclature_id
            )
            SELECT
                n.id,
                n.name,
                n.sku,
                c.name_ru as category_name,
                lm.last_date as last_movement_date,
                lm.last_type as last_movement_type,
                julianday('now') - julianday(COALESCE(lm.last_date, '2000-01-01')) as days_inactive,
                COALESCE((SELECT SUM(quantity) FROM stocks WHERE nomenclature_id = n.id), 0) as current_stock,
                COALESCE((SELECT SUM(quantity * COALESCE(b.purchase_price, 0))
                         FROM stocks s
                         LEFT JOIN batches b ON s.batch_id = b.id
                         WHERE s.nomenclature_id = n.id), 0) as stock_amount
            FROM nomenclatures n
            LEFT JOIN last_movements lm ON n.id = lm.nomenclature_id
            LEFT JOIN categories c ON n.category_id = c.id
            WHERE n.is_active = 1
                AND (lm.last_date IS NULL OR julianday('now') - julianday(lm.last_date) > ?)
        """
        params = [days]

        if category_id:
            query += " AND n.category_id = ?"
            params.append(category_id)

        query += " ORDER BY days_inactive DESC"

        inactive_result = db.execute_query(query, params, fetch_all=True)

        inactive_list = []
        if inactive_result:
            for item in inactive_result:
                if item:
                    item_dict = dict(item)
                    item_dict['days_inactive'] = int(float(item_dict['days_inactive'] or 0))
                    item_dict['current_stock'] = int(item_dict['current_stock'] or 0)
                    item_dict['stock_amount'] = float(item_dict['stock_amount'] or 0)
                    inactive_list.append(item_dict)

        # Сводка
        summary = {
            'total_inactive': len(inactive_list),
            'with_stock': sum(1 for i in inactive_list if i['current_stock'] > 0),
            'total_amount': sum(i['stock_amount'] for i in inactive_list)
        }

        return render_template('reports/inactive.html',
                             categories=categories,
                             inactive=inactive_list,
                             summary=summary)

    except Exception as e:
        logger.error(f'Ошибка формирования отчета по неактивным: {e}')
        flash('Ошибка формирования отчета', 'error')
        return redirect(url_for('reports.reports'))


@reports_bp.route('/reports/turnover', endpoint='report_turnover')
@login_required
def report_turnover():
    """Отчет по оборачиваемости"""
    try:
        db = get_db()
        period = int(request.args.get('period', 30))
        category_id = request.args.get('category_id')
        warehouse_id = request.args.get('warehouse_id')

        # Категории и склады для фильтров
        categories_result = db.get_all_categories()
        categories = []
        if categories_result:
            for c in categories_result:
                if isinstance(c, dict):
                    categories.append(c)
                else:
                    categories.append(dict(c))

        warehouses_result = db.execute_query("SELECT id, name FROM warehouses WHERE is_active = 1", fetch_all=True)
        warehouses = []
        if warehouses_result:
            for w in warehouses_result:
                if isinstance(w, dict):
                    warehouses.append(w)
                else:
                    warehouses.append(dict(w))

        # Расчет оборачиваемости
        query = """
            WITH sales_data AS (
                SELECT
                    di.nomenclature_id,
                    COALESCE(SUM(di.quantity), 0) as period_sales,
                    COALESCE(AVG(di.price), 0) as avg_price,
                    COUNT(DISTINCT d.id) as sales_count
                FROM document_items di
                JOIN documents d ON di.document_id = d.id
                WHERE d.document_type IN ('issuance', 'write_off')
                    AND d.status = 'posted'
                    AND d.document_date >= date('now', '-' || ? || ' days')
                GROUP BY di.nomenclature_id
            ),
            stock_data AS (
                SELECT
                    nomenclature_id,
                    COALESCE(AVG(quantity), 0) as avg_stock
                FROM stocks
                WHERE 1=1
        """

        params = [period]

        if warehouse_id:
            query += " AND warehouse_id = ?"
            params.append(warehouse_id)

        query += """
                GROUP BY nomenclature_id
            )
            SELECT
                n.id,
                n.name,
                n.sku,
                c.name_ru as category_name,
                COALESCE(sd.period_sales, 0) as period_sales,
                COALESCE(sd.avg_price, 0) as avg_price,
                COALESCE(std.avg_stock, 0) as avg_stock,
                CASE
                    WHEN COALESCE(sd.period_sales, 0) > 0
                    THEN (COALESCE(std.avg_stock, 0) * ?) / sd.period_sales
                    ELSE 999
                END as turnover_days,
                CASE
                    WHEN COALESCE(std.avg_stock, 0) > 0
                    THEN COALESCE(sd.period_sales, 0) / ? / std.avg_stock
                    ELSE 0
                END as speed
            FROM nomenclatures n
            LEFT JOIN sales_data sd ON n.id = sd.nomenclature_id
            LEFT JOIN stock_data std ON n.id = std.nomenclature_id
            LEFT JOIN categories c ON n.category_id = c.id
            WHERE n.is_active = 1
        """

        params.extend([period, period])

        if category_id:
            query += " AND n.category_id = ?"
            params.append(category_id)

        query += " ORDER BY turnover_days"

        turnover_result = db.execute_query(query, params, fetch_all=True)

        turnover_list = []
        if turnover_result:
            for item in turnover_result:
                if item:
                    item_dict = dict(item)
                    item_dict['period_sales'] = float(item_dict['period_sales'] or 0)
                    item_dict['avg_price'] = float(item_dict['avg_price'] or 0)
                    item_dict['avg_stock'] = float(item_dict['avg_stock'] or 0)
                    item_dict['turnover_days'] = float(item_dict['turnover_days'] or 999)
                    item_dict['speed'] = float(item_dict['speed'] or 0)
                    turnover_list.append(item_dict)

        # Средние показатели
        valid_turnover = [t['turnover_days'] for t in turnover_list if t['turnover_days'] < 999]
        avg_turnover = sum(valid_turnover) / len(valid_turnover) if valid_turnover else 0
        avg_stock = sum(t['avg_stock'] for t in turnover_list) / len(turnover_list) if turnover_list else 0
        sales_speed = sum(t['speed'] for t in turnover_list) / len(turnover_list) if turnover_list else 0
        slow_moving = sum(1 for t in turnover_list if t['turnover_days'] > 90)

        # Данные для графика (топ-10)
        chart_labels = []
        chart_data = []
        for t in turnover_list[:10]:
            name = t['name']
            if len(name) > 20:
                name = name[:20] + '...'
            chart_labels.append(name)
            chart_data.append(t['turnover_days'])

        return render_template('reports/turnover.html',
                             categories=categories,
                             warehouses=warehouses,
                             turnover=turnover_list,
                             avg_turnover=avg_turnover,
                             avg_stock=avg_stock,
                             sales_speed=sales_speed,
                             slow_moving=slow_moving,
                             chart_labels=json.dumps(chart_labels),
                             chart_data=json.dumps(chart_data))

    except Exception as e:
        logger.error(f'Ошибка формирования отчета по оборачиваемости: {e}')
        flash('Ошибка формирования отчета', 'error')
        return redirect(url_for('reports.reports'))


@reports_bp.route('/reports/profit-loss', endpoint='report_profit_loss')
@login_required
def report_profit_loss():
    """Отчет по прибылям/убыткам"""
    try:
        db = get_db()
        date_from = request.args.get('date_from', (datetime.now().replace(day=1)).strftime('%Y-%m-%d'))
        date_to = request.args.get('date_to', datetime.now().strftime('%Y-%m-%d'))

        # Доходы (продажи)
        income_data = db.execute_query("""
            SELECT
                COALESCE(SUM(di.amount), 0) as total
            FROM document_items di
            JOIN documents d ON di.document_id = d.id
            WHERE d.document_type IN ('issuance')
                AND d.status = 'posted'
                AND d.document_date BETWEEN ? AND ?
        """, (date_from, date_to), fetch_all=False)
        income = float(income_data['total']) if income_data and income_data['total'] else 0

        # Расходы (закупки)
        expenses_data = db.execute_query("""
            SELECT
                COALESCE(SUM(di.amount), 0) as total
            FROM document_items di
            JOIN documents d ON di.document_id = d.id
            WHERE d.document_type IN ('receipt')
                AND d.status = 'posted'
                AND d.document_date BETWEEN ? AND ?
        """, (date_from, date_to), fetch_all=False)
        expenses = float(expenses_data['total']) if expenses_data and expenses_data['total'] else 0

        profit = income - expenses
        margin = (profit / income * 100) if income > 0 else 0

        # Прибыль по категориям
        category_profit_result = db.execute_query("""
            WITH sales AS (
                SELECT
                    n.category_id,
                    COALESCE(SUM(di.quantity), 0) as sold_quantity,
                    COALESCE(SUM(di.amount), 0) as revenue,
                    COALESCE(SUM(di.quantity * COALESCE(b.purchase_price, 0)), 0) as cost
                FROM document_items di
                JOIN documents d ON di.document_id = d.id
                JOIN nomenclatures n ON di.nomenclature_id = n.id
                LEFT JOIN batches b ON di.batch_id = b.id
                WHERE d.document_type IN ('issuance')
                    AND d.status = 'posted'
                    AND d.document_date BETWEEN ? AND ?
                GROUP BY n.category_id
            )
            SELECT
                c.name_ru as name,
                COALESCE(s.sold_quantity, 0) as sold_quantity,
                COALESCE(s.revenue, 0) as revenue,
                COALESCE(s.cost, 0) as cost,
                COALESCE(s.revenue, 0) - COALESCE(s.cost, 0) as profit,
                CASE
                    WHEN COALESCE(s.revenue, 0) > 0
                    THEN (COALESCE(s.revenue, 0) - COALESCE(s.cost, 0)) / s.revenue * 100
                    ELSE 0
                END as margin
            FROM categories c
            LEFT JOIN sales s ON c.id = s.category_id
            WHERE c.is_active = 1
            ORDER BY profit DESC
        """, (date_from, date_to), fetch_all=True)

        category_profit_list = []
        if category_profit_result:
            for item in category_profit_result:
                if item:
                    cat_dict = dict(item)
                    cat_dict['sold_quantity'] = float(cat_dict['sold_quantity'] or 0)
                    cat_dict['revenue'] = float(cat_dict['revenue'] or 0)
                    cat_dict['cost'] = float(cat_dict['cost'] or 0)
                    cat_dict['profit'] = float(cat_dict['profit'] or 0)
                    cat_dict['margin'] = float(cat_dict['margin'] or 0)
                    category_profit_list.append(cat_dict)

        # Расчет доли
        total_profit = sum(c['profit'] for c in category_profit_list)
        for cat in category_profit_list:
            cat['share'] = (cat['profit'] / total_profit * 100) if total_profit > 0 else 0

        # Топ-10 прибыльных позиций
        top_profitable_result = db.execute_query("""
            SELECT
                n.name,
                n.sku,
                COALESCE(SUM(di.quantity), 0) as sold_quantity,
                COALESCE(SUM(di.amount), 0) as revenue,
                COALESCE(SUM(di.quantity * COALESCE(b.purchase_price, 0)), 0) as cost,
                COALESCE(SUM(di.amount), 0) - COALESCE(SUM(di.quantity * COALESCE(b.purchase_price, 0)), 0) as profit,
                CASE
                    WHEN COALESCE(SUM(di.amount), 0) > 0
                    THEN (COALESCE(SUM(di.amount), 0) - COALESCE(SUM(di.quantity * COALESCE(b.purchase_price, 0)), 0)) / SUM(di.amount) * 100
                    ELSE 0
                END as margin
            FROM document_items di
            JOIN documents d ON di.document_id = d.id
            JOIN nomenclatures n ON di.nomenclature_id = n.id
            LEFT JOIN batches b ON di.batch_id = b.id
            WHERE d.document_type IN ('issuance')
                AND d.status = 'posted'
                AND d.document_date BETWEEN ? AND ?
            GROUP BY n.id, n.name, n.sku
            ORDER BY profit DESC
            LIMIT 10
        """, (date_from, date_to), fetch_all=True)

        top_profitable_list = []
        if top_profitable_result:
            for item in top_profitable_result:
                if item:
                    t_dict = dict(item)
                    t_dict['sold_quantity'] = float(t_dict['sold_quantity'] or 0)
                    t_dict['revenue'] = float(t_dict['revenue'] or 0)
                    t_dict['cost'] = float(t_dict['cost'] or 0)
                    t_dict['profit'] = float(t_dict['profit'] or 0)
                    t_dict['margin'] = float(t_dict['margin'] or 0)
                    top_profitable_list.append(t_dict)

        # Данные для графиков
        dates = []
        daily_income = []
        daily_expenses = []

        # Получаем данные по дням
        daily_data = db.execute_query("""
            SELECT
                d.document_date,
                COALESCE(SUM(CASE WHEN d.document_type IN ('issuance') THEN di.amount ELSE 0 END), 0) as income,
                COALESCE(SUM(CASE WHEN d.document_type IN ('receipt') THEN di.amount ELSE 0 END), 0) as expenses
            FROM documents d
            JOIN document_items di ON d.id = di.document_id
            WHERE d.status = 'posted'
                AND d.document_date BETWEEN ? AND ?
            GROUP BY d.document_date
            ORDER BY d.document_date
        """, (date_from, date_to), fetch_all=True)

        if daily_data:
            for day in daily_data:
                if day:
                    dates.append(day['document_date'])
                    daily_income.append(float(day['income'] or 0))
                    daily_expenses.append(float(day['expenses'] or 0))

        # Структура расходов
        expense_structure = db.execute_query("""
            SELECT
                c.name_ru as category,
                COALESCE(SUM(di.amount), 0) as amount
            FROM document_items di
            JOIN documents d ON di.document_id = d.id
            JOIN nomenclatures n ON di.nomenclature_id = n.id
            JOIN categories c ON n.category_id = c.id
            WHERE d.document_type IN ('receipt')
                AND d.status = 'posted'
                AND d.document_date BETWEEN ? AND ?
            GROUP BY c.id, c.name_ru
            ORDER BY amount DESC
        """, (date_from, date_to), fetch_all=True)

        expense_labels = []
        expense_data = []
        if expense_structure:
            for e in expense_structure:
                if e:
                    expense_labels.append(e['category'])
                    expense_data.append(float(e['amount'] or 0))

        return render_template('reports/profit_loss.html',
                             income=income,
                             expenses=expenses,
                             profit=profit,
                             margin=margin,
                             category_profit=category_profit_list,
                             top_profitable=top_profitable_list,
                             chart_dates=json.dumps(dates),
                             chart_income=json.dumps(daily_income),
                             chart_expenses=json.dumps(daily_expenses),
                             expense_labels=json.dumps(expense_labels),
                             expense_data=json.dumps(expense_data))

    except Exception as e:
        logger.error(f'Ошибка формирования отчета по прибылям: {e}')
        flash('Ошибка формирования отчета', 'error')
        return redirect(url_for('reports.reports'))


@reports_bp.route('/reports/employee-issuance', endpoint='report_employee_issuance')
@login_required
def report_employee_issuance():
    """Отчет по выдаче сотрудникам"""
    try:
        db = get_db()
        date_from = request.args.get('date_from', (datetime.now().replace(day=1)).strftime('%Y-%m-%d'))
        date_to = request.args.get('date_to', datetime.now().strftime('%Y-%m-%d'))
        employee_id = request.args.get('employee_id')

        # Список сотрудников для фильтра
        employees = db.execute_query("""
            SELECT id, last_name || ' ' || first_name || COALESCE(' ' || middle_name, '') as full_name
            FROM employees WHERE is_active = 1 ORDER BY last_name
        """, fetch_all=True) or []

        # Статистика по сотрудникам
        employee_stats = db.execute_query("""
            WITH employee_issues AS (
                SELECT
                    e.id,
                    e.last_name || ' ' || e.first_name || COALESCE(' ' || e.middle_name, '') as full_name,
                    e.position,
                    d.name as department_name,
                    COUNT(DISTINCT di.id) as total_issued,
                    SUM(CASE WHEN i.status = 'in_use' THEN 1 ELSE 0 END) as on_hands,
                    SUM(CASE WHEN i.actual_return_date IS NOT NULL THEN 1 ELSE 0 END) as returned,
                    SUM(CASE WHEN i.expected_return_date < date('now') AND i.actual_return_date IS NULL THEN 1 ELSE 0 END) as overdue,
                    COALESCE(SUM(i.purchase_price), 0) as total_amount
                FROM employees e
                LEFT JOIN departments d ON e.department_id = d.id
                LEFT JOIN instances i ON e.id = i.employee_id
                LEFT JOIN document_items di ON i.id = di.instance_id
                LEFT JOIN documents doc ON di.document_id = doc.id
                WHERE doc.document_date BETWEEN ? AND ?
                GROUP BY e.id, e.last_name, e.first_name, e.middle_name, e.position, d.name
            )
            SELECT * FROM employee_issues
            WHERE total_issued > 0
            ORDER BY on_hands DESC
        """, (date_from, date_to), fetch_all=True) or []

        # Сводка
        summary = {
            'total_issued': sum(e['total_issued'] for e in employee_stats),
            'on_hands': sum(e['on_hands'] for e in employee_stats),
            'employees': len(employee_stats),
            'total_amount': sum(e['total_amount'] for e in employee_stats)
        }

        # Детализация выдач
        issuances_query = """
            SELECT
                doc.document_date as issued_date,
                e.last_name || ' ' || e.first_name || COALESCE(' ' || e.middle_name, '') as employee_name,
                doc.id as document_id,
                doc.document_number,
                n.name as nomenclature_name,
                i.inventory_number,
                di.quantity,
                i.expected_return_date,
                i.actual_return_date,
                CASE
                    WHEN i.expected_return_date < date('now') AND i.actual_return_date IS NULL
                    THEN 1 ELSE 0
                END as is_overdue
            FROM instances i
            JOIN employees e ON i.employee_id = e.id
            JOIN document_items di ON i.id = di.instance_id
            JOIN documents doc ON di.document_id = doc.id
            JOIN nomenclatures n ON i.nomenclature_id = n.id
            WHERE doc.document_type = 'issuance'
                AND doc.status = 'posted'
                AND doc.document_date BETWEEN ? AND ?
        """
        issuances_params = [date_from, date_to]

        if employee_id:
            issuances_query += " AND e.id = ?"
            issuances_params.append(employee_id)

        issuances_query += " ORDER BY doc.document_date DESC"

        issuances = db.execute_query(issuances_query, issuances_params, fetch_all=True) or []

        # Данные для графика
        chart_labels = []
        chart_data = []
        for emp in employee_stats[:10]:  # Топ-10 сотрудников
            chart_labels.append(emp['full_name'])
            chart_data.append(emp['on_hands'])

        return render_template('reports/employee_issuance.html',
                             employees=[dict(e) for e in employees],
                             employee_stats=[dict(e) for e in employee_stats],
                             issuances=[dict(i) for i in issuances],
                             summary=summary,
                             chart_labels=json.dumps(chart_labels),
                             chart_data=json.dumps(chart_data))

    except Exception as e:
        logger.error(f'Ошибка формирования отчета по выдачам: {e}')
        flash('Ошибка формирования отчета', 'error')
        return redirect(url_for('reports.reports'))


@reports_bp.route('/reports/variation-popularity', endpoint='report_variation_popularity')
@login_required
def report_variation_popularity():
    """Отчет по популярности модификаций"""
    try:
        db = get_db()

        # Получаем данные из представления
        data = db.execute_query("""
            SELECT * FROM v_variation_popularity
            LIMIT 100
        """, fetch_all=True)

        # Статистика по размерам
        size_stats = db.execute_query("""
            SELECT
                size,
                COUNT(*) as models_count,
                SUM(total_issued) as total_issued
            FROM v_variation_popularity
            WHERE size IS NOT NULL
            GROUP BY size
            ORDER BY total_issued DESC
        """, fetch_all=True)

        # Статистика по цветам
        color_stats = db.execute_query("""
            SELECT
                color,
                COUNT(*) as models_count,
                SUM(total_issued) as total_issued
            FROM v_variation_popularity
            WHERE color IS NOT NULL
            GROUP BY color
            ORDER BY total_issued DESC
        """, fetch_all=True)

        return render_template('reports/variation_popularity.html',
                             data=[dict(d) for d in data] if data else [],
                             size_stats=[dict(s) for s in size_stats] if size_stats else [],
                             color_stats=[dict(c) for c in color_stats] if color_stats else [])

    except Exception as e:
        logger.error(f'Ошибка отчета по модификациям: {e}')
        flash('Ошибка формирования отчета', 'error')
        return redirect(url_for('reports.reports'))
