"""
Blueprint: Suppliers
Routes: /suppliers, /suppliers/add, /suppliers/<id>/edit,
        /suppliers/<id>/view, /suppliers/<id>/delete,
        /api/suppliers/search
"""
import logging
import traceback
from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash
from extensions import csrf
from routes.common import login_required, get_db
from utils.search import build_where

logger = logging.getLogger('routes.suppliers')

suppliers_bp = Blueprint('suppliers', __name__)


@suppliers_bp.route('/suppliers', endpoint='suppliers_list')
@login_required
def suppliers_list():
    """Список поставщиков с поиском"""
    try:
        db = get_db()
        search_query = request.args.get('search', '').strip()

        where = "WHERE is_active = 1"
        params: list = []

        if search_query:
            where += build_where(
                ['LOWER(name)', 'LOWER(code)', 'LOWER(inn)', 'LOWER(contact_person)'],
                search_query, params
            )

        suppliers = db.execute_query(f"""
            SELECT * FROM suppliers
            {where}
            ORDER BY name
        """, params, fetch_all=True)

        return render_template('suppliers/list.html',
                               suppliers=[dict(s) for s in suppliers] if suppliers else [],
                               search_query=search_query)
    except Exception as e:
        logger.error(f'Ошибка загрузки поставщиков: {e}')
        flash('Ошибка загрузки поставщиков', 'error')
        return redirect(url_for('dashboard'))


@suppliers_bp.route('/suppliers/add', methods=['GET', 'POST'], endpoint='add_supplier')
@login_required
def add_supplier():
    """Создание нового поставщика"""
    db = get_db()

    if request.method == 'POST':
        try:
            code = request.form.get('code', '').strip()
            name = request.form.get('name', '').strip()

            if not code or not name:
                flash('Код и наименование обязательны', 'error')
                return redirect(url_for('suppliers.add_supplier'))

            # Проверка уникальности кода
            existing = db.execute_query(
                "SELECT id FROM suppliers WHERE code = ?",
                (code,),
                fetch_all=False
            )
            if existing:
                flash('Поставщик с таким кодом уже существует', 'error')
                return redirect(url_for('suppliers.add_supplier'))

            # Вставляем только существующие колонки
            db.execute_query("""
                INSERT INTO suppliers (
                    code, name, full_name, inn, kpp,
                    contact_person, phone, email, address,
                    is_active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                code,
                name,
                request.form.get('full_name') or None,
                request.form.get('inn') or None,
                request.form.get('kpp') or None,
                request.form.get('contact_person') or None,
                request.form.get('phone') or None,
                request.form.get('email') or None,
                request.form.get('address') or None,
                1 if 'is_active' in request.form else 0
            ))

            flash('Поставщик успешно создан', 'success')
            return redirect(url_for('suppliers.suppliers_list'))

        except Exception as e:
            logger.debug(f"Ошибка создания поставщика: {e}")
            traceback.print_exc()
            flash(f'Ошибка создания поставщика: {str(e)}', 'error')

    return render_template('suppliers/form.html',
                         title='Новый поставщик',
                         supplier=None)


@suppliers_bp.route('/suppliers/<int:id>/edit', methods=['GET', 'POST'], endpoint='edit_supplier')
@login_required
def edit_supplier(id):
    """Редактирование поставщика"""
    db = get_db()

    if request.method == 'POST':
        try:
            code = request.form.get('code', '').strip()
            name = request.form.get('name', '').strip()

            if not code or not name:
                flash('Код и наименование обязательны', 'error')
                return redirect(url_for('suppliers.edit_supplier', id=id))

            # Проверка уникальности кода (исключая текущего)
            existing = db.execute_query(
                "SELECT id FROM suppliers WHERE code = ? AND id != ?",
                (code, id),
                fetch_all=False
            )
            if existing:
                flash('Поставщик с таким кодом уже существует', 'error')
                return redirect(url_for('suppliers.edit_supplier', id=id))

            # Обновляем только существующие колонки
            db.execute_query("""
                UPDATE suppliers
                SET code = ?, name = ?, full_name = ?, inn = ?, kpp = ?,
                    contact_person = ?, phone = ?, email = ?, address = ?,
                    is_active = ?
                WHERE id = ?
            """, (
                code,
                name,
                request.form.get('full_name') or None,
                request.form.get('inn') or None,
                request.form.get('kpp') or None,
                request.form.get('contact_person') or None,
                request.form.get('phone') or None,
                request.form.get('email') or None,
                request.form.get('address') or None,
                1 if 'is_active' in request.form else 0,
                id
            ))

            flash('Поставщик обновлен', 'success')
            return redirect(url_for('suppliers.suppliers_list'))

        except Exception as e:
            logger.debug(f"Ошибка обновления поставщика: {e}")
            traceback.print_exc()
            flash(f'Ошибка обновления поставщика: {str(e)}', 'error')

    supplier = db.execute_query("SELECT * FROM suppliers WHERE id = ?", (id,), fetch_all=False)
    if not supplier:
        flash('Поставщик не найден', 'error')
        return redirect(url_for('suppliers.suppliers_list'))

    return render_template('suppliers/form.html',
                         title='Редактирование поставщика',
                         supplier=dict(supplier))


@suppliers_bp.route('/suppliers/<int:id>/view', endpoint='supplier_details')
@login_required
def supplier_details(id):
    """Просмотр поставщика"""
    try:
        db = get_db()

        supplier = db.execute_query("SELECT * FROM suppliers WHERE id = ?", (id,), fetch_all=False)
        if not supplier:
            flash('Поставщик не найден', 'error')
            return redirect(url_for('suppliers.suppliers_list'))

        # Получаем историю поставок
        deliveries = db.execute_query("""
            SELECT d.document_date, d.document_number, d.id as document_id,
                   n.name as nomenclature_name, di.quantity, di.amount
            FROM documents d
            JOIN document_items di ON d.id = di.document_id
            JOIN nomenclatures n ON di.nomenclature_id = n.id
            WHERE d.supplier_id = ? AND d.status = 'posted'
            ORDER BY d.document_date DESC
            LIMIT 50
        """, (id,), fetch_all=True) or []

        return render_template('suppliers/view.html',
                             supplier=dict(supplier),
                             deliveries=[dict(d) for d in deliveries])

    except Exception as e:
        logger.error(f'Ошибка просмотра поставщика: {e}')
        flash('Ошибка просмотра поставщика', 'error')
        return redirect(url_for('suppliers.suppliers_list'))


@suppliers_bp.route('/suppliers/<int:id>/delete', methods=['POST'], endpoint='delete_supplier')
@login_required
def delete_supplier(id):
    """Удаление поставщика"""
    try:
        db = get_db()

        # Проверяем, есть ли связанные документы
        docs = db.execute_query(
            "SELECT COUNT(*) as cnt FROM documents WHERE supplier_id = ?",
            (id,), fetch_all=False
        )

        if docs and docs['cnt'] > 0:
            flash('Нельзя удалить поставщика, по которому есть документы', 'error')
            return redirect(url_for('suppliers.suppliers_list'))

        # Проверяем, есть ли связанные партии
        batches = db.execute_query(
            "SELECT COUNT(*) as cnt FROM batches WHERE supplier_id = ?",
            (id,), fetch_all=False
        )

        if batches and batches['cnt'] > 0:
            flash('Нельзя удалить поставщика, по которому есть партии', 'error')
            return redirect(url_for('suppliers.suppliers_list'))

        # Мягкое удаление (если есть is_deleted) или физическое удаление
        has_deleted = db.column_exists('suppliers', 'is_deleted')

        if has_deleted:
            db.execute_query(
                "UPDATE suppliers SET is_deleted = 1, deleted_at = CURRENT_TIMESTAMP WHERE id = ?",
                (id,)
            )
        else:
            db.execute_query("DELETE FROM suppliers WHERE id = ?", (id,))

        flash('Поставщик удален', 'success')

    except Exception as e:
        logger.debug(f"Ошибка удаления поставщика: {e}")
        flash('Ошибка удаления поставщика', 'error')

    return redirect(url_for('suppliers.suppliers_list'))


# API для поиска поставщиков
@suppliers_bp.route('/api/suppliers/search', endpoint='api_suppliers_search')
@login_required
def api_suppliers_search():
    """Поиск поставщиков (регистронезависимый)"""
    try:
        query = request.args.get('q', '')
        db = get_db()

        sql_params: list = []
        search_cond = build_where(
            ['LOWER(name)', 'LOWER(code)', 'LOWER(inn)', 'LOWER(contact_person)'],
            query, sql_params
        )
        suppliers = db.execute_query(f"""
            SELECT id, code, name, inn
            FROM suppliers
            WHERE is_active = 1
                {search_cond}
            ORDER BY name
            LIMIT 20
        """, sql_params, fetch_all=True)

        return jsonify([dict(s) for s in suppliers] if suppliers else [])
    except Exception as e:
        return jsonify([])
