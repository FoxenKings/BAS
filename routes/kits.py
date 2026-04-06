"""
Blueprint: kits
Маршруты для управления комплектами.
"""
import time
import logging
import traceback
from datetime import datetime
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, flash
from extensions import csrf
from routes.common import login_required, admin_required, get_db

logger = logging.getLogger('routes')

kits_bp = Blueprint('kits', __name__)

# ============ УПРАВЛЕНИЕ КОМПЛЕКТАМИ (KITS) ============

@kits_bp.route('/kits', endpoint='kits_list')
@login_required
def kits_list():
    """Список комплектов"""
    try:
        db = get_db()

        # Получаем все номенклатуры, которые являются комплектами
        kits = db.execute_query("""
            SELECT n.*,
                   (SELECT COUNT(*) FROM kit_specifications WHERE kit_nomenclature_id = n.id) as components_count,
                   (SELECT COUNT(*) FROM instances WHERE nomenclature_id = n.id) as instances_count
            FROM nomenclatures n
            WHERE n.accounting_type = 'kit' OR n.id IN (SELECT DISTINCT kit_nomenclature_id FROM kit_specifications)
            ORDER BY n.name
            LIMIT 500
        """, fetch_all=True)

        return render_template('kits/list.html',
                             kits=[dict(k) for k in kits] if kits else [])
    except Exception as e:
        logger.error(f'Ошибка загрузки комплектов: {e}')
        flash('Ошибка загрузки комплектов', 'error')
        return redirect(url_for('dashboard'))

@kits_bp.route('/kits/<int:id>', endpoint='kit_view')
@login_required
def kit_view(id):
    """Просмотр комплекта"""
    try:
        db = get_db()

        # Информация о комплекте
        kit = db.execute_query("""
            SELECT n.*, c.name_ru as category_name
            FROM nomenclatures n
            LEFT JOIN categories c ON n.category_id = c.id
            WHERE n.id = ?
        """, (id,), fetch_all=False)

        if not kit:
            flash('Комплект не найден', 'error')
            return redirect(url_for('kits.kits_list'))

        # Преобразуем в словарь
        kit = dict(kit)

        # Спецификация комплекта
        components = db.execute_query("""
            SELECT ks.*,
                   n.name as component_name,
                   n.sku as component_sku,
                   n.unit as component_unit,
                   c.name_ru as category_name
            FROM kit_specifications ks
            JOIN nomenclatures n ON ks.component_nomenclature_id = n.id
            LEFT JOIN categories c ON n.category_id = c.id
            WHERE ks.kit_nomenclature_id = ?
            ORDER BY n.name
        """, (id,), fetch_all=True)

        # Все доступные компоненты для добавления
        available_components = db.execute_query("""
            SELECT n.id, n.name, n.sku, n.unit, c.name_ru as category_name
            FROM nomenclatures n
            LEFT JOIN categories c ON n.category_id = c.id
            WHERE n.id != ? AND n.is_active = 1
            AND n.id NOT IN (
                SELECT component_nomenclature_id
                FROM kit_specifications
                WHERE kit_nomenclature_id = ?
            )
            ORDER BY n.name
            LIMIT 50
        """, (id, id), fetch_all=True)

        # Преобразуем в списки словарей
        components_list = [dict(c) for c in components] if components else []
        available_list = [dict(a) for a in available_components] if available_components else []

        logger.debug(f"Комплект {kit.get('name')}, компонентов: {len(components_list)}, доступно: {len(available_list)}")

        return render_template('kits/view.html',
                             kit=kit,
                             components=components_list,
                             available_components=available_list)
    except Exception as e:
        logger.debug(f"Ошибка просмотра комплекта: {e}")
        import traceback
        traceback.print_exc()
        flash('Ошибка просмотра комплекта', 'error')
        return redirect(url_for('kits.kits_list'))

@kits_bp.route('/kits/create', methods=['GET', 'POST'], endpoint='kit_create')
@login_required
def kit_create():
    """Создание нового комплекта"""
    if request.method == 'POST':
        try:
            db = get_db()

            # Создаем номенклатуру-комплект
            data = {
                'sku': request.form.get('sku'),
                'name': request.form.get('name'),
                'description': request.form.get('description'),
                'category_id': request.form.get('category_id'),
                'accounting_type': 'kit',  # Специальный тип для комплектов
                'unit': request.form.get('unit', 'компл'),
                'is_active': 'is_active' in request.form
            }

            result = db.create_nomenclature(data, session['user_id'])

            if result['success']:
                kit_id = result['id']

                # Добавляем компоненты (batch insert вместо N+1)
                component_ids = request.form.getlist('component_id[]')
                quantities = request.form.getlist('quantity[]')

                specs = [
                    (kit_id, int(cid), int(qty))
                    for cid, qty in zip(component_ids, quantities)
                    if cid and qty
                ]
                if specs:
                    for kit_id_s, cid_s, qty_s in specs:
                        db.execute_query(
                            "INSERT INTO kit_specifications (kit_nomenclature_id, component_nomenclature_id, quantity) VALUES (?, ?, ?)",
                            (kit_id_s, cid_s, qty_s)
                        )

                flash('Комплект успешно создан', 'success')
                return redirect(url_for('kits.kit_view', id=kit_id))
            else:
                flash(result['message'], 'error')

        except Exception as e:
            logger.error(f'Ошибка создания комплекта: {e}')
            flash('Ошибка создания комплекта', 'error')

    # GET запрос - показываем форму
    db = get_db()
    categories = db.get_all_categories()
    nomenclatures = db.search_nomenclatures(limit=1000)

    return render_template('kits/form.html',
                         title='Новый комплект',
                         kit=None,
                         categories=categories,
                         nomenclatures=nomenclatures)

@kits_bp.route('/kits/<int:id>/edit', methods=['GET', 'POST'], endpoint='kit_edit')
@login_required
def kit_edit(id):
    """Редактирование комплекта"""
    db = get_db()

    if request.method == 'POST':
        try:
            # Обновляем основную информацию
            data = {
                'sku': request.form.get('sku'),
                'name': request.form.get('name'),
                'description': request.form.get('description'),
                'category_id': request.form.get('category_id'),
                'unit': request.form.get('unit', 'компл'),
                'is_active': 'is_active' in request.form
            }

            result = db.update_nomenclature(id, data, session['user_id'])

            if result['success']:
                # Удаляем старую спецификацию
                db.execute_query("DELETE FROM kit_specifications WHERE kit_nomenclature_id = ?", (id,))

                # Добавляем новую спецификацию
                component_ids = request.form.getlist('component_id[]')
                quantities = request.form.getlist('quantity[]')

                specs = [(id, int(cid), int(qty)) for cid, qty in zip(component_ids, quantities) if cid and qty]
                if specs:
                    for kit_id_s, cid_s, qty_s in specs:
                        db.execute_query(
                            "INSERT INTO kit_specifications (kit_nomenclature_id, component_nomenclature_id, quantity) VALUES (?, ?, ?)",
                            (kit_id_s, cid_s, qty_s)
                        )

                flash('Комплект обновлен', 'success')
                return redirect(url_for('kits.kit_view', id=id))
            else:
                flash(result['message'], 'error')

        except Exception as e:
            logger.error(f'Ошибка обновления комплекта: {e}')
            flash('Ошибка обновления комплекта', 'error')

    # GET запрос
    kit = db.execute_query("SELECT * FROM nomenclatures WHERE id = ?", (id,), fetch_all=False)
    if not kit:
        flash('Комплект не найден', 'error')
        return redirect(url_for('kits.kits_list'))

    # Текущие компоненты
    components = db.execute_query("""
        SELECT ks.*, n.name, n.sku
        FROM kit_specifications ks
        JOIN nomenclatures n ON ks.component_nomenclature_id = n.id
        WHERE ks.kit_nomenclature_id = ?
    """, (id,), fetch_all=True)

    categories = db.get_all_categories()
    nomenclatures = db.search_nomenclatures(limit=1000)

    return render_template('kits/form.html',
                         title='Редактирование комплекта',
                         kit=dict(kit),
                         components=[dict(c) for c in components] if components else [],
                         categories=categories,
                         nomenclatures=nomenclatures)

@kits_bp.route('/kits/<int:id>/add_component', methods=['POST'], endpoint='kit_add_component')
@login_required
def kit_add_component(id):
    """Добавление компонента в комплект"""
    try:
        db = get_db()
        data = request.json

        logger.debug(f"Добавление компонента в комплект {id}, данные: {data}")

        component_id = data.get('component_id')
        quantity = data.get('quantity', 1)

        if not component_id:
            return jsonify({'success': False, 'error': 'Не выбран компонент'})

        # Проверяем существование комплекта
        kit = db.execute_query("SELECT id FROM nomenclatures WHERE id = ?", (id,), fetch_all=False)
        if not kit:
            return jsonify({'success': False, 'error': 'Комплект не найден'})

        # Проверяем существование компонента
        component = db.execute_query("SELECT id FROM nomenclatures WHERE id = ?", (component_id,), fetch_all=False)
        if not component:
            return jsonify({'success': False, 'error': 'Компонент не найден'})

        # Проверяем, не пытаемся ли добавить комплект в самого себя
        if id == component_id:
            return jsonify({'success': False, 'error': 'Нельзя добавить комплект в самого себя'})

        # Проверяем, существует ли уже такой компонент
        existing = db.execute_query("""
            SELECT id, quantity FROM kit_specifications
            WHERE kit_nomenclature_id = ? AND component_nomenclature_id = ?
        """, (id, component_id), fetch_all=False)

        try:
            if existing:
                # Обновляем количество
                db.execute_query("""
                    UPDATE kit_specifications
                    SET quantity = quantity + ?
                    WHERE kit_nomenclature_id = ? AND component_nomenclature_id = ?
                """, (quantity, id, component_id))
                message = f'Количество компонента увеличено на {quantity}'
            else:
                # Добавляем новый компонент
                db.execute_query("""
                    INSERT INTO kit_specifications (kit_nomenclature_id, component_nomenclature_id, quantity)
                    VALUES (?, ?, ?)
                """, (id, component_id, quantity))
                message = f'Компонент добавлен (количество: {quantity})'

            logger.debug(f"Компонент добавлен: {message}")

            return jsonify({'success': True, 'message': message})

        except Exception as e:
            logger.error(f"Ошибка SQL при добавлении компонента: {e}")
            return jsonify({'success': False, 'error': f'Ошибка базы данных: {str(e)}'})

    except Exception as e:
        logger.debug(f"Ошибка добавления компонента: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)})

@kits_bp.route('/kits/<int:kit_id>/remove_component/<int:component_id>', methods=['POST'], endpoint='kit_remove_component')
@login_required
def kit_remove_component(kit_id, component_id):
    """Удаление компонента из комплекта"""
    try:
        db = get_db()

        # Проверяем существование
        existing = db.execute_query("""
            SELECT id FROM kit_specifications
            WHERE kit_nomenclature_id = ? AND component_nomenclature_id = ?
        """, (kit_id, component_id), fetch_all=False)

        if not existing:
            return jsonify({'success': False, 'error': 'Компонент не найден в комплекте'})

        # Удаляем компонент
        db.execute_query("""
            DELETE FROM kit_specifications
            WHERE kit_nomenclature_id = ? AND component_nomenclature_id = ?
        """, (kit_id, component_id))

        # Логируем действие
        db.log_user_action(
            user_id=session['user_id'],
            action='remove_component',
            entity_type='kit',
            entity_id=kit_id,
            details=f'Удален компонент {component_id} из комплекта {kit_id}'
        )

        return jsonify({'success': True, 'message': 'Компонент удален'})

    except Exception as e:
        logger.debug(f"Ошибка удаления компонента: {e}")
        return jsonify({'success': False, 'error': str(e)})

@kits_bp.route('/kits/<int:id>/create_instance', methods=['POST'], endpoint='kit_create_instance')
@login_required
def kit_create_instance(id):
    """Создание экземпляра комплекта"""
    try:
        db = get_db()
        data = request.json

        inventory_number = data.get('inventory_number')
        location_id = data.get('location_id')

        if not inventory_number:
            # Генерируем инвентарный номер
            year = datetime.now().year
            inventory_number = f"KIT-{year}-{int(time.time()) % 10000:04d}"

        # Создаем экземпляр комплекта
        result = db.create_kit_instance(id, inventory_number, location_id, session['user_id'])

        return jsonify(result)

    except Exception as e:
        logger.error(f'Ошибка создания экземпляра комплекта: {e}')
        return jsonify({'success': False, 'error': str(e)})

@kits_bp.route('/kits/<int:id>/delete', methods=['POST'], endpoint='kit_delete')
@login_required
def kit_delete(id):
    """Удаление комплекта"""
    try:
        db = get_db()

        # Проверяем, есть ли созданные экземпляры
        instances = db.execute_query(
            "SELECT COUNT(*) as cnt FROM instances WHERE nomenclature_id = ?",
            (id,), fetch_all=False
        )

        if instances and instances['cnt'] > 0:
            flash('Нельзя удалить комплект, по которому есть экземпляры', 'error')
            return redirect(url_for('kits.kits_list'))

        # Удаляем спецификацию
        db.execute_query("DELETE FROM kit_specifications WHERE kit_nomenclature_id = ?", (id,))

        # Удаляем номенклатуру
        db.execute_query("DELETE FROM nomenclatures WHERE id = ?", (id,))

        flash('Комплект удален', 'success')

    except Exception as e:
        logger.error(f'Ошибка удаления комплекта: {e}')
        flash('Ошибка удаления комплекта', 'error')

    return redirect(url_for('kits.kits_list'))

@kits_bp.route('/debug/kit/<int:kit_id>', endpoint='debug_kit')
@admin_required
def debug_kit(kit_id):
    """Отладка комплекта"""
    from flask import current_app
    if not current_app.debug:
        return jsonify({'error': 'Debug-only endpoint'}), 403
    try:
        db = get_db()

        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Отладка комплекта</title>
            <style>
                body {{ font-family: Arial; padding: 20px; background: #f5f5f5; }}
                .container {{ max-width: 1200px; margin: 0 auto; }}
                .card {{ background: white; border-radius: 8px; padding: 20px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                table {{ border-collapse: collapse; width: 100%; background: white; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #4CAF50; color: white; }}
                .success {{ color: green; }}
                .error {{ color: red; }}
                pre {{ background: #f4f4f4; padding: 10px; border-radius: 4px; overflow-x: auto; }}
                .btn {{ background: #4CAF50; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; }}
                .btn:hover {{ background: #45a049; }}
                input, select {{ padding: 8px; margin: 5px; border: 1px solid #ddd; border-radius: 4px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Отладка комплекта ID: {kit_id}</h1>
        """

        # Получаем информацию о комплекте
        kit_row = db.execute_query("SELECT * FROM nomenclatures WHERE id = ?", (kit_id,), fetch_all=False)
        if kit_row:
            kit_dict = dict(kit_row)
            html += f"""
            <div class="card">
                <h2>Информация о комплекте</h2>
                <table>
                    <tr><th>Поле</th><th>Значение</th></tr>
                    <tr><td>ID</td><td>{kit_dict.get('id')}</td></tr>
                    <tr><td>SKU</td><td><code>{kit_dict.get('sku')}</code></td></tr>
                    <tr><td>Наименование</td><td>{kit_dict.get('name')}</td></tr>
                    <tr><td>Тип учета</td><td>{kit_dict.get('accounting_type')}</td></tr>
                    <tr><td>Активен</td><td>{'✅' if kit_dict.get('is_active') else '❌'}</td></tr>
                </table>
            </div>
            """
        else:
            html += f'<div class="card error">Комплект с ID {kit_id} не найден!</div>'

        # Проверяем существование таблицы kit_specifications
        table_exists = db.execute_query("SELECT name FROM sqlite_master WHERE type='table' AND name='kit_specifications'", fetch_all=False)

        html += f"""
        <div class="card">
            <h2>Таблица kit_specifications существует: {'✅' if table_exists else '❌'}</h2>
        """

        if table_exists:
            # Получаем структуру таблицы
            columns = db.execute_query("PRAGMA table_info(kit_specifications)", fetch_all=True) or []
            html += "<h3>Структура таблицы:</h3><ul>"
            for col in columns:
                html += f"<li>{col['name']} ({col['type']})</li>"
            html += "</ul>"

            # Получаем спецификацию комплекта
            specs = db.execute_query("""
                SELECT ks.id, ks.kit_nomenclature_id, ks.component_nomenclature_id, ks.quantity,
                       n.name, n.sku, n.accounting_type
                FROM kit_specifications ks
                LEFT JOIN nomenclatures n ON ks.component_nomenclature_id = n.id
                WHERE ks.kit_nomenclature_id = ?
            """, (kit_id,), fetch_all=True) or []

            html += f"<h3>Компоненты комплекта ({len(specs)}):</h3>"
            if specs:
                html += """
                <table>
                    <tr>
                        <th>ID</th>
                        <th>Component ID</th>
                        <th>Количество</th>
                        <th>Наименование</th>
                        <th>SKU</th>
                        <th>Тип учета</th>
                    </tr>
                """
                for s in specs:
                    html += f"""
                    <tr>
                        <td>{s['id']}</td>
                        <td>{s['component_nomenclature_id']}</td>
                        <td>{s['quantity']}</td>
                        <td>{s['name'] or ''}</td>
                        <td><code>{s['sku'] or ''}</code></td>
                        <td>{s['accounting_type'] or ''}</td>
                    </tr>
                    """
                html += "</table>"
            else:
                html += "<p>Нет компонентов</p>"

            # Получаем доступные номенклатуры для добавления
            nomenclatures = db.execute_query("""
                SELECT id, sku, name, accounting_type
                FROM nomenclatures
                WHERE is_active = 1 AND id != ?
                ORDER BY name
                LIMIT 50
            """, (kit_id,), fetch_all=True) or []

            if nomenclatures:
                html += """
                <h3>Добавить компонент:</h3>
                <form id="addForm" onsubmit="event.preventDefault(); testAdd();">
                    <select id="comp_id" style="width: 300px;">
                """
                for n in nomenclatures:
                    html += f"<option value='{n['id']}'>{n['sku']} - {n['name']} ({n['accounting_type']})</option>"
                html += """
                    </select>
                    <input type="number" id="qty" value="1" min="1" style="width: 80px;">
                    <button type="submit" class="btn">Добавить компонент</button>
                </form>
                <div id="result" style="margin-top: 20px;"></div>
                """
            else:
                html += "<p>Нет доступных номенклатур для добавления</p>"

        html += "</div>"

        # Добавляем JavaScript
        html += """
            <script>
            function testAdd() {
                const kitId = """ + str(kit_id) + """;
                const compId = document.getElementById('comp_id').value;
                const qty = document.getElementById('qty').value;

                const resultDiv = document.getElementById('result');
                resultDiv.innerHTML = '<p>Отправка запроса...</p>';

                fetch(`/kits/${kitId}/add_component`, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        component_id: parseInt(compId),
                        quantity: parseInt(qty)
                    })
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        resultDiv.innerHTML = '<div class="success">✅ ' + data.message + '</div>';
                        setTimeout(() => location.reload(), 1500);
                    } else {
                        resultDiv.innerHTML = '<div class="error">❌ Ошибка: ' + data.error + '</div>';
                    }
                })
                .catch(error => {
                    resultDiv.innerHTML = '<div class="error">❌ Ошибка: ' + error + '</div>';
                });
            }
            </script>
        """

        html += """
            </div>
        </body>
        </html>
        """
        return html

    except Exception as e:
        error_msg = f"<h1>Ошибка</h1><pre>{traceback.format_exc()}</pre>"
        return error_msg, 500

@kits_bp.route('/api/kits/<int:kit_id>/components', endpoint='api_kit_components')
@login_required
def api_kit_components(kit_id):
    """API для получения состава комплекта"""
    try:
        db = get_db()
        components = db.execute_query("""
            SELECT ks.*, n.name as component_name, n.sku
            FROM kit_specifications ks
            JOIN nomenclatures n ON ks.component_nomenclature_id = n.id
            WHERE ks.kit_nomenclature_id = ?
        """, (kit_id,), fetch_all=True)

        result = []
        for comp in components or []:
            result.append(dict(comp))

        return jsonify(result)
    except Exception as e:
        logger.error(f'Ошибка получения состава комплекта: {e}')
        return jsonify([])
