"""
Blueprint: Categories
Routes: /categories, /categories/create, /categories/<id>/edit,
        /categories/<id>/delete, /categories/add (stub redirect),
        /category-rules (stub)
"""
import logging
from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from routes.common import login_required, get_db

logger = logging.getLogger('routes.categories')

categories_bp = Blueprint('categories', __name__)


@categories_bp.route('/categories', endpoint='categories_list')
@login_required
def categories_list():
    """Список категорий в виде дерева"""
    try:
        db = get_db()

        # Получаем все активные категории, сортируем по lft для правильного порядка
        categories = db.execute_query("""
            SELECT id, code, name_ru as name, parent_id, level, lft, rgt,
                   type as item_type, accounting_type, account_method,
                   description, sort_order, is_active, path
            FROM categories
            WHERE is_active = 1
            ORDER BY lft
        """, fetch_all=True)

        if not categories:
            return render_template('categories/list.html', categories=[])

        # Преобразуем в список словарей
        categories_list = [dict(cat) for cat in categories]

        # Строим дерево на основе lft, rgt
        root_categories = []
        categories_by_id = {}

        # Сначала индексируем все категории
        for cat in categories_list:
            cat['children'] = []
            categories_by_id[cat['id']] = cat

        # Определяем родителей по level
        for cat in categories_list:
            if cat['level'] == 0:
                root_categories.append(cat)
            else:
                # Ищем родителя (категорию с level на 1 меньше и с lft < текущего lft < rgt)
                for parent in categories_list:
                    if (parent['level'] == cat['level'] - 1 and
                        parent['lft'] < cat['lft'] and
                        parent['rgt'] > cat['rgt']):
                        parent['children'].append(cat)
                        break

        # Добавляем children_count для каждого узла
        for cat in categories_list:
            cat['children_count'] = len(cat['children'])

        return render_template('categories/list.html',
                             categories=root_categories,
                             all_categories=categories_list)

    except Exception as e:
        logger.error(f'Ошибка загрузки категорий: {e}')
        flash('Ошибка загрузки категорий', 'error')
        return redirect(url_for('dashboard'))


@categories_bp.route('/categories/create', methods=['GET', 'POST'], endpoint='categories_create')
@login_required
def categories_create():
    """Создание категории"""
    if request.method == 'POST':
        try:
            db = get_db()
            name = request.form.get('name')
            if not name:
                flash('Название категории обязательно', 'error')
                return redirect(url_for('categories.categories_create'))

            data = {
                'name': name,
                'code': request.form.get('code') or name[:20].upper().replace(' ', '_'),
                'description': request.form.get('description'),
                'parent_id': request.form.get('parent_id') or None,
                'type': request.form.get('type', 'material'),
                'accounting_type': request.form.get('accounting_type', 'inventory'),
                'account_method': request.form.get('account_method', 'mixed'),
                'sort_order': request.form.get('sort_order', 500),
                'is_active': 'is_active' in request.form,
            }
            result = db.create_category(data, session['user_id'])
            if result['success']:
                flash('Категория создана', 'success')
                return redirect(url_for('categories.categories_list'))
            else:
                flash(result['message'], 'error')
        except Exception as e:
            logger.error(f'Ошибка создания категории: {e}')
            flash('Ошибка создания категории', 'error')

    db = get_db()
    all_categories = db.get_all_categories(include_inactive=True)
    return render_template('categories/form.html', category=None, all_categories=all_categories)


@categories_bp.route('/categories/<int:id>/edit', methods=['GET', 'POST'], endpoint='categories_edit')
@login_required
def categories_edit(id):
    """Редактирование категории"""
    db = get_db()

    if request.method == 'POST':
        try:
            data = {
                'code': request.form.get('code'),
                'name': request.form.get('name'),
                'description': request.form.get('description'),
                'parent_id': request.form.get('parent_id') or None,
                'type': request.form.get('type', request.form.get('item_type')),
                'accounting_type': request.form.get('accounting_type'),
                'account_method': request.form.get('account_method'),
                'sort_order': request.form.get('sort_order', 500),
                'is_active': 'is_active' in request.form,
            }

            result = db.update_category(id, data, session['user_id'])

            if result['success']:
                flash(result['message'], 'success')
                return redirect(url_for('categories.categories_list'))
            else:
                flash(result['message'], 'error')

        except Exception as e:
            logger.error(f'Ошибка обновления категории: {e}')
            flash('Ошибка обновления категории', 'error')

    category = db.get_category_by_id(id)
    if not category:
        flash('Категория не найдена', 'error')
        return redirect(url_for('categories.categories_list'))

    all_categories = db.get_all_categories()
    # ИСПРАВЛЕНО: используем form.html вместо edit.html
    return render_template('categories/form.html',
                         title='Редактирование категории',
                         category=category,
                         all_categories=all_categories)


@categories_bp.route('/categories/<int:id>/delete', methods=['POST'], endpoint='categories_delete')
@login_required
def categories_delete(id):
    """Удаление категории"""
    try:
        db = get_db()
        result = db.delete_category(id, session.get('user_id'))
        if result['success']:
            flash('Категория удалена', 'success')
        else:
            flash(result['message'], 'error')
    except Exception as e:
        logger.error(f'Ошибка удаления категории: {e}')
        flash('Ошибка удаления категории', 'error')

    return redirect(url_for('categories.categories_list'))


@categories_bp.route('/categories/add', endpoint='add_category')
@login_required
def add_category():
    """Редирект на правильный маршрут создания категории"""
    return redirect(url_for('categories.categories_create'))


@categories_bp.route('/category-rules', endpoint='category_rules_list')
@login_required
def category_rules_list():
    flash('Модуль правил категорий в разработке', 'info')
    return redirect(url_for('dashboard'))
