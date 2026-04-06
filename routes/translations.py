"""
Blueprint: translations
Управление переводами полей (field_translations).
"""
import logging
from flask import Blueprint, render_template, request, session, redirect, url_for, flash
from routes.common import login_required, get_db

logger = logging.getLogger('routes.translations')

translations_bp = Blueprint('translations', __name__)

# ============ УПРАВЛЕНИЕ ПЕРЕВОДАМИ ============

@translations_bp.route('/translations', endpoint='translations_list')
@login_required
def translations_list():
    """Список переводов полей"""
    if session.get('role') != 'admin':
        flash('Доступ запрещен', 'error')
        return redirect(url_for('dashboard'))
    
    try:
        db = get_db()
        translations = db.execute_query("""
            SELECT * FROM field_translations 
            ORDER BY table_name, display_order, field_name
        """, fetch_all=True)
        
        # Группировка по таблицам
        tables = {}
        for t in translations or []:
            t_dict = dict(t)
            if t_dict['table_name'] not in tables:
                tables[t_dict['table_name']] = []
            tables[t_dict['table_name']].append(t_dict)
        
        return render_template('translations/list.html', tables=tables)
    except Exception as e:
        logger.error(f'Ошибка загрузки переводов: {e}')
        flash('Ошибка загрузки переводов', 'error')
        return redirect(url_for('dashboard'))

@translations_bp.route('/translations/edit/<int:id>', methods=['GET', 'POST'], endpoint='edit_translation')
@login_required
def edit_translation(id):
    """Редактирование перевода"""
    if session.get('role') != 'admin':
        flash('Доступ запрещен', 'error')
        return redirect(url_for('dashboard'))
    
    db = get_db()
    
    if request.method == 'POST':
        try:
            display_name = request.form.get('display_name')
            description = request.form.get('description')
            import_enabled = 'import_enabled' in request.form
            export_enabled = 'export_enabled' in request.form
            display_order = request.form.get('display_order', 0)
            
            db.execute_query("""
                UPDATE field_translations 
                SET display_name = ?, description = ?, 
                    import_enabled = ?, export_enabled = ?,
                    display_order = ?
                WHERE id = ?
            """, (display_name, description, import_enabled, export_enabled, display_order, id))
            
            flash('Перевод обновлен', 'success')
            return redirect(url_for('translations.translations_list'))
        except Exception as e:
            logger.error(f'Ошибка обновления перевода: {e}')
            flash('Ошибка обновления перевода', 'error')
    
    translation = db.execute_query(
        "SELECT * FROM field_translations WHERE id = ?", 
        (id,), 
        fetch_all=False
    )
    
    if not translation:
        flash('Перевод не найден', 'error')
        return redirect(url_for('translations.translations_list'))
    
    return render_template('translations/edit.html', translation=dict(translation))

@translations_bp.route('/translations/reset', methods=['POST'], endpoint='reset_translations')
@login_required
def reset_translations():
    """Сброс переводов к значениям по умолчанию"""
    if session.get('role') != 'admin':
        flash('Доступ запрещен', 'error')
        return redirect(url_for('dashboard'))
    
    try:
        db = get_db()
        
        # Очищаем существующие переводы
        db.execute_query("DELETE FROM field_translations")
        
        # Вставляем переводы по умолчанию
        default_translations = [
            ('nomenclatures', 'sku', 'Артикул', 'Уникальный код номенклатуры'),
            ('nomenclatures', 'name', 'Наименование', 'Полное наименование'),
            ('nomenclatures', 'category_id', 'Категория', 'Категория номенклатуры'),
            ('nomenclatures', 'model', 'Модель', 'Модель изделия'),
            ('nomenclatures', 'manufacturer', 'Производитель', 'Компания-производитель'),
            ('nomenclatures', 'unit', 'Ед. изм.', 'Единица измерения'),
            ('nomenclatures', 'accounting_type', 'Тип учета', 'individual/batch/quantitative'),
            ('nomenclatures', 'min_stock', 'Мин. запас', 'Минимальный запас на складе'),
            
            ('instances', 'inventory_number', 'Инв. номер', 'Уникальный инвентарный номер'),
            ('instances', 'serial_number', 'Серийный номер', 'Заводской номер'),
            ('instances', 'status', 'Статус', 'Текущий статус'),
            ('instances', 'condition', 'Состояние', 'Техническое состояние'),
            ('instances', 'location_id', 'Местоположение', 'Физическое расположение'),
            ('instances', 'warehouse_id', 'Склад', 'Склад хранения'),
            ('instances', 'employee_id', 'Сотрудник', 'Ответственный/держатель'),
            ('instances', 'purchase_date', 'Дата покупки', 'Дата приобретения'),
            ('instances', 'purchase_price', 'Цена', 'Цена приобретения'),
            
            ('batches', 'batch_number', 'Номер партии', 'Номер партии от поставщика'),
            ('batches', 'expiry_date', 'Срок годности', 'Дата истечения срока'),
            ('batches', 'quality_status', 'Статус качества', 'Карантин/Одобрено/Брак'),
            
            ('stocks', 'quantity', 'Количество', 'Текущее количество'),
            ('stocks', 'reserved_quantity', 'Зарезервировано', 'Количество в резерве'),
            ('stocks', 'available_quantity', 'Доступно', 'Доступное количество'),
        ]
        
        for table, field, display, desc in default_translations:
            db.execute_query("""
                INSERT INTO field_translations 
                    (table_name, field_name, display_name, description, import_enabled, export_enabled, display_order)
                VALUES (?, ?, ?, ?, 1, 1, 0)
            """, (table, field, display, desc))
        
        flash('Переводы сброшены к значениям по умолчанию', 'success')
    except Exception as e:
        logger.error(f'Ошибка сброса переводов: {e}')
        flash('Ошибка сброса переводов', 'error')
    
    return redirect(url_for('translations.translations_list'))
