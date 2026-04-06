"""
Blueprint: export_routes
Экспорт данных в Excel/ZIP.
"""
import re
import logging
import pandas as pd
from io import BytesIO
from datetime import datetime
from flask import Blueprint, render_template, request, session, redirect, url_for, flash, send_file
from routes.common import login_required, admin_required, get_db
from extensions import limiter

logger = logging.getLogger('routes.export')

export_bp = Blueprint('export', __name__)

# ============ ЭКСПОРТ ============

def export_to_excel(data, filename, sheet_name='Лист1'):
    """Универсальная функция экспорта данных в Excel файл"""
    output = BytesIO()
    if data:
        df = pd.DataFrame(data)
    else:
        df = pd.DataFrame()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    output.seek(0)
    safe_filename = re.sub(r'[^\w\-_.]', '_', filename)
    download_name = f"{safe_filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=download_name
    )

@export_bp.route('/export/nomenclatures', endpoint='export_nomenclatures')
@login_required
@limiter.limit("10 per minute")
def export_nomenclatures():
    """Экспорт номенклатуры"""
    try:
        db = get_db()
        data = db.execute_query("""
            SELECT n.*, c.name_ru as category_name 
            FROM nomenclatures n
            LEFT JOIN categories c ON n.category_id = c.id
            ORDER BY n.name
        """, fetch_all=True)
        
        return export_to_excel(
            data=[dict(row) for row in data] if data else [],
            filename='nomenclatures',
            sheet_name='Номенклатура'
        )
    except Exception as e:
        logger.error(f'Ошибка экспорта номенклатуры: {e}')
        flash('Ошибка экспорта', 'error')
        return redirect(url_for('import_export.import_export'))

@export_bp.route('/export/instances', endpoint='export_instances')
@login_required
def export_instances():
    """Экспорт экземпляров"""
    try:
        db = get_db()
        data = db.execute_query("""
            SELECT i.*, n.name as nomenclature_name, n.sku,
                   l.name as location_name, w.name as warehouse_name,
                   e.last_name || ' ' || e.first_name || COALESCE(' ' || e.middle_name, '') as employee_name
            FROM instances i
            LEFT JOIN nomenclatures n ON i.nomenclature_id = n.id
            LEFT JOIN locations l ON i.location_id = l.id
            LEFT JOIN warehouses w ON i.warehouse_id = w.id
            LEFT JOIN employees e ON i.employee_id = e.id
            ORDER BY i.created_at DESC
        """, fetch_all=True)
        
        return export_to_excel(
            data=[dict(row) for row in data] if data else [],
            filename='instances',
            sheet_name='Экземпляры'
        )
    except Exception as e:
        logger.error(f'Ошибка экспорта экземпляров: {e}')
        flash('Ошибка экспорта', 'error')
        return redirect(url_for('import_export.import_export'))

@export_bp.route('/export/batches', endpoint='export_batches')
@login_required
def export_batches():
    """Экспорт партий"""
    try:
        db = get_db()
        data = db.execute_query("""
            SELECT b.*, n.name as nomenclature_name, n.sku,
                   s.name as supplier_name
            FROM batches b
            LEFT JOIN nomenclatures n ON b.nomenclature_id = n.id
            LEFT JOIN suppliers s ON b.supplier_id = s.id
            ORDER BY b.created_at DESC
        """, fetch_all=True)
        
        return export_to_excel(
            data=[dict(row) for row in data] if data else [],
            filename='batches',
            sheet_name='Партии'
        )
    except Exception as e:
        logger.error(f'Ошибка экспорта партий: {e}')
        flash('Ошибка экспорта', 'error')
        return redirect(url_for('import_export.import_export'))

@export_bp.route('/export/stocks', endpoint='export_stocks')
@login_required
def export_stocks():
    """Экспорт остатков"""
    try:
        db = get_db()
        data = db.execute_query("""
            SELECT s.*, n.name as nomenclature_name, n.sku,
                   w.name as warehouse_name, sb.code as storage_bin_code,
                   b.batch_number,
                   (s.quantity - s.reserved_quantity) as available
            FROM stocks s
            LEFT JOIN nomenclatures n ON s.nomenclature_id = n.id
            LEFT JOIN warehouses w ON s.warehouse_id = w.id
            LEFT JOIN storage_bins sb ON s.storage_bin_id = sb.id
            LEFT JOIN batches b ON s.batch_id = b.id
            ORDER BY w.name, n.name
        """, fetch_all=True)
        
        return export_to_excel(
            data=[dict(row) for row in data] if data else [],
            filename='stocks',
            sheet_name='Остатки'
        )
    except Exception as e:
        logger.error(f'Ошибка экспорта остатков: {e}')
        flash('Ошибка экспорта', 'error')
        return redirect(url_for('import_export.import_export'))

@export_bp.route('/export/documents', endpoint='export_documents')
@login_required
def export_documents():
    """Экспорт документов"""
    try:
        date_from = request.args.get('date_from', (datetime.now().replace(day=1)).strftime('%Y-%m-%d'))
        date_to = request.args.get('date_to', datetime.now().strftime('%Y-%m-%d'))
        
        db = get_db()
        data = db.execute_query("""
            SELECT d.*, u.username as created_by_name,
                   w_from.name as from_warehouse,
                   w_to.name as to_warehouse,
                   e.last_name || ' ' || e.first_name || COALESCE(' ' || e.middle_name, '') as employee_name,
                   s.name as supplier_name
            FROM documents d
            LEFT JOIN users u ON d.created_by = u.id
            LEFT JOIN warehouses w_from ON d.from_warehouse_id = w_from.id
            LEFT JOIN warehouses w_to ON d.to_warehouse_id = w_to.id
            LEFT JOIN employees e ON d.employee_id = e.id
            LEFT JOIN suppliers s ON d.supplier_id = s.id
            WHERE d.document_date BETWEEN ? AND ?
            ORDER BY d.document_date DESC
        """, (date_from, date_to), fetch_all=True)
        
        return export_to_excel(
            data=[dict(row) for row in data] if data else [],
            filename=f'documents_{date_from}_{date_to}',
            sheet_name='Документы'
        )
    except Exception as e:
        logger.error(f'Ошибка экспорта документов: {e}')
        flash('Ошибка экспорта', 'error')
        return redirect(url_for('import_export.import_export'))

@export_bp.route('/export/full', endpoint='export_full')
@login_required
@limiter.limit("3 per minute")
def export_full():
    """Полный экспорт всех данных"""
    try:
        import zipfile
        from io import BytesIO
        import tempfile
        import os
        
        db = get_db()
        zip_buffer = BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # Экспорт каждой таблицы
            tables = [
                ('nomenclatures', 'Номенклатура'),
                ('categories', 'Категории'),
                ('instances', 'Экземпляры'),
                ('batches', 'Партии'),
                ('stocks', 'Остатки'),
                ('warehouses', 'Склады'),
                ('storage_bins', 'Ячейки'),
                ('suppliers', 'Поставщики'),
                ('employees', 'Сотрудники'),
                ('documents', 'Документы')
            ]
            
            _ALLOWED_EXPORT_TABLES = {
                'nomenclatures', 'categories', 'instances', 'batches',
                'stocks', 'warehouses', 'storage_bins', 'suppliers',
                'employees', 'documents'
            }
            for table, name in tables:
                if table not in _ALLOWED_EXPORT_TABLES:
                    continue
                data = db.execute_query(f"SELECT * FROM {table}", fetch_all=True)
                if data:
                    df = pd.DataFrame([dict(row) for row in data])
                    excel_buffer = BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                        df.to_excel(writer, index=False, sheet_name=name)
                    excel_buffer.seek(0)
                    zip_file.writestr(f'{table}.xlsx', excel_buffer.getvalue())
        
        zip_buffer.seek(0)
        
        return send_file(
            zip_buffer,
            mimetype='application/zip',
            as_attachment=True,
            download_name=f'full_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.zip'
        )
    except Exception as e:
        logger.error(f'Ошибка полного экспорта: {e}')
        flash('Ошибка экспорта', 'error')
        return redirect(url_for('import_export.import_export'))

@export_bp.route('/export/kits', endpoint='export_kits')
@login_required
def export_kits():
    """Экспорт комплектов"""
    try:
        db = get_db()
        
        # Получаем все комплекты
        kits = db.execute_query("""
            SELECT n.*, c.name_ru as category_name,
                   (SELECT COUNT(*) FROM kit_specifications WHERE kit_nomenclature_id = n.id) as components_count
            FROM nomenclatures n
            LEFT JOIN categories c ON n.category_id = c.id
            WHERE n.accounting_type = 'kit' OR n.id IN (SELECT DISTINCT kit_nomenclature_id FROM kit_specifications)
            ORDER BY n.name
        """, fetch_all=True)
        
        # Преобразуем в список словарей
        kits_list = []
        for kit in kits or []:
            kit_dict = dict(kit)
            
            # Получаем компоненты комплекта
            components = db.execute_query("""
                SELECT ks.component_nomenclature_id, ks.quantity, n.name, n.sku
                FROM kit_specifications ks
                JOIN nomenclatures n ON ks.component_nomenclature_id = n.id
                WHERE ks.kit_nomenclature_id = ?
            """, (kit['id'],), fetch_all=True)
            
            # Формируем строку компонентов в удобном формате
            if components:
                comp_parts = []
                for comp in components:
                    comp_parts.append(f"{comp['name']}:{comp['quantity']}")
                kit_dict['components'] = ', '.join(comp_parts)
            else:
                kit_dict['components'] = ''
            
            # Убираем лишние поля
            kit_dict.pop('id', None)
            kit_dict.pop('attributes', None)
            kit_dict.pop('created_by', None)
            kit_dict.pop('updated_by', None)
            
            kits_list.append(kit_dict)
        
        return export_to_excel(
            data=kits_list,
            filename='kits',
            sheet_name='Комплекты'
        )
    except Exception as e:
        logger.error(f'Ошибка экспорта комплектов: {e}')
        flash('Ошибка экспорта', 'error')
        return redirect(url_for('import_export.import_export'))

@export_bp.route('/export/kit/<int:id>', endpoint='export_kit_detail')
@login_required
def export_kit_detail(id):
    """Экспорт детальной информации о комплекте"""
    try:
        db = get_db()
        
        # Информация о комплекте
        kit = db.get_nomenclature_by_id(id)
        if not kit:
            flash('Комплект не найден', 'error')
            return redirect(url_for('kits.kits_list'))
        
        # Компоненты комплекта
        components = db.execute_query("""
            SELECT ks.*, n.name, n.sku, n.unit
            FROM kit_specifications ks
            JOIN nomenclatures n ON ks.component_nomenclature_id = n.id
            WHERE ks.kit_nomenclature_id = ?
            ORDER BY n.name
        """, (id,), fetch_all=True)
        
        # Создаем данные для экспорта
        data = []
        
        # Заголовок
        data.append(['ИНФОРМАЦИЯ О КОМПЛЕКТЕ'])
        data.append(['ID', kit['id']])
        data.append(['Артикул', kit['sku']])
        data.append(['Наименование', kit['name']])
        data.append(['Категория', kit.get('category_name', '')])
        data.append(['Описание', kit.get('description', '')])
        data.append([''])
        
        # Компоненты
        data.append(['СОСТАВ КОМПЛЕКТА'])
        data.append(['ID компонента', 'Артикул', 'Наименование', 'Количество', 'Ед. изм.'])
        
        for comp in components or []:
            data.append([
                comp['component_nomenclature_id'],
                comp['sku'],
                comp['name'],
                comp['quantity'],
                comp['unit']
            ])
        
        # Создаем Excel файл
        import pandas as pd
        from io import BytesIO
        
        df = pd.DataFrame(data)
        output = BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name=f'Комплект {kit["sku"]}', header=False)
        
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'kit_{kit["sku"]}_{datetime.now().strftime("%Y%m%d")}.xlsx'
        )
        
    except Exception as e:
        logger.error(f'Ошибка экспорта комплекта: {e}')
        flash('Ошибка экспорта', 'error')
        return redirect(url_for('kits.kits_list'))
    

# ============ ОЧИСТКА СЧЁТЧИКОВ ИМПОРТА ============

@export_bp.route('/import/clear-counters', endpoint='clear_import_counters')
@admin_required
def clear_import_counters():
    """Очистка счётчиков импорта (для отладки)."""
    from flask import current_app
    if hasattr(current_app, 'import_counters'):
        with current_app.import_lock:
            current_app.import_counters.clear()
    flash('Счётчики импорта очищены', 'success')
    return redirect(url_for('import_export.import_export'))
