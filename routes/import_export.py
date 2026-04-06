"""
Blueprint: Импорт, экспорт и история изменений.
"""
import logging
import json
import os
import re
import zipfile
import pandas as pd
from io import BytesIO
from datetime import datetime
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, flash, send_file
from extensions import csrf, limiter
from routes.common import login_required, admin_required, manager_required, get_db
from utils.search import build_where
from routes.utils import generate_unique_barcode, is_barcode_unique
from routes.nomenclatures import UNITS

logger = logging.getLogger('routes.import_export')

import_export_bp = Blueprint('import_export', __name__)


# ============ УНИВЕРСАЛЬНЫЙ ИМПОРТ ============

@import_export_bp.route('/import-export', endpoint='import_export')
@login_required
def import_export():
    """Главная страница импорта/экспорта"""
    return render_template('import_export/index.html')

import tempfile
import pickle
import uuid
import time
from datetime import datetime, timedelta
import pandas as pd
import sqlite3

# Хранилище временных данных импорта
_import_sessions = {}

@import_export_bp.route('/import/universal', methods=['GET', 'POST'], endpoint='import_universal')
@login_required
@limiter.limit("10 per minute")
def import_universal():
    """Универсальный импорт с предпросмотром"""
    if request.method == 'POST':
        action = request.form.get('action', 'upload')
        
        if action == 'upload':
            return handle_file_upload()
        elif action == 'preview':
            return handle_preview()
        elif action == 'import':
            return handle_import_selected()
    
    return render_template('import_export/universal.html')

def handle_file_upload():
    """Обработка загрузки файла и анализ данных"""
    try:
        if 'file' not in request.files:
            flash('Файл не загружен', 'error')
            return redirect(url_for('import_export.import_universal'))
        
        file = request.files['file']
        if file.filename == '':
            flash('Файл не выбран', 'error')
            return redirect(url_for('import_export.import_universal'))
        
        # Создаем уникальную сессию для этого импорта
        session_id = str(uuid.uuid4())
        
        # Сохраняем файл временно
        temp_dir = tempfile.mkdtemp()
        temp_path = os.path.join(temp_dir, file.filename)
        file.save(temp_path)
        
        # Читаем все листы Excel
        excel_file = pd.ExcelFile(temp_path)
        
        # === ЗАГРУЗКА СПРАВОЧНИКОВ ===
        db = get_db()
        
        # Поставщики из БД
        suppliers_dict = {}
        db_suppliers = db.execute_query("SELECT id, name FROM suppliers WHERE is_active = 1", fetch_all=True)
        for s in db_suppliers or []:
            suppliers_dict[s['name']] = s['id']
        
        # Склады из БД
        warehouses_dict = {}
        db_warehouses = db.execute_query("SELECT id, name FROM warehouses WHERE is_active = 1", fetch_all=True)
        for w in db_warehouses or []:
            warehouses_dict[w['name']] = w['id']
        
        # === АНАЛИЗ ОСНОВНОГО ЛИСТА ===
        main_sheet = None
        for sheet in ['Шаблон', 'Sheet1', 'Лист1', 'Data']:
            if sheet in excel_file.sheet_names:
                main_sheet = sheet
                break
        
        if not main_sheet:
            flash('Не найден лист с данными', 'error')
            return redirect(url_for('import_export.import_universal'))
        
        df = pd.read_excel(temp_path, sheet_name=main_sheet)
        df = df.dropna(how='all')
        
        # Анализируем данные
        preview_data = []
        statistics = {
            'total_rows': 0,
            'valid_rows': 0,
            'by_type': {'individual': 0, 'batch': 0, 'quantitative': 0},
            'by_warehouse': {},
            'by_supplier': {},
            'total_quantity': 0,
            'total_amount': 0
        }
        
        for idx, row in df.iterrows():
            # Пропускаем строки с формулами
            if len(row) < 3 or pd.isna(row.iloc[2]) or str(row.iloc[2]).startswith('='):
                continue
            
            statistics['total_rows'] += 1
            
            # Анализируем строку
            analysis = analyze_row(row, idx, suppliers_dict, warehouses_dict)
            
            if analysis['valid']:
                statistics['valid_rows'] += 1
                
                acc_type = analysis['data'].get('accounting_type', 'quantitative')
                statistics['by_type'][acc_type] = statistics['by_type'].get(acc_type, 0) + 1
                
                warehouse = analysis['data'].get('warehouse', 'Неизвестно')
                statistics['by_warehouse'][warehouse] = statistics['by_warehouse'].get(warehouse, 0) + 1
                
                supplier = analysis['data'].get('supplier_name', 'Не указан')
                statistics['by_supplier'][supplier] = statistics['by_supplier'].get(supplier, 0) + 1
                
                statistics['total_quantity'] += analysis['data'].get('quantity', 0)
                statistics['total_amount'] += analysis['data'].get('quantity', 0) * analysis['data'].get('price', 0)
            
            preview_data.append(analysis)
        
        # Сохраняем данные в сессии
        _import_sessions[session_id] = {
            'temp_path': temp_path,
            'temp_dir': temp_dir,
            'data': preview_data,
            'statistics': statistics,
            'suppliers': suppliers_dict,
            'warehouses': warehouses_dict,
            'filename': file.filename
        }
        
        session['import_session_id'] = session_id
        
        return render_template('import_export/preview.html',
                             session_id=session_id,
                             data=preview_data,
                             statistics=statistics,
                             filename=file.filename)
        
    except Exception as e:
        logger.error(f'Ошибка при загрузке файла: {e}')
        flash(f'Ошибка при загрузке файла: {str(e)}', 'error')
        return redirect(url_for('import_export.import_universal'))

def analyze_row(row, idx, suppliers_dict, warehouses_dict):
    """Анализ одной строки данных"""
    result = {
        'row_num': idx + 2,
        'valid': False,
        'warnings': [],
        'errors': [],
        'data': {},
        'suggestions': {}
    }
    
    try:
        # БАЗОВЫЙ АНАЛИЗ
        name = str(row.iloc[2]).strip() if len(row) > 2 and pd.notna(row.iloc[2]) else ''
        if not name:
            result['errors'].append('Отсутствует наименование')
            return result
        
        result['data']['name'] = name
        
        # ТИП УЧЕТА
        acc_type = str(row.iloc[0]).strip().lower() if len(row) > 0 and pd.notna(row.iloc[0]) else 'количественный'
        acc_type_map = {
            'индивидуальный': 'individual',
            'партионный': 'batch',
            'количественный': 'quantitative'
        }
        result['data']['accounting_type'] = acc_type_map.get(acc_type, 'quantitative')
        
        # КОЛИЧЕСТВО
        try:
            quantity = float(row.iloc[3]) if len(row) > 3 and pd.notna(row.iloc[3]) else 1
            if quantity <= 0:
                result['warnings'].append('Количество <= 0, будет использовано 1')
                quantity = 1
            result['data']['quantity'] = quantity
        except Exception:
            result['warnings'].append('Некорректное количество, будет использовано 1')
            result['data']['quantity'] = 1
        
        # ЕДИНИЦА ИЗМЕРЕНИЯ
        unit = str(row.iloc[4]).strip() if len(row) > 4 and pd.notna(row.iloc[4]) else 'шт.'
        result['data']['unit'] = unit
        
        # СКЛАД
        warehouse_name = str(row.iloc[5]).strip() if len(row) > 5 and pd.notna(row.iloc[5]) else 'Основной'
        result['data']['warehouse'] = warehouse_name
        
        if warehouse_name in warehouses_dict:
            result['data']['warehouse_id'] = warehouses_dict[warehouse_name]
        else:
            result['warnings'].append(f'Склад "{warehouse_name}" не найден, будет создан')
            result['suggestions']['warehouse'] = 'create'
        
        # ДАТА
        date_val = row.iloc[6] if len(row) > 6 else None
        if pd.notna(date_val):
            try:
                if isinstance(date_val, datetime):
                    result['data']['date'] = date_val.strftime('%Y-%m-%d')
                else:
                    result['data']['date'] = str(date_val).strip()
            except Exception:
                result['data']['date'] = datetime.now().strftime('%Y-%m-%d')
        else:
            result['data']['date'] = datetime.now().strftime('%Y-%m-%d')
        
        # ПОСТАВЩИК
        supplier_name = str(row.iloc[7]).strip() if len(row) > 7 and pd.notna(row.iloc[7]) else None
        result['data']['supplier_name'] = supplier_name
        
        if supplier_name:
            if supplier_name in suppliers_dict:
                result['data']['supplier_id'] = suppliers_dict[supplier_name]
            else:
                result['warnings'].append(f'Поставщик "{supplier_name}" не найден, будет создан')
                result['suggestions']['supplier'] = 'create'
        
        # ПАРТИЯ
        batch = str(row.iloc[8]).strip() if len(row) > 8 and pd.notna(row.iloc[8]) else None
        result['data']['batch_number'] = batch if batch and batch.lower() != 'nan' else None
        
        # ДАТА ПРОИЗВОДСТВА
        prod_date = row.iloc[9] if len(row) > 9 else None
        if pd.notna(prod_date):
            try:
                if isinstance(prod_date, datetime):
                    result['data']['production_date'] = prod_date.strftime('%Y-%m-%d')
                else:
                    result['data']['production_date'] = str(prod_date).strip()
            except Exception:
                pass
        
        # СРОК ГОДНОСТИ
        expiry = row.iloc[10] if len(row) > 10 else None
        if pd.notna(expiry):
            try:
                # Если это число (дни)
                if isinstance(expiry, (int, float)):
                    result['data']['expiry_days'] = int(expiry)
                # Если это дата
                elif isinstance(expiry, datetime):
                    result['data']['expiry_date'] = expiry.strftime('%Y-%m-%d')
                else:
                    expiry_str = str(expiry).strip()
                    if expiry_str.isdigit():
                        result['data']['expiry_days'] = int(expiry_str)
            except Exception:
                pass
        
        # ЦЕНА
        price = row.iloc[11] if len(row) > 11 else None
        try:
            result['data']['price'] = float(price) if pd.notna(price) else 0
        except Exception:
            result['data']['price'] = 0
        
        # ОСНОВАНИЕ
        reason = row.iloc[12] if len(row) > 12 and pd.notna(row.iloc[12]) else ''
        result['data']['reason'] = str(reason).strip()
        
        # ТЕХНИЧЕСКИЕ ХАРАКТЕРИСТИКИ
        if len(row) > 13 and pd.notna(row.iloc[13]):
            result['data']['manufacturer'] = str(row.iloc[13]).strip()
        if len(row) > 14 and pd.notna(row.iloc[14]):
            result['data']['model'] = str(row.iloc[14]).strip()
        if len(row) > 15 and pd.notna(row.iloc[15]):
            result['data']['brand'] = str(row.iloc[15]).strip()
        
        # Если дошли до сюда без критических ошибок
        if not result['errors']:
            result['valid'] = True
        
    except Exception as e:
        result['errors'].append(f'Ошибка анализа: {str(e)}')
    
    return result

def handle_preview():
    """Обработка предпросмотра и подтверждения"""
    session_id = request.form.get('session_id')
    action = request.form.get('preview_action')
    
    if session_id not in _import_sessions:
        flash('Сессия импорта устарела', 'error')
        return redirect(url_for('import_export.import_universal'))
    
    session_data = _import_sessions[session_id]
    
    if action == 'refresh':
        # Обновляем предпросмотр с новыми настройками
        selected_rows = request.form.getlist('selected_rows')
        options = {
            'create_missing_suppliers': 'create_missing_suppliers' in request.form,
            'create_missing_warehouses': 'create_missing_warehouses' in request.form,
            'group_by_supplier': 'group_by_supplier' in request.form,
            'group_by_date': 'group_by_date' in request.form,
            'create_documents': 'create_documents' in request.form,
            'document_type': request.form.get('document_type', 'receipt')
        }
        
        session_data['options'] = options
        session_data['selected_rows'] = [int(r) for r in selected_rows if r.isdigit()]
        
        return render_template('import_export/preview.html',
                             session_id=session_id,
                             data=session_data['data'],
                             statistics=session_data['statistics'],
                             filename=session_data['filename'],
                             options=options,
                             selected_rows=session_data['selected_rows'])
    
    elif action == 'import':
        # Переходим к импорту
        return redirect(url_for('import_export.import_execute', session_id=session_id))

@import_export_bp.route('/import/execute/<session_id>', methods=['GET', 'POST'], endpoint='import_execute')
@login_required
def import_execute(session_id):
    """Выполнение импорта выбранных строк"""
    
    if session_id not in _import_sessions:
        flash('Сессия импорта устарела', 'error')
        return redirect(url_for('import_export.import_universal'))
    
    session_data = _import_sessions[session_id]
    selected_rows = session_data.get('selected_rows', [])
    options = session_data.get('options', {})
    
    if not selected_rows:
        selected_rows = [i for i, d in enumerate(session_data['data']) if d['valid']]
    
    db = get_db()
    stats = {
        'total': len(selected_rows),
        'processed': 0,
        'created_nomenclatures': 0,
        'created_documents': 0,
        'errors': 0
    }
    results = []
    
    # Начинаем транзакцию
    db.connection.execute("BEGIN TRANSACTION")
    
    try:
        # Группируем строки для создания документов
        document_groups = {}
        
        for idx in selected_rows:
            if idx >= len(session_data['data']):
                continue
            
            row_data = session_data['data'][idx]['data']
            
            # Создаем или находим номенклатуру
            nomen_result = create_or_update_nomenclature(row_data, db, session['user_id'], options)
            
            if nomen_result['success']:
                stats['created_nomenclatures'] += 1 if nomen_result.get('created') else 0
                
                # Определяем ключ для группировки документа
                if options.get('group_by_supplier') and row_data.get('supplier_name'):
                    group_key = f"{row_data.get('date', '')}_{row_data.get('supplier_name', '')}"
                elif options.get('group_by_date'):
                    group_key = row_data.get('date', '')
                else:
                    group_key = f"row_{idx}"
                
                if group_key not in document_groups:
                    document_groups[group_key] = {
                        'date': row_data.get('date'),
                        'supplier_id': row_data.get('supplier_id'),
                        'warehouse_id': row_data.get('warehouse_id'),
                        'reason': row_data.get('reason', 'Импорт из Excel'),
                        'items': []
                    }
                
                # Добавляем позицию в документ
                document_groups[group_key]['items'].append({
                    'nomenclature_id': nomen_result['nomenclature_id'],
                    'quantity': row_data['quantity'],
                    'price': row_data['price'],
                    'batch_number': row_data.get('batch_number'),
                    'expiry_date': row_data.get('expiry_date'),
                    'accounting_type': row_data['accounting_type']
                })
                
                stats['processed'] += 1
                results.append({
                    'row': idx + 2,
                    'success': True,
                    'message': f"Номенклатура ID: {nomen_result['nomenclature_id']}"
                })
            else:
                stats['errors'] += 1
                results.append({
                    'row': idx + 2,
                    'success': False,
                    'message': nomen_result.get('message', 'Ошибка создания номенклатуры')
                })
        
        # Создаем документы для каждой группы
        if options.get('create_documents', True):
            for group_key, group_data in document_groups.items():
                doc_result = create_document_from_group(group_data, db, session['user_id'])
                if doc_result['success']:
                    stats['created_documents'] += 1
        
        db.connection.commit()
        
        # Очищаем временные файлы
        cleanup_import_session(session_id)
        
        return render_template('import_export/results.html',
                             stats=stats,
                             results=results,
                             session_id=session_id)
        
    except Exception as e:
        db.connection.rollback()
        logger.error(f'Ошибка при импорте: {e}')
        flash(f'Ошибка при импорте: {str(e)}', 'error')
        return redirect(url_for('import_export.import_universal'))

def create_or_update_nomenclature(data, db, user_id, options):
    """Создание или обновление номенклатуры"""
    try:
        # Ищем существующую по названию
        existing = db.execute_query(
            "SELECT id FROM nomenclatures WHERE name = ?",
            (data['name'],), fetch_all=False
        )
        
        if existing:
            # Обновляем существующую
            update_data = {
                'unit': data.get('unit'),
                'manufacturer': data.get('manufacturer'),
                'model': data.get('model'),
                'brand': data.get('brand')
            }
            update_data = {k: v for k, v in update_data.items() if v is not None}
            
            if update_data:
                db.update_nomenclature(existing['id'], update_data, user_id)
            
            return {
                'success': True,
                'nomenclature_id': existing['id'],
                'created': False
            }
        
        # Создаем новую
        nomen_data = {
            'name': data['name'],
            'sku': generate_sku(data['name']),
            'unit': data.get('unit', 'шт.'),
            'accounting_type': data.get('accounting_type', 'quantitative'),
            'manufacturer': data.get('manufacturer'),
            'model': data.get('model'),
            'brand': data.get('brand'),
            'is_active': 1
        }
        
        result = db.create_nomenclature(nomen_data, user_id)
        
        if result.get('success'):
            return {
                'success': True,
                'nomenclature_id': result.get('nomenclature_id') or result.get('id'),
                'created': True
            }
        else:
            return {'success': False, 'message': result.get('message')}
            
    except Exception as e:
        return {'success': False, 'message': str(e)}

def create_document_from_group(group_data, db, user_id):
    """Создание документа из группы позиций"""
    try:
        # Генерируем номер документа
        doc_number = f"IMP-{datetime.now().strftime('%Y%m%d')}-{int(time.time()) % 10000:04d}"
        
        # Создаем документ
        doc_row = db.execute_query("""
            INSERT INTO documents (
                document_type, document_number, document_date, status,
                to_warehouse_id, supplier_id, reason, created_by, created_at
            ) VALUES (?, ?, ?, 'draft', ?, ?, ?, ?, CURRENT_TIMESTAMP)
            RETURNING id
        """, (
            'receipt',
            doc_number,
            group_data['date'] or datetime.now().strftime('%Y-%m-%d'),
            group_data['warehouse_id'],
            group_data['supplier_id'],
            group_data['reason'],
            user_id
        ), fetch_all=False)

        document_id = doc_row['id']
        
        # Добавляем позиции
        for item in group_data['items']:
            db.execute_query("""
                INSERT INTO document_items (
                    document_id, nomenclature_id, quantity, price,
                    batch_number, expiry_date, created_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                document_id,
                item['nomenclature_id'],
                item['quantity'],
                item['price'],
                item.get('batch_number'),
                item.get('expiry_date'),
                user_id
            ))
        
        return {'success': True, 'document_id': document_id}
        
    except Exception as e:
        return {'success': False, 'message': str(e)}

def generate_sku(name):
    """Генерация SKU из названия"""
    import re
    import time
    
    words = re.sub(r'[^\w\s]', ' ', name).split()
    sku_parts = []
    
    for word in words[:3]:
        if word and len(word) > 0:
            part = word[:3].upper()
            sku_parts.append(part)
    
    if not sku_parts:
        sku_parts = ['NOM']
    
    timestamp = str(int(time.time()))[-6:]
    return '-'.join(sku_parts) + '-' + timestamp

def cleanup_import_session(session_id):
    """Очистка временных файлов сессии"""
    if session_id in _import_sessions:
        session_data = _import_sessions[session_id]
        try:
            if os.path.exists(session_data['temp_path']):
                os.remove(session_data['temp_path'])
            if os.path.exists(session_data['temp_dir']):
                os.rmdir(session_data['temp_dir'])
        except Exception:
            pass
        del _import_sessions[session_id]

@import_export_bp.route('/import/template/enhanced', endpoint='download_enhanced_template')
@login_required
def download_enhanced_template():
    """Скачивание улучшенного шаблона для импорта"""
    import pandas as pd
    from io import BytesIO
    from datetime import datetime
    
    db = get_db()
    output = BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # ============ ЛИСТ 1: Шаблон (основные данные) ============
        template_data = [
            # A           B           C               D           E       F       G           H           I       J           K       L       M
            ['ТИП_УЧЕТА', 'ВИД',      'НАИМЕНОВАНИЕ', 'АРТИКУЛ',  'ШК',  'ЕД.ИЗМ.', 'КАТЕГОРИЯ', 'ID_НОМЕНКЛАТУРЫ', # Основные поля
            'ДЕЙСТВИЕ', 'ЦЕНА',      'ПОСТАВЩИК',    'СКЛАД',    'ДАТА', 'ОСНОВАНИЕ',                          # Для документов
            'ПРОИЗВОДИТЕЛЬ', 'МОДЕЛЬ', 'БРЕНД', 'СТРАНА',                                                    # Характеристики
            'МИН_ЗАПАС', 'ТОЧКА_ЗАКАЗА', 'СРОК_ГОДНОСТИ_ДНЕЙ',                                              # Нормирование
            'ЕСТЬ_СЕРИЙНЫЕ', 'ЕСТЬ_СРОКИ', 'ТРЕБУЕТ_ПОВЕРКУ', 'ТРЕБУЕТ_ТО'],                                 # Флаги
            
            # ПРИМЕР 1: Новая номенклатура (количественный учет)
            ['quantitative', 'Номенклатура', 'Бумага офисная А4 500л', '', '', 'уп.', 'Канцелярия', '',
            'create', '320', 'ООО "СЕЛЛЕНА"', 'Основной', '2026-03-05', 'Закупка',
            'SvetoCopy', '', '', 'Россия',
            '10', '5', '',
            '0', '0', '0', '0'],
            
            # ПРИМЕР 2: Существующая номенклатура (обновление)
            ['individual', 'Номенклатура', 'Ноутбук ASUS ROG', 'NOM-001-123456', '', 'шт.', 'Ноутбуки', '123',
            'update', '', '', '', '', '',
            'ASUS', 'ROG Strix G15', 'ASUS', 'Китай',
            '', '', '',
            '1', '0', '0', '1'],
            
            # ПРИМЕР 3: Создание комплекта (сразу с компонентами)
            ['kit', 'Комплект', 'Сварочный пост PRO', 'KIT-001-789012', '', 'компл', 'Сварочное оборудование', '',
            'create', '15000', 'ООО "СЕЛЛЕНА"', 'Основной', '2026-03-05', 'Закупка',
            'Resanta', 'САИ-160 + комплект', 'Resanta', 'Россия',
            '1', '0', '',
            '1', '0', '0', '1',
            'КОМПОНЕНТЫ:Сварочный аппарат инверторный(1,individual),Маска хамелеон(1,individual),Электроды 3мм(10,batch),Щетка металлическая(1,quantitative)'],
            
            # ПРИМЕР 4: Добавление компонентов к существующему комплекту
            ['kit', 'Комплект', 'Сварочный пост PRO', '', '', 'компл', '', '123',
            'add_components', '', '', '', '', '',
            '', '', '', '',
            '', '', '',
            '', '', '', '',
            'КОМПОНЕНТЫ:Круг отрезной(5,quantitative),Очки защитные(1,individual)'],
            
            # ПРИМЕР 5: Преобразование обычной номенклатуры в комплект
            ['kit', 'Комплект', 'Сварочный пост PRO', 'NOM-001-123456', '', 'компл', 'Сварочное оборудование', '123',
            'convert_to_kit', '', '', '', '', '',
            'Resanta', 'САИ-160 + комплект', 'Resanta', 'Россия',
            '1', '0', '',
            '1', '0', '0', '1',
            'КОМПОНЕНТЫ:Сварочный аппарат(1,individual),Маска(1,individual),Электроды(10,batch)'],
        ]
        
        df_template = pd.DataFrame(template_data)
        df_template.to_excel(writer, index=False, sheet_name='Шаблон', header=False)
        
        # ============ ЛИСТ 2: ИНСТРУКЦИЯ (с таблицей допустимых значений) ============
        instruction_data = [
            ['ИНСТРУКЦИЯ ПО ЗАПОЛНЕНИЮ ШАБЛОНА ПОСТУПЛЕНИЯ', '', '', ''],
            ['', '', '', ''],
            ['ДОПУСТИМЫЕ ЗНАЧЕНИЯ:', '', '', ''],
            ['ТИП УЧЕТА', 'ВИД', 'ЕД.ИЗМ.', 'СКЛАДЫ'],
            ['Индивидуальный', 'Номенклатура', 'шт.', 'Основной'],
            ['Партионный', 'Элемент', 'г.', 'БПЛА'],
            ['Количественный', 'Комплект', 'кг.', 'АКБ'],
            ['', '', 'мл.', 'Инструментальный'],
            ['', '', 'л.', 'Материалы УМО'],
            ['', '', 'м.', 'Материалы УПК'],
            ['', '', 'м²', ''],
            ['', '', 'м.п.', ''],
            ['', '', 'компл.', ''],
            ['', '', 'уп.', ''],
            ['', '', '', ''],
            ['ПРАВИЛА ЗАПОЛНЕНИЯ:', '', '', ''],
            ['1. Заполняйте данные начиная с 1-й строки (после заголовков)', '', '', ''],
            ['2. Не изменяйте названия колонок', '', '', ''],
            ['3. Поля НАИМЕНОВАНИЕ, КОЛИЧЕСТВО, СКЛАД, ДАТА обязательны', '', '', ''],
            ['4. Дата в формате: ГГГГ-ММ-ДД (например: 2026-03-03)', '', '', ''],
            ['5. Для количества используйте число (можно дробное)', '', '', ''],
            ['6. Партия и срок годности заполняются при необходимости', '', '', ''],
            ['', '', '', ''],
            ['ПРИМЕРЫ ЗАПОЛНЕНИЯ находятся в первых строках листа "Шаблон"', '', '', ''],
            ['', '', '', ''],
            ['ПОЯСНЕНИЯ ПО КОЛОНКАМ:', '', '', ''],
            ['- Серийные номера: автоматически помечает, если нужен серийный номер', '', '', ''],
            ['- Сроки годности: автоматически помечает, если нужен срок годности', '', '', ''],
        ]
        
        df_instruction = pd.DataFrame(instruction_data)
        df_instruction.to_excel(writer, index=False, sheet_name='Инструкция', header=False)
        
        # ============ ЛИСТ 3: ПОСТАВЩИКИ (с ID) ============
        suppliers_data = [
            ['ID', 'НАИМЕНОВАНИЕ ПОСТАВЩИКА'],
        ]
        
        # Получаем поставщиков из БД
        suppliers = db.execute_query(
            "SELECT id, name FROM suppliers WHERE is_active = 1 ORDER BY name",
            fetch_all=True
        ) or []
        
        for s in suppliers:
            suppliers_data.append([s['id'], s['name']])
        
        # Добавляем примеры, если поставщиков мало
        if len(suppliers) < 5:
            example_suppliers = [
                [999, 'ИП Вилкин К.В.'],
                [998, 'ООО "СЕЛЛЕНА"'],
                [997, 'ИП Брусницын Д.Н.'],
            ]
            for ex in example_suppliers:
                if ex[1] not in [s['name'] for s in suppliers]:
                    suppliers_data.append(ex)
        
        df_suppliers = pd.DataFrame(suppliers_data[1:], columns=suppliers_data[0])
        df_suppliers.to_excel(writer, index=False, sheet_name='Поставщики')
        
        # ============ ЛИСТ 4: НОМЕНКЛАТУРА (справочник) ============
        nomen_data = [
            ['НАИМЕНОВАНИЕ', 'АРТИКУЛ', 'КАТЕГОРИЯ', 'ТИП УЧЕТА', 'ЕД.ИЗМ.', 'ID']
        ]
        
        # Получаем номенклатуру из БД (топ-50)
        nomenclatures = db.search_nomenclatures(limit=50)
        
        for n in nomenclatures:
            nomen_data.append([
                n['name'][:50] + ('...' if len(n['name']) > 50 else ''),
                n['sku'],
                n.get('category_name', ''),
                n.get('accounting_type', ''),
                n.get('unit', 'шт.'),
                n['id']
            ])
        
        df_nomen = pd.DataFrame(nomen_data[1:], columns=nomen_data[0])
        df_nomen.to_excel(writer, index=False, sheet_name='Номенклатура')
        
        # ============ ЛИСТ 5: КАТЕГОРИИ (справочник) ============
        categories_data = [
            ['ID', 'НАЗВАНИЕ', 'ПОЛНЫЙ ПУТЬ', 'ТИП УЧЕТА']
        ]
        
        categories = db.get_all_categories()
        for cat in categories:
            # Формируем полный путь
            path_parts = []
            if cat.get('path'):
                path_parts = [p['name'] for p in cat['path']]
            path_parts.append(cat['name'])
            full_path = ' > '.join(path_parts)
            
            categories_data.append([
                cat['id'],
                cat['name'],
                full_path,
                cat.get('accounting_type', '')
            ])
        
        df_categories = pd.DataFrame(categories_data[1:], columns=categories_data[0])
        df_categories.to_excel(writer, index=False, sheet_name='Категории')
        
        # ============ ЛИСТ 6: СКЛАДЫ (справочник) ============
        warehouses_data = [
            ['ID', 'НАЗВАНИЕ', 'ТИП', 'АДРЕС']
        ]
        
        warehouses = db.execute_query(
            "SELECT id, name, type, location_id FROM warehouses WHERE is_active = 1",
            fetch_all=True
        ) or []
        
        for w in warehouses:
            warehouses_data.append([
                w['id'],
                w['name'],
                w.get('type', ''),
                ''
            ])
        
        df_warehouses = pd.DataFrame(warehouses_data[1:], columns=warehouses_data[0])
        df_warehouses.to_excel(writer, index=False, sheet_name='Склады')
        
        # ============ ЛИСТ 7: ЕДИНИЦЫ ИЗМЕРЕНИЯ ============
        units_data = [
            ['ЗНАЧЕНИЕ', 'НАЗВАНИЕ', 'КАТЕГОРИЯ']
        ]
        
        for unit in UNITS:
            units_data.append([
                unit['value'],
                unit['label'],
                unit.get('category', 'common')
            ])
        
        df_units = pd.DataFrame(units_data[1:], columns=units_data[0])
        df_units.to_excel(writer, index=False, sheet_name='Единицы измерения')
        
        # ============ ЛИСТ 8: ПРИМЕРЫ ЗАПОЛНЕНИЯ ============
        examples_data = [
            ['ТИП УЧЕТА', 'ВИД', 'СИТУАЦИЯ', 'ПРИМЕР ЗАПОЛНЕНИЯ'],
            ['Партионный', 'Элемент', 'Материалы с ограниченным сроком годности', 
             'Краска, химия, продукты, где важны партии и сроки'],
            ['Индивидуальный', 'Номенклатура', 'Дорогостоящее оборудование, инструменты', 
             'Ноутбуки, станки, приборы - каждый экземпляр уникален'],
            ['Количественный', 'Номенклатура', 'Расходные материалы, канцелярия', 
             'Бумага, ручки, салфетки - учитывается только количество'],
            ['Партионный', 'Комплект', 'Наборы с ограниченным сроком', 
             'Аптечки, наборы расходников с единым сроком годности'],
            ['Индивидуальный', 'Комплект', 'Сложное оборудование в сборе', 
             'Рабочая станция (системный блок + монитор + периферия)'],
        ]
        
        df_examples = pd.DataFrame(examples_data)
        df_examples.to_excel(writer, index=False, sheet_name='Примеры', header=False)
    
    output.seek(0)
    
    filename = f"import_template_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=filename
    )

def create_instance_from_item(item, document_id, db, user_id):
    """Создание экземпляра для индивидуального учета"""
    try:
        # Генерируем инвентарный номер
        year = datetime.now().year
        inventory_number = f"{year}-{int(time.time()) % 1000000:06d}"
        
        inst_row = db.execute_query("""
            INSERT INTO instances (
                nomenclature_id, inventory_number, status,
                purchase_price, created_by, created_at
            ) VALUES (?, ?, 'in_stock', ?, ?, CURRENT_TIMESTAMP)
            RETURNING id
        """, (
            item['nomenclature_id'],
            inventory_number,
            item['price'],
            user_id
        ), fetch_all=False)

        instance_id = inst_row['id']
        
        # Связываем с документом
        db.execute_query("""
            UPDATE document_items SET instance_id = ?
            WHERE document_id = ? AND nomenclature_id = ?
        """, (instance_id, document_id, item['nomenclature_id']))
        
    except Exception as e:
        logger.error(f"Ошибка создания экземпляра: {e}")

def calculate_expiry_date(data):
    """Расчет даты истечения срока"""
    if data.get('production_date') and data.get('expiry_days'):
        try:
            prod_date = datetime.strptime(data['production_date'], '%Y-%m-%d')
            expiry = prod_date + timedelta(days=data['expiry_days'])
            return expiry.strftime('%Y-%m-%d')
        except Exception:
            pass
    return None
    
# ============ ШАБЛОНЫ ДЛЯ ИМПОРТА ============

@import_export_bp.route('/import-export/template/receipt', endpoint='download_receipt_template')
@login_required
def download_receipt_template():
    """Скачивание простого шаблона для импорта поступлений (БЕЗ ФОРМУЛ)"""
    import pandas as pd
    from io import BytesIO
    from datetime import datetime
    
    db = get_db()
    output = BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # ============ ЛИСТ 1: ШАБЛОН ============
        template_data = {
            'ТИП': ['Партионный', 'Партионный', ''],
            'ВИД': ['Элемент', 'Элемент', ''],
            'НАИМЕНОВАНИЕ': ['Смазка силиконовая аэрозольная Si-M 165 грамм', 
                             'Смазка силиконовая аэрозольная Si-M 165 грамм', ''],
            'КОЛИЧЕСТВО': [4, 4, ''],
            'ЕД.ИЗМ.': ['шт.', 'шт.', ''],
            'СКЛАД': ['Основной', 'Основной', ''],
            'ДАТА': ['2026-03-03', '2025-12-10', ''],
            'ПОСТАВЩИК': ['ООО "СЕЛЛЕНА"', 'ООО "СЕЛЛЕНА"', ''],
            'ПАРТИЯ': ['', '', ''],
            'ДАТА ПРОИЗВОДСТВА': ['2025-10-07', '2025-09-09', ''],
            'СРОК ГОДНОСТИ': ['2026-10-07', '2026-09-09', ''],
            'ЦЕНА': [255, 255, ''],
            'ОСНОВАНИЕ': ['Поступление', 'Поступление', ''],
            'Производитель': ['Пента Юниор', 'Пента Юниор', ''],
            'Модель': ['SI-M', 'SI-M', ''],
            'Бренд': ['Пента Юниор', 'Пента Юниор', '']
        }
        
        df_template = pd.DataFrame(template_data)
        df_template.to_excel(writer, index=False, sheet_name='Шаблон')
        
        # ============ ЛИСТ 2: ИНСТРУКЦИЯ ============
        instruction_data = [
            ['ИНСТРУКЦИЯ ПО ЗАПОЛНЕНИЮ ШАБЛОНА ПОСТУПЛЕНИЯ'],
            [''],
            ['ДОПУСТИМЫЕ ЗНАЧЕНИЯ:'],
            ['- ТИП: Индивидуальный, Партионный, Количественный'],
            ['- ВИД: Номенклатура, Элемент, Комплект'],
            ['- ЕД.ИЗМ.: шт., г., кг., мл., л., м., м², м.п., компл., уп.'],
            ['- СКЛАДЫ: Основной, БПЛА, АКБ, Инструментальный, Склад УПК'],
            ['- ПОСТАВЩИКИ: ООО "СЕЛЛЕНА", ИП Вилкин К.В., ИП Брусницын Д.Н.'],
            [''],
            ['ПРАВИЛА ЗАПОЛНЕНИЯ:'],
            ['1. Заполняйте данные начиная с 1-й строки (после заголовков)'],
            ['2. Не изменяйте названия колонок'],
            ['3. Поля НАИМЕНОВАНИЕ, КОЛИЧЕСТВО, СКЛАД, ДАТА обязательны'],
            ['4. Дата в формате: ГГГГ-ММ-ДД (например: 2026-03-03)'],
            ['5. Для количества используйте число (можно дробное)'],
            ['6. Партия и срок годности заполняются при необходимости'],
            [''],
            ['ПРИМЕРЫ ЗАПОЛНЕНИЯ находятся в первых двух строках'],
        ]
        
        df_instruction = pd.DataFrame(instruction_data)
        df_instruction.to_excel(writer, index=False, sheet_name='Инструкция', header=False)
        
        # ============ ЛИСТ 3: СПРАВОЧНИКИ ============
        # Склады
        warehouses = db.execute_query("SELECT name FROM warehouses WHERE is_active = 1", fetch_all=True) or []
        warehouse_list = [w['name'] for w in warehouses]
        
        # Поставщики
        suppliers = db.execute_query("SELECT name FROM suppliers WHERE is_active = 1", fetch_all=True) or []
        supplier_list = [s['name'] for s in suppliers]
        
        # Категории (только первые 20)
        categories = db.get_all_categories()
        category_list = [cat['name'] for cat in categories][:20]
        
        # Единицы измерения
        units = ['шт.', 'г.', 'кг.', 'мл.', 'л.', 'м.', 'м²', 'м.п.', 'компл.', 'уп.']
        
        # Типы учета
        types = ['Индивидуальный', 'Партионный', 'Количественный']
        
        # Виды
        vidy = ['Номенклатура', 'Элемент', 'Комплект']
        
        # Создаем DataFrame для справочников
        max_rows = max(len(warehouse_list), len(supplier_list), len(category_list), 
                       len(units), len(types), len(vidy))
        
        ref_data = {
            'СКЛАДЫ': warehouse_list + [''] * (max_rows - len(warehouse_list)),
            'ПОСТАВЩИКИ': supplier_list + [''] * (max_rows - len(supplier_list)),
            'КАТЕГОРИИ': category_list + [''] * (max_rows - len(category_list)),
            'ЕД.ИЗМ.': units + [''] * (max_rows - len(units)),
            'ТИПЫ УЧЕТА': types + [''] * (max_rows - len(types)),
            'ВИДЫ': vidy + [''] * (max_rows - len(vidy))
        }
        
        df_ref = pd.DataFrame(ref_data)
        df_ref.to_excel(writer, index=False, sheet_name='Справочники')
        
        # ============ ЛИСТ 4: НОМЕНКЛАТУРА (ТОП-50) ============
        nomenclatures = db.search_nomenclatures(limit=50)
        nomen_data = [['НАИМЕНОВАНИЕ', 'АРТИКУЛ', 'КАТЕГОРИЯ', 'ТИП УЧЕТА', 'ЕД.ИЗМ.']]
        for n in nomenclatures:
            nomen_data.append([
                n['name'][:50] + ('...' if len(n['name']) > 50 else ''),
                n['sku'],
                n.get('category_name', ''),
                n.get('accounting_type', ''),
                n.get('unit', 'шт.')
            ])
        
        df_nomen = pd.DataFrame(nomen_data[1:], columns=nomen_data[0])
        df_nomen.to_excel(writer, index=False, sheet_name='Номенклатура')
    
    output.seek(0)
    
    filename = f"template_receipt_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=filename
    )

      
# ============ API ДЛЯ ГЕНЕРАЦИИ ШТРИХ-КОДА ============

@import_export_bp.route('/api/generate-barcode', methods=['POST'], endpoint='api_generate_barcode')
@login_required
@limiter.limit("30 per minute")
def api_generate_barcode():
    """API для генерации штрих-кода"""
    try:
        data = request.json
        sku = data.get('sku')
        name = data.get('name')
        current_id = data.get('current_id')
        
        barcode = generate_unique_barcode(sku, name)
        
        # Проверяем уникальность
        if not is_barcode_unique(barcode, current_id):
            # Если не уникален, генерируем другой
            for _ in range(10):
                barcode = generate_unique_barcode(sku, name)
                if is_barcode_unique(barcode, current_id):
                    break
        
        return jsonify({
            'success': True,
            'barcode': barcode
        })
        
    except Exception as e:
        logger.error(f'Ошибка генерации штрих-кода: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500

# ============ API ДЛЯ ЦЕЛИ РАСХОДОВАНИЯ ============

@import_export_bp.route('/api/expense-purposes', endpoint='api_expense_purposes')
@login_required
def api_expense_purposes():
    """API для получения целей расходования"""
    try:
        db = get_db()
        category = request.args.get('category')
        
        query = "SELECT * FROM expense_purposes WHERE is_active = 1"
        params = []
        
        if category:
            query += " AND category = ?"
            params.append(category)
        
        query += " ORDER BY sort_order, name"
        
        purposes = db.execute_query(query, params, fetch_all=True)
        
        result = []
        for p in purposes:
            result.append(dict(p))
        
        return jsonify(result)
    except Exception as e:
        logger.error(f'Ошибка получения целей: {e}')
        return jsonify([])

@import_export_bp.route('/api/expense-purposes/<int:id>', endpoint='api_expense_purpose')
@login_required
def api_expense_purpose(id):
    """API для получения конкретной цели"""
    try:
        db = get_db()
        purpose = db.execute_query(
            "SELECT * FROM expense_purposes WHERE id = ?", 
            (id,), 
            fetch_all=False
        )
        
        if purpose:
            return jsonify(dict(purpose))
        return jsonify({'error': 'Цель не найдена'}), 404
    except Exception as e:
        logger.error(f'Ошибка получения цели: {e}')
        return jsonify({'error': str(e)}), 500
          
# ============ ТЕСТОВЫЙ МАРШРУТ ДЛЯ ГЕНЕРАЦИИ ============

@import_export_bp.route('/test/generate-barcodes', endpoint='test_generate_barcodes')
@admin_required
def test_generate_barcodes():
    """Тестовая генерация штрих-кодов для существующих записей"""
    db = get_db()
    nomenclatures = db.execute_query(
        "SELECT id, sku, name FROM nomenclatures WHERE barcode IS NULL OR barcode = ''",
        fetch_all=True
    )
    generated = 0
    for nom in nomenclatures or []:
        barcode = generate_unique_barcode(nom['sku'], nom['name'])
        if barcode and is_barcode_unique(barcode):
            db.execute_query(
                "UPDATE nomenclatures SET barcode = ? WHERE id = ?",
                (barcode, nom['id'])
            )
            generated += 1
    flash(f'Сгенерировано {generated} штрих-кодов', 'success')
    return redirect(url_for('nomenclatures.nomenclatures_list'))

@import_export_bp.route('/debug/attach-boot-instances', endpoint='debug_attach_boot_instances')
@admin_required
def debug_attach_boot_instances():
    """Привязка экземпляров сапог к модификациям"""
    from flask import current_app
    if not current_app.debug:
        return __import__("flask").jsonify({"error": "Debug-only endpoint"}), 403
    try:
        db = get_db()
        
        # Получаем все модификации для номенклатуры 900
        variations = db.execute_query("""
            SELECT id, size FROM nomenclature_variations 
            WHERE nomenclature_id = 900
            ORDER BY size
        """, fetch_all=True)
        
        # Получаем непривязанные экземпляры
        instances = db.execute_query("""
            SELECT id, inventory_number FROM instances 
            WHERE nomenclature_id = 900 AND variation_id IS NULL
            ORDER BY id
        """, fetch_all=True)
        
        if not instances:
            return "Нет экземпляров для обновления"
        
        results = []
        
        # Назначаем модификации: первому 43, остальным 41-42
        for i, inst in enumerate(instances):
            if i == 0:  # Первому экземпляру (2026-001068) даем размер 43
                var = variations[1] if len(variations) > 1 else variations[0]
            else:  # Остальным (2026-001069, 2026-001070) даем размер 41-42
                var = variations[0]
            
            db.execute_query("""
                UPDATE instances 
                SET variation_id = ?,
                    siz_size = ?,
                    siz_color = 'черный'
                WHERE id = ?
            """, (var['id'], var['size'], inst['id']))
            
            results.append(f"✅ Экземпляр {inst['inventory_number']} → размер {var['size']}")
        
        db.connection.commit()
        
        html = "<h1>Привязка экземпляров</h1>"
        html += "<ul>"
        for r in results:
            html += f"<li>{r}</li>"
        html += "</ul>"
        html += '<p><a href="/nomenclatures/900">Вернуться к номенклатуре</a></p>'
        
        return html
        
    except Exception as e:
        return f"❌ Ошибка: {str(e)}"

@import_export_bp.route('/debug/check-boot-instances', endpoint='debug_check_boot_instances')
@admin_required
def debug_check_boot_instances():
    """Проверка привязки экземпляров сапог"""
    from flask import current_app
    if not current_app.debug:
        return __import__("flask").jsonify({"error": "Debug-only endpoint"}), 403
    try:
        db = get_db()
        
        instances = db.execute_query("""
            SELECT 
                i.id,
                i.inventory_number,
                i.variation_id,
                nv.size,
                nv.color,
                nv.sku as variation_sku
            FROM instances i
            LEFT JOIN nomenclature_variations nv ON i.variation_id = nv.id
            WHERE i.nomenclature_id = 900
            ORDER BY i.id
        """, fetch_all=True)
        
        html = "<h1>Проверка экземпляров сапог</h1>"
        html += "<table border='1'><tr><th>ID</th><th>Инв. номер</th><th>variation_id</th><th>Размер</th><th>Цвет</th><th>SKU модификации</th></tr>"
        
        for inst in instances or []:
            html += f"<tr>"
            html += f"<td>{inst['id']}</td>"
            html += f"<td>{inst['inventory_number']}</td>"
            html += f"<td>{inst['variation_id'] or 'NULL'}</td>"
            html += f"<td>{inst['size'] or '—'}</td>"
            html += f"<td>{inst['color'] or '—'}</td>"
            html += f"<td>{inst['variation_sku'] or '—'}</td>"
            html += f"</tr>"
        
        html += "</table>"
        html += '<p><a href="/nomenclatures/900">Вернуться к номенклатуре</a></p>'
        
        return html
        
    except Exception as e:
        return f"Ошибка: {str(e)}"
        
@import_export_bp.route('/debug/check-purposes', endpoint='debug_check_purposes')
@admin_required
def debug_check_purposes():
    from flask import current_app
    if not current_app.debug:
        return jsonify({'error': 'Debug-only endpoint'}), 403
    if session.get('role') != 'admin':
        return "Доступ запрещен"
    
    db = get_db()

    # Проверяем, существует ли таблица
    table_exists = db.execute_query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='expense_purposes'",
        fetch_all=False
    )

    if not table_exists:
        return "❌ Таблица expense_purposes НЕ существует! Нужно создать."

    # Проверяем структуру таблицы
    columns = db.execute_query("PRAGMA table_info(expense_purposes)", fetch_all=True) or []

    # Проверяем количество записей
    count_row = db.execute_query("SELECT COUNT(*) as cnt FROM expense_purposes", fetch_all=False)
    count = [count_row['cnt']] if count_row else [0]

    # Получаем все записи
    purposes = db.execute_query("SELECT * FROM expense_purposes", fetch_all=True) or []
    
    result = f"""
    <h1>Проверка таблицы expense_purposes</h1>
    
    <h2>Таблица существует: {'✅' if table_exists else '❌'}</h2>
    
    <h3>Структура таблицы:</h3>
    <ul>
    """
    for col in columns:
        result += f"<li>{col[1]} ({col[2]})</li>"
    
    result += f"</ul>"
    
    result += f"<h3>Всего записей: {count[0] if count else 0}</h3>"
    
    if purposes:
        result += "<h3>Содержимое таблицы:</h3><table border='1'><tr>"
        # Заголовки
        for key in purposes[0].keys():
            result += f"<th>{key}</th>"
        result += "</tr>"
        
        # Данные
        for row in purposes:
            result += "<tr>"
            for val in row:
                result += f"<td>{val}</td>"
            result += "</tr>"
        result += "</table>"
    else:
        result += "<p>❌ Таблица пуста! Нужно добавить данные.</p>"
    
    return result

@import_export_bp.route('/debug/check-transfer/<int:document_id>', endpoint='debug_check_transfer')
@admin_required
def debug_check_transfer(document_id):
    """Проверка документа перемещения"""
    from flask import current_app
    if not current_app.debug:
        return jsonify({'error': 'Debug-only endpoint'}), 403
    try:
        db = get_db()
        
        # Получаем документ
        doc = db.execute_query("SELECT * FROM documents WHERE id = ?", (document_id,), fetch_all=False)
        if not doc:
            return f"Документ {document_id} не найден"
        
        doc = dict(doc)
        
        html = f"<h1>Проверка документа перемещения {doc['document_number']}</h1>"
        html += f"<p>Тип: {doc['document_type']}</p>"
        html += f"<p>Статус: {doc['status']}</p>"
        html += f"<p>Склад отправитель: {doc['from_warehouse_id']}</p>"
        html += f"<p>Склад получатель: {doc['to_warehouse_id']}</p>"
        
        # Получаем позиции
        items = db.execute_query("SELECT * FROM document_items WHERE document_id = ?", (document_id,), fetch_all=True)
        
        html += "<h2>Позиции:</h2>"
        
        for item in items:
            item_dict = dict(item)
            html += f"<p>Номенклатура ID: {item_dict['nomenclature_id']}, Количество: {item_dict['quantity']}</p>"
            
            # Получаем информацию о номенклатуре
            nomen = db.execute_query("SELECT name, sku FROM nomenclatures WHERE id = ?", 
                                     (item_dict['nomenclature_id'],), fetch_all=False)
            if nomen:
                html += f"<p>Наименование: {nomen['name']} ({nomen['sku']})</p>"
            
            # Проверяем остатки на складе отправителе
            from_stock = db.execute_query("""
                SELECT * FROM stocks 
                WHERE nomenclature_id = ? AND warehouse_id = ?
            """, (item_dict['nomenclature_id'], doc['from_warehouse_id']), fetch_all=True)
            
            html += "<h3>Остатки на складе отправителе:</h3>"
            if from_stock:
                for s in from_stock:
                    s_dict = dict(s)
                    html += f"<p>Количество: {s_dict['quantity']}</p>"
            else:
                html += "<p>❌ Нет остатков на складе отправителе!</p>"
            
            # Проверяем остатки на складе получателе
            to_stock = db.execute_query("""
                SELECT * FROM stocks 
                WHERE nomenclature_id = ? AND warehouse_id = ?
            """, (item_dict['nomenclature_id'], doc['to_warehouse_id']), fetch_all=True)
            
            html += "<h3>Остатки на складе получателе:</h3>"
            if to_stock:
                for s in to_stock:
                    s_dict = dict(s)
                    html += f"<p>Количество: {s_dict['quantity']}</p>"
            else:
                html += "<p>Нет остатков на складе получателе</p>"
        
        return html
        
    except Exception as e:
        import traceback
        return f"<pre>Ошибка: {str(e)}\n{traceback.format_exc()}</pre>"

@import_export_bp.route('/debug/fix-silicon-stock', endpoint='debug_fix_silicon_stock')
@admin_required
def debug_fix_silicon_stock():
    """Исправление остатка силиконовой смазки"""
    from flask import current_app
    if not current_app.debug:
        return jsonify({'error': 'Debug-only endpoint'}), 403
    try:
        db = get_db()
        
        # ID номенклатуры
        nomenclature_id = 886
        # Склад Основной (обычно ID=1)
        warehouse_id = 1
        # Новое количество
        new_quantity = 11
        
        # Проверяем, есть ли уже остатки
        existing = db.execute_query("""
            SELECT id, quantity FROM stocks 
            WHERE nomenclature_id = ? AND warehouse_id = ?
        """, (nomenclature_id, warehouse_id), fetch_all=True)
        
        result = []
        if existing:
            for e in existing:
                result.append(f"ID: {e['id']}, Количество: {e['quantity']}")
        
        # Удаляем все старые записи
        db.execute_query("DELETE FROM stocks WHERE nomenclature_id = ?", (nomenclature_id,))

        # Создаем новую с правильным количеством
        db.execute_query("""
            INSERT INTO stocks (nomenclature_id, warehouse_id, quantity)
            VALUES (?, ?, ?)
        """, (nomenclature_id, warehouse_id, new_quantity))
        
        db.connection.commit()
        
        return f"""
        <h1>✅ Остаток исправлен</h1>
        <p>Номенклатура: Силиконовая смазка (165 грамм) ID 886</p>
        <p>Склад: Основной</p>
        <p>Было: {', '.join(result) if result else 'нет остатков'}</p>
        <p>Стало: {new_quantity} шт.</p>
        <p><a href="/stocks">Перейти к остаткам</a></p>
        """
        
    except Exception as e:
        return f"<h1>❌ Ошибка</h1><pre>{str(e)}</pre>"
    
@import_export_bp.route('/debug/check-silicon', endpoint='debug_check_silicon')
@admin_required
def debug_check_silicon():
    """Проверка силиконовых смазок"""
    from flask import current_app
    if not current_app.debug:
        return jsonify({'error': 'Debug-only endpoint'}), 403
    try:
        db = get_db()
        
        # Ищем все номенклатуры со словом "силикон"
        nomenclatures = db.execute_query("""
            SELECT id, name, sku FROM nomenclatures 
            WHERE name LIKE '%силикон%' OR name LIKE '%смазк%'
            ORDER BY name
        """, fetch_all=True)
        
        html = "<h1>Силиконовые смазки в номенклатуре</h1>"
        
        if not nomenclatures:
            return "<h1>Ничего не найдено</h1>"
        
        html += "<table border='1'><tr><th>ID</th><th>Наименование</th><th>Артикул</th><th>Остатки</th></tr>"
        
        for n in nomenclatures:
            n_dict = dict(n)
            
            # Остатки по этой номенклатуре
            stocks = db.execute_query("""
                SELECT s.*, w.name as warehouse_name 
                FROM stocks s
                LEFT JOIN warehouses w ON s.warehouse_id = w.id
                WHERE s.nomenclature_id = ?
            """, (n_dict['id'],), fetch_all=True)
            
            stock_info = "<br>".join([f"{s['warehouse_name']}: {s['quantity']} шт." for s in stocks]) if stocks else "Нет остатков"
            
            html += f"<tr>"
            html += f"<td>{n_dict['id']}</td>"
            html += f"<td>{n_dict['name']}</td>"
            html += f"<td>{n_dict['sku']}</td>"
            html += f"<td>{stock_info}</td>"
            html += f"</tr>"
        
        html += "</table>"
        
        return html
        
    except Exception as e:
        return f"Ошибка: {str(e)}"
        
