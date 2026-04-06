"""
Blueprint: nomenclatures
Маршруты для номенклатуры, изображений, API поиска и категорий.
"""
import os
import re
import time
import logging
import threading
import traceback
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, flash, send_file
from werkzeug.utils import secure_filename
from PIL import Image
from extensions import csrf, limiter
from routes.common import login_required, admin_required, get_db
from utils.search import build_where
from utils.validators import validate_json
from schemas.nomenclature import QuickCreateNomenclatureSchema, VariationSchema

logger = logging.getLogger('routes')

nomenclatures_bp = Blueprint('nomenclatures', __name__)

# ============ КЭШ ДЛЯ МИНИАТЮР (thread-safe, ограничен 500 записями, TTL 5 мин) ============
_thumbnail_cache: dict = {}       # cache_key -> response | 'not_found'
_thumbnail_cache_time: dict = {}  # cache_key -> timestamp
_thumbnail_cache_lock = threading.Lock()
_THUMBNAIL_CACHE_MAX = 500
_THUMBNAIL_CACHE_TTL = 300  # секунд


def _cache_set(key: str, value, ts: float) -> None:
    """Thread-safe запись в кэш миниатюр с ограничением размера."""
    with _thumbnail_cache_lock:
        if len(_thumbnail_cache) >= _THUMBNAIL_CACHE_MAX:
            # Удаляем самую старую запись
            oldest = min(_thumbnail_cache_time, key=_thumbnail_cache_time.get)
            _thumbnail_cache.pop(oldest, None)
            _thumbnail_cache_time.pop(oldest, None)
        _thumbnail_cache[key] = value
        _thumbnail_cache_time[key] = ts


def _cache_del(key: str) -> None:
    """Thread-safe удаление из кэша миниатюр."""
    with _thumbnail_cache_lock:
        _thumbnail_cache.pop(key, None)
        _thumbnail_cache_time.pop(key, None)

# ============ КЭШ ДЛЯ ПОИСКА НОМЕНКЛАТУР (TTL 30 сек) ============
_search_cache: dict = {}
_search_cache_time: dict = {}
_search_cache_lock = threading.Lock()
_SEARCH_CACHE_MAX = 200
_SEARCH_CACHE_TTL = 30  # секунд


def _search_cache_get(key: str):
    with _search_cache_lock:
        ts = _search_cache_time.get(key, 0)
        if time.time() - ts < _SEARCH_CACHE_TTL:
            return _search_cache.get(key)
    return None


def _search_cache_set(key: str, value) -> None:
    with _search_cache_lock:
        if len(_search_cache) >= _SEARCH_CACHE_MAX:
            oldest = min(_search_cache_time, key=_search_cache_time.get)
            _search_cache.pop(oldest, None)
            _search_cache_time.pop(oldest, None)
        _search_cache[key] = value
        _search_cache_time[key] = time.time()


# ============ КОНСТАНТА ДЛЯ "НЕТ ИЗОБРАЖЕНИЯ" ============
import flask
_DEFAULT_NO_IMAGE_ABS = None

def _get_no_image_abs():
    global _DEFAULT_NO_IMAGE_ABS
    if _DEFAULT_NO_IMAGE_ABS is None:
        from flask import current_app
        _DEFAULT_NO_IMAGE_ABS = os.path.join(current_app.root_path, 'static', 'img', 'no-image.png')
    return _DEFAULT_NO_IMAGE_ABS

# ============ ЕДИНИЦЫ ИЗМЕРЕНИЯ ============
UNITS = [
    {'value': 'шт.', 'label': 'штуки', 'category': 'common'},
    {'value': 'м', 'label': 'метры', 'category': 'length'},
    {'value': 'м²', 'label': 'квадратные метры', 'category': 'area'},
    {'value': 'м³', 'label': 'кубические метры', 'category': 'volume'},
    {'value': 'м.п', 'label': 'погонные метры', 'category': 'length'},
    {'value': 'л', 'label': 'литры', 'category': 'volume'},
    {'value': 'кг', 'label': 'килограммы', 'category': 'weight'},
    {'value': 'г', 'label': 'граммы', 'category': 'weight'},
    {'value': 'т', 'label': 'тонны', 'category': 'weight'},
    {'value': 'уп', 'label': 'упаковка', 'category': 'pack'},
    {'value': 'кор', 'label': 'коробка', 'category': 'pack'},
    {'value': 'компл', 'label': 'комплект', 'category': 'pack'},
    {'value': 'пара', 'label': 'пара', 'category': 'pair'},
]

# ============ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ============

def allowed_file(filename):
    """Проверка разрешенного расширения файла"""
    from flask import current_app
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in current_app.config.get('ALLOWED_EXTENSIONS', {'png', 'jpg', 'jpeg', 'gif'})

def create_thumbnail(input_path, output_path, size=(200, 200)):
    """Создание миниатюры изображения"""
    try:
        with Image.open(input_path) as img:
            if img.mode in ('RGBA', 'P'):
                img = img.convert('RGB')
            img.thumbnail(size, Image.Resampling.LANCZOS)
            background = Image.new('RGB', size, (255, 255, 255))
            offset = ((size[0] - img.size[0]) // 2, (size[1] - img.size[1]) // 2)
            background.paste(img, offset)
            background.save(output_path, 'JPEG', quality=85)
            return True
    except Exception as e:
        logger.error(f"Ошибка создания миниатюры: {e}")
        return False

# ============ МАРШРУТ ДЛЯ МИНИАТЮР (С КЭШИРОВАНИЕМ) ============

@nomenclatures_bp.route('/api/nomenclatures/<int:id>/thumbnail', endpoint='get_nomenclature_thumbnail')
@login_required
def get_nomenclature_thumbnail(id):
    """Получение миниатюры для списка с кэшированием и повторными попытками"""

    # Проверяем кэш (thread-safe)
    cache_key = f"thumb_{id}"
    current_time = time.time()

    with _thumbnail_cache_lock:
        if cache_key in _thumbnail_cache:
            cache_age = current_time - _thumbnail_cache_time.get(cache_key, 0)
            if cache_age < _THUMBNAIL_CACHE_TTL:
                cached_result = _thumbnail_cache[cache_key]
                if cached_result == 'not_found':
                    logger.debug(f"Кэш: изображение не найдено для ID {id}")
                    return send_file(_get_no_image_abs())
                elif cached_result:
                    logger.debug(f"Кэш: возвращаем результат для ID {id}")
                    return cached_result
            else:
                logger.debug(f"Кэш устарел для ID {id}, удаляем")
                _cache_del(cache_key)

    # Выполняем запрос через стандартный интерфейс БД
    def try_query():
        try:
            db = get_db()
            table_exists = db.execute_query(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='nomenclature_images'",
                fetch_all=False
            )
            if not table_exists:
                _cache_set(cache_key, 'not_found', current_time)
                return None

            row = db.execute_query(
                "SELECT file_path FROM nomenclature_images WHERE nomenclature_id = ? AND is_primary = 1 LIMIT 1",
                (int(id),),
                fetch_all=False
            )
            if row and row.get('file_path'):
                return row['file_path']
            else:
                _cache_set(cache_key, 'not_found', current_time)
                return None
        except Exception as e:
            logger.error(f"SQLite ошибка для ID {id}: {str(e)}")
            return None

    # Выполняем запрос
    file_path = try_query()

    # Отправка файла
    if file_path:
        try:
            from flask import current_app
            logger.debug(f"Найдено изображение: {file_path}")

            if file_path.startswith('/static/'):
                file_path = file_path[8:]

            static_root = os.path.realpath(os.path.join(current_app.root_path, 'static'))
            full_path = os.path.realpath(os.path.join(static_root, file_path.replace('/', os.sep)))
            if not full_path.startswith(static_root + os.sep) and full_path != static_root:
                logger.warning(f"Попытка path traversal: {file_path!r}")
                return send_file(_get_no_image_abs())
            logger.debug(f"Полный путь к файлу: {full_path}")

            if os.path.exists(full_path):
                base, ext = os.path.splitext(full_path)
                thumb_path = base + '_thumb.jpg'
                logger.debug(f"Путь к миниатюре: {thumb_path}")

                if os.path.exists(thumb_path):
                    logger.debug(f"Отправляем миниатюру: {thumb_path}")
                    result = send_file(thumb_path)
                    _cache_set(cache_key, result, current_time)
                    return result
                else:
                    logger.debug(f"Отправляем оригинал: {full_path}")
                    result = send_file(full_path)
                    _cache_set(cache_key, result, current_time)
                    return result
            else:
                logger.debug(f"Файл не существует: {full_path}")
                _cache_set(cache_key, 'not_found', current_time)
                return send_file(_get_no_image_abs())

        except Exception as e:
            logger.error(f"Ошибка при отправке файла для ID {id}: {str(e)}")
            _cache_set(cache_key, 'not_found', current_time)
            return send_file(_get_no_image_abs())

    logger.debug(f"Изображение не найдено для ID {id}, возвращаем no-image.png")
    return send_file(_get_no_image_abs())

# ============ API ДЛЯ РАБОТЫ С МОДИФИКАЦИЯМИ ============

@nomenclatures_bp.route('/api/nomenclatures/<int:id>/variations', endpoint='api_nomenclature_variations')
@login_required
def api_nomenclature_variations(id):
    """API для получения модификаций номенклатуры"""
    try:
        db = get_db()
        variations = db.get_variations(id)
        return jsonify(variations)
    except Exception as e:
        logger.error(f'Ошибка получения модификаций: {e}')
        return jsonify([])

@nomenclatures_bp.route('/api/nomenclatures/<int:id>/variations/create', methods=['POST'], endpoint='api_create_variation')
@login_required
def api_create_variation(id):
    """Создание модификации для номенклатуры"""
    try:
        db = get_db()
        data, err = validate_json(VariationSchema)
        if err:
            return err

        result = db.create_variation(id, data, session['user_id'])
        return jsonify(result)

    except Exception as e:
        logger.error(f'Ошибка создания модификации: {e}')
        return jsonify({'success': False, 'message': str(e)})

@nomenclatures_bp.route('/api/variations/<int:id>', endpoint='api_get_variation')
@login_required
def api_get_variation(id):
    """Получение данных модификации"""
    try:
        db = get_db()
        row = db.execute_query("SELECT * FROM nomenclature_variations WHERE id = ?", (id,), fetch_all=False)

        if row:
            return jsonify(dict(row))
        return jsonify({'error': 'Не найдено'}), 404

    except Exception as e:
        logger.error(f'Ошибка получения модификации: {e}')
        return jsonify({'error': str(e)}), 500

@nomenclatures_bp.route('/api/variations/<int:id>/update', methods=['POST'], endpoint='api_update_variation')
@login_required
def api_update_variation(id):
    """Обновление модификации"""
    try:
        db = get_db()
        data, err = validate_json(VariationSchema)
        if err:
            return err

        result = db.update_variation(id, data, session['user_id'])
        return jsonify(result)

    except Exception as e:
        logger.error(f'Ошибка обновления модификации: {e}')
        return jsonify({'success': False, 'message': str(e)})

@nomenclatures_bp.route('/api/variations/<int:id>/delete', methods=['POST'], endpoint='api_delete_variation')
@login_required
def api_delete_variation(id):
    """Удаление модификации"""
    try:
        db = get_db()
        result = db.delete_variation(id, session['user_id'])
        return jsonify(result)

    except Exception as e:
        logger.error(f'Ошибка удаления модификации: {e}')
        return jsonify({'success': False, 'message': str(e)})

# ============ МАРШРУТЫ ДЛЯ ИЗОБРАЖЕНИЙ ============

@nomenclatures_bp.route('/api/nomenclatures/<int:id>/upload-image', methods=['POST'], endpoint='upload_nomenclature_image')
@limiter.limit("10 per minute")
@login_required
def upload_nomenclature_image(id):
    """Загрузка изображения для номенклатуры"""
    try:
        from flask import current_app
        db = get_db()

        # Проверяем существование номенклатуры
        nomenclature = db.get_nomenclature_by_id(id)
        if not nomenclature:
            return jsonify({'success': False, 'error': 'Номенклатура не найдена'}), 404

        # Проверяем наличие файла
        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'Файл не загружен'}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({'success': False, 'error': 'Файл не выбран'}), 400

        if not allowed_file(file.filename):
            return jsonify({'success': False, 'error': 'Недопустимый тип файла'}), 400

        # Создаем директорию для загрузок если её нет
        upload_folder = os.path.join(current_app.config.get('UPLOAD_FOLDER', 'static/uploads'), 'nomenclatures', str(id))
        os.makedirs(upload_folder, exist_ok=True)

        # Безопасное имя файла - используем secure_filename
        filename = secure_filename(file.filename)  # <--- ЗДЕСЬ ИСПОЛЬЗУЕТСЯ

        # Добавляем timestamp для уникальности
        name, ext = os.path.splitext(filename)
        timestamp = str(int(time.time()))
        new_filename = f"{name}_{timestamp}{ext}"
        thumbnail_filename = f"{name}_{timestamp}_thumb.jpg"

        # Полные пути
        file_path = os.path.join(upload_folder, new_filename)
        thumbnail_path = os.path.join(upload_folder, thumbnail_filename)

        # Сохраняем оригинал
        file.save(file_path)

        # Проверяем, что загруженный файл действительно является изображением (MIME-верификация)
        try:
            with Image.open(file_path) as img:
                img.verify()
        except Exception:
            os.remove(file_path)
            return jsonify({'success': False, 'error': 'Файл не является допустимым изображением'}), 400

        # Создаем миниатюру
        create_thumbnail(file_path, thumbnail_path, (200, 200))

        # Относительные пути для веба
        web_path = f"/static/uploads/nomenclatures/{id}/{new_filename}"
        thumb_web_path = f"/static/uploads/nomenclatures/{id}/{thumbnail_filename}"

        # Получаем параметр is_primary
        is_primary = request.form.get('is_primary', 'false').lower() == 'true'

        # Если это основное изображение, сбрасываем флаг у других
        if is_primary:
            db.execute_query(
                "UPDATE nomenclature_images SET is_primary = 0 WHERE nomenclature_id = ?",
                (id,)
            )

        # Сохраняем в БД
        img_row = db.execute_query("""
            INSERT INTO nomenclature_images
            (nomenclature_id, filename, original_filename, file_path, file_size, mime_type, is_primary, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING id
        """, (
            id,
            new_filename,
            file.filename,
            web_path,
            os.path.getsize(file_path),
            file.mimetype,
            1 if is_primary else 0,
            session['user_id']
        ), fetch_all=False)

        image_id = img_row['id'] if img_row else None

        # Очищаем кэш миниатюр для этой номенклатуры
        _cache_del(f"thumb_{id}")

        return jsonify({
            'success': True,
            'image_id': image_id,
            'url': web_path,
            'thumbnail': thumb_web_path,
            'message': 'Изображение загружено'
        })

    except Exception as e:
        logger.error(f'Ошибка загрузки изображения: {e}')
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500

@nomenclatures_bp.route('/api/nomenclatures/<int:id>/images', endpoint='get_nomenclature_images')
@login_required
def get_nomenclature_images(id):
    """Получение всех изображений номенклатуры"""
    try:
        db = get_db()

        images = db.execute_query("""
            SELECT * FROM nomenclature_images
            WHERE nomenclature_id = ?
            ORDER BY is_primary DESC, sort_order, created_at DESC
        """, (id,), fetch_all=True)

        result = []
        for img in images or []:
            img_dict = dict(img)
            # Добавляем полные URL
            img_dict['full_url'] = img_dict['file_path']
            # Генерируем путь к миниатюре
            path_parts = img_dict['file_path'].rsplit('.', 1)
            img_dict['thumbnail_url'] = f"{path_parts[0]}_thumb.jpg" if len(path_parts) > 1 else img_dict['file_path']
            result.append(img_dict)

        return jsonify(result)

    except Exception as e:
        logger.error(f'Ошибка получения изображений: {e}')
        return jsonify([])

@nomenclatures_bp.route('/api/nomenclatures/images/<int:image_id>/delete', methods=['POST'], endpoint='delete_nomenclature_image')
@login_required
def delete_nomenclature_image(image_id):
    """Удаление изображения"""
    try:
        from flask import current_app
        db = get_db()

        # Получаем информацию об изображении
        image = db.execute_query(
            "SELECT * FROM nomenclature_images WHERE id = ?",
            (image_id,), fetch_all=False
        )

        if not image:
            return jsonify({'success': False, 'error': 'Изображение не найдено'}), 404

        # Удаляем файлы
        base_path = current_app.config.get('UPLOAD_FOLDER', 'static/uploads')

        # Оригинал
        original_path = os.path.join(
            base_path,
            'nomenclatures',
            str(image['nomenclature_id']),
            image['filename']
        )
        if os.path.exists(original_path):
            os.remove(original_path)

        # Миниатюра
        name, ext = os.path.splitext(image['filename'])
        thumbnail_path = os.path.join(
            base_path,
            'nomenclatures',
            str(image['nomenclature_id']),
            f"{name}_thumb.jpg"
        )
        if os.path.exists(thumbnail_path):
            os.remove(thumbnail_path)

        # Удаляем из БД
        db.execute_query("DELETE FROM nomenclature_images WHERE id = ?", (image_id,))

        return jsonify({'success': True, 'message': 'Изображение удалено'})

    except Exception as e:
        logger.error(f'Ошибка удаления изображения: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500

@nomenclatures_bp.route('/api/nomenclatures/images/<int:image_id>/set-primary', methods=['POST'], endpoint='set_primary_image')
@login_required
def set_primary_image(image_id):
    """Установка основного изображения"""
    try:
        db = get_db()

        # Получаем информацию об изображении
        image = db.execute_query(
            "SELECT nomenclature_id FROM nomenclature_images WHERE id = ?",
            (image_id,), fetch_all=False
        )

        if not image:
            return jsonify({'success': False, 'error': 'Изображение не найдено'}), 404

        # Сбрасываем флаг у всех изображений этой номенклатуры
        db.execute_query(
            "UPDATE nomenclature_images SET is_primary = 0 WHERE nomenclature_id = ?",
            (image['nomenclature_id'],)
        )

        # Устанавливаем новое основное
        db.execute_query(
            "UPDATE nomenclature_images SET is_primary = 1 WHERE id = ?",
            (image_id,)
        )

        return jsonify({'success': True, 'message': 'Основное изображение обновлено'})

    except Exception as e:
        logger.error(f'Ошибка установки основного изображения: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500

# ============ НОМЕНКЛАТУРА ============

@nomenclatures_bp.route('/nomenclatures', endpoint='nomenclatures_list')
@login_required
def nomenclatures_list():
    """Список номенклатуры с поиском"""
    try:
        db = get_db()

        # Параметры пагинации
        page = request.args.get('page', 1, type=int)
        per_page = 50
        if page < 1:
            page = 1

        # Получаем параметры поиска
        search_query = request.args.get('search', '').strip()
        category_id = request.args.get('category')
        accounting_type = request.args.get('accounting_type')

        # Базовая WHERE-часть (общая для COUNT и SELECT)
        joins = """
            FROM nomenclatures n
            LEFT JOIN categories c ON n.category_id = c.id
        """
        where = " WHERE 1=1"
        params = []

        # Добавляем фильтр для мягкого удаления, если есть колонка is_deleted
        has_deleted = db.column_exists('nomenclatures', 'is_deleted')
        if has_deleted:
            where += " AND (n.is_deleted IS NULL OR n.is_deleted = 0)"

        if search_query:
            where += build_where(
                ['LOWER(n.name)', 'LOWER(n.sku)', 'LOWER(n.model)', 'LOWER(n.manufacturer)'],
                search_query, params
            )

        if category_id and category_id.isdigit():
            where += " AND n.category_id = ?"
            params.append(int(category_id))

        if accounting_type:
            where += " AND n.accounting_type = ?"
            params.append(accounting_type)

        # Счётчики по типам учёта (без LIMIT)
        counts_rows = db.execute_query(
            f"SELECT n.accounting_type, COUNT(*) as cnt {joins}{where} GROUP BY n.accounting_type",
            params, fetch_all=True
        )
        counts = {'all': 0, 'individual': 0, 'batch': 0, 'quantitative': 0, 'kit': 0}
        for row in (counts_rows or []):
            r = dict(row)
            t = r.get('accounting_type', '')
            cnt = r.get('cnt', 0)
            counts['all'] += cnt
            if t in counts:
                counts[t] = cnt

        # Пагинация
        total = counts['all']
        pages = max(1, (total + per_page - 1) // per_page)
        if page > pages:
            page = pages
        offset = (page - 1) * per_page

        # Основной запрос
        query = f"""
            SELECT n.*, c.name_ru as category_name,
                   (SELECT COUNT(*) FROM nomenclature_images WHERE nomenclature_id = n.id) as images_count,
                   (SELECT COUNT(*) FROM instances WHERE nomenclature_id = n.id) as instances_count,
                   (SELECT COUNT(*) FROM batches WHERE nomenclature_id = n.id) as batches_count,
                   (SELECT COALESCE(SUM(quantity),0) FROM stocks WHERE nomenclature_id = n.id) as total_stock,
                   (SELECT COUNT(*) FROM kit_specifications WHERE kit_nomenclature_id = n.id) as components_count,
                   (SELECT COUNT(*) FROM nomenclature_variations WHERE nomenclature_id = n.id) as has_variations
            {joins}{where}
            ORDER BY n.name
            LIMIT ? OFFSET ?
        """
        nomenclatures = db.execute_query(query, params + [per_page, offset], fetch_all=True)

        # Преобразуем Row объекты в словари
        nomenclatures_list = [dict(row) for row in nomenclatures] if nomenclatures else []

        # Получаем все категории для фильтра
        categories = db.get_all_categories()

        # Определяем активную вкладку
        active_tab = accounting_type if accounting_type else 'all'

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

        return render_template('nomenclatures/list.html',
                             nomenclatures=nomenclatures_list,
                             categories=categories,
                             counts=counts,
                             active_tab=active_tab,
                             search=search_query,
                             category_id=category_id,
                             pagination=pagination)
    except Exception as e:
        logger.error(f"Ошибка загрузки номенклатуры: {e}")
        import traceback
        traceback.print_exc()
        flash('Ошибка загрузки номенклатуры', 'error')
        return redirect(url_for('dashboard'))

@nomenclatures_bp.route('/nomenclatures/<int:id>', endpoint='view_nomenclature')
@login_required
def view_nomenclature(id):
    """Просмотр детальной информации о номенклатуре"""
    try:
        db = get_db()
        nomenclature = db.get_nomenclature_by_id(id)

        if not nomenclature:
            flash('Номенклатура не найдена', 'error')
            return redirect(url_for('nomenclatures.nomenclatures_list'))

        # Получаем связанные данные в зависимости от типа учета
        if nomenclature['accounting_type'] == 'individual':
            instances = db.execute_query("""
                SELECT i.*, l.name as location_name, e.full_name as employee_name
                FROM instances i
                LEFT JOIN locations l ON i.location_id = l.id
                LEFT JOIN employees e ON i.employee_id = e.id
                WHERE i.nomenclature_id = ?
                ORDER BY i.created_at DESC
            """, (id,), fetch_all=True)
            nomenclature['instances'] = [dict(i) for i in instances] if instances else []

        elif nomenclature['accounting_type'] == 'batch':
            batches = db.execute_query("""
                SELECT b.*,
                       (SELECT SUM(quantity) FROM stocks WHERE batch_id = b.id) as total_quantity
                FROM batches b
                WHERE b.nomenclature_id = ?
                ORDER BY b.expiry_date ASC
            """, (id,), fetch_all=True)
            nomenclature['batches'] = [dict(b) for b in batches] if batches else []

        elif nomenclature['accounting_type'] == 'quantitative':
            stocks = db.execute_query("""
                SELECT s.*, w.name as warehouse_name, sb.code as bin_code
                FROM stocks s
                LEFT JOIN warehouses w ON s.warehouse_id = w.id
                LEFT JOIN storage_bins sb ON s.storage_bin_id = sb.id
                WHERE s.nomenclature_id = ?
                ORDER BY w.name
            """, (id,), fetch_all=True)
            nomenclature['stocks'] = [dict(s) for s in stocks] if stocks else []

        elif nomenclature['accounting_type'] == 'kit':
            components = db.execute_query("""
                SELECT ks.*, n.name as component_name, n.sku, n.unit
                FROM kit_specifications ks
                JOIN nomenclatures n ON ks.component_nomenclature_id = n.id
                WHERE ks.kit_nomenclature_id = ?
                ORDER BY n.name
            """, (id,), fetch_all=True)
            nomenclature['components'] = [dict(c) for c in components] if components else []

        return render_template('nomenclatures/view.html', nomenclature=nomenclature)

    except Exception as e:
        logger.debug(f"Ошибка просмотра номенклатуры: {e}")
        import traceback
        traceback.print_exc()
        flash('Ошибка просмотра номенклатуры', 'error')
        return redirect(url_for('nomenclatures.nomenclatures_list'))

@nomenclatures_bp.route('/nomenclatures/add', methods=['GET', 'POST'], endpoint='add_nomenclature')
@login_required
def add_nomenclature():
    """Создание новой позиции номенклатуры"""
    db = get_db()

    if request.method == 'POST':
        try:
            data = {
                'sku': request.form.get('sku'),
                'barcode': request.form.get('barcode'),
                'name': request.form.get('name'),
                'description': request.form.get('description'),
                'category_id': request.form.get('category_id'),
                'manufacturer': request.form.get('manufacturer'),
                'model': request.form.get('model'),
                'brand': request.form.get('brand'),
                'unit': request.form.get('unit', 'шт.'),
                'min_stock': request.form.get('min_stock', 0),
                'accounting_type': request.form.get('accounting_type'),
                'reorder_point': request.form.get('reorder_point', 0),
                'shelf_life_days': request.form.get('shelf_life_days'),
                'has_serial_numbers': 'has_serial_numbers' in request.form,
                'has_expiry_dates': 'has_expiry_dates' in request.form,
                'requires_calibration': 'requires_calibration' in request.form,
                'requires_maintenance': 'requires_maintenance' in request.form,
                'is_active': 'is_active' in request.form
            }

            result = db.create_nomenclature(data, session['user_id'])

            if result['success']:
                flash(result['message'], 'success')
                return redirect(url_for('nomenclatures.nomenclatures_list'))
            else:
                flash(result['message'], 'error')

        except Exception as e:
            logger.error(f'Ошибка создания номенклатуры: {e}')
            flash('Ошибка создания номенклатуры', 'error')

    # ПОЛУЧАЕМ КАТЕГОРИИ - ИСПРАВЛЕННАЯ ВЕРСИЯ
    try:
        # Пробуем получить дерево категорий
        categories_tree = db.get_category_tree()
        
        # Если дерево пустое, получаем простой список
        if not categories_tree:
            # Получаем все категории простым запросом
            all_categories = db.execute_query("""
                SELECT id, name_ru as name, parent_id, level 
                FROM categories 
                WHERE is_active = 1 
                ORDER BY lft
            """, fetch_all=True)
            
            categories_list = []
            if all_categories:
                for cat in all_categories:
                    cat_dict = dict(cat)
                    categories_list.append(cat_dict)
                categories_tree = categories_list
            else:
                categories_tree = []
                
    except Exception as e:
        logger.error(f"Ошибка получения категорий: {e}")
        traceback.print_exc()
        categories_tree = []
        flash('Ошибка загрузки категорий', 'warning')

    return render_template('nomenclatures/form.html',
                         title='Новая позиция',
                         nomenclature=None,
                         categories_tree=categories_tree,
                         units=UNITS)

@nomenclatures_bp.route('/nomenclatures/<int:id>/edit', methods=['GET', 'POST'], endpoint='edit_nomenclature')
@login_required
def edit_nomenclature(id):
    """Редактирование номенклатуры"""
    db = get_db()

    if request.method == 'POST':
        try:
            data = {
                'sku': request.form.get('sku'),
                'barcode': request.form.get('barcode'),
                'name': request.form.get('name'),
                'description': request.form.get('description'),
                'category_id': request.form.get('category_id'),
                'manufacturer': request.form.get('manufacturer'),
                'model': request.form.get('model'),
                'brand': request.form.get('brand'),
                'unit': request.form.get('unit', 'шт.'),
                'min_stock': request.form.get('min_stock', 0),
                'accounting_type': request.form.get('accounting_type'),
                'reorder_point': request.form.get('reorder_point', 0),
                'shelf_life_days': request.form.get('shelf_life_days'),
                'has_serial_numbers': 'has_serial_numbers' in request.form,
                'has_expiry_dates': 'has_expiry_dates' in request.form,
                'requires_calibration': 'requires_calibration' in request.form,
                'requires_maintenance': 'requires_maintenance' in request.form,
                'is_active': 'is_active' in request.form
            }

            result = db.update_nomenclature(id, data, session['user_id'])

            if result['success']:
                flash(result['message'], 'success')
                return redirect(url_for('nomenclatures.nomenclatures_list'))
            else:
                flash(result['message'], 'error')

        except Exception as e:
            logger.error(f'Ошибка обновления номенклатуры: {e}')
            flash('Ошибка обновления номенклатуры', 'error')

    nomenclature = db.get_nomenclature_by_id(id)
    if not nomenclature:
        flash('Позиция не найдена', 'error')
        return redirect(url_for('nomenclatures.nomenclatures_list'))

    # ✅ ИЗМЕНЕНО: теперь используем get_category_tree()
    categories_tree = db.get_category_tree()

    return render_template('nomenclatures/form.html',
                         title='Редактирование позиции',
                         nomenclature=nomenclature,
                         categories_tree=categories_tree,
                         units=UNITS)

@nomenclatures_bp.route('/nomenclatures/<int:id>/delete', methods=['POST'], endpoint='delete_nomenclature')
@login_required
def delete_nomenclature(id):
    """Удаление номенклатуры"""
    try:
        db = get_db()

        # Проверяем, есть ли связанные экземпляры
        instances = db.execute_query(
            "SELECT COUNT(*) as cnt FROM instances WHERE nomenclature_id = ?",
            (id,),
            fetch_all=False
        )

        if instances and instances['cnt'] > 0:
            flash('Нельзя удалить позицию, у которой есть экземпляры', 'error')
            return redirect(url_for('nomenclatures.nomenclatures_list'))

        # Проверяем, есть ли остатки на складах
        stocks = db.execute_query(
            "SELECT COUNT(*) as cnt FROM stocks WHERE nomenclature_id = ?",
            (id,),
            fetch_all=False
        )

        if stocks and stocks['cnt'] > 0:
            flash('Нельзя удалить позицию, у которой есть остатки на складах', 'error')
            return redirect(url_for('nomenclatures.nomenclatures_list'))

        # Проверяем, есть ли связанные партии
        batches = db.execute_query(
            "SELECT COUNT(*) as cnt FROM batches WHERE nomenclature_id = ?",
            (id,),
            fetch_all=False
        )

        if batches and batches['cnt'] > 0:
            flash('Нельзя удалить позицию, у которой есть партии', 'error')
            return redirect(url_for('nomenclatures.nomenclatures_list'))

        # Проверяем, есть ли связанные документы
        documents = db.execute_query("""
            SELECT COUNT(*) as cnt FROM document_items WHERE nomenclature_id = ?
        """, (id,), fetch_all=False)

        if documents and documents['cnt'] > 0:
            flash('Нельзя удалить позицию, по которой есть движения в документах', 'error')
            return redirect(url_for('nomenclatures.nomenclatures_list'))

        # Проверяем, есть ли колонка is_deleted
        has_deleted = db.column_exists('nomenclatures', 'is_deleted')

        if has_deleted:
            # Мягкое удаление
            db.execute_query(
                "UPDATE nomenclatures SET is_deleted = 1 WHERE id = ?",
                (id,)
            )
        else:
            # Физическое удаление
            db.execute_query("DELETE FROM nomenclatures WHERE id = ?", (id,))

        flash('Позиция удалена', 'success')

        # Логируем действие
        db.log_user_action(
            user_id=session['user_id'],
            action='delete',
            entity_type='nomenclature',
            entity_id=id,
            details=f'Удалена номенклатура ID {id}'
        )

    except Exception as e:
        logger.debug(f"Ошибка удаления номенклатуры: {e}")
        import traceback
        traceback.print_exc()
        flash(f'Ошибка удаления номенклатуры: {str(e)}', 'error')

    return redirect(url_for('nomenclatures.nomenclatures_list'))

# ============ API ДЛЯ НОМЕНКЛАТУРЫ ============

@nomenclatures_bp.route('/api/nomenclatures/<int:id>', endpoint='api_get_nomenclature')
@login_required
def api_get_nomenclature(id):
    """API для получения информации о номенклатуре"""
    try:
        db = get_db()
        logger.debug(f"🔍 API запрос номенклатуры ID: {id}")

        nomenclature = db.get_nomenclature_by_id(id)

        if not nomenclature:
            logger.error(f"❌ Номенклатура с ID {id} не найдена")
            return jsonify({'error': 'Номенклатура не найдена'}), 404

        # Возвращаем только нужные поля
        result = {
            'id': nomenclature['id'],
            'name': nomenclature['name'],
            'sku': nomenclature['sku'],
            'unit': nomenclature.get('unit', 'шт.'),
            'accounting_type': nomenclature.get('accounting_type'),
            'category_id': nomenclature.get('category_id'),
            'category_name': nomenclature.get('category_name'),
            'has_serial_numbers': nomenclature.get('has_serial_numbers', 0),
            'has_expiry_dates': nomenclature.get('has_expiry_dates', 0),
            'requires_calibration': nomenclature.get('requires_calibration', 0),
            'requires_maintenance': nomenclature.get('requires_maintenance', 0)
        }

        logger.info(f"✅ Отправляем данные: {result}")
        return jsonify(result)

    except Exception as e:
        logger.error(f"❌ Ошибка API: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@nomenclatures_bp.route('/api/nomenclatures/quick-create', methods=['POST'], endpoint='api_quick_create_nomenclature')
@login_required
@limiter.limit("20 per minute")
def api_quick_create_nomenclature():
    """Быстрое создание номенклатуры с возможностью создания модификации"""
    try:
        db = get_db()

        # Валидация через Marshmallow
        data, err = validate_json(QuickCreateNomenclatureSchema)
        if err:
            return err

        name = data['name']
        category_id = data['category_id'] or 1
        accounting_type = data['accounting_type']

        # Генерация SKU
        import time
        import re
        words = re.sub(r'[^\w\s]', ' ', name).split()
        sku_parts = [word[:3].upper() for word in words[:3] if word]
        if not sku_parts:
            sku_parts = ['NOM']

        base_sku = '-'.join(sku_parts)
        timestamp = str(int(time.time()))[-6:]
        sku = f"{base_sku}-{timestamp}"

        # Проверка уникальности
        existing = db.execute_query(
            "SELECT id FROM nomenclatures WHERE sku = ?",
            (sku,), fetch_all=False
        )
        if existing:
            import random
            sku = f"{base_sku}-{timestamp}{random.randint(10, 99)}"

        # Создание номенклатуры
        nomenclature_data = {
            'name': name,
            'sku': sku,
            'category_id': category_id,
            'unit': data['unit'],
            'accounting_type': accounting_type,
            'description': data['description'],
            'is_active': 1
        }

        result = db.create_nomenclature(nomenclature_data, session['user_id'])

        if result.get('success'):
            new_id = result.get('nomenclature_id') or result.get('id')

            # ========== СОЗДАНИЕ МОДИФИКАЦИИ, ЕСЛИ УКАЗАНЫ РАЗМЕР ИЛИ ЦВЕТ ==========
            variation_id = None
            if data['create_variation'] and (data.get('variation_size') or data.get('variation_color')):
                size = data.get('variation_size')
                color = data.get('variation_color')

                # Генерируем SKU для модификации
                var_sku = f"{sku}"
                if size:
                    size_clean = ''.join(c for c in size if c.isalnum())
                    var_sku += f"-{size_clean}"
                if color:
                    var_sku += f"-{color[:3].upper()}"

                # Создаем модификацию
                var_result = db.create_variation(new_id, {
                    'sku': var_sku,
                    'size': size,
                    'color': color,
                    'is_active': 1
                }, session['user_id'])

                if var_result.get('success'):
                    variation_id = var_result.get('id')

            # Получаем полную информацию
            new_nomenclature = db.get_nomenclature_by_id(new_id)

            return jsonify({
                'success': True,
                'id': new_id,
                'name': name,
                'sku': sku,
                'category_name': new_nomenclature.get('category_name', '') if new_nomenclature else '',
                'unit': nomenclature_data['unit'],
                'accounting_type': accounting_type,
                'variation_id': variation_id  # Возвращаем ID созданной модификации
            })
        else:
            return jsonify({'success': False, 'error': result.get('message', 'Ошибка создания')})

    except Exception as e:
        logger.error(f'Ошибка быстрого создания номенклатуры: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500

@nomenclatures_bp.route('/api/nomenclatures/<int:id>/available-instances', endpoint='api_get_available_instances')
@login_required
def api_nomenclature_available_instances(id):
    """Получение доступных экземпляров для выдачи"""
    try:
        warehouse_id = request.args.get('warehouse_id')
        if not warehouse_id:
            return jsonify({'error': 'Не указан склад'}), 400

        db = get_db()
        instances = db.execute_query("""
            SELECT i.id, i.inventory_number, i.serial_number, i.status, i.condition,
                   l.name as location_name, w.name as warehouse_name
            FROM instances i
            LEFT JOIN locations l ON i.location_id = l.id
            LEFT JOIN warehouses w ON i.warehouse_id = w.id
            WHERE i.nomenclature_id = ?
                AND i.warehouse_id = ?
                AND i.status IN ('in_stock', 'available')
            ORDER BY i.inventory_number
        """, (id, warehouse_id), fetch_all=True)

        result = []
        for inst in instances or []:
            inst_dict = dict(inst)
            # Добавляем отформатированное название для отображения
            inst_dict['display_name'] = f"{inst_dict['inventory_number']} {inst_dict['serial_number'] or ''}"
            result.append(inst_dict)

        return jsonify(result)
    except Exception as e:
        logger.error(f'Ошибка получения экземпляров: {e}')
        return jsonify([])

# для отображения типа учета
@nomenclatures_bp.route('/api/categories/<int:category_id>/accounting-type', endpoint='api_get_category_accounting_type')
@login_required
def api_category_accounting_type(category_id):
    """Получение типа учета категории"""
    try:
        db = get_db()

        # Получаем категорию
        category = db.execute_query(
            "SELECT id, name_ru as name, accounting_type FROM categories WHERE id = ?",
            (category_id,), fetch_all=False
        )

        if not category:
            return jsonify({'error': 'Категория не найдена'}), 404

        # Название типа учета для отображения
        type_names = {
            'individual': 'Индивидуальный',
            'batch': 'Партионный',
            'quantitative': 'Количественный'
        }

        return jsonify({
            'accounting_type': category['accounting_type'] or 'individual',
            'accounting_type_name': type_names.get(category['accounting_type'], 'Индивидуальный')
        })

    except Exception as e:
        logger.error(f'Ошибка получения типа учета категории: {e}')
        return jsonify({'error': str(e)}), 500

@nomenclatures_bp.route('/api/categories/search', endpoint='api_categories_search')
@login_required
def api_categories_search():
    """Поиск категорий по названию (без учета регистра)"""
    try:
        query = request.args.get('q', '').strip()
        if len(query) < 2:
            return jsonify([])

        db = get_db()

        # Поиск без учета регистра с помощью LOWER
        categories = db.execute_query("""
            SELECT id, name_ru as name, path as full_path, type as item_type
            FROM categories
            WHERE is_active = 1
                AND (LOWER(name_ru) LIKE LOWER(?) OR LOWER(path) LIKE LOWER(?))
            ORDER BY
                CASE
                    WHEN LOWER(name_ru) = LOWER(?) THEN 1
                    WHEN LOWER(name_ru) LIKE LOWER(?) THEN 2
                    WHEN LOWER(name_ru) LIKE LOWER(?) THEN 3
                    ELSE 4
                END,
                name_ru
            LIMIT 20
        """, (
            f'%{query}%',
            f'%{query}%',
            query,
            f'{query}%',
            f'%{query}%'
        ), fetch_all=True)

        result = []
        for cat in categories or []:
            result.append({
                'id': cat['id'],
                'name': cat['name'],
                'full_path': cat['full_path'] or cat['name'],
                'type': cat['item_type']
            })

        return jsonify(result)

    except Exception as e:
        logger.error(f'Ошибка поиска категорий: {e}')
        return jsonify([])

@nomenclatures_bp.route('/api/nomenclatures/<int:id>/instances', endpoint='api_get_nomenclature_instances')
@login_required
def api_nomenclature_instances(id):
    """Получение доступных экземпляров номенклатуры"""
    try:
        status = request.args.get('status', 'in_stock')
        db = get_db()
        instances = db.execute_query("""
            SELECT id, inventory_number, serial_number
            FROM instances
            WHERE nomenclature_id = ? AND status = ?
            ORDER BY inventory_number
        """, (id, status), fetch_all=True)

        return jsonify([dict(i) for i in instances] if instances else [])
    except Exception as e:
        return jsonify([])

@nomenclatures_bp.route('/api/nomenclatures/<int:id>/batches', endpoint='api_get_nomenclature_batches')
@login_required
def api_nomenclature_batches(id):
    """Получение активных партий номенклатуры с остатками"""
    try:
        db = get_db()
        batches = db.execute_query("""
            SELECT b.id, b.batch_number,
                   (SELECT SUM(quantity) FROM stocks WHERE batch_id = b.id) as quantity
            FROM batches b
            WHERE b.nomenclature_id = ? AND b.is_active = 1
            ORDER BY b.expiry_date ASC
        """, (id,), fetch_all=True)
        return jsonify([dict(b) for b in batches] if batches else [])
    except Exception as e:
        return jsonify([])


@nomenclatures_bp.route('/api/categories/<int:id>', endpoint='api_get_category')
@login_required
def api_get_category(id):
    """Получение информации о категории"""
    try:
        db = get_db()
        category = db.get_category_by_id(id)
        if category:
            return jsonify(category)
        return jsonify({'error': 'Категория не найдена'}), 404
    except Exception as e:
        logger.error(f'Ошибка получения категории: {e}')
        return jsonify({'error': str(e)}), 500

@nomenclatures_bp.route('/api/categories/<int:id>/variation-settings', endpoint='api_get_category_variation_settings')
@login_required
def api_category_variation_settings(id):
    """Получение настроек модификаций для категории"""
    try:
        db = get_db()
        settings = db.execute_query("""
            SELECT * FROM category_variation_settings
            WHERE category_id = ?
            ORDER BY sort_order
        """, (id,), fetch_all=True)

        result = []
        for s in settings or []:
            result.append(dict(s))

        return jsonify(result)
    except Exception as e:
        logger.error(f'Ошибка получения настроек: {e}')
        return jsonify([])

@nomenclatures_bp.route('/api/nomenclatures/search', endpoint='api_nomenclatures_search')
@login_required
@limiter.limit("60 per minute")
def api_nomenclatures_search():
    """API для поиска номенклатуры по тексту"""
    try:
        query = request.args.get('q', '')
        db = get_db()

        logger.debug(f"🔍 Поисковый запрос: '{query}'")

        if len(query) < 2:
            logger.error("❌ Запрос слишком короткий")
            return jsonify([])

        # Проверяем кэш
        _cache_key = query.lower().strip()
        _cached = _search_cache_get(_cache_key)
        if _cached is not None:
            return jsonify(_cached)

        # Умный поиск: регистронезависимый + транслитерация + токены
        sql_params: list = []
        search_cond = build_where(
            ['LOWER(n.name)', 'LOWER(n.sku)'],
            query, sql_params
        )

        sql = f"""
            SELECT n.id, n.sku, n.name, n.unit, n.accounting_type,
                   c.name_ru as category_name,
                   (SELECT COUNT(*) FROM nomenclature_variations WHERE nomenclature_id = n.id) as has_variations
            FROM nomenclatures n
            LEFT JOIN categories c ON n.category_id = c.id
            WHERE n.is_active = 1
                {search_cond}
            ORDER BY n.name
            LIMIT 20
        """

        logger.debug(f"📝 SQL: {sql}")
        logger.debug(f"📊 Параметры: {sql_params}")

        nomenclatures = db.execute_query(sql, sql_params, fetch_all=True)

        logger.info(f"✅ Найдено результатов: {len(nomenclatures) if nomenclatures else 0}")

        result = []
        for n in nomenclatures or []:
            result.append({
                'id': n['id'],
                'name': n['name'],
                'sku': n['sku'],
                'unit': n['unit'] or 'шт',
                'accounting_type': n['accounting_type'] or 'individual',
                'category': n['category_name'] or '',
                'has_variations': n['has_variations'] > 0
            })
            logger.debug(f"  - {n['name']} (ID: {n['id']}, has_variations: {n['has_variations']})")

        _search_cache_set(_cache_key, result)
        return jsonify(result)

    except Exception as e:
        logger.error(f"❌ Ошибка поиска: {e}")
        import traceback
        traceback.print_exc()
        return jsonify([])
