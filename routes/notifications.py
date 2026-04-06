"""
Blueprint: Notifications
Routes: /notifications, /notifications/<id>/read, /notifications/read-all,
        /notifications/<id>/delete, /notifications/clear-all,
        /api/qr/document/<id>, /api/qr/instance/<id>,
        /api/notifications/counts, /debug-notifications
"""
import logging
from math import ceil
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, flash
from extensions import csrf
from routes.common import login_required, admin_required, get_db

logger = logging.getLogger('routes.notifications')

notifications_bp = Blueprint('notifications', __name__)


@notifications_bp.route('/notifications', endpoint='notifications_list')
@login_required
def notifications_list():
    """Список уведомлений"""
    try:
        db = get_db()
        page = request.args.get('page', 1, type=int)
        per_page = 20
        show = request.args.get('show', 'all')
        type_filter = request.args.get('type')
        status_filter = request.args.get('status')
        date_from = request.args.get('date_from')

        # Базовый запрос
        query = """
            SELECT n.*,
                   CASE
                       WHEN n.type = 'expired' THEN 'Просрочено'
                       WHEN n.type = 'expiry' THEN 'Истекает срок'
                       WHEN n.type = 'low_stock' THEN 'Малый остаток'
                       WHEN n.type = 'calibration' THEN 'Поверка'
                       WHEN n.type = 'maintenance' THEN 'Обслуживание'
                       ELSE 'Системное'
                   END as type_name
            FROM notifications n
            WHERE n.user_id = ? OR n.user_id IS NULL
        """
        params = [session['user_id']]

        if show == 'unread':
            query += " AND n.is_read = 0"
        elif show == 'expiring':
            query += " AND n.type IN ('expiry', 'calibration', 'maintenance') AND n.is_read = 0"

        if type_filter:
            query += " AND n.type = ?"
            params.append(type_filter)

        if status_filter == 'unread':
            query += " AND n.is_read = 0"
        elif status_filter == 'read':
            query += " AND n.is_read = 1"

        if date_from:
            query += " AND DATE(n.created_at) >= ?"
            params.append(date_from)

        query += " ORDER BY n.created_at DESC"

        # Получаем уведомления с пагинацией
        notifications = db.execute_query(query, params, fetch_all=True)

        # Статистика
        stats = {
            'total': len(notifications) if notifications else 0,
            'unread': sum(1 for n in notifications if not n['is_read']) if notifications else 0,
            'expiring': sum(1 for n in notifications if n['type'] in ['expiry', 'calibration', 'maintenance'] and not n['is_read']) if notifications else 0,
            'expired': sum(1 for n in notifications if n['type'] == 'expired' and not n['is_read']) if notifications else 0,
            'low_stock': sum(1 for n in notifications if n['type'] == 'low_stock' and not n['is_read']) if notifications else 0
        }

        # Пагинация
        total = len(notifications) if notifications else 0
        pages = ceil(total / per_page)
        start = (page - 1) * per_page
        end = start + per_page

        paginated_notifications = notifications[start:end] if notifications else []

        pagination = {
            'page': page,
            'pages': pages,
            'total': total,
            'has_prev': page > 1,
            'has_next': page < pages,
            'prev_num': page - 1,
            'next_num': page + 1
        }

        return render_template('notifications/list.html',
                             notifications=[dict(n) for n in paginated_notifications] if paginated_notifications else [],
                             stats=stats,
                             pagination=pagination)

    except Exception as e:
        logger.error(f'Ошибка загрузки уведомлений: {e}')
        flash('Ошибка загрузки уведомлений', 'error')
        return redirect(url_for('dashboard'))


@notifications_bp.route('/notifications/<int:id>/read', methods=['POST'], endpoint='mark_notification_read')
@login_required
def mark_notification_read(id):
    """Отметить уведомление как прочитанное"""
    try:
        db = get_db()

        db.execute_query("""
            UPDATE notifications
            SET is_read = 1, read_at = CURRENT_TIMESTAMP
            WHERE id = ? AND (user_id = ? OR user_id IS NULL)
        """, (id, session['user_id']))

        return jsonify({'success': True})

    except Exception as e:
        logger.error(f'Ошибка отметки уведомления: {e}')
        return jsonify({'success': False, 'error': str(e)})


@notifications_bp.route('/notifications/read-all', methods=['POST'], endpoint='mark_all_read')
@login_required
def mark_all_read():
    """Отметить все уведомления как прочитанные"""
    try:
        db = get_db()

        db.execute_query("""
            UPDATE notifications
            SET is_read = 1, read_at = CURRENT_TIMESTAMP
            WHERE (user_id = ? OR user_id IS NULL) AND is_read = 0
        """, (session['user_id'],))

        return jsonify({'success': True})

    except Exception as e:
        logger.error(f'Ошибка отметки всех уведомлений: {e}')
        return jsonify({'success': False, 'error': str(e)})


@notifications_bp.route('/notifications/<int:id>/delete', methods=['POST'], endpoint='delete_notification')
@login_required
def delete_notification(id):
    """Удалить уведомление"""
    try:
        db = get_db()

        db.execute_query("""
            DELETE FROM notifications
            WHERE id = ? AND (user_id = ? OR user_id IS NULL)
        """, (id, session['user_id']))

        return jsonify({'success': True})

    except Exception as e:
        logger.error(f'Ошибка удаления уведомления: {e}')
        return jsonify({'success': False, 'error': str(e)})


@notifications_bp.route('/notifications/clear-all', methods=['POST'], endpoint='clear_all_notifications')
@login_required
def clear_all_notifications():
    """Очистить все уведомления"""
    try:
        db = get_db()

        db.execute_query("""
            DELETE FROM notifications
            WHERE user_id = ? OR user_id IS NULL
        """, (session['user_id'],))

        return jsonify({'success': True})

    except Exception as e:
        logger.error(f'Ошибка очистки уведомлений: {e}')
        return jsonify({'success': False, 'error': str(e)})


@notifications_bp.route('/api/qr/document/<int:id>', endpoint='api_qr_document')
@login_required
def api_qr_document(id):
    """Генерирует QR-код для документа (PNG base64)"""
    try:
        import qrcode
        import io
        import base64
        db = get_db()
        doc = db.execute_query("SELECT document_number FROM documents WHERE id = ?", (id,), fetch_all=False)
        if not doc:
            return jsonify({'error': 'not found'}), 404
        data = f"DOC:{doc['document_number']} ID:{id}"
        qr = qrcode.QRCode(version=1, box_size=4, border=2)
        qr.add_data(data)
        qr.make(fit=True)
        img = qr.make_image(fill_color='black', back_color='white')
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        b64 = base64.b64encode(buf.getvalue()).decode('utf-8')
        return jsonify({'qr': f'data:image/png;base64,{b64}', 'data': data})
    except ImportError:
        return jsonify({'error': 'qrcode library not installed'}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@notifications_bp.route('/api/qr/instance/<int:id>', endpoint='api_qr_instance')
@login_required
def api_qr_instance(id):
    """Генерирует QR-код для экземпляра (PNG base64)"""
    try:
        import qrcode
        import io
        import base64
        db = get_db()
        inst = db.execute_query(
            "SELECT inventory_number, serial_number FROM instances WHERE id = ?",
            (id,), fetch_all=False
        )
        if not inst:
            return jsonify({'error': 'not found'}), 404
        num = inst['inventory_number'] or inst['serial_number'] or str(id)
        data = f"INST:{num}"
        qr = qrcode.QRCode(version=1, box_size=4, border=2)
        qr.add_data(data)
        qr.make(fit=True)
        img = qr.make_image(fill_color='black', back_color='white')
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        b64 = base64.b64encode(buf.getvalue()).decode('utf-8')
        return jsonify({'qr': f'data:image/png;base64,{b64}', 'data': data})
    except ImportError:
        return jsonify({'error': 'qrcode library not installed'}), 503
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@notifications_bp.route('/api/notifications/counts', endpoint='api_notification_counts')
@login_required
def api_notification_counts():
    """API для получения количества уведомлений"""
    try:
        db = get_db()

        counts = db.execute_query("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN is_read = 0 THEN 1 ELSE 0 END) as unread,
                SUM(CASE WHEN type IN ('expiry', 'calibration', 'maintenance') AND is_read = 0 THEN 1 ELSE 0 END) as expiring,
                SUM(CASE WHEN type = 'expired' AND is_read = 0 THEN 1 ELSE 0 END) as expired,
                SUM(CASE WHEN type = 'low_stock' AND is_read = 0 THEN 1 ELSE 0 END) as low_stock
            FROM notifications
            WHERE user_id = ? OR user_id IS NULL
        """, (session['user_id'],), fetch_all=False)

        return jsonify({
            'total': counts['total'] or 0,
            'unread': counts['unread'] or 0,
            'expiring': counts['expiring'] or 0,
            'expired': counts['expired'] or 0,
            'low_stock': counts['low_stock'] or 0
        })

    except Exception as e:
        logger.error(f'Ошибка получения счетчиков: {e}')
        return jsonify({'error': str(e)}), 500


@notifications_bp.route('/debug-notifications', endpoint='debug_notifications')
@admin_required
def debug_notifications():
    """Отладка - просмотр всех уведомлений"""
    from flask import current_app
    if not current_app.debug:
        from flask import jsonify
        return jsonify({'error': 'Debug-only endpoint'}), 403
    try:
        db = get_db()
        notifications = db.execute_query("""
            SELECT * FROM notifications ORDER BY created_at DESC LIMIT 20
        """, fetch_all=True)

        result = "<h1>Уведомления в БД:</h1><pre>"
        for n in notifications:
            result += f"\nID: {n['id']}, Тип: {n['type']}, Заголовок: {n['title']}, Прочитано: {n['is_read']}"
        result += "</pre>"

        return result
    except Exception as e:
        return f"Ошибка: {e}"
