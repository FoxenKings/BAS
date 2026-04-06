"""
Blueprint: logs
История изменений (user_logs).
"""
import json
import logging
from collections import defaultdict
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, flash
from routes.common import login_required, get_db
from utils.search import build_where

logger = logging.getLogger('routes.logs')

logs_bp = Blueprint('logs', __name__)

# ============ ИСТОРИЯ ИЗМЕНЕНИЙ ============

@logs_bp.route('/logs', endpoint='logs_list')
@login_required
def logs_list():
    """Общая история изменений"""
    try:
        db = get_db()
        
        # Параметры фильтрации
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        user_id = request.args.get('user_id')
        action = request.args.get('action')
        entity_type = request.args.get('entity_type')
        entity_id = request.args.get('entity_id')
        search = request.args.get('search')
        
        # Базовый запрос
        query = """
            SELECT l.*, u.username 
            FROM user_logs l
            LEFT JOIN users u ON l.user_id = u.id
            WHERE 1=1
        """
        params = []
        
        if date_from:
            query += " AND DATE(l.created_at) >= ?"
            params.append(date_from)
        
        if date_to:
            query += " AND DATE(l.created_at) <= ?"
            params.append(date_to)
        
        if user_id:
            query += " AND l.user_id = ?"
            params.append(user_id)
        
        if action:
            query += " AND l.action = ?"
            params.append(action)
        
        if entity_type:
            query += " AND l.entity_type = ?"
            params.append(entity_type)
        
        if entity_id:
            query += " AND l.entity_id = ?"
            params.append(entity_id)
        
        if search:
            query += build_where(
                ['LOWER(l.details)', 'LOWER(l.old_value)', 'LOWER(l.new_value)'],
                search, params
            )
        
        query += " ORDER BY l.created_at DESC LIMIT 1000"
        
        logs = db.execute_query(query, params, fetch_all=True)
        
        # Статистика
        stats = {
            'total': len(logs) if logs else 0,
            'create': sum(1 for l in logs if l['action'] == 'create') if logs else 0,
            'update': sum(1 for l in logs if l['action'] == 'update') if logs else 0,
            'delete': sum(1 for l in logs if l['action'] == 'delete') if logs else 0
        }
        
        # Данные для графика активности (последние 30 дней)
        from collections import defaultdict
        daily_stats = defaultdict(int)
        
        if logs:
            for log in logs:
                date = log['created_at'][:10]  # YYYY-MM-DD
                daily_stats[date] += 1
        
        # Сортируем даты
        sorted_dates = sorted(daily_stats.keys())[-30:]  # Последние 30 дней
        chart_labels = json.dumps(sorted_dates)
        chart_data = json.dumps([daily_stats[date] for date in sorted_dates])
        
        # Пользователи для фильтра
        users = db.execute_query("SELECT id, username FROM users ORDER BY username", fetch_all=True)
        
        return render_template('logs/list.html',
                             logs=[dict(l) for l in logs] if logs else [],
                             stats=stats,
                             chart_labels=chart_labels,
                             chart_data=chart_data,
                             users=[dict(u) for u in users] if users else [])
                             
    except Exception as e:
        logger.error(f'Ошибка загрузки истории: {e}')
        flash('Ошибка загрузки истории изменений', 'error')
        return redirect(url_for('dashboard'))

@logs_bp.route('/logs/object/<string:entity_type>/<int:entity_id>', endpoint='object_history')
@login_required
def object_history(entity_type, entity_id):
    """История изменений конкретного объекта"""
    try:
        db = get_db()
        
        logs = db.execute_query("""
            SELECT l.*, u.username 
            FROM user_logs l
            LEFT JOIN users u ON l.user_id = u.id
            WHERE l.entity_type = ? AND l.entity_id = ?
            ORDER BY l.created_at DESC
        """, (entity_type, entity_id), fetch_all=True)
        
        # Статистика
        total_count = len(logs) if logs else 0
        create_count = sum(1 for l in logs if l['action'] == 'create') if logs else 0
        update_count = sum(1 for l in logs if l['action'] == 'update') if logs else 0
        delete_count = sum(1 for l in logs if l['action'] == 'delete') if logs else 0
        
        return render_template('logs/object_history.html',
                             logs=[dict(l) for l in logs] if logs else [],
                             entity_type=entity_type,
                             entity_id=entity_id,
                             total_count=total_count,
                             create_count=create_count,
                             update_count=update_count,
                             delete_count=delete_count)
                             
    except Exception as e:
        logger.error(f'Ошибка загрузки истории объекта: {e}')
        flash('Ошибка загрузки истории объекта', 'error')
        return redirect(url_for('logs.logs_list'))

@logs_bp.route('/logs/user/<int:user_id>', endpoint='user_history')
@login_required
def user_history(user_id):
    """История действий конкретного пользователя"""
    try:
        db = get_db()
        
        # Информация о пользователе
        user = db.execute_query("SELECT username FROM users WHERE id = ?", (user_id,), fetch_all=False)
        
        logs = db.execute_query("""
            SELECT l.*, u.username 
            FROM user_logs l
            LEFT JOIN users u ON l.user_id = u.id
            WHERE l.user_id = ?
            ORDER BY l.created_at DESC
            LIMIT 500
        """, (user_id,), fetch_all=True)
        
        return render_template('logs/user_history.html',
                             logs=[dict(l) for l in logs] if logs else [],
                             user=dict(user) if user else {'username': 'Пользователь'},
                             user_id=user_id)
                             
    except Exception as e:
        logger.error(f'Ошибка загрузки истории пользователя: {e}')
        flash('Ошибка загрузки истории пользователя', 'error')
        return redirect(url_for('logs.logs_list'))

@logs_bp.route('/logs/clear', methods=['POST'], endpoint='clear_logs')
@login_required
def clear_logs():
    """Очистка старых записей (только для админа)"""
    if session.get('role') != 'admin':
        return jsonify({'success': False, 'error': 'Доступ запрещен'})
    
    try:
        db = get_db()
        days = request.json.get('days', 90)  # По умолчанию храним 90 дней
        
        # Удаляем записи старше N дней
        db.execute_query("""
            DELETE FROM user_logs 
            WHERE created_at < DATE('now', '-' || ? || ' days')
        """, (days,))
        
        return jsonify({'success': True, 'message': f'Старые записи удалены'})
        
    except Exception as e:
        logger.error(f'Ошибка очистки логов: {e}')
        return jsonify({'success': False, 'error': str(e)})

# API для получения истории
@logs_bp.route('/api/logs/<string:entity_type>/<int:entity_id>', endpoint='api_object_history')
@login_required
def api_object_history(entity_type, entity_id):
    """API для получения истории объекта"""
    try:
        db = get_db()
        
        logs = db.execute_query("""
            SELECT l.*, u.username 
            FROM user_logs l
            LEFT JOIN users u ON l.user_id = u.id
            WHERE l.entity_type = ? AND l.entity_id = ?
            ORDER BY l.created_at DESC
        """, (entity_type, entity_id), fetch_all=True)
        
        result = []
        for log in logs:
            log_dict = dict(log)
            # Парсим JSON поля если нужно
            if log_dict.get('old_value'):
                try:
                    log_dict['old_value'] = json.loads(log_dict['old_value'])
                except Exception:
                    pass
            if log_dict.get('new_value'):
                try:
                    log_dict['new_value'] = json.loads(log_dict['new_value'])
                except Exception:
                    pass
            result.append(log_dict)
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

