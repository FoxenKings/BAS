"""
Общие утилиты для всех Blueprint-модулей:
- Декораторы login_required, admin_required
- Доступ к БД через get_db()
- Общий логгер
"""
import logging
from functools import wraps
from flask import session, redirect, url_for, flash, request

# Логгер модуля (отдельный от app)
logger = logging.getLogger('routes')


# ─── Декораторы ─────────────────────────────────────────────────────────────

def login_required(f):
    """Требует авторизации. Редиректит на /login если не авторизован."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Требуется авторизация', 'error')
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated_function


def admin_required(f):
    """Требует роли admin. Редиректит на dashboard для остальных ролей."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Требуется авторизация', 'error')
            return redirect(url_for('auth.login'))
        if session.get('role') != 'admin':
            flash('Доступ запрещён: требуются права администратора', 'error')
            return redirect(url_for('dashboard'))
        return f(*args, **kwargs)
    return decorated_function


def manager_required(f):
    """Требует роли admin или manager."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Требуется авторизация', 'error')
            return redirect(url_for('auth.login'))
        if session.get('role') not in ('admin', 'manager'):
            flash('Недостаточно прав', 'error')
            return redirect(url_for('dashboard'))
        return f(*args, **kwargs)
    return decorated_function


# ─── БД ─────────────────────────────────────────────────────────────────────

def get_db():
    """Возвращает текущий экземпляр Database. Импортируется из database модуля."""
    from database import get_db as _get_db
    return _get_db()
