"""
Задачи планировщика для фоновых уведомлений.
Вызываются APScheduler из app.py.
"""
import logging
import threading
from datetime import datetime
from database import get_db

logger = logging.getLogger('services.scheduler_tasks')

# ============ HEARTBEAT TRACKER (M-12) ============
_heartbeat_lock = threading.Lock()
_heartbeat: dict = {}  # job_id -> datetime последнего запуска


def _beat(job_id: str) -> None:
    """Фиксирует момент успешного завершения задачи."""
    with _heartbeat_lock:
        _heartbeat[job_id] = datetime.now()


def get_scheduler_health() -> dict:
    """Возвращает статус здоровья планировщика: время последнего запуска каждой задачи."""
    from datetime import timedelta
    with _heartbeat_lock:
        snapshot = dict(_heartbeat)
    now = datetime.now()
    result = {}
    thresholds = {
        'check_notifications': timedelta(hours=7),
        'cleanup_login_attempts': timedelta(hours=2),
        'cleanup_old_notifications': timedelta(hours=25),
    }
    for job_id, last_run in snapshot.items():
        age = now - last_run
        threshold = thresholds.get(job_id, timedelta(hours=24))
        result[job_id] = {
            'last_run': last_run.strftime('%Y-%m-%d %H:%M:%S'),
            'age_minutes': int(age.total_seconds() / 60),
            'status': 'ok' if age <= threshold else 'overdue',
        }
    # Задачи, которые ещё ни разу не запускались
    for job_id in thresholds:
        if job_id not in result:
            result[job_id] = {'last_run': None, 'age_minutes': None, 'status': 'never_run'}
    return result


def check_expiring_batches():
    """Проверка истекающих партий"""
    try:
        db = get_db()

        has_notifications = db.column_exists('notifications', 'id')
        if not has_notifications:
            return

        expiring = db.execute_query("""
            SELECT b.*, n.name as nomenclature_name
            FROM batches b
            JOIN nomenclatures n ON b.nomenclature_id = n.id
            WHERE b.expiry_date IS NOT NULL
                AND b.expiry_date BETWEEN date('now') AND date('now', '+30 days')
                AND b.is_active = 1
        """, fetch_all=True)

        for batch in expiring or []:
            expiry_date = datetime.strptime(batch['expiry_date'], '%Y-%m-%d')
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            days_left = (expiry_date - today).days

            existing = db.execute_query("""
                SELECT id FROM notifications
                WHERE type = 'expiry' AND entity_type = 'batch' AND entity_id = ?
                AND DATE(created_at) = DATE('now')
            """, (batch['id'],), fetch_all=False)

            if not existing:
                db.execute_query("""
                    INSERT INTO notifications (user_id, type, title, message, entity_type, entity_id, expiry_date)
                    VALUES (NULL, 'expiry', ?, ?, 'batch', ?, ?)
                """, (
                    'Истекает срок годности',
                    f'Партия {batch["batch_number"]} ({batch["nomenclature_name"]}) истекает через {days_left} дней',
                    batch['id'],
                    batch['expiry_date']
                ))

        expired = db.execute_query("""
            SELECT b.*, n.name as nomenclature_name
            FROM batches b
            JOIN nomenclatures n ON b.nomenclature_id = n.id
            WHERE b.expiry_date IS NOT NULL
                AND b.expiry_date < date('now')
                AND b.is_active = 1
        """, fetch_all=True)

        for batch in expired or []:
            existing = db.execute_query("""
                SELECT id FROM notifications
                WHERE type = 'expired' AND entity_type = 'batch' AND entity_id = ?
                AND DATE(created_at) = DATE('now')
            """, (batch['id'],), fetch_all=False)

            if not existing:
                db.execute_query("""
                    INSERT INTO notifications (user_id, type, title, message, entity_type, entity_id, expiry_date)
                    VALUES (NULL, 'expired', ?, ?, 'batch', ?, ?)
                """, (
                    'Партия просрочена',
                    f'Партия {batch["batch_number"]} ({batch["nomenclature_name"]}) просрочена',
                    batch['id'],
                    batch['expiry_date']
                ))

        db.connection.commit()

    except Exception as e:
        logger.error(f'Ошибка проверки партий: {e}')


def check_low_stock():
    """Проверка малых остатков"""
    try:
        db = get_db()

        has_notifications = db.column_exists('notifications', 'id')
        if not has_notifications:
            return

        low_stock = db.execute_query("""
            SELECT n.id, n.name, n.sku, n.min_stock,
                   COALESCE(SUM(s.quantity), 0) as total_quantity
            FROM nomenclatures n
            LEFT JOIN stocks s ON n.id = s.nomenclature_id
            WHERE n.min_stock > 0 AND n.is_active = 1
            GROUP BY n.id
            HAVING total_quantity <= n.min_stock
        """, fetch_all=True)

        for item in low_stock or []:
            existing = db.execute_query("""
                SELECT id FROM notifications
                WHERE type = 'low_stock' AND entity_type = 'nomenclature' AND entity_id = ?
                AND DATE(created_at) = DATE('now')
            """, (item['id'],), fetch_all=False)

            if not existing:
                db.execute_query("""
                    INSERT INTO notifications (user_id, type, title, message, entity_type, entity_id)
                    VALUES (NULL, 'low_stock', ?, ?, 'nomenclature', ?)
                """, (
                    'Малый остаток',
                    f'{item["name"]} ({item["sku"]}) - остаток {item["total_quantity"]} при мин. запасе {item["min_stock"]}',
                    item['id']
                ))

        db.connection.commit()

    except Exception as e:
        logger.error(f'Ошибка проверки остатков: {e}')


def check_calibration():
    """Проверка необходимости поверки"""
    try:
        db = get_db()

        has_notifications = db.column_exists('notifications', 'id')
        if not has_notifications:
            return

        expiring_cal = db.execute_query("""
            SELECT i.*, n.name as nomenclature_name
            FROM instances i
            JOIN nomenclatures n ON i.nomenclature_id = n.id
            WHERE i.next_calibration IS NOT NULL
                AND i.next_calibration BETWEEN date('now') AND date('now', '+30 days')
                AND i.status NOT IN ('written_off')
        """, fetch_all=True)

        for inst in expiring_cal or []:
            days_left = (datetime.strptime(inst['next_calibration'], '%Y-%m-%d') - datetime.now()).days

            existing = db.execute_query("""
                SELECT id FROM notifications
                WHERE type = 'calibration' AND entity_type = 'instance' AND entity_id = ?
                AND DATE(created_at) = DATE('now')
            """, (inst['id'],), fetch_all=False)

            if not existing:
                db.execute_query("""
                    INSERT INTO notifications (user_id, type, title, message, entity_type, entity_id, expiry_date)
                    VALUES (NULL, 'calibration', ?, ?, 'instance', ?, ?)
                """, (
                    'Требуется поверка',
                    f'{inst["nomenclature_name"]} (инв. {inst["inventory_number"]}) - поверка истекает через {days_left} дней',
                    inst['id'],
                    inst['next_calibration']
                ))

        overdue_cal = db.execute_query("""
            SELECT i.*, n.name as nomenclature_name
            FROM instances i
            JOIN nomenclatures n ON i.nomenclature_id = n.id
            WHERE i.next_calibration IS NOT NULL
                AND i.next_calibration < date('now')
                AND i.status NOT IN ('written_off')
        """, fetch_all=True)

        for inst in overdue_cal or []:
            existing = db.execute_query("""
                SELECT id FROM notifications
                WHERE type = 'calibration' AND entity_type = 'instance' AND entity_id = ?
                AND DATE(created_at) = DATE('now')
            """, (inst['id'],), fetch_all=False)

            if not existing:
                db.execute_query("""
                    INSERT INTO notifications (user_id, type, title, message, entity_type, entity_id, expiry_date)
                    VALUES (NULL, 'calibration', ?, ?, 'instance', ?, ?)
                """, (
                    'Просрочена поверка',
                    f'{inst["nomenclature_name"]} (инв. {inst["inventory_number"]}) - поверка просрочена',
                    inst['id'],
                    inst['next_calibration']
                ))

        db.connection.commit()

    except Exception as e:
        logger.error(f'Ошибка проверки поверок: {e}')


def check_maintenance():
    """Проверка необходимости обслуживания"""
    try:
        db = get_db()

        has_notifications = db.column_exists('notifications', 'id')
        if not has_notifications:
            return

        expiring_maint = db.execute_query("""
            SELECT i.*, n.name as nomenclature_name
            FROM instances i
            JOIN nomenclatures n ON i.nomenclature_id = n.id
            WHERE i.next_maintenance IS NOT NULL
                AND i.next_maintenance BETWEEN date('now') AND date('now', '+30 days')
                AND i.status NOT IN ('written_off')
        """, fetch_all=True)

        for inst in expiring_maint or []:
            days_left = (datetime.strptime(inst['next_maintenance'], '%Y-%m-%d') - datetime.now()).days

            existing = db.execute_query("""
                SELECT id FROM notifications
                WHERE type = 'maintenance' AND entity_type = 'instance' AND entity_id = ?
                AND DATE(created_at) = DATE('now')
            """, (inst['id'],), fetch_all=False)

            if not existing:
                db.execute_query("""
                    INSERT INTO notifications (user_id, type, title, message, entity_type, entity_id, expiry_date)
                    VALUES (NULL, 'maintenance', ?, ?, 'instance', ?, ?)
                """, (
                    'Требуется обслуживание',
                    f'{inst["nomenclature_name"]} (инв. {inst["inventory_number"]}) - ТО через {days_left} дней',
                    inst['id'],
                    inst['next_maintenance']
                ))

        overdue_maint = db.execute_query("""
            SELECT i.*, n.name as nomenclature_name
            FROM instances i
            JOIN nomenclatures n ON i.nomenclature_id = n.id
            WHERE i.next_maintenance IS NOT NULL
                AND i.next_maintenance < date('now')
                AND i.status NOT IN ('written_off')
        """, fetch_all=True)

        for inst in overdue_maint or []:
            existing = db.execute_query("""
                SELECT id FROM notifications
                WHERE type = 'maintenance' AND entity_type = 'instance' AND entity_id = ?
                AND DATE(created_at) = DATE('now')
            """, (inst['id'],), fetch_all=False)

            if not existing:
                db.execute_query("""
                    INSERT INTO notifications (user_id, type, title, message, entity_type, entity_id, expiry_date)
                    VALUES (NULL, 'maintenance', ?, ?, 'instance', ?, ?)
                """, (
                    'Просрочено обслуживание',
                    f'{inst["nomenclature_name"]} (инв. {inst["inventory_number"]}) - ТО просрочено',
                    inst['id'],
                    inst['next_maintenance']
                ))

        db.connection.commit()

    except Exception as e:
        logger.error(f'Ошибка проверки ТО: {e}')


def check_all_notifications():
    """Проверка всех типов уведомлений (вызывается планировщиком)"""
    try:
        db = get_db()
        has_table = db.execute_query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='notifications'",
            fetch_all=False
        )

        if not has_table:
            logger.warning('Таблица notifications не существует, пропускаем проверку')
            return

        check_expiring_batches()
        check_low_stock()
        check_calibration()
        check_maintenance()
        _beat('check_notifications')
        logger.info('Проверка уведомлений выполнена')
    except Exception as e:
        logger.error(f'Ошибка при проверке уведомлений: {e}')


def cleanup_login_attempts():
    """Очистка устаревших записей блокировки входа (старше 15 минут)."""
    try:
        import time
        cutoff = time.time() - 900  # 15 минут
        db = get_db()
        deleted = db.execute_query(
            "DELETE FROM login_attempts WHERE attempted_at < ?", (cutoff,)
        )
        if deleted:
            logger.debug(f'Удалено {deleted} устаревших записей login_attempts')
        _beat('cleanup_login_attempts')
    except Exception as e:
        logger.error(f'Ошибка очистки login_attempts: {e}')


def cleanup_old_notifications():
    """Удаляет прочитанные уведомления старше 30 дней и непрочитанные старше 90 дней (L-7)."""
    try:
        db = get_db()
        has_table = db.execute_query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='notifications'",
            fetch_all=False
        )
        if not has_table:
            return

        db.execute_query("""
            DELETE FROM notifications
            WHERE (is_read = 1 AND created_at < datetime('now', '-30 days'))
               OR (is_read = 0 AND created_at < datetime('now', '-90 days'))
        """)
        _beat('cleanup_old_notifications')
        logger.info('Очистка старых уведомлений выполнена')
    except Exception as e:
        logger.error(f'Ошибка очистки уведомлений: {e}')
