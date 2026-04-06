"""
NotificationService — бизнес-логика уведомлений.

Инкапсулирует:
- создание уведомлений
- отметку как прочитанных
- подсчёт непрочитанных
- очистку устаревших
"""
import logging
from exceptions import NotFoundError

logger = logging.getLogger('services.notification')

# Допустимые типы и приоритеты уведомлений
NOTIFICATION_TYPES = ('info', 'warning', 'error', 'success', 'calibration', 'expiry', 'low_stock')
NOTIFICATION_PRIORITIES = ('low', 'medium', 'high', 'critical')


class NotificationService:
    def __init__(self, db):
        self.db = db

    # ─── Создание ────────────────────────────────────────────────────────────

    def create(
        self,
        title: str,
        message: str,
        *,
        notification_type: str = 'info',
        priority: str = 'medium',
        entity_type: str = '',
        entity_id: int | None = None,
        user_id: int | None = None,
    ) -> int:
        """
        Создаёт уведомление. Возвращает ID созданной записи.

        Raises:
            ValueError: если тип или приоритет неизвестны
        """
        if notification_type not in NOTIFICATION_TYPES:
            raise ValueError(f"Неизвестный тип уведомления: {notification_type}")
        if priority not in NOTIFICATION_PRIORITIES:
            raise ValueError(f"Неизвестный приоритет: {priority}")

        self.db.execute_query(
            """
            INSERT INTO notifications (title, message, type, priority, entity_type, entity_id, user_id, is_read, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, CURRENT_TIMESTAMP)
            """,
            (title, message, notification_type, priority, entity_type, entity_id, user_id)
        )

        row = self.db.execute_query("SELECT last_insert_rowid() as id", fetch_all=False)
        new_id = row['id'] if row else 0
        logger.debug(f"Создано уведомление #{new_id}: {title[:50]}")
        return new_id

    # ─── Статус ──────────────────────────────────────────────────────────────

    def mark_read(self, notification_id: int) -> None:
        """Отмечает уведомление как прочитанное."""
        rows = self.db.execute_query(
            "UPDATE notifications SET is_read = 1, read_at = CURRENT_TIMESTAMP WHERE id = ?",
            (notification_id,)
        )
        if rows == 0:
            raise NotFoundError(f"Уведомление #{notification_id} не найдено", entity="notification", entity_id=notification_id)

    def mark_all_read(self, user_id: int | None = None) -> int:
        """Отмечает все уведомления (или конкретного пользователя) как прочитанные. Возвращает количество."""
        if user_id is not None:
            self.db.execute_query(
                "UPDATE notifications SET is_read = 1, read_at = CURRENT_TIMESTAMP WHERE user_id = ? AND is_read = 0",
                (user_id,)
            )
        else:
            self.db.execute_query(
                "UPDATE notifications SET is_read = 1, read_at = CURRENT_TIMESTAMP WHERE is_read = 0"
            )
        row = self.db.execute_query("SELECT changes() as n", fetch_all=False)
        return row['n'] if row else 0

    # ─── Подсчёт ─────────────────────────────────────────────────────────────

    def count_unread(self, user_id: int | None = None) -> int:
        """Возвращает количество непрочитанных уведомлений."""
        if user_id is not None:
            row = self.db.execute_query(
                "SELECT COUNT(*) as cnt FROM notifications WHERE is_read = 0 AND (user_id = ? OR user_id IS NULL)",
                (user_id,), fetch_all=False
            )
        else:
            row = self.db.execute_query(
                "SELECT COUNT(*) as cnt FROM notifications WHERE is_read = 0",
                fetch_all=False
            )
        return row['cnt'] if row else 0

    # ─── Очистка ─────────────────────────────────────────────────────────────

    def delete_old(self, days: int = 30) -> int:
        """Удаляет прочитанные уведомления старше N дней. Возвращает количество удалённых."""
        self.db.execute_query(
            "DELETE FROM notifications WHERE is_read = 1 AND created_at < datetime('now', ?)",
            (f'-{int(days)} days',)
        )
        row = self.db.execute_query("SELECT changes() as n", fetch_all=False)
        count = row['n'] if row else 0
        if count:
            logger.info(f"Удалено {count} устаревших уведомлений (старше {days} дней)")
        return count
