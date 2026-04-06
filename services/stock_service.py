"""
StockService — бизнес-логика управления складскими остатками.

Инкапсулирует:
- получение остатков по складу / номенклатуре
- обновление (приход/расход)
- проверку минимальных остатков
"""
import logging
from exceptions import NotFoundError, BusinessRuleError, ValidationError

logger = logging.getLogger('services.stock')


class StockService:
    def __init__(self, db):
        self.db = db

    # ─── Остатки ─────────────────────────────────────────────────────────────

    def get_stock(self, nomenclature_id: int, warehouse_id: int) -> dict | None:
        """Возвращает запись остатка или None."""
        return self.db.execute_query(
            "SELECT * FROM stocks WHERE nomenclature_id = ? AND warehouse_id = ?",
            (nomenclature_id, warehouse_id), fetch_all=False
        )

    def get_low_stock_items(self, threshold: float = 0) -> list:
        """Возвращает номенклатуры с остатком ≤ threshold."""
        return self.db.execute_query(
            """
            SELECT s.*, n.name, n.unit
            FROM stocks s
            JOIN nomenclatures n ON s.nomenclature_id = n.id
            WHERE s.quantity <= ?
            ORDER BY s.quantity ASC
            """,
            (threshold,), fetch_all=True
        ) or []

    # ─── Движения ────────────────────────────────────────────────────────────

    def adjust_stock(self, nomenclature_id: int, warehouse_id: int, delta: float, user_id: int) -> dict:
        """
        Изменяет остаток на delta (положительное — приход, отрицательное — расход).

        Raises:
            NotFoundError: если склад или номенклатура не существуют
            BusinessRuleError: если расход превышает остаток
        """
        if delta == 0:
            return {'success': True, 'message': 'Нет изменений'}

        stock = self.get_stock(nomenclature_id, warehouse_id)

        if delta < 0:
            current_qty = stock['quantity'] if stock else 0
            if current_qty + delta < 0:
                raise BusinessRuleError(
                    f"Недостаточно остатка: есть {current_qty}, требуется {abs(delta)}",
                    rule="insufficient_stock"
                )

        if stock:
            self.db.execute_query(
                "UPDATE stocks SET quantity = quantity + ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (delta, stock['id'])
            )
        else:
            if delta < 0:
                raise BusinessRuleError("Нельзя списать с нулевого остатка", rule="insufficient_stock")
            self.db.execute_query(
                "INSERT INTO stocks (nomenclature_id, warehouse_id, quantity, created_at, updated_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                (nomenclature_id, warehouse_id, delta)
            )

        logger.info(f"Остаток nomenclature={nomenclature_id} warehouse={warehouse_id} изменён на {delta:+} пользователем #{user_id}")
        return {'success': True, 'message': f'Остаток изменён на {delta:+}'}

    def move_stock(self, nomenclature_id: int, from_warehouse_id: int,
                   to_warehouse_id: int, quantity: float, user_id: int,
                   batch_id: int | None = None) -> dict:
        """
        Перемещает quantity единиц со склада from_warehouse_id на to_warehouse_id.

        Raises:
            BusinessRuleError: если на источнике недостаточно остатка
        """
        if quantity <= 0:
            raise ValidationError("Количество должно быть положительным")

        if from_warehouse_id == to_warehouse_id:
            raise BusinessRuleError("Склад источника и назначения совпадают", rule="same_warehouse")

        # Проверяем источник
        src = self.db.execute_query(
            "SELECT id, quantity FROM stocks WHERE nomenclature_id = ? AND warehouse_id = ? AND (batch_id = ? OR (batch_id IS NULL AND ? IS NULL))",
            (nomenclature_id, from_warehouse_id, batch_id, batch_id), fetch_all=False
        )
        if not src or src['quantity'] < quantity:
            available = src['quantity'] if src else 0
            raise BusinessRuleError(
                f"Недостаточно на складе #{from_warehouse_id}: есть {available}, нужно {quantity}",
                rule="insufficient_stock"
            )

        new_src_qty = src['quantity'] - quantity

        # Списываем с источника
        if new_src_qty == 0:
            self.db.execute_query("DELETE FROM stocks WHERE id = ?", (src['id'],))
        else:
            self.db.execute_query(
                "UPDATE stocks SET quantity = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (new_src_qty, src['id'])
            )

        # Добавляем на целевой склад
        dst = self.db.execute_query(
            "SELECT id, quantity FROM stocks WHERE nomenclature_id = ? AND warehouse_id = ? AND (batch_id = ? OR (batch_id IS NULL AND ? IS NULL))",
            (nomenclature_id, to_warehouse_id, batch_id, batch_id), fetch_all=False
        )
        if dst:
            self.db.execute_query(
                "UPDATE stocks SET quantity = quantity + ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (quantity, dst['id'])
            )
        else:
            self.db.execute_query(
                "INSERT INTO stocks (nomenclature_id, warehouse_id, batch_id, quantity, created_at, updated_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                (nomenclature_id, to_warehouse_id, batch_id, quantity)
            )

        logger.info(
            f"Перемещение: nomenclature={nomenclature_id} {quantity} ед. "
            f"склад#{from_warehouse_id} → склад#{to_warehouse_id} пользователь#{user_id}"
        )
        return {'success': True, 'message': f'Перемещено {quantity} ед. со склада #{from_warehouse_id} на #{to_warehouse_id}'}

    def set_stock(self, nomenclature_id: int, warehouse_id: int,
                  quantity: float, user_id: int, batch_id: int | None = None) -> dict:
        """Устанавливает точное количество (используется при инвентаризации)."""
        if quantity < 0:
            raise ValidationError("Количество не может быть отрицательным")

        existing = self.db.execute_query(
            "SELECT id FROM stocks WHERE nomenclature_id = ? AND warehouse_id = ? AND (batch_id = ? OR (batch_id IS NULL AND ? IS NULL))",
            (nomenclature_id, warehouse_id, batch_id, batch_id), fetch_all=False
        )

        if quantity == 0:
            if existing:
                self.db.execute_query("DELETE FROM stocks WHERE id = ?", (existing['id'],))
        elif existing:
            self.db.execute_query(
                "UPDATE stocks SET quantity = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (quantity, existing['id'])
            )
        else:
            self.db.execute_query(
                "INSERT INTO stocks (nomenclature_id, warehouse_id, batch_id, quantity, created_at, updated_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                (nomenclature_id, warehouse_id, batch_id, quantity)
            )

        logger.info(f"Инвентаризация: nomenclature={nomenclature_id} warehouse={warehouse_id} → {quantity} ед. пользователь#{user_id}")
        return {'success': True, 'message': f'Остаток установлен: {quantity}'}
