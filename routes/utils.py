"""
Общие утилиты для blueprint-модулей.
Функции, используемые несколькими blueprints.
"""
import hashlib
import random
import re
import logging
from datetime import datetime

logger = logging.getLogger('routes.utils')


def generate_unique_barcode(sku=None, name=None, category_id=None):
    """
    Генерация уникального штрих-кода.
    Формат: 13-значный EAN-13 совместимый код.
    """
    from routes.common import get_db
    db = get_db()

    existing_barcodes = set()
    try:
        rows = db.execute_query("SELECT barcode FROM nomenclatures WHERE barcode IS NOT NULL", fetch_all=True) or []
        for row in rows:
            if row['barcode']:
                existing_barcodes.add(str(row['barcode']))
    except Exception as e:
        logger.debug(f"Ошибка получения существующих штрих-кодов: {e}")

    def generate_ean13():
        prefix = '20'
        random_part = ''.join([str(random.randint(0, 9)) for _ in range(10)])
        ean12 = prefix + random_part
        total = 0
        for i, digit in enumerate(ean12):
            if i % 2 == 0:
                total += int(digit)
            else:
                total += int(digit) * 3
        checksum = (10 - (total % 10)) % 10
        return ean12 + str(checksum)

    def generate_from_sku(sku_str, name_str):
        if sku_str:
            clean_sku = re.sub(r'[^A-Za-z0-9]', '', sku_str)
            if len(clean_sku) >= 6:
                return clean_sku[:12]
            else:
                return clean_sku + ''.join([str(random.randint(0, 9)) for _ in range(12 - len(clean_sku))])
        elif name_str:
            words = name_str.split()
            code = ''
            for word in words[:3]:
                if word:
                    code += word[0].upper()
            timestamp = str(int(datetime.now().timestamp()))[-8:]
            return code + timestamp
        return None

    strategies = [
        lambda: generate_ean13(),
        lambda: '20' + ''.join([str(random.randint(0, 9)) for _ in range(10)]) + str(random.randint(0, 9)),
        lambda: str(int(datetime.now().timestamp()))[-13:],
        lambda: hashlib.md5(f"{sku or ''}{name or ''}{random.random()}".encode()).hexdigest()[:13].upper(),
    ]

    for strategy in strategies:
        for attempt in range(5):
            barcode = strategy()
            if len(barcode) > 13:
                barcode = barcode[:13]
            elif len(barcode) < 13:
                barcode = barcode.zfill(13)
            if not barcode.isdigit():
                barcode = ''.join(filter(str.isdigit, barcode))
                if len(barcode) < 13:
                    barcode = barcode.ljust(13, '0')
                elif len(barcode) > 13:
                    barcode = barcode[:13]
            if barcode not in existing_barcodes:
                existing_barcodes.add(barcode)
                return barcode

    timestamp = str(int(datetime.now().timestamp() * 1000))[-13:]
    return timestamp.zfill(13)


def is_barcode_unique(barcode, exclude_id=None):
    """Проверка уникальности штрих-кода."""
    try:
        from routes.common import get_db
        db = get_db()
        query = "SELECT id FROM nomenclatures WHERE barcode = ?"
        params = [barcode]
        if exclude_id:
            query += " AND id != ?"
            params.append(exclude_id)
        existing = db.execute_query(query, params, fetch_all=False)
        return existing is None
    except Exception as e:
        logger.debug(f"Ошибка проверки штрих-кода: {e}")
        return False
