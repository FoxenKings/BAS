"""
Скрипт резервного копирования базы данных assets.db.

Использование:
    python backup_db.py              — создать резервную копию сейчас
    python backup_db.py --list       — показать список резервных копий
    python backup_db.py --clean      — удалить копии старше 30 дней

Автоматический запуск настраивается через backup_schedule.bat
"""
import os
import sys
import sqlite3
import shutil
import argparse
import logging
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler

# ─── Пути ────────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
DB_PATH    = os.path.join(BASE_DIR, 'data', 'assets.db')
BACKUP_DIR = os.path.join(BASE_DIR, 'backups')
LOG_DIR    = os.path.join(BASE_DIR, 'logs')
KEEP_DAYS  = 30  # сколько дней хранить резервные копии

# ─── Логирование ─────────────────────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)
_handler = RotatingFileHandler(
    os.path.join(LOG_DIR, 'backup.log'),
    maxBytes=2 * 1024 * 1024,
    backupCount=3,
    encoding='utf-8'
)
_handler.setFormatter(logging.Formatter(
    '%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S'
))
logging.basicConfig(level=logging.INFO, handlers=[_handler, logging.StreamHandler()])
log = logging.getLogger('backup')


def check_db_integrity(db_path: str) -> bool:
    """Проверяет целостность SQLite БД через PRAGMA integrity_check."""
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        cursor = conn.execute('PRAGMA integrity_check')
        result = cursor.fetchone()
        conn.close()
        ok = result and result[0] == 'ok'
        if not ok:
            log.error(f'PRAGMA integrity_check: {result}')
        return ok
    except Exception as e:
        log.error(f'Ошибка проверки целостности: {e}')
        return False


def create_backup() -> bool:
    """Создаёт резервную копию через SQLite .backup() API. Возвращает True при успехе."""
    if not os.path.exists(DB_PATH):
        log.error(f'База данных не найдена: {DB_PATH}')
        return False

    # Проверяем целостность исходной БД
    if not check_db_integrity(DB_PATH):
        log.error('Резервная копия отменена: БД повреждена!')
        return False

    os.makedirs(BACKUP_DIR, exist_ok=True)

    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
    backup_path = os.path.join(BACKUP_DIR, f'backup_{timestamp}.db')

    try:
        src = sqlite3.connect(DB_PATH, timeout=10)
        dst = sqlite3.connect(backup_path)
        src.backup(dst)
        dst.close()
        src.close()

        size_mb = os.path.getsize(backup_path) / 1024 / 1024
        log.info(f'Резервная копия создана: {os.path.basename(backup_path)} ({size_mb:.1f} МБ)')

        # Обслуживание основной БД после бэкапа
        try:
            conn_main = sqlite3.connect(DB_PATH, timeout=10)
            conn_main.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            conn_main.execute("ANALYZE")
            conn_main.close()
            log.info('Обслуживание БД: WAL checkpoint + ANALYZE выполнены')
        except Exception as ex:
            log.warning(f'Обслуживание БД не удалось: {ex}')

        return True
    except Exception as e:
        log.error(f'Ошибка создания резервной копии: {e}')
        if os.path.exists(backup_path):
            os.remove(backup_path)
        return False


def clean_old_backups(keep_days: int = KEEP_DAYS) -> int:
    """Удаляет резервные копии старше keep_days дней. Возвращает количество удалённых файлов."""
    if not os.path.exists(BACKUP_DIR):
        return 0

    cutoff = datetime.now() - timedelta(days=keep_days)
    removed = 0

    for filename in os.listdir(BACKUP_DIR):
        if not filename.startswith('backup_') or not filename.endswith('.db'):
            continue
        filepath = os.path.join(BACKUP_DIR, filename)
        mtime = datetime.fromtimestamp(os.path.getmtime(filepath))
        if mtime < cutoff:
            try:
                os.remove(filepath)
                log.info(f'Удалена старая копия: {filename} (создана {mtime.strftime("%Y-%m-%d")})')
                removed += 1
            except Exception as e:
                log.warning(f'Не удалось удалить {filename}: {e}')

    return removed


def list_backups() -> list[dict]:
    """Возвращает список резервных копий с метаданными."""
    if not os.path.exists(BACKUP_DIR):
        return []

    backups = []
    for filename in sorted(os.listdir(BACKUP_DIR), reverse=True):
        if not filename.startswith('backup_') or not filename.endswith('.db'):
            continue
        filepath = os.path.join(BACKUP_DIR, filename)
        size_mb = os.path.getsize(filepath) / 1024 / 1024
        mtime = datetime.fromtimestamp(os.path.getmtime(filepath))
        backups.append({
            'filename': filename,
            'path': filepath,
            'size_mb': round(size_mb, 1),
            'created': mtime,
        })
    return backups


def print_backup_list():
    """Выводит список резервных копий в консоль."""
    backups = list_backups()
    if not backups:
        print('Резервных копий нет.')
        return
    print(f'\n{"Файл":<35} {"Размер":>8}  {"Дата создания"}')
    print('-' * 65)
    for b in backups:
        print(f'{b["filename"]:<35} {b["size_mb"]:>6.1f} МБ  {b["created"].strftime("%Y-%m-%d %H:%M")}')
    print(f'\nВсего: {len(backups)} копий')


def main():
    parser = argparse.ArgumentParser(description='Резервное копирование assets.db')
    parser.add_argument('--list', action='store_true', help='Показать список резервных копий')
    parser.add_argument('--clean', action='store_true', help='Удалить копии старше 30 дней')
    parser.add_argument('--keep-days', type=int, default=KEEP_DAYS,
                        help=f'Сколько дней хранить копии (по умолчанию {KEEP_DAYS})')
    args = parser.parse_args()

    if args.list:
        print_backup_list()
        return 0

    if args.clean:
        removed = clean_old_backups(args.keep_days)
        print(f'Удалено старых копий: {removed}')
        return 0

    # По умолчанию — создать резервную копию и почистить старые
    success = create_backup()
    clean_old_backups(args.keep_days)
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
