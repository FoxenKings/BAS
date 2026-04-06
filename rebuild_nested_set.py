"""
Пересчёт lft, rgt, level для таблицы categories на основе parent_id.
Только активные категории включаются в nested set.
Деактивированные получают lft=0, rgt=1.
"""
import sqlite3
import sys

DB_PATH = 'data/assets.db'


def rebuild(db_path: str, dry_run: bool = False) -> None:
    sys.stdout.reconfigure(encoding='utf-8')
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Загружаем все активные категории
    rows = conn.execute(
        'SELECT id, parent_id, sort_order, name_ru FROM categories WHERE is_active=1 ORDER BY COALESCE(sort_order, 9999), id'
    ).fetchall()

    # Строим словарь: parent_id -> список детей (уже отсортированных)
    children: dict[int | None, list] = {}
    for r in rows:
        pid = r['parent_id']
        children.setdefault(pid, []).append(dict(r))

    # Рекурсивный обход — присваиваем lft, rgt, level
    updates: list[tuple[int, int, int, int]] = []  # (lft, rgt, level, id)
    counter = [0]

    def traverse(parent_id, level):
        for cat in children.get(parent_id, []):
            counter[0] += 1
            lft = counter[0]
            traverse(cat['id'], level + 1)
            counter[0] += 1
            rgt = counter[0]
            updates.append((lft, rgt, level, cat['id']))

    traverse(None, 0)

    if dry_run:
        print(f'Dry run: {len(updates)} категорий будут обновлены')
        for lft, rgt, level, cat_id in updates[:20]:
            print(f'  id={cat_id} lft={lft} rgt={rgt} level={level}')
        if len(updates) > 20:
            print(f'  ... и ещё {len(updates) - 20}')
        return

    # Применяем обновления
    with conn:
        conn.executemany(
            'UPDATE categories SET lft=?, rgt=?, level=? WHERE id=?',
            updates
        )
        # Деактивированные — сбрасываем в нейтральные значения (не мешают дереву)
        conn.execute('UPDATE categories SET lft=0, rgt=1, level=0 WHERE is_active=0')

    print(f'Обновлено {len(updates)} активных категорий.')

    # Верификация
    bad = conn.execute('''
        SELECT COUNT(*) as cnt FROM categories c
        WHERE c.is_active=1
          AND c.parent_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM categories p WHERE p.id=c.parent_id AND p.lft < c.lft AND p.rgt > c.rgt
          )
    ''').fetchone()['cnt']

    if bad:
        print(f'ВНИМАНИЕ: {bad} категорий имеют некорректный вложенный путь!')
    else:
        print('Проверка пройдена: все вложенные пути корректны.')

    # Показываем исправленные категории
    print()
    print('=== Исправленные категории (ранее lft=0) ===')
    fixed = conn.execute('''
        SELECT c.id, c.name_ru, c.parent_id, c.level, c.lft, c.rgt, p.name_ru as parent_name
        FROM categories c
        LEFT JOIN categories p ON p.id = c.parent_id
        WHERE c.id IN (159,160,161,162,163,164,165,166)
        ORDER BY c.lft
    ''').fetchall()
    for r in fixed:
        print(f'  [{r["id"]:3d}] {r["name_ru"]} | parent={r["parent_name"]} | level={r["level"]} | lft={r["lft"]} rgt={r["rgt"]}')

    conn.close()


if __name__ == '__main__':
    dry = '--dry-run' in sys.argv
    path = DB_PATH
    for arg in sys.argv[1:]:
        if arg.startswith('--db='):
            path = arg[5:]
    rebuild(path, dry_run=dry)
