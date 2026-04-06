"""
Утилиты умного поиска:
  - Нормализация регистра и Unicode
  - Транслитерация (кириллица ↔ латиница)
  - Токенизация (поиск по нескольким словам)
  - Генерация SQL-условий для SQLite LIKE
"""
import re
import unicodedata

# Whitelist для SQL-выражений полей: разрешены идентификаторы, точки, скобки, пробелы
# Примеры допустимых: 'LOWER(n.name)', 'n.sku', 'COALESCE(a.name, b.name)'
_ALLOWED_FIELD_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_.()\s,]*$')

# ---------------------------------------------------------------------------
# Таблицы транслитерации
# ---------------------------------------------------------------------------

_CYR_TO_LAT: dict[str, str] = {
    'а': 'a',  'б': 'b',  'в': 'v',  'г': 'g',  'д': 'd',
    'е': 'e',  'ё': 'yo', 'ж': 'zh', 'з': 'z',  'и': 'i',
    'й': 'y',  'к': 'k',  'л': 'l',  'м': 'm',  'н': 'n',
    'о': 'o',  'п': 'p',  'р': 'r',  'с': 's',  'т': 't',
    'у': 'u',  'ф': 'f',  'х': 'kh', 'ц': 'ts', 'ч': 'ch',
    'ш': 'sh', 'щ': 'sch','ъ': '',   'ы': 'y',  'ь': '',
    'э': 'e',  'ю': 'yu', 'я': 'ya',
}

# Порядок важен: сначала длинные сочетания
_LAT_TO_CYR_MULTI: list[tuple[str, str]] = [
    ('sch', 'щ'), ('kh',  'х'), ('zh',  'ж'), ('ts',  'ц'),
    ('ch',  'ч'), ('sh',  'ш'), ('yo',  'ё'), ('yu',  'ю'),
    ('ya',  'я'),
]

_LAT_TO_CYR_SINGLE: dict[str, str] = {
    'a': 'а', 'b': 'б', 'v': 'в', 'g': 'г', 'd': 'д',
    'e': 'е', 'z': 'з', 'i': 'и', 'y': 'й', 'k': 'к',
    'l': 'л', 'm': 'м', 'n': 'н', 'o': 'о', 'p': 'п',
    'r': 'р', 's': 'с', 't': 'т', 'u': 'у', 'f': 'ф',
    'h': 'х',
}


# ---------------------------------------------------------------------------
# Низкоуровневые функции
# ---------------------------------------------------------------------------

def _normalize(text: str) -> str:
    """Приводит к нижнему регистру + Unicode NFC."""
    text = unicodedata.normalize('NFC', text)
    return text.lower().strip()


def _has_cyrillic(text: str) -> bool:
    return any('\u0400' <= ch <= '\u04FF' for ch in text)


def _has_latin(text: str) -> bool:
    return any('a' <= ch <= 'z' for ch in text.lower())


def _translit_to_latin(text: str) -> str:
    """Кириллица → латиница (ГОСТ-подобная схема)."""
    return ''.join(_CYR_TO_LAT.get(ch, ch) for ch in text.lower())


def _translit_to_cyrillic(text: str) -> str:
    """Латиница → кириллица (обратная транслитерация)."""
    result = text.lower()
    for lat, cyr in _LAT_TO_CYR_MULTI:
        result = result.replace(lat, cyr)
    return ''.join(_LAT_TO_CYR_SINGLE.get(ch, ch) for ch in result)


# ---------------------------------------------------------------------------
# Публичный API
# ---------------------------------------------------------------------------

def normalize(query: str) -> str:
    """Нормализует строку поиска: strip + lower + NFC."""
    return _normalize(query)


def generate_variants(token: str) -> list[str]:
    """
    Возвращает список уникальных вариантов токена для поиска:
      - оригинальный (нижний регистр)
      - транслитерированный (lat→cyr или cyr→lat)

    Пример: 'molotok' → ['molotok', 'молоток']
             'молоток' → ['молоток', 'molotok']
             'Молоток' → ['молоток', 'molotok']
    """
    t = _normalize(token)
    if not t:
        return []

    variants: list[str] = [t]

    if _has_latin(t) and not _has_cyrillic(t):
        cyr = _translit_to_cyrillic(t)
        if cyr and cyr != t:
            variants.append(cyr)
    elif _has_cyrillic(t):
        lat = _translit_to_latin(t)
        if lat and lat != t:
            variants.append(lat)

    return variants


def tokenize(query: str) -> list[str]:
    """
    Разбивает поисковый запрос на токены.
    Минимальная длина токена — 2 символа.

    Пример: 'красный Молоток' → ['красный', 'молоток']
    """
    q = _normalize(query)
    raw = re.split(r'[\s\-_,;/\\]+', q)
    return [t for t in raw if len(t) >= 2]


def build_where(fields: list[str], query: str, params: list) -> str:
    """
    Строит SQL-фрагмент условий для умного поиска.

    Стратегия:
      - Разбиваем запрос на токены
      - Для каждого токена генерируем варианты (orig + translit)
      - Каждый токен должен встретиться хотя бы в одном поле (AND между токенами, OR между полями/вариантами)

    Args:
        fields:  список SQL-выражений, напр. ['LOWER(n.name)', 'LOWER(n.sku)']
        query:   строка от пользователя
        params:  список, в который дописываются placeholder-значения

    Returns:
        SQL-фрагмент, начинающийся с ' AND', или '' если запрос пустой.

    Пример использования::

        where = "WHERE 1=1"
        p = []
        where += build_where(['LOWER(n.name)', 'LOWER(n.sku)'], search_query, p)
        rows = db.execute(f"SELECT * FROM nomenclatures n {where}", p)
    """
    for field in fields:
        if not _ALLOWED_FIELD_RE.match(field):
            raise ValueError(f"Недопустимое SQL-выражение поля: {field!r}")

    if not query or not query.strip():
        return ''

    tokens = tokenize(query)
    if not tokens:
        return ''

    token_parts: list[str] = []

    for token in tokens:
        variants = generate_variants(token)
        field_conds: list[str] = []

        for variant in variants:
            pattern = f'%{variant}%'
            for field in fields:
                field_conds.append(f'{field} LIKE ?')
                params.append(pattern)

        if field_conds:
            token_parts.append(f"({' OR '.join(field_conds)})")

    if not token_parts:
        return ''

    return ' AND ' + ' AND '.join(token_parts)
