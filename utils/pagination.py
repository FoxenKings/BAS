"""
Утилита пагинации для Flask-маршрутов.

Использование:
    from utils.pagination import Paginator

    @app.route('/nomenclatures')
    def list_nomenclatures():
        page = request.args.get('page', 1, type=int)
        total = db.count_nomenclatures()
        pager = Paginator(total=total, page=page, per_page=20)
        items = db.get_nomenclatures(limit=pager.per_page, offset=pager.offset)
        return render_template('list.html', items=items, pager=pager)

Шаблон Jinja2:
    {% if pager.pages > 1 %}
      <nav>
        <ul class="pagination">
          {% for p in pager.iter_pages() %}
            {% if p %}
              <li class="page-item {% if p == pager.page %}active{% endif %}">
                <a class="page-link" href="?page={{ p }}">{{ p }}</a>
              </li>
            {% else %}
              <li class="page-item disabled"><span class="page-link">…</span></li>
            {% endif %}
          {% endfor %}
        </ul>
      </nav>
    {% endif %}
"""
from __future__ import annotations

DEFAULT_PER_PAGE = 20
MAX_PER_PAGE = 200


class Paginator:
    """Простой объект пагинации без зависимостей от ORM."""

    def __init__(self, total: int, page: int = 1, per_page: int = DEFAULT_PER_PAGE):
        self.total = max(0, total)
        self.per_page = min(max(1, per_page), MAX_PER_PAGE)
        self.pages = max(1, -(-self.total // self.per_page))  # ceil division
        self.page = max(1, min(page, self.pages))

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.per_page

    @property
    def has_prev(self) -> bool:
        return self.page > 1

    @property
    def has_next(self) -> bool:
        return self.page < self.pages

    @property
    def prev_page(self) -> int | None:
        return self.page - 1 if self.has_prev else None

    @property
    def next_page(self) -> int | None:
        return self.page + 1 if self.has_next else None

    def iter_pages(self, left_edge: int = 2, left_current: int = 2,
                   right_current: int = 3, right_edge: int = 2):
        """Итератор страниц для отображения виджета пагинации.

        Возвращает номера страниц (int) или None (разрыв «…»).
        """
        last = 0
        for num in range(1, self.pages + 1):
            in_left = num <= left_edge
            in_right = num > self.pages - right_edge
            near_current = (self.page - left_current <= num <= self.page + right_current)
            if in_left or in_right or near_current:
                if last and num - last > 1:
                    yield None
                yield num
                last = num
