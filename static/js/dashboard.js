/**
 * dashboard.js — Система настраиваемого дашборда v2
 * inventory_bot_V11
 *
 * Возможности:
 *  - Показать / скрыть виджеты (toggle)
 *  - Свернуть / развернуть виджеты (collapse)
 *  - Перетаскивание виджетов (HTML5 drag-and-drop)
 *  - Режим кастомизации (customize mode — показывает handles)
 *  - Конфигурация сохраняется в localStorage + sync с сервером
 *  - Панель настройки (боковое выдвижное меню)
 *  - Интеграция с Chart.js (перекраска при смене темы)
 */

(function () {
    'use strict';

    /* ── Описание виджетов ─────────────────────────────────────── */
    const WIDGETS = [
        { id: 'kpi_primary',      label: 'KPI: основные показатели',   icon: 'fa-chart-bar',           defaultVisible: true,  defaultOrder: 0  },
        { id: 'kpi_secondary',    label: 'KPI: склады / люди',          icon: 'fa-users',               defaultVisible: true,  defaultOrder: 1  },
        { id: 'quick_actions',    label: 'Быстрые действия',            icon: 'fa-bolt',                defaultVisible: true,  defaultOrder: 2  },
        { id: 'chart_movement',   label: 'График движения (30 дней)',   icon: 'fa-chart-line',          defaultVisible: true,  defaultOrder: 3  },
        { id: 'chart_doctypes',   label: 'Типы документов',             icon: 'fa-chart-pie',           defaultVisible: true,  defaultOrder: 4  },
        { id: 'chart_warehouse',  label: 'Остатки по складам',          icon: 'fa-warehouse',           defaultVisible: true,  defaultOrder: 5  },
        { id: 'top5_lowstock',    label: 'Топ-5 / Критические остатки', icon: 'fa-exclamation-triangle', defaultVisible: true, defaultOrder: 6  },
        { id: 'expiring_batches', label: 'Истекающие партии',           icon: 'fa-calendar-times',      defaultVisible: true,  defaultOrder: 7  },
        { id: 'drafts_activity',  label: 'Черновики + Активность',      icon: 'fa-history',             defaultVisible: true,  defaultOrder: 8  },
    ];

    const LS_KEY = 'dashboard_config_v2';

    /* ── Config helpers ─────────────────────────────────────────── */
    function defaultConfig() {
        const cfg = {};
        WIDGETS.forEach(w => {
            cfg[w.id] = { visible: w.defaultVisible, order: w.defaultOrder, collapsed: false };
        });
        return cfg;
    }

    function loadConfig() {
        try {
            const raw = localStorage.getItem(LS_KEY);
            if (!raw) return defaultConfig();
            const saved = JSON.parse(raw);
            const def = defaultConfig();
            Object.keys(def).forEach(id => {
                if (!saved[id]) {
                    saved[id] = def[id];
                } else {
                    // Добавляем новые поля к существующим виджетам
                    if (saved[id].collapsed === undefined) saved[id].collapsed = false;
                }
            });
            return saved;
        } catch (e) {
            return defaultConfig();
        }
    }

    function saveConfig(cfg) {
        localStorage.setItem(LS_KEY, JSON.stringify(cfg));
        // Async sync to server
        fetch('/api/dashboard/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(cfg)
        }).catch(() => {});
    }

    /* ── Apply config to DOM ───────────────────────────────────── */
    function applyConfig(cfg) {
        const container = document.getElementById('dashboardWidgetContainer');
        if (!container) return;

        // Сортируем виджеты по order
        const ordered = Object.keys(cfg)
            .filter(id => document.querySelector(`[data-widget-id="${id}"]`))
            .sort((a, b) => (cfg[a].order || 0) - (cfg[b].order || 0));

        ordered.forEach(id => {
            const el = document.querySelector(`[data-widget-id="${id}"]`);
            if (!el) return;
            const vis = cfg[id].visible !== false;
            el.dataset.visible = vis ? 'true' : 'false';
            el.style.display = vis ? '' : 'none';

            // Состояние свёрнутости
            if (cfg[id].collapsed) {
                el.classList.add('widget-collapsed');
            } else {
                el.classList.remove('widget-collapsed');
            }

            container.appendChild(el); // Reorder
        });
    }

    /* ── Добавляем кнопку collapse к каждому виджету ──────────── */
    function initCollapseButtons(cfg) {
        const container = document.getElementById('dashboardWidgetContainer');
        if (!container) return;

        container.querySelectorAll('[data-widget-id]').forEach(widget => {
            const wid = widget.dataset.widgetId;
            const handle = widget.querySelector('.dash-widget-handle');
            if (!handle) return;

            // Убираем старую кнопку если есть
            const existing = handle.querySelector('.widget-collapse-btn');
            if (existing) existing.remove();

            const btn = document.createElement('button');
            btn.className = 'widget-collapse-btn';
            btn.title = 'Свернуть / развернуть';
            btn.type = 'button';
            btn.innerHTML = '<i class="fas fa-chevron-up"></i>';
            handle.appendChild(btn);

            btn.addEventListener('click', function (e) {
                e.stopPropagation();
                e.preventDefault();
                const isCollapsed = widget.classList.toggle('widget-collapsed');
                if (cfg[wid]) cfg[wid].collapsed = isCollapsed;
                saveConfig(cfg);
                // Обновляем список в панели
                syncPanelCollapseState(cfg);
            });
        });
    }

    /* ── Синхронизируем значки collapse в панели ──────────────── */
    function syncPanelCollapseState(cfg) {
        document.querySelectorAll('.widget-toggle-item').forEach(item => {
            const wid = item.dataset.wid;
            if (!cfg[wid]) return;
            const collapseBtn = item.querySelector('.wi-collapse');
            if (collapseBtn) {
                const isCollapsed = !!cfg[wid].collapsed;
                collapseBtn.title = isCollapsed ? 'Развернуть' : 'Свернуть';
                collapseBtn.querySelector('i').className = isCollapsed
                    ? 'fas fa-expand-alt'
                    : 'fas fa-compress-alt';
            }
        });
    }

    /* ── Panel: render widget list ─────────────────────────────── */
    function renderPanelWidgetList(cfg) {
        const list = document.getElementById('widgetToggleList');
        if (!list) return;

        const ordered = [...WIDGETS].sort((a, b) => {
            const oa = cfg[a.id] ? cfg[a.id].order : a.defaultOrder;
            const ob = cfg[b.id] ? cfg[b.id].order : b.defaultOrder;
            return oa - ob;
        });

        list.innerHTML = ordered.map(w => {
            const vis       = cfg[w.id] ? cfg[w.id].visible   : true;
            const collapsed = cfg[w.id] ? cfg[w.id].collapsed  : false;
            return `
            <div class="widget-toggle-item" data-wid="${w.id}" draggable="true">
                <div class="wi-icon"><i class="fas ${w.icon}"></i></div>
                <div class="wi-label">${w.label}</div>
                <button type="button" class="btn btn-link p-0 me-1 wi-collapse text-muted"
                        title="${collapsed ? 'Развернуть' : 'Свернуть'}" style="font-size:0.78rem;">
                    <i class="fas ${collapsed ? 'fa-expand-alt' : 'fa-compress-alt'}"></i>
                </button>
                <div class="form-check form-switch mb-0 me-1">
                    <input class="form-check-input widget-vis-toggle" type="checkbox"
                           data-wid="${w.id}" ${vis ? 'checked' : ''}
                           style="cursor:pointer;" title="Показать / скрыть">
                </div>
                <div class="wi-drag"><i class="fas fa-grip-vertical"></i></div>
            </div>`;
        }).join('');

        // Collapse toggle из панели
        list.querySelectorAll('.wi-collapse').forEach(btn => {
            btn.addEventListener('click', function (e) {
                e.stopPropagation();
                const wid = this.closest('.widget-toggle-item').dataset.wid;
                if (!cfg[wid]) return;
                const isCollapsed = !cfg[wid].collapsed;
                cfg[wid].collapsed = isCollapsed;
                // Применяем к DOM-виджету
                const widget = document.querySelector(`[data-widget-id="${wid}"]`);
                if (widget) widget.classList.toggle('widget-collapsed', isCollapsed);
                saveConfig(cfg);
                // Обновляем иконку
                this.title = isCollapsed ? 'Развернуть' : 'Свернуть';
                this.querySelector('i').className = isCollapsed ? 'fas fa-expand-alt' : 'fas fa-compress-alt';
            });
        });

        initPanelDragDrop(cfg);
    }

    /* ── Panel drag-and-drop (reorder list) ────────────────────── */
    function initPanelDragDrop(cfg) {
        const list = document.getElementById('widgetToggleList');
        if (!list) return;

        let dragSrc = null;

        list.querySelectorAll('.widget-toggle-item').forEach(item => {
            item.addEventListener('dragstart', function (e) {
                dragSrc = this;
                e.dataTransfer.effectAllowed = 'move';
                setTimeout(() => this.style.opacity = '0.4', 0);
            });
            item.addEventListener('dragend', function () {
                this.style.opacity = '';
                list.querySelectorAll('.widget-toggle-item').forEach(i => i.classList.remove('drag-over-panel'));
            });
            item.addEventListener('dragover', function (e) {
                e.preventDefault();
                e.dataTransfer.dropEffect = 'move';
                if (this !== dragSrc) this.classList.add('drag-over-panel');
                return false;
            });
            item.addEventListener('dragleave', function () {
                this.classList.remove('drag-over-panel');
            });
            item.addEventListener('drop', function (e) {
                e.preventDefault();
                this.classList.remove('drag-over-panel');
                if (dragSrc === this) return;

                const items = [...list.querySelectorAll('.widget-toggle-item')];
                const srcIdx = items.indexOf(dragSrc);
                const dstIdx = items.indexOf(this);
                if (srcIdx < dstIdx) {
                    list.insertBefore(dragSrc, this.nextSibling);
                } else {
                    list.insertBefore(dragSrc, this);
                }

                const newOrder = [...list.querySelectorAll('.widget-toggle-item')];
                newOrder.forEach((el, idx) => {
                    const wid = el.dataset.wid;
                    if (cfg[wid]) cfg[wid].order = idx;
                });

                saveConfig(cfg);
                applyConfig(cfg);
            });
        });
    }

    /* ── Dashboard DOM drag-and-drop ───────────────────────────── */
    function initDashboardDragDrop(cfg) {
        const container = document.getElementById('dashboardWidgetContainer');
        if (!container) return;

        let dragEl = null;

        container.querySelectorAll('[data-widget-id]').forEach(widget => {
            const handle = widget.querySelector('.dash-widget-handle');
            if (!handle) return;

            handle.addEventListener('mousedown', () => { widget.draggable = true; });
            widget.addEventListener('dragend', () => {
                widget.draggable = false;
                widget.classList.remove('dragging');
                container.querySelectorAll('[data-widget-id]').forEach(w => w.classList.remove('drag-over'));
                dragEl = null;
                container.querySelectorAll('[data-widget-id]').forEach((el, idx) => {
                    const wid = el.dataset.widgetId;
                    if (cfg[wid]) cfg[wid].order = idx;
                });
                saveConfig(cfg);
                renderPanelWidgetList(cfg);
            });

            widget.addEventListener('dragstart', function (e) {
                dragEl = this;
                e.dataTransfer.effectAllowed = 'move';
                setTimeout(() => this.classList.add('dragging'), 0);
            });
            widget.addEventListener('dragover', function (e) {
                e.preventDefault();
                if (this !== dragEl && dragEl) this.classList.add('drag-over');
            });
            widget.addEventListener('dragleave', function () {
                this.classList.remove('drag-over');
            });
            widget.addEventListener('drop', function (e) {
                e.preventDefault();
                this.classList.remove('drag-over');
                if (!dragEl || dragEl === this) return;

                const widgets = [...container.querySelectorAll('[data-widget-id]')];
                const srcIdx = widgets.indexOf(dragEl);
                const dstIdx = widgets.indexOf(this);
                if (srcIdx < dstIdx) {
                    container.insertBefore(dragEl, this.nextSibling);
                } else {
                    container.insertBefore(dragEl, this);
                }
            });
        });
    }

    /* ── Customize mode toggle ─────────────────────────────────── */
    function initCustomizeModeToggle(cfg) {
        const panelBody = document.querySelector('.dash-config-panel-body');
        if (!panelBody) return;

        // Вставляем кнопку перед подсказкой
        const hint = panelBody.querySelector('.text-muted');
        const toggle = document.createElement('button');
        toggle.type = 'button';
        toggle.id = 'dashCustomizeToggle';
        toggle.className = 'dash-customize-toggle';
        const isActive = document.body.classList.contains('dashboard-customize-mode');
        toggle.innerHTML = `
            <i class="fas fa-${isActive ? 'lock-open' : 'magic'} me-1"></i>
            ${isActive ? 'Режим редактирования: ВКЛ' : 'Включить режим редактирования'}
            <span class="dct-status">${isActive ? '✦ активен' : ''}</span>
        `;
        toggle.classList.toggle('active', isActive);
        panelBody.insertBefore(toggle, hint);

        // Разделитель
        const divLabel = document.createElement('div');
        divLabel.className = 'dcp-section-label mt-2';
        divLabel.textContent = 'ВИДЖЕТЫ';
        panelBody.insertBefore(divLabel, hint);

        toggle.addEventListener('click', function () {
            const active = document.body.classList.toggle('dashboard-customize-mode');
            localStorage.setItem('dashboard_customize_mode', active ? '1' : '0');
            this.classList.toggle('active', active);
            this.innerHTML = `
                <i class="fas fa-${active ? 'lock-open' : 'magic'} me-1"></i>
                ${active ? 'Режим редактирования: ВКЛ' : 'Включить режим редактирования'}
                <span class="dct-status">${active ? '✦ активен' : ''}</span>
            `;
        });

        // Восстанавливаем состояние
        if (localStorage.getItem('dashboard_customize_mode') === '1') {
            document.body.classList.add('dashboard-customize-mode');
            toggle.classList.add('active');
            toggle.innerHTML = `
                <i class="fas fa-lock-open me-1"></i>
                Режим редактирования: ВКЛ
                <span class="dct-status">✦ активен</span>
            `;
        }
    }

    /* ── Open / Close panel ────────────────────────────────────── */
    function openPanel() {
        document.getElementById('dashConfigPanel').classList.add('open');
        document.getElementById('dashConfigBackdrop').classList.add('show');
        document.body.style.overflow = 'hidden';
    }
    function closePanel() {
        document.getElementById('dashConfigPanel').classList.remove('open');
        document.getElementById('dashConfigBackdrop').classList.remove('show');
        document.body.style.overflow = '';
    }

    /* ── Init ──────────────────────────────────────────────────── */
    function init() {
        const container = document.getElementById('dashboardWidgetContainer');
        if (!container) return; // Только на дашборде

        let cfg = loadConfig();
        applyConfig(cfg);
        initCollapseButtons(cfg);
        renderPanelWidgetList(cfg);
        initDashboardDragDrop(cfg);
        initCustomizeModeToggle(cfg);

        // Открыть/закрыть панель
        const btn = document.getElementById('dashConfigBtn');
        if (btn) {
            btn.addEventListener('click', function () {
                this.classList.add('spinning');
                setTimeout(() => this.classList.remove('spinning'), 400);
                openPanel();
            });
        }

        const closeBtn = document.getElementById('dashConfigClose');
        if (closeBtn) closeBtn.addEventListener('click', closePanel);

        const backdrop = document.getElementById('dashConfigBackdrop');
        if (backdrop) backdrop.addEventListener('click', closePanel);

        // Сброс конфигурации
        const resetBtn = document.getElementById('dashConfigReset');
        if (resetBtn) {
            resetBtn.addEventListener('click', function () {
                cfg = defaultConfig();
                saveConfig(cfg);
                applyConfig(cfg);
                initCollapseButtons(cfg);
                renderPanelWidgetList(cfg);
                // Сбрасываем customize mode
                document.body.classList.remove('dashboard-customize-mode');
                localStorage.removeItem('dashboard_customize_mode');
                if (typeof toastr !== 'undefined') {
                    toastr.success('Дашборд сброшен к настройкам по умолчанию');
                }
            });
        }

        // Переключение видимости виджета
        document.addEventListener('change', function (e) {
            if (!e.target.classList.contains('widget-vis-toggle')) return;
            const wid = e.target.dataset.wid;
            if (!cfg[wid]) return;
            cfg[wid].visible = e.target.checked;
            saveConfig(cfg);
            applyConfig(cfg);
        });

        // Keyboard shortcut: Ctrl+Shift+E — toggle customize mode
        document.addEventListener('keydown', function (e) {
            if (e.ctrlKey && e.shiftKey && e.key === 'E') {
                e.preventDefault();
                document.body.classList.toggle('dashboard-customize-mode');
                const active = document.body.classList.contains('dashboard-customize-mode');
                localStorage.setItem('dashboard_customize_mode', active ? '1' : '0');
                const toggle = document.getElementById('dashCustomizeToggle');
                if (toggle) {
                    toggle.classList.toggle('active', active);
                    toggle.innerHTML = `
                        <i class="fas fa-${active ? 'lock-open' : 'magic'} me-1"></i>
                        ${active ? 'Режим редактирования: ВКЛ' : 'Включить режим редактирования'}
                        <span class="dct-status">${active ? '✦ активен' : ''}</span>
                    `;
                }
            }
        });
    }

    // Запускаем после загрузки DOM
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    // Публичный API
    window.DashboardConfig = {
        open:  openPanel,
        close: closePanel,
        // Позволяет внешнему коду регистрировать Chart.js инстанции
        registerChart: function (id, chart) {
            if (!window._dashCharts) window._dashCharts = {};
            window._dashCharts[id] = chart;
        },
    };

})();
